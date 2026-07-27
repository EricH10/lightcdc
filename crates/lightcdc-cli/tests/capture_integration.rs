use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use lightcdc_core::{ChangeEvent, Operation, SourceConfig};
use lightcdc_postgres::ReplicationReader;
use lightcdc_storage::{LogOpenOptions, PersistTransactionOutcome, RedbEventStore};
use tempfile::TempDir;
use tokio::time::timeout;
use tokio_postgres::{Client, NoTls};

static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker compose postgres on localhost:5432"]
async fn captures_insert_update_delete_into_redb() -> anyhow::Result<()> {
    let fixture = PgFixture::create().await?;
    let temp = TempDir::new()?;
    let store = open_store(temp.path().to_path_buf())?;
    let mut reader = ReplicationReader::connect(fixture.source.clone()).await?;
    reader.set_next_sequence(store.next_sequence()?);

    fixture
        .client
        .batch_execute(&format!(
            r#"
            BEGIN;

            INSERT INTO public.{table} (customer_email, total_cents)
            VALUES ('ada@example.com', 4200);

            UPDATE public.{table}
            SET status = 'paid'
            WHERE customer_email = 'ada@example.com';

            DELETE FROM public.{table}
            WHERE customer_email = 'ada@example.com';

            COMMIT;
            "#,
            table = fixture.table
        ))
        .await?;

    let events = capture_new_events(&mut reader, &store, &fixture.source_name, 3).await?;
    reader.shutdown().await?;

    assert_eq!(
        operations(&events),
        [Operation::Insert, Operation::Update, Operation::Delete]
    );
    assert_eq!(events[0].sequence, 1);
    assert_eq!(events[1].sequence, 2);
    assert_eq!(events[2].sequence, 3);
    assert_eq!(events[0].schema, "public");
    assert_eq!(events[0].table, fixture.table);
    let transaction_id = events[0]
        .transaction
        .as_ref()
        .and_then(|transaction| transaction.transaction_id);
    assert!(transaction_id.is_some());
    assert!(events.iter().all(|event| {
        event
            .transaction
            .as_ref()
            .is_some_and(|transaction| transaction.transaction_id == transaction_id)
    }));
    assert_json_field(
        events[0].after.as_deref(),
        "customer_email",
        "ada@example.com",
    )?;
    assert_json_field(events[1].before.as_deref(), "status", "created")?;
    assert_json_field(events[1].after.as_deref(), "status", "paid")?;
    assert_json_field(events[2].before.as_deref(), "status", "paid")?;
    assert!(store.source_offset(&fixture.source_name)?.is_some());

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker compose postgres on localhost:5432"]
async fn resumes_from_redb_after_persisting_without_acknowledging_postgres() -> anyhow::Result<()> {
    let fixture = PgFixture::create().await?;
    let temp = TempDir::new()?;
    let store = open_store(temp.path().to_path_buf())?;

    let mut first_reader = ReplicationReader::connect(fixture.source.clone()).await?;
    first_reader.set_next_sequence(store.next_sequence()?);

    fixture
        .client
        .execute(
            &format!(
                "INSERT INTO public.{} (customer_email, total_cents) VALUES ($1, $2)",
                fixture.table
            ),
            &[&"grace@example.com", &9900_i64],
        )
        .await?;

    let first_transaction = timeout(Duration::from_secs(15), first_reader.next_transaction())
        .await??
        .ok_or_else(|| anyhow::anyhow!("replication stream ended before transaction commit"))?;
    let first_ack_lsn = first_transaction.ack_lsn;
    assert_eq!(
        store.persist_transaction(
            &first_transaction.events,
            &fixture.source_name,
            &first_ack_lsn.to_string(),
        )?,
        PersistTransactionOutcome::Persisted
    );
    let first_events = first_transaction.events;

    // Deliberately do not call first_reader.ack(first_ack_lsn).
    first_reader.shutdown().await?;
    assert_eq!(first_events[0].sequence, 1);

    let source_offset = store.source_offset(&fixture.source_name)?;
    let mut resumed_reader =
        ReplicationReader::connect_from(fixture.source.clone(), source_offset.as_deref()).await?;
    resumed_reader.set_next_sequence(store.next_sequence()?);

    fixture
        .client
        .execute(
            &format!(
                "INSERT INTO public.{} (customer_email, total_cents) VALUES ($1, $2)",
                fixture.table
            ),
            &[&"katherine@example.com", &12345_i64],
        )
        .await?;

    let resumed_events =
        capture_new_events(&mut resumed_reader, &store, &fixture.source_name, 1).await?;
    resumed_reader.shutdown().await?;

    assert_eq!(resumed_events.len(), 1);
    assert_eq!(resumed_events[0].sequence, 2);
    assert_json_field(
        resumed_events[0].after.as_deref(),
        "customer_email",
        "katherine@example.com",
    )?;
    assert_eq!(store.replay_from(1, 10)?.len(), 2);

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker compose postgres on localhost:5432"]
async fn does_not_emit_or_persist_an_uncommitted_source_transaction() -> anyhow::Result<()> {
    let fixture = PgFixture::create().await?;
    let temp = TempDir::new()?;
    let store = open_store(temp.path().to_path_buf())?;
    let mut reader = ReplicationReader::connect(fixture.source.clone()).await?;
    reader.set_next_sequence(store.next_sequence()?);

    fixture.client.batch_execute("BEGIN").await?;
    fixture
        .client
        .execute(
            &format!(
                "INSERT INTO public.{} (customer_email, total_cents) VALUES ($1, $2)",
                fixture.table
            ),
            &[&"uncommitted@example.com", &7777_i64],
        )
        .await?;

    assert!(
        timeout(Duration::from_millis(300), reader.next_transaction())
            .await
            .is_err(),
        "reader emitted a transaction before PostgreSQL committed it"
    );
    assert!(store.replay_from(1, 10)?.is_empty());
    assert_eq!(store.source_offset(&fixture.source_name)?, None);

    fixture.client.batch_execute("COMMIT").await?;
    let transaction = timeout(Duration::from_secs(15), reader.next_transaction())
        .await??
        .ok_or_else(|| anyhow::anyhow!("replication stream ended before transaction commit"))?;
    let ack_lsn = transaction.ack_lsn;
    assert_eq!(transaction.events.len(), 1);
    assert_eq!(
        store.persist_transaction(
            &transaction.events,
            &fixture.source_name,
            &ack_lsn.to_string(),
        )?,
        PersistTransactionOutcome::Persisted
    );
    reader.ack(ack_lsn);
    reader.shutdown().await?;

    assert_eq!(store.replay_from(1, 10)?.len(), 1);
    assert_eq!(
        store.source_offset(&fixture.source_name)?,
        Some(ack_lsn.to_string())
    );

    fixture.cleanup().await?;
    Ok(())
}

async fn capture_new_events(
    reader: &mut ReplicationReader,
    store: &RedbEventStore,
    source_name: &str,
    count: usize,
) -> anyhow::Result<Vec<ChangeEvent>> {
    timeout(Duration::from_secs(15), async {
        let mut events = Vec::with_capacity(count);

        while events.len() < count {
            let Some(transaction) = reader.next_transaction().await? else {
                anyhow::bail!("replication stream ended before capture completed");
            };
            let ack_lsn = transaction.ack_lsn;

            match store.persist_transaction(
                &transaction.events,
                source_name,
                &ack_lsn.to_string(),
            )? {
                PersistTransactionOutcome::Persisted => {
                    events.extend(transaction.events);
                }
                PersistTransactionOutcome::AlreadyPersisted => {
                    reader.set_next_sequence(store.next_sequence()?);
                }
            }

            reader.ack(ack_lsn);
        }

        Ok(events)
    })
    .await?
}

fn open_store(path: PathBuf) -> anyhow::Result<RedbEventStore> {
    Ok(RedbEventStore::open(&LogOpenOptions {
        data_dir: path,
        database_file: "capture-test.redb".to_owned(),
    })?)
}

fn operations(events: &[ChangeEvent]) -> Vec<Operation> {
    events.iter().map(|event| event.operation).collect()
}

fn assert_json_field(bytes: Option<&[u8]>, field: &str, expected: &str) -> anyhow::Result<()> {
    let bytes = bytes.ok_or_else(|| anyhow::anyhow!("missing row payload"))?;
    let value: serde_json::Value = serde_json::from_slice(bytes)?;
    assert_eq!(value[field], expected);
    Ok(())
}

struct PgFixture {
    client: Client,
    source: SourceConfig,
    source_name: String,
    table: String,
    publication: String,
    slot: String,
}

impl PgFixture {
    async fn create() -> anyhow::Result<Self> {
        let suffix = unique_suffix();
        let table = format!("orders_{suffix}");
        let publication = format!("pub_{suffix}");
        let slot = format!("slot_{suffix}");
        let source_name = format!("lightcdc:{slot}");

        let source = SourceConfig {
            name: "default".to_owned(),
            host: "localhost".to_owned(),
            port: 5432,
            database: "lightcdc".to_owned(),
            user: "lightcdc".to_owned(),
            password: "lightcdc".to_owned(),
            publication: publication.clone(),
            slot: slot.clone(),
        };

        let (client, connection) =
            tokio_postgres::connect(&source.connection_string(), NoTls).await?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                eprintln!("postgres integration test connection task failed: {error}");
            }
        });

        client
            .batch_execute(&format!(
                r#"
                CREATE TABLE public.{table} (
                    id BIGSERIAL PRIMARY KEY,
                    customer_email TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'created',
                    total_cents BIGINT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );

                ALTER TABLE public.{table} REPLICA IDENTITY FULL;
                CREATE PUBLICATION {publication} FOR TABLE public.{table};
                "#,
            ))
            .await?;

        client
            .query_one(
                "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
                &[&slot],
            )
            .await?;

        Ok(Self {
            client,
            source,
            source_name,
            table,
            publication,
            slot,
        })
    }

    async fn cleanup(&self) -> anyhow::Result<()> {
        let cleanup_sql = format!(
            r#"
            DROP PUBLICATION IF EXISTS {publication};
            DROP TABLE IF EXISTS public.{table};
            "#,
            publication = self.publication,
            table = self.table
        );

        let _ = self.client.batch_execute(&cleanup_sql).await;
        let _ = self
            .client
            .execute("SELECT pg_drop_replication_slot($1)", &[&self.slot])
            .await;

        Ok(())
    }
}

fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();
    let counter = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{nanos}_{counter}")
}
