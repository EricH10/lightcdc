use std::{
    env,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use lightcdc_core::{ChangeEvent, Config, Operation, PostgresSourceConfig};
use lightcdc_postgres::ReplicationReader;
use lightcdc_storage::{LogOpenOptions, PersistTransactionOutcome, RedbEventStore};
use tempfile::TempDir;
use tokio::time::{sleep, timeout};
use tokio_postgres::{Client, NoTls};

static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);
const CRASH_WORKER_ENV: &str = "LIGHTCDC_CRASH_CAPTURE_WORKER";
const CRASH_CONFIG_ENV: &str = "LIGHTCDC_CRASH_CONFIG";
const CRASH_STAGE_ENV: &str = "LIGHTCDC_CRASH_STAGE";
const CRASH_MARKER_ENV: &str = "LIGHTCDC_CRASH_MARKER";

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker compose postgres on localhost:5432"]
async fn process_kill_before_redb_commit_replays_the_transaction() -> anyhow::Result<()> {
    let fixture = PgFixture::create().await?;
    let temp = TempDir::new()?;
    let config_path = temp.path().join("lightcdc-test.toml");
    let marker = temp.path().join("before-persist.marker");
    write_test_config(&config_path, temp.path(), &fixture)?;

    fixture.insert("before-kill@example.com", 5100).await?;
    let mut worker = spawn_crash_worker(&config_path, &marker, "before_persist")?;
    wait_for_path(&marker).await?;
    worker.kill_and_wait()?;

    let store = open_store(temp.path().to_path_buf())?;
    assert!(store.replay_from(1, 10)?.is_empty());
    assert_eq!(store.source_offset(&fixture.source_name)?, None);

    let mut reader = connect_reader(&fixture.source, None).await?;
    reader.set_next_sequence(store.next_sequence()?);
    let events = capture_new_events(&mut reader, &store, &fixture.source_name, 1).await?;
    reader.shutdown().await?;

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].sequence, 1);
    assert_json_field(
        events[0].after.as_deref(),
        "customer_email",
        "before-kill@example.com",
    )?;
    assert!(store.source_offset(&fixture.source_name)?.is_some());

    drop(store);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker compose postgres on localhost:5432"]
async fn process_kill_after_redb_commit_resumes_without_duplicate_events() -> anyhow::Result<()> {
    let fixture = PgFixture::create().await?;
    let temp = TempDir::new()?;
    let config_path = temp.path().join("lightcdc-test.toml");
    let marker = temp.path().join("after-persist.marker");
    write_test_config(&config_path, temp.path(), &fixture)?;

    fixture
        .insert("persisted-before-kill@example.com", 6100)
        .await?;
    let mut worker = spawn_crash_worker(&config_path, &marker, "after_persist")?;
    wait_for_path(&marker).await?;
    worker.kill_and_wait()?;

    let store = open_store(temp.path().to_path_buf())?;
    let stored_offset = store.source_offset(&fixture.source_name)?;
    assert_eq!(store.replay_from(1, 10)?.len(), 1);
    assert!(stored_offset.is_some());

    let mut reader = connect_reader(&fixture.source, stored_offset.as_deref()).await?;
    reader.set_next_sequence(store.next_sequence()?);
    fixture.insert("after-restart@example.com", 6200).await?;
    let events = capture_new_events(&mut reader, &store, &fixture.source_name, 1).await?;
    reader.shutdown().await?;

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].sequence, 2);
    assert_json_field(
        events[0].after.as_deref(),
        "customer_email",
        "after-restart@example.com",
    )?;
    assert_eq!(store.replay_from(1, 10)?.len(), 2);

    drop(store);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker compose postgres on localhost:5432"]
async fn capture_reconnects_after_postgres_terminates_replication_backend() -> anyhow::Result<()> {
    let fixture = PgFixture::create().await?;
    let temp = TempDir::new()?;
    let config_path = temp.path().join("lightcdc-test.toml");
    write_test_config(&config_path, temp.path(), &fixture)?;

    let initial_lsn = fixture.confirmed_flush_lsn().await?;
    let mut capture = ChildGuard::new(
        Command::new(env!("CARGO_BIN_EXE_lightcdc"))
            .args([
                "capture",
                "--config",
                config_path
                    .to_str()
                    .ok_or_else(|| anyhow::anyhow!("test config path is not UTF-8"))?,
                "--max-events",
                "2",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()?,
    );

    let first_pid = fixture.wait_for_replication_pid(None).await?;
    fixture.insert("before-reconnect@example.com", 7100).await?;
    fixture
        .wait_for_confirmed_flush_lsn_change(initial_lsn.as_deref())
        .await?;
    fixture.terminate_backend(first_pid).await?;

    let second_pid = fixture.wait_for_replication_pid(Some(first_pid)).await?;
    assert_ne!(second_pid, first_pid);
    fixture.insert("after-reconnect@example.com", 7200).await?;
    let status = capture.wait_for_exit().await?;
    assert!(status.success(), "capture process failed with {status}");

    let store = open_store(temp.path().to_path_buf())?;
    let events = store.replay_from(1, 10)?;
    assert_eq!(events.len(), 2);
    assert_json_field(
        events[0].after.as_deref(),
        "customer_email",
        "before-reconnect@example.com",
    )?;
    assert_json_field(
        events[1].after.as_deref(),
        "customer_email",
        "after-reconnect@example.com",
    )?;

    drop(store);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "internal child process used by process-kill tests"]
async fn crash_capture_worker() -> anyhow::Result<()> {
    if env::var_os(CRASH_WORKER_ENV).is_none() {
        return Ok(());
    }

    let config_path = required_env(CRASH_CONFIG_ENV)?;
    let stage = required_env(CRASH_STAGE_ENV)?;
    let marker = PathBuf::from(required_env(CRASH_MARKER_ENV)?);
    let config = Config::from_path(config_path)?;
    let store = open_store(PathBuf::from(&config.runtime.data_dir))?;
    let source = match config.source {
        lightcdc_core::SourceConfig::Postgres(source) => source,
        lightcdc_core::SourceConfig::Mysql(_) => anyhow::bail!("expected postgres source"),
    };
    let source_offset = store.source_offset(&source.name)?;
    let mut reader =
        ReplicationReader::connect_from(source.clone(), source_offset.as_deref()).await?;
    reader.set_next_sequence(store.next_sequence()?);

    let transaction = timeout(Duration::from_secs(15), reader.next_transaction())
        .await??
        .ok_or_else(|| anyhow::anyhow!("replication stream ended before crash boundary"))?;

    if stage == "before_persist" {
        signal_ready_and_wait_for_kill(&marker)?;
    }

    let ack_lsn = transaction.ack_lsn;
    store.persist_transaction(&transaction.events, &source.name, &ack_lsn.to_string())?;

    if stage == "after_persist" {
        signal_ready_and_wait_for_kill(&marker)?;
    }

    anyhow::bail!("unknown crash stage {stage:?}")
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

async fn connect_reader(
    source: &PostgresSourceConfig,
    source_offset: Option<&str>,
) -> anyhow::Result<ReplicationReader> {
    timeout(Duration::from_secs(15), async {
        loop {
            match ReplicationReader::connect_from(source.clone(), source_offset).await {
                Ok(reader) => return Ok(reader),
                Err(error) => {
                    eprintln!("waiting for killed replication connection to close: {error}");
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
    })
    .await?
}

fn write_test_config(
    config_path: &std::path::Path,
    data_dir: &std::path::Path,
    fixture: &PgFixture,
) -> anyhow::Result<()> {
    let source_name = serde_json::to_string(&fixture.source_name)?;
    let host = serde_json::to_string(&fixture.source.host)?;
    let database = serde_json::to_string(&fixture.source.database)?;
    let user = serde_json::to_string(&fixture.source.user)?;
    let password = serde_json::to_string(&fixture.source.password)?;
    let publication = serde_json::to_string(&fixture.publication)?;
    let slot = serde_json::to_string(&fixture.slot)?;
    let data_dir = serde_json::to_string(&data_dir.display().to_string())?;
    let table = serde_json::to_string(&format!("public.{}", fixture.table))?;

    std::fs::write(
        config_path,
        format!(
            r#"[source]
type = "postgres"
name = {source_name}
host = {host}
port = {port}
database = {database}
user = {user}
password = {password}
publication = {publication}
slot = {slot}

[runtime]
data_dir = {data_dir}
storage_file = "capture-test.redb"
channel_capacity = 1024
shutdown_timeout_ms = 10000

[logging]
level = "info"

[[streams]]
name = "orders"
source = {source_name}
tables = [{table}]
"#,
            port = fixture.source.port,
        ),
    )?;
    Ok(())
}

fn spawn_crash_worker(
    config_path: &std::path::Path,
    marker: &std::path::Path,
    stage: &str,
) -> anyhow::Result<ChildGuard> {
    let child = Command::new(env::current_exe()?)
        .args([
            "--exact",
            "crash_capture_worker",
            "--ignored",
            "--nocapture",
        ])
        .env(CRASH_WORKER_ENV, "1")
        .env(CRASH_CONFIG_ENV, config_path)
        .env(CRASH_STAGE_ENV, stage)
        .env(CRASH_MARKER_ENV, marker)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()?;
    Ok(ChildGuard::new(child))
}

async fn wait_for_path(path: &std::path::Path) -> anyhow::Result<()> {
    timeout(Duration::from_secs(15), async {
        while !path.exists() {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("timed out waiting for {}", path.display()))
}

fn signal_ready_and_wait_for_kill(marker: &std::path::Path) -> anyhow::Result<()> {
    std::fs::write(marker, b"ready")?;
    loop {
        std::thread::park();
    }
}

fn required_env(name: &str) -> anyhow::Result<String> {
    env::var(name).map_err(|_| anyhow::anyhow!("required environment variable {name} is missing"))
}

struct ChildGuard {
    child: Option<Child>,
}

impl ChildGuard {
    fn new(child: Child) -> Self {
        Self { child: Some(child) }
    }

    fn kill_and_wait(&mut self) -> anyhow::Result<()> {
        if let Some(mut child) = self.child.take() {
            child.kill()?;
            child.wait()?;
        }
        Ok(())
    }

    async fn wait_for_exit(&mut self) -> anyhow::Result<std::process::ExitStatus> {
        let status = timeout(Duration::from_secs(20), async {
            loop {
                let child = self
                    .child
                    .as_mut()
                    .ok_or_else(|| anyhow::anyhow!("child process is no longer available"))?;
                if let Some(status) = child.try_wait()? {
                    return Ok::<_, anyhow::Error>(status);
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .map_err(|_| anyhow::anyhow!("timed out waiting for child process"))??;
        self.child = None;
        Ok(status)
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
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
    source: PostgresSourceConfig,
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

        let source = PostgresSourceConfig {
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

    async fn insert(&self, email: &str, total_cents: i64) -> anyhow::Result<()> {
        self.client
            .execute(
                &format!(
                    "INSERT INTO public.{} (customer_email, total_cents) VALUES ($1, $2)",
                    self.table
                ),
                &[&email, &total_cents],
            )
            .await?;
        Ok(())
    }

    async fn confirmed_flush_lsn(&self) -> anyhow::Result<Option<String>> {
        let row = self
            .client
            .query_one(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
                &[&self.slot],
            )
            .await?;
        Ok(row.get(0))
    }

    async fn wait_for_confirmed_flush_lsn_change(
        &self,
        previous: Option<&str>,
    ) -> anyhow::Result<String> {
        timeout(Duration::from_secs(15), async {
            loop {
                if let Some(current) = self.confirmed_flush_lsn().await?
                    && Some(current.as_str()) != previous
                {
                    return Ok(current);
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await?
    }

    async fn wait_for_replication_pid(&self, previous: Option<i32>) -> anyhow::Result<i32> {
        timeout(Duration::from_secs(15), async {
            loop {
                let row = self
                    .client
                    .query_one(
                        "SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1",
                        &[&self.slot],
                    )
                    .await?;
                let active_pid: Option<i32> = row.get(0);
                if let Some(active_pid) = active_pid
                    && Some(active_pid) != previous
                {
                    return Ok(active_pid);
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await?
    }

    async fn terminate_backend(&self, pid: i32) -> anyhow::Result<()> {
        let terminated: bool = self
            .client
            .query_one("SELECT pg_terminate_backend($1)", &[&pid])
            .await?
            .get(0);
        if !terminated {
            anyhow::bail!("PostgreSQL did not terminate replication backend {pid}");
        }
        Ok(())
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
