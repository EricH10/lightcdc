use std::{
    env,
    fs::File,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use lightcdc_core::{ChangeEvent, Config, Operation, SourceConfig};
use lightcdc_postgres::{ReplicationReader, validate_source_config_with_plan};
use lightcdc_storage::{
    LogOpenOptions, PersistTransactionOutcome, RedbEventStore, TransactionBufferError,
    TransactionBufferOptions, TransactionEvents,
};
use tempfile::TempDir;
use tokio::time::{sleep, timeout};
use tokio_postgres::{Client, NoTls};

static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);
const CRASH_WORKER_ENV: &str = "LIGHTCDC_CRASH_CAPTURE_WORKER";
const CRASH_CONFIG_ENV: &str = "LIGHTCDC_CRASH_CONFIG";
const CRASH_STAGE_ENV: &str = "LIGHTCDC_CRASH_STAGE";
const CRASH_MARKER_ENV: &str = "LIGHTCDC_CRASH_MARKER";
const LARGE_TRANSACTION_ROWS: usize = 1_000;

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
        store.persist_transaction_events(
            &first_transaction.events,
            &fixture.source_name,
            &first_ack_lsn.to_string(),
        )?,
        PersistTransactionOutcome::Persisted
    );
    let first_events = first_transaction.events.load()?;

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
        store.persist_transaction_events(
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
async fn captures_a_large_transaction_as_one_atomic_batch() -> anyhow::Result<()> {
    let fixture = PgFixture::create().await?;
    let temp = TempDir::new()?;
    let store = open_store(temp.path().to_path_buf())?;
    let buffer_options = test_buffer_options(temp.path(), &fixture.source_name, 1_024, 10_000);
    let mut reader =
        ReplicationReader::connect_from_with_buffer(fixture.source.clone(), None, buffer_options)
            .await?;
    reader.set_next_sequence(store.next_sequence()?);

    fixture
        .client
        .execute(
            &format!(
                r#"
                INSERT INTO public.{table} (customer_email, total_cents)
                SELECT
                    'bulk-' || value || '@example.com',
                    value
                FROM generate_series(1, {row_count}) AS value
                "#,
                table = fixture.table,
                row_count = LARGE_TRANSACTION_ROWS,
            ),
            &[],
        )
        .await?;

    let transaction = next_transaction(&mut reader).await?;
    assert!(transaction.events.is_staged());
    let TransactionEvents::Staged(staged) = &transaction.events else {
        unreachable!("staged transaction variant");
    };
    let staging_path = staged.path().to_path_buf();
    assert!(staging_path.exists());
    let events = transaction.events.load()?;
    assert_eq!(events.len(), LARGE_TRANSACTION_ROWS);
    assert_eq!(events.first().map(|event| event.sequence), Some(1));
    assert_eq!(
        events.last().map(|event| event.sequence),
        Some(LARGE_TRANSACTION_ROWS as u64)
    );
    let transaction_id = events[0]
        .transaction
        .as_ref()
        .and_then(|metadata| metadata.transaction_id);
    assert!(transaction_id.is_some());
    assert!(events.iter().all(|event| {
        event.operation == Operation::Insert
            && event
                .transaction
                .as_ref()
                .is_some_and(|metadata| metadata.transaction_id == transaction_id)
    }));

    let ack_lsn = transaction.ack_lsn;
    assert_eq!(
        store.persist_transaction_events(
            &transaction.events,
            &fixture.source_name,
            &ack_lsn.to_string(),
        )?,
        PersistTransactionOutcome::Persisted
    );
    reader.ack(ack_lsn);
    reader.shutdown().await?;
    drop(transaction);
    assert!(!staging_path.exists());

    assert_eq!(
        store.replay_from(1, LARGE_TRANSACTION_ROWS + 1)?.len(),
        LARGE_TRANSACTION_ROWS
    );
    assert_eq!(
        store.source_offset(&fixture.source_name)?,
        Some(ack_lsn.to_string())
    );

    drop(store);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker compose postgres on localhost:5432"]
async fn hard_transaction_limit_fails_without_persisting_or_acknowledging() -> anyhow::Result<()> {
    let fixture = PgFixture::create().await?;
    let temp = TempDir::new()?;
    let store = open_store(temp.path().to_path_buf())?;
    let buffer_options = test_buffer_options(temp.path(), &fixture.source_name, 1, 1);
    let staging_dir = buffer_options
        .staging_dir
        .clone()
        .ok_or_else(|| anyhow::anyhow!("test staging directory is missing"))?;
    let mut reader =
        ReplicationReader::connect_from_with_buffer(fixture.source.clone(), None, buffer_options)
            .await?;
    reader.set_next_sequence(store.next_sequence()?);

    fixture
        .client
        .batch_execute(&format!(
            r#"
            BEGIN;
            INSERT INTO public.{table} (customer_email, total_cents)
            VALUES ('limit-1@example.com', 1);
            INSERT INTO public.{table} (customer_email, total_cents)
            VALUES ('limit-2@example.com', 2);
            COMMIT;
            "#,
            table = fixture.table,
        ))
        .await?;

    let error = match timeout(Duration::from_secs(15), reader.next_transaction()).await? {
        Ok(_) => anyhow::bail!("transaction should exceed the event limit"),
        Err(error) => error,
    };
    assert!(error.is_fatal_capture_error());
    assert!(matches!(
        error,
        lightcdc_postgres::PostgresError::TransactionBuffer(
            TransactionBufferError::EventLimitExceeded {
                attempted: 2,
                maximum: 1
            }
        )
    ));
    assert!(store.replay_from(1, 10)?.is_empty());
    assert_eq!(store.source_offset(&fixture.source_name)?, None);
    reader.shutdown().await?;
    drop(reader);
    assert_eq!(staging_file_count(&staging_dir)?, 0);

    drop(store);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker compose postgres on localhost:5432"]
async fn captures_primary_key_for_delete_with_default_replica_identity() -> anyhow::Result<()> {
    let fixture = PgFixture::create().await?;
    fixture
        .client
        .batch_execute(&format!(
            "ALTER TABLE public.{} REPLICA IDENTITY DEFAULT",
            fixture.table
        ))
        .await?;
    let mut reader = ReplicationReader::connect(fixture.source.clone()).await?;
    reader.set_next_sequence(1);

    fixture.insert("delete-key@example.com", 8100).await?;
    let inserted = next_nonempty_transaction(&mut reader).await?;
    assert_eq!(inserted.events.len(), 1);

    fixture
        .client
        .execute(
            &format!(
                "DELETE FROM public.{} WHERE customer_email = $1",
                fixture.table
            ),
            &[&"delete-key@example.com"],
        )
        .await?;
    let deleted = next_nonempty_transaction(&mut reader).await?;
    reader.shutdown().await?;
    let deleted_events = deleted.events.load()?;

    assert_eq!(deleted_events.len(), 1);
    assert_eq!(deleted_events[0].operation, Operation::Delete);
    assert_json_field(deleted_events[0].key.as_deref(), "id", "1")?;
    assert!(deleted_events[0].before.is_none());
    assert!(deleted_events[0].after.is_none());

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker compose postgres on localhost:5432"]
async fn marks_an_unchanged_toast_value_without_losing_the_old_value() -> anyhow::Result<()> {
    let fixture = PgFixture::create().await?;
    fixture
        .client
        .batch_execute(&format!(
            "ALTER TABLE public.{} ADD COLUMN description TEXT",
            fixture.table
        ))
        .await?;
    let mut reader = ReplicationReader::connect(fixture.source.clone()).await?;
    reader.set_next_sequence(1);

    fixture
        .client
        .execute(
            &format!(
                r#"
                INSERT INTO public.{table} (customer_email, total_cents, description)
                SELECT
                    'toast@example.com',
                    8200,
                    string_agg(md5(value::text), '')
                FROM generate_series(1, 2_000) AS value
                "#,
                table = fixture.table,
            ),
            &[],
        )
        .await?;
    let inserted = next_nonempty_transaction(&mut reader).await?;
    assert_eq!(inserted.events.len(), 1);

    fixture
        .client
        .execute(
            &format!(
                "UPDATE public.{} SET status = 'paid' WHERE customer_email = $1",
                fixture.table
            ),
            &[&"toast@example.com"],
        )
        .await?;
    let updated = next_nonempty_transaction(&mut reader).await?;
    reader.shutdown().await?;
    let updated_events = updated.events.load()?;

    assert_eq!(updated_events.len(), 1);
    let before = json_payload(updated_events[0].before.as_deref())?;
    let after = json_payload(updated_events[0].after.as_deref())?;
    assert!(
        before["description"]
            .as_str()
            .is_some_and(|description| description.len() > 10_000)
    );
    assert_eq!(
        after["description"],
        serde_json::json!({ "__unchanged_toast": true })
    );
    assert_eq!(after["status"], "paid");

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker compose postgres on localhost:5432"]
async fn emits_a_truncate_event_for_the_affected_table() -> anyhow::Result<()> {
    let fixture = PgFixture::create().await?;
    let mut reader = ReplicationReader::connect(fixture.source.clone()).await?;
    reader.set_next_sequence(1);

    fixture.insert("truncate@example.com", 8300).await?;
    let inserted = next_nonempty_transaction(&mut reader).await?;
    assert_eq!(inserted.events.len(), 1);

    fixture
        .client
        .batch_execute(&format!("TRUNCATE TABLE public.{}", fixture.table))
        .await?;
    let truncated = next_transaction(&mut reader).await?;
    reader.shutdown().await?;
    let truncated_events = truncated.events.load()?;

    assert_eq!(truncated_events.len(), 1);
    assert_eq!(truncated_events[0].operation, Operation::Truncate);
    assert_eq!(truncated_events[0].schema, "public");
    assert_eq!(truncated_events[0].table, fixture.table);
    assert!(truncated_events[0].key.is_none());
    assert!(truncated_events[0].before.is_none());
    assert!(truncated_events[0].after.is_none());

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker compose postgres on localhost:5432"]
async fn refreshes_relation_metadata_after_adding_a_column() -> anyhow::Result<()> {
    let fixture = PgFixture::create().await?;
    let mut reader = ReplicationReader::connect(fixture.source.clone()).await?;
    reader.set_next_sequence(1);

    fixture.insert("before-schema@example.com", 8400).await?;
    let before_schema_change = next_nonempty_transaction(&mut reader).await?;
    assert_eq!(before_schema_change.events.len(), 1);
    let before_schema_events = before_schema_change.events.load()?;
    assert!(
        !json_payload(before_schema_events[0].after.as_deref())?
            .as_object()
            .is_some_and(|row| row.contains_key("priority"))
    );

    fixture
        .client
        .batch_execute(&format!(
            "ALTER TABLE public.{} ADD COLUMN priority TEXT NOT NULL DEFAULT 'normal'",
            fixture.table
        ))
        .await?;
    fixture
        .client
        .execute(
            &format!(
                "INSERT INTO public.{} (customer_email, total_cents, priority) VALUES ($1, $2, $3)",
                fixture.table
            ),
            &[&"after-schema@example.com", &8500_i64, &"urgent"],
        )
        .await?;

    let after_schema_change = next_nonempty_transaction(&mut reader).await?;
    reader.shutdown().await?;
    let after_schema_events = after_schema_change.events.load()?;

    assert_eq!(after_schema_events.len(), 1);
    assert_json_field(
        after_schema_events[0].after.as_deref(),
        "priority",
        "urgent",
    )?;

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
    let buffer_options = test_buffer_options(temp.path(), &fixture.source_name, 1, 10_000);
    let staging_dir = buffer_options
        .staging_dir
        .clone()
        .ok_or_else(|| anyhow::anyhow!("test staging directory is missing"))?;
    assert!(staging_file_count(&staging_dir)? > 0);

    let store = open_store(temp.path().to_path_buf())?;
    assert!(store.replay_from(1, 10)?.is_empty());
    assert_eq!(store.source_offset(&fixture.source_name)?, None);

    let mut reader = connect_reader(&fixture.source, None, buffer_options).await?;
    reader.set_next_sequence(store.next_sequence()?);
    let events = capture_new_events(&mut reader, &store, &fixture.source_name, 1).await?;
    assert_eq!(staging_file_count(&staging_dir)?, 0);
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
    let buffer_options = test_buffer_options(temp.path(), &fixture.source_name, 1, 10_000);
    let staging_dir = buffer_options
        .staging_dir
        .clone()
        .ok_or_else(|| anyhow::anyhow!("test staging directory is missing"))?;
    assert!(staging_file_count(&staging_dir)? > 0);

    let store = open_store(temp.path().to_path_buf())?;
    let stored_offset = store.source_offset(&fixture.source_name)?;
    assert_eq!(store.replay_from(1, 10)?.len(), 1);
    assert!(stored_offset.is_some());

    let mut reader =
        connect_reader(&fixture.source, stored_offset.as_deref(), buffer_options).await?;
    reader.set_next_sequence(store.next_sequence()?);
    fixture.insert("after-restart@example.com", 6200).await?;
    let events = capture_new_events(&mut reader, &store, &fixture.source_name, 1).await?;
    assert_eq!(staging_file_count(&staging_dir)?, 0);
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
#[ignore = "requires docker compose postgres on localhost:5432"]
async fn capture_groups_small_source_transactions_into_one_redb_commit() -> anyhow::Result<()> {
    let fixture = PgFixture::create().await?;
    let temp = TempDir::new()?;
    let config_path = temp.path().join("lightcdc-test.toml");
    let metrics_path = temp.path().join("capture.jsonl");
    write_test_config_with_batch(&config_path, temp.path(), &fixture, 10, 100, 5_000)?;

    let mut capture = ChildGuard::new(
        Command::new(env!("CARGO_BIN_EXE_lightcdc"))
            .args([
                "capture",
                "--config",
                config_path
                    .to_str()
                    .ok_or_else(|| anyhow::anyhow!("test config path is not UTF-8"))?,
                "--max-events",
                "10",
                "--output",
                "none",
                "--metrics-file",
                metrics_path
                    .to_str()
                    .ok_or_else(|| anyhow::anyhow!("metrics path is not UTF-8"))?,
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()?,
    );

    fixture.wait_for_replication_pid(None).await?;
    for index in 0..10 {
        fixture
            .insert(&format!("batch-{index}@example.com"), index)
            .await?;
    }

    let status = capture.wait_for_exit().await?;
    assert!(status.success(), "capture process failed with {status}");

    let reports = std::fs::read_to_string(&metrics_path)?;
    let report: serde_json::Value = serde_json::from_str(
        reports
            .lines()
            .last()
            .ok_or_else(|| anyhow::anyhow!("capture metrics file is empty"))?,
    )?;
    assert_eq!(report["transactions_total"], 10);
    assert_eq!(report["events_total"], 10);
    assert_eq!(report["storage_commits_total"], 1);
    assert_eq!(report["transactions_per_storage_commit"], 10.0);
    assert_eq!(report["events_per_storage_commit"], 10.0);

    let store = open_store(temp.path().to_path_buf())?;
    assert_eq!(store.replay_from(1, 20)?.len(), 10);
    assert!(store.source_offset(&fixture.source_name)?.is_some());

    drop(store);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker compose postgres on localhost:5432"]
async fn capture_runtime_prunes_events_to_the_configured_count() -> anyhow::Result<()> {
    let fixture = PgFixture::create().await?;
    let temp = TempDir::new()?;
    let config_path = temp.path().join("lightcdc-test.toml");
    let log_path = temp.path().join("lightcdc.log");
    write_test_config_with_retention(&config_path, temp.path(), &fixture, 3)?;

    let mut capture = ChildGuard::new(
        Command::new(env!("CARGO_BIN_EXE_lightcdc"))
            .args([
                "capture",
                "--config",
                config_path
                    .to_str()
                    .ok_or_else(|| anyhow::anyhow!("test config path is not UTF-8"))?,
                "--output",
                "none",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::from(File::create(&log_path)?))
            .spawn()?,
    );

    fixture.wait_for_replication_pid(None).await?;
    for index in 0..10 {
        fixture
            .insert(&format!("retention-{index}@example.com"), index)
            .await?;
    }
    let target_lsn = fixture.current_wal_lsn().await?;
    fixture.wait_for_confirmed_flush_lsn(&target_lsn).await?;
    sleep(Duration::from_millis(250)).await;
    capture.kill_and_wait()?;

    let store = open_store(temp.path().to_path_buf())?;
    assert_eq!(store.stats()?.event_count, 3);
    assert_eq!(store.first_sequence()?, Some(8));
    assert_eq!(store.last_sequence()?, Some(10));
    assert_eq!(store.next_sequence()?, 11);

    drop(store);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker compose postgres on localhost:5432"]
async fn capture_exits_for_a_missing_publication_instead_of_retrying() -> anyhow::Result<()> {
    let fixture = PgFixture::create().await?;
    let temp = TempDir::new()?;
    let config_path = temp.path().join("lightcdc-test.toml");
    write_test_config(&config_path, temp.path(), &fixture)?;
    fixture
        .client
        .batch_execute(&format!("DROP PUBLICATION {}", fixture.publication))
        .await?;

    let mut capture = ChildGuard::new(
        Command::new(env!("CARGO_BIN_EXE_lightcdc"))
            .args([
                "capture",
                "--config",
                config_path
                    .to_str()
                    .ok_or_else(|| anyhow::anyhow!("test config path is not UTF-8"))?,
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    );

    let status = capture.wait_for_exit().await?;
    assert!(
        !status.success(),
        "capture should stop for a missing publication"
    );

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker compose postgres on localhost:5432"]
async fn capture_exits_when_a_configured_table_is_missing_from_the_publication()
-> anyhow::Result<()> {
    let fixture = PgFixture::create().await?;
    let temp = TempDir::new()?;
    let config_path = temp.path().join("lightcdc-test.toml");
    write_test_config(&config_path, temp.path(), &fixture)?;
    fixture
        .client
        .batch_execute(&format!(
            "ALTER PUBLICATION {} DROP TABLE public.{}",
            fixture.publication, fixture.table
        ))
        .await?;

    let mut capture = ChildGuard::new(
        Command::new(env!("CARGO_BIN_EXE_lightcdc"))
            .args([
                "capture",
                "--config",
                config_path
                    .to_str()
                    .ok_or_else(|| anyhow::anyhow!("test config path is not UTF-8"))?,
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    );

    let status = capture.wait_for_exit().await?;
    assert!(
        !status.success(),
        "capture should stop when a configured table is not published"
    );

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker compose postgres on localhost:5432"]
async fn capture_filters_unconfigured_tables_from_a_broad_publication() -> anyhow::Result<()> {
    let fixture = PgFixture::create().await?;
    let unrelated_table = fixture.create_unrelated_table().await?;
    fixture
        .client
        .batch_execute(&format!(
            "ALTER PUBLICATION {} ADD TABLE public.{}",
            fixture.publication, unrelated_table
        ))
        .await?;
    let temp = TempDir::new()?;
    let config_path = temp.path().join("lightcdc-test.toml");
    write_test_config(&config_path, temp.path(), &fixture)?;
    let config = Config::from_path(&config_path)?;
    let capture_plan = config.capture_plan()?;
    let alignment = validate_source_config_with_plan(&config.source, &capture_plan).await?;
    assert_eq!(
        alignment.unnecessary_published_tables,
        [format!("public.{unrelated_table}")]
    );
    let store = open_store(temp.path().to_path_buf())?;
    let mut reader = ReplicationReader::connect_from_with_buffer_and_plan(
        config.source.clone(),
        None,
        config_buffer_options(&config),
        capture_plan,
    )
    .await?;
    reader.set_next_sequence(store.next_sequence()?);

    fixture
        .client
        .batch_execute(&format!(
            r#"
            BEGIN;
            INSERT INTO public.{unrelated_table} (payload) VALUES ('ignore me');
            INSERT INTO public.{table} (customer_email, total_cents)
            VALUES ('selected@example.com', 4200);
            COMMIT;
            "#,
            table = fixture.table
        ))
        .await?;

    let transaction = timeout(Duration::from_secs(15), reader.next_transaction())
        .await??
        .ok_or_else(|| anyhow::anyhow!("replication stream ended before transaction commit"))?;
    let events = transaction.events.load()?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].sequence, 1);
    assert_eq!(events[0].table, fixture.table);
    store.persist_transaction_events(
        &transaction.events,
        &config.source.name,
        &transaction.ack_lsn.to_string(),
    )?;
    reader.ack(transaction.ack_lsn);
    reader.shutdown().await?;
    assert_eq!(store.stats()?.event_count, 1);

    fixture
        .client
        .batch_execute(&format!("DROP TABLE public.{unrelated_table}"))
        .await?;
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker compose postgres on localhost:5432"]
async fn logical_heartbeats_advance_the_slot_during_only_unrelated_writes() -> anyhow::Result<()> {
    let fixture = PgFixture::create().await?;
    let unrelated_table = fixture.create_unrelated_table().await?;
    let temp = TempDir::new()?;
    let config_path = temp.path().join("lightcdc-test.toml");
    write_test_config_with_heartbeat(&config_path, temp.path(), &fixture, 50)?;
    let mut capture = ChildGuard::new(
        Command::new(env!("CARGO_BIN_EXE_lightcdc"))
            .args([
                "capture",
                "--config",
                config_path
                    .to_str()
                    .ok_or_else(|| anyhow::anyhow!("test config path is not UTF-8"))?,
                "--output",
                "none",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    );
    fixture.wait_for_replication_pid(None).await?;
    let initial = fixture.confirmed_flush_lsn().await?;
    fixture
        .wait_for_confirmed_flush_lsn_change(initial.as_deref())
        .await?;

    fixture
        .client
        .execute(
            &format!("INSERT INTO public.{unrelated_table} (payload) VALUES ($1)"),
            &[&"not captured"],
        )
        .await?;
    let unrelated_write_lsn = fixture.current_wal_lsn().await?;
    fixture
        .wait_for_confirmed_flush_lsn(&unrelated_write_lsn)
        .await?;

    capture.kill_and_wait()?;
    let store = open_store(temp.path().to_path_buf())?;
    assert_eq!(store.stats()?.event_count, 0);
    assert!(store.source_offset(&fixture.source_name)?.is_some());
    drop(store);

    fixture
        .client
        .batch_execute(&format!("DROP TABLE public.{unrelated_table}"))
        .await?;
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
    let source_offset = store.source_offset(&config.source.name)?;
    let buffer_options = config_buffer_options(&config);
    let mut reader = ReplicationReader::connect_from_with_buffer(
        config.source.clone(),
        source_offset.as_deref(),
        buffer_options,
    )
    .await?;
    reader.set_next_sequence(store.next_sequence()?);

    let transaction = timeout(Duration::from_secs(15), reader.next_transaction())
        .await??
        .ok_or_else(|| anyhow::anyhow!("replication stream ended before crash boundary"))?;

    if stage == "before_persist" {
        signal_ready_and_wait_for_kill(&marker)?;
    }

    let ack_lsn = transaction.ack_lsn;
    store.persist_transaction_events(
        &transaction.events,
        &config.source.name,
        &ack_lsn.to_string(),
    )?;

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

            match store.persist_transaction_events(
                &transaction.events,
                source_name,
                &ack_lsn.to_string(),
            )? {
                PersistTransactionOutcome::Persisted => {
                    events.extend(transaction.events.load()?);
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

async fn next_transaction(
    reader: &mut ReplicationReader,
) -> anyhow::Result<lightcdc_postgres::CapturedTransaction> {
    timeout(Duration::from_secs(30), reader.next_transaction())
        .await??
        .ok_or_else(|| anyhow::anyhow!("replication stream ended before transaction commit"))
}

async fn next_nonempty_transaction(
    reader: &mut ReplicationReader,
) -> anyhow::Result<lightcdc_postgres::CapturedTransaction> {
    timeout(Duration::from_secs(30), async {
        loop {
            let transaction = reader
                .next_transaction()
                .await?
                .ok_or_else(|| anyhow::anyhow!("replication stream ended before row change"))?;
            if !transaction.events.is_empty() {
                return Ok(transaction);
            }
        }
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
    source: &SourceConfig,
    source_offset: Option<&str>,
    buffer_options: TransactionBufferOptions,
) -> anyhow::Result<ReplicationReader> {
    timeout(Duration::from_secs(15), async {
        loop {
            match ReplicationReader::connect_from_with_buffer(
                source.clone(),
                source_offset,
                buffer_options.clone(),
            )
            .await
            {
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
    write_test_config_with_batch(config_path, data_dir, fixture, 100, 500, 5)
}

fn write_test_config_with_batch(
    config_path: &std::path::Path,
    data_dir: &std::path::Path,
    fixture: &PgFixture,
    capture_batch_max_transactions: usize,
    capture_batch_max_events: usize,
    capture_batch_max_delay_ms: u64,
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
transaction_memory_threshold_bytes = 1
max_transaction_bytes = 67108864
max_transaction_events = 10000
capture_batch_max_transactions = {capture_batch_max_transactions}
capture_batch_max_events = {capture_batch_max_events}
capture_batch_max_bytes = 4194304
capture_batch_max_delay_ms = {capture_batch_max_delay_ms}

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

fn write_test_config_with_retention(
    config_path: &std::path::Path,
    data_dir: &std::path::Path,
    fixture: &PgFixture,
    retention_max_events: u64,
) -> anyhow::Result<()> {
    write_test_config(config_path, data_dir, fixture)?;
    let config = std::fs::read_to_string(config_path)?;
    let config = config.replace(
        "capture_batch_max_delay_ms = 5",
        &format!(
            "capture_batch_max_delay_ms = 5\n\
             retention_max_events = {retention_max_events}\n\
             retention_check_interval_ms = 10\n\
             retention_delete_batch_size = 100"
        ),
    );
    std::fs::write(config_path, config)?;
    Ok(())
}

fn write_test_config_with_heartbeat(
    config_path: &std::path::Path,
    data_dir: &std::path::Path,
    fixture: &PgFixture,
    heartbeat_interval_ms: u64,
) -> anyhow::Result<()> {
    write_test_config(config_path, data_dir, fixture)?;
    let config = std::fs::read_to_string(config_path)?;
    let config = config.replace(
        "shutdown_timeout_ms = 10000",
        &format!("shutdown_timeout_ms = 10000\nheartbeat_interval_ms = {heartbeat_interval_ms}"),
    );
    std::fs::write(config_path, config)?;
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

fn config_buffer_options(config: &Config) -> TransactionBufferOptions {
    TransactionBufferOptions::bounded(
        std::path::Path::new(&config.runtime.data_dir).join("staging"),
        &config.source.name,
        config.runtime.transaction_memory_threshold_bytes,
        config.runtime.max_transaction_bytes,
        config.runtime.max_transaction_events,
    )
}

fn test_buffer_options(
    data_dir: &std::path::Path,
    source_name: &str,
    memory_threshold_bytes: u64,
    max_transaction_events: usize,
) -> TransactionBufferOptions {
    TransactionBufferOptions::bounded(
        data_dir.join("staging"),
        source_name,
        memory_threshold_bytes,
        64 * 1024 * 1024,
        max_transaction_events,
    )
}

fn staging_file_count(staging_dir: &std::path::Path) -> anyhow::Result<usize> {
    if !staging_dir.exists() {
        return Ok(0);
    }

    Ok(std::fs::read_dir(staging_dir)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .path()
                .extension()
                .and_then(|extension| extension.to_str())
                == Some("lightcdc-stage")
        })
        .count())
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
    let value = json_payload(bytes)?;
    assert_eq!(value[field], expected);
    Ok(())
}

fn json_payload(bytes: Option<&[u8]>) -> anyhow::Result<serde_json::Value> {
    let bytes = bytes.ok_or_else(|| anyhow::anyhow!("missing row payload"))?;
    Ok(serde_json::from_slice(bytes)?)
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

    async fn create_unrelated_table(&self) -> anyhow::Result<String> {
        let table = format!("unrelated_{}", unique_suffix());
        self.client
            .batch_execute(&format!(
                "CREATE TABLE public.{table} (
                    id BIGSERIAL PRIMARY KEY,
                    payload TEXT NOT NULL
                )"
            ))
            .await?;
        Ok(table)
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

    async fn current_wal_lsn(&self) -> anyhow::Result<String> {
        let row = self
            .client
            .query_one("SELECT pg_current_wal_lsn()::text", &[])
            .await?;
        Ok(row.get(0))
    }

    async fn wait_for_confirmed_flush_lsn(&self, target: &str) -> anyhow::Result<()> {
        timeout(Duration::from_secs(15), async {
            loop {
                let row = self
                    .client
                    .query_one(
                        "SELECT COALESCE(confirmed_flush_lsn >= ($2::text)::pg_lsn, false)
                         FROM pg_replication_slots
                         WHERE slot_name = $1",
                        &[&self.slot, &target],
                    )
                    .await?;
                if row.get::<_, bool>(0) {
                    return Ok(());
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await?
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
