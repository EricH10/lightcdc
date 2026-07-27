use std::{
    env,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use lightcdc_core::{ChangeEvent, MySqlSourceConfig, Operation};
use lightcdc_mysql::ReplicationReader;
use lightcdc_storage::{LogOpenOptions, PersistTransactionOutcome, RedbEventStore};
use mysql_async::{Conn, Opts, OptsBuilder, prelude::Queryable};
use tempfile::TempDir;
use tokio::time::{sleep, timeout};

static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker compose mysql on localhost:3306"]
async fn captures_insert_update_delete_into_redb() -> anyhow::Result<()> {
    let mut fixture = MySqlFixture::create().await?;
    let temp = TempDir::new()?;
    let store = open_store(temp.path().to_path_buf())?;
    lightcdc_mysql::validate_source_config(&fixture.source).await?;
    let mut reader = ReplicationReader::connect_from(fixture.source.clone(), None).await?;
    reader.set_next_sequence(store.next_sequence()?);

    fixture.connection.query_drop("START TRANSACTION").await?;
    fixture
        .connection
        .query_drop(format!(
            "INSERT INTO `{}` (customer_email, total_cents) \
             VALUES ('ada@example.com', 4200)",
            fixture.table
        ))
        .await?;
    fixture
        .connection
        .query_drop(format!(
            "UPDATE `{}` SET status = 'paid' \
             WHERE customer_email = 'ada@example.com'",
            fixture.table
        ))
        .await?;
    fixture
        .connection
        .query_drop(format!(
            "DELETE FROM `{}` WHERE customer_email = 'ada@example.com'",
            fixture.table
        ))
        .await?;
    fixture.connection.query_drop("COMMIT").await?;

    let transaction = timeout(Duration::from_secs(15), reader.next_transaction())
        .await??
        .ok_or_else(|| anyhow::anyhow!("MySQL binlog stream ended before commit"))?;
    let checkpoint = transaction.checkpoint.to_string();
    assert_eq!(
        store.persist_transaction(&transaction.events, &fixture.source.name, &checkpoint)?,
        PersistTransactionOutcome::Persisted
    );
    let events = transaction.events;
    reader.shutdown().await?;

    assert_eq!(
        operations(&events),
        [Operation::Insert, Operation::Update, Operation::Delete]
    );
    assert_eq!(
        events
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert!(events.iter().all(|event| event.schema == "lightcdc"));
    assert!(events.iter().all(|event| event.table == fixture.table));
    assert_json_field(
        events[0].after.as_deref(),
        "customer_email",
        "ada@example.com",
    )?;
    assert_json_field(events[1].before.as_deref(), "status", "created")?;
    assert_json_field(events[1].after.as_deref(), "status", "paid")?;
    assert_json_field(events[2].before.as_deref(), "status", "paid")?;
    assert_json_number(events[0].key.as_deref(), "id", 1)?;
    assert_eq!(store.source_offset(&fixture.source.name)?, Some(checkpoint));

    drop(store);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker compose mysql on localhost:3306"]
async fn resumes_from_durable_binlog_position() -> anyhow::Result<()> {
    let fixture = MySqlFixture::create().await?;
    let temp = TempDir::new()?;
    let store = open_store(temp.path().to_path_buf())?;
    let mut first_reader = ReplicationReader::connect_from(fixture.source.clone(), None).await?;
    first_reader.set_next_sequence(store.next_sequence()?);

    fixture.insert("first@example.com", 5100).await?;
    let first = next_transaction(&mut first_reader).await?;
    let first_checkpoint = first.checkpoint.to_string();
    assert_eq!(
        store.persist_transaction(&first.events, &fixture.source.name, &first_checkpoint)?,
        PersistTransactionOutcome::Persisted
    );
    first_reader.shutdown().await?;

    let source_offset = store.source_offset(&fixture.source.name)?;
    let mut resumed =
        ReplicationReader::connect_from(fixture.source.clone(), source_offset.as_deref()).await?;
    resumed.set_next_sequence(store.next_sequence()?);
    fixture.insert("second@example.com", 5200).await?;
    let second = next_transaction(&mut resumed).await?;
    let second_checkpoint = second.checkpoint.to_string();
    assert_eq!(
        store.persist_transaction(&second.events, &fixture.source.name, &second_checkpoint)?,
        PersistTransactionOutcome::Persisted
    );
    resumed.shutdown().await?;

    assert_eq!(second.events.len(), 1);
    assert_eq!(second.events[0].sequence, 2);
    assert_json_field(
        second.events[0].after.as_deref(),
        "customer_email",
        "second@example.com",
    )?;
    assert_eq!(store.replay_from(1, 10)?.len(), 2);
    assert_eq!(
        store.source_offset(&fixture.source.name)?,
        Some(second_checkpoint)
    );

    drop(store);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker compose mysql on localhost:3306"]
async fn does_not_persist_an_uncommitted_mysql_transaction() -> anyhow::Result<()> {
    let mut fixture = MySqlFixture::create().await?;
    let temp = TempDir::new()?;
    let store = open_store(temp.path().to_path_buf())?;
    let mut reader = ReplicationReader::connect_from(fixture.source.clone(), None).await?;
    reader.set_next_sequence(store.next_sequence()?);

    fixture.connection.query_drop("START TRANSACTION").await?;
    fixture
        .connection
        .query_drop(format!(
            "INSERT INTO `{}` (customer_email, total_cents) \
             VALUES ('pending@example.com', 6400)",
            fixture.table
        ))
        .await?;

    assert!(
        timeout(Duration::from_millis(300), reader.next_transaction())
            .await
            .is_err(),
        "reader emitted a transaction before MySQL committed it"
    );
    assert!(store.replay_from(1, 10)?.is_empty());
    assert_eq!(store.source_offset(&fixture.source.name)?, None);

    fixture.connection.query_drop("COMMIT").await?;
    let transaction = next_transaction(&mut reader).await?;
    let checkpoint = transaction.checkpoint.to_string();
    assert_eq!(
        store.persist_transaction(&transaction.events, &fixture.source.name, &checkpoint)?,
        PersistTransactionOutcome::Persisted
    );
    reader.shutdown().await?;

    assert_eq!(store.replay_from(1, 10)?.len(), 1);
    assert_eq!(store.source_offset(&fixture.source.name)?, Some(checkpoint));

    drop(store);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker compose mysql on localhost:3306"]
async fn cli_captures_mysql_event_and_exits_at_max_events() -> anyhow::Result<()> {
    let mut fixture = MySqlFixture::create().await?;
    let temp = TempDir::new()?;
    let config_path = temp.path().join("lightcdc-mysql-test.toml");
    write_test_config(&config_path, temp.path(), &fixture)?;
    let existing_binlog_clients = fixture.binlog_client_ids().await?;

    let mut capture = ChildGuard::new(
        Command::new(env!("CARGO_BIN_EXE_lightcdc"))
            .args([
                "capture",
                "--config",
                config_path
                    .to_str()
                    .ok_or_else(|| anyhow::anyhow!("test config path is not UTF-8"))?,
                "--max-events",
                "1",
            ])
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()?,
    );

    fixture
        .wait_for_new_binlog_client(&existing_binlog_clients)
        .await?;
    fixture.insert("cli@example.com", 7300).await?;
    let status = capture.wait_for_exit().await?;
    assert!(status.success(), "capture process failed with {status}");

    let store = open_store(temp.path().to_path_buf())?;
    let events = store.replay_from(1, 10)?;
    assert_eq!(events.len(), 1);
    assert_json_field(
        events[0].after.as_deref(),
        "customer_email",
        "cli@example.com",
    )?;
    assert!(store.source_offset(&fixture.source.name)?.is_some());

    drop(store);
    fixture.cleanup().await?;
    Ok(())
}

async fn next_transaction(
    reader: &mut ReplicationReader,
) -> anyhow::Result<lightcdc_mysql::CapturedTransaction> {
    timeout(Duration::from_secs(15), reader.next_transaction())
        .await??
        .ok_or_else(|| anyhow::anyhow!("MySQL binlog stream ended before commit"))
}

fn open_store(path: PathBuf) -> anyhow::Result<RedbEventStore> {
    Ok(RedbEventStore::open(&LogOpenOptions {
        data_dir: path,
        database_file: "mysql-capture-test.redb".to_owned(),
    })?)
}

fn write_test_config(
    config_path: &std::path::Path,
    data_dir: &std::path::Path,
    fixture: &MySqlFixture,
) -> anyhow::Result<()> {
    let source_name = serde_json::to_string(&fixture.source.name)?;
    let host = serde_json::to_string(&fixture.source.host)?;
    let database = serde_json::to_string(&fixture.source.database)?;
    let user = serde_json::to_string(&fixture.source.user)?;
    let password = serde_json::to_string(&fixture.source.password)?;
    let data_dir = serde_json::to_string(&data_dir.display().to_string())?;
    let table = serde_json::to_string(&format!("lightcdc.{}", fixture.table))?;

    std::fs::write(
        config_path,
        format!(
            r#"[source]
type = "mysql"
name = {source_name}
host = {host}
port = {port}
database = {database}
user = {user}
password = {password}
server_id = {server_id}

[runtime]
data_dir = {data_dir}
storage_file = "mysql-capture-test.redb"
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
            server_id = fixture.source.server_id,
        ),
    )?;
    Ok(())
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

fn assert_json_number(bytes: Option<&[u8]>, field: &str, expected: u64) -> anyhow::Result<()> {
    let bytes = bytes.ok_or_else(|| anyhow::anyhow!("missing key payload"))?;
    let value: serde_json::Value = serde_json::from_slice(bytes)?;
    assert_eq!(value[field], expected);
    Ok(())
}

struct MySqlFixture {
    connection: Conn,
    source: MySqlSourceConfig,
    table: String,
}

impl MySqlFixture {
    async fn create() -> anyhow::Result<Self> {
        let suffix = unique_suffix();
        let table = format!("orders_{suffix}");
        let source = MySqlSourceConfig {
            name: format!("mysql:{suffix}"),
            host: "localhost".to_owned(),
            port: 3306,
            database: "lightcdc".to_owned(),
            user: "lightcdc".to_owned(),
            password: "lightcdc".to_owned(),
            server_id: 10_000 + TEST_COUNTER.fetch_add(1, Ordering::Relaxed) as u32,
        };
        let mut connection = Conn::new(connection_options(&source)).await?;

        connection
            .query_drop(format!(
                r#"
                CREATE TABLE `{table}` (
                    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    customer_email VARCHAR(255) NOT NULL,
                    status VARCHAR(32) NOT NULL DEFAULT 'created',
                    total_cents BIGINT NOT NULL,
                    PRIMARY KEY (id)
                ) ENGINE=InnoDB
                "#
            ))
            .await?;

        Ok(Self {
            connection,
            source,
            table,
        })
    }

    async fn insert(&self, email: &str, total_cents: i64) -> anyhow::Result<()> {
        let mut connection = Conn::new(connection_options(&self.source)).await?;
        connection
            .exec_drop(
                format!(
                    "INSERT INTO `{}` (customer_email, total_cents) VALUES (?, ?)",
                    self.table
                ),
                (email, total_cents),
            )
            .await?;
        connection.disconnect().await?;
        Ok(())
    }

    async fn binlog_client_ids(&mut self) -> anyhow::Result<Vec<u64>> {
        Ok(self
            .connection
            .query(
                "SELECT ID FROM information_schema.PROCESSLIST \
                 WHERE USER = 'lightcdc' AND COMMAND LIKE 'Binlog Dump%'",
            )
            .await?)
    }

    async fn wait_for_new_binlog_client(&mut self, existing: &[u64]) -> anyhow::Result<()> {
        timeout(Duration::from_secs(15), async {
            loop {
                let ids = self.binlog_client_ids().await?;
                if ids.iter().any(|id| !existing.contains(id)) {
                    return Ok::<_, anyhow::Error>(());
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .map_err(|_| anyhow::anyhow!("timed out waiting for MySQL binlog client"))??;
        Ok(())
    }

    async fn cleanup(mut self) -> anyhow::Result<()> {
        self.connection
            .query_drop(format!("DROP TABLE IF EXISTS `{}`", self.table))
            .await?;
        self.connection.disconnect().await?;
        Ok(())
    }
}

fn connection_options(source: &MySqlSourceConfig) -> Opts {
    OptsBuilder::default()
        .ip_or_hostname(&source.host)
        .tcp_port(source.port)
        .user(Some(&source.user))
        .pass(Some(&source.password))
        .db_name(Some(&source.database))
        .prefer_socket(false)
        .into()
}

fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();
    format!("{nanos:x}")
}

struct ChildGuard {
    child: Option<Child>,
}

impl ChildGuard {
    fn new(child: Child) -> Self {
        Self { child: Some(child) }
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
