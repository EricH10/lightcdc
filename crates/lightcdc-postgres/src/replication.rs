use std::collections::BTreeSet;
use std::str::FromStr;
use std::time::Duration;

use lightcdc_core::{
    CapturePlan, ChangeEvent, Operation, SourceConfig, SourceMetadata, TransactionMetadata,
};
use lightcdc_storage::{
    TransactionBuffer, TransactionBufferError, TransactionBufferOptions, TransactionEvents,
};
use pgwire_replication::{
    client::{ReplicationClient, ReplicationEvent},
    config::{ReplicationConfig, TlsConfig},
    error::PgWireError,
    lsn::Lsn,
};
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout_at};
use tokio_postgres::{Client, NoTls};
use tracing::{debug, info};

use crate::decoder::{DecodeError, PgOutputDecoder, PgOutputMessage, Relation, RowChange};

const POSTGRES_EPOCH_UNIX_MS: i64 = 946_684_800_000;

/// Represents failures while connecting to or decoding PostgreSQL replication.
#[derive(Debug, Error)]
pub enum PostgresError {
    #[error("failed to connect to PostgreSQL: {0}")]
    Connect(#[from] tokio_postgres::Error),

    #[error("replication protocol error: {0}")]
    Replication(#[from] PgWireError),

    #[error("invalid PostgreSQL source configuration: {0}")]
    Configuration(String),

    #[error("invalid replication stream state: {0}")]
    StreamState(String),

    #[error("pgoutput decode error: {0}")]
    Decode(#[from] DecodeError),

    #[error("transaction buffering error: {0}")]
    TransactionBuffer(#[from] TransactionBufferError),
}

/// Reads PostgreSQL logical replication messages and emits committed transactions.
pub struct ReplicationReader {
    source: SourceConfig,
    client: ReplicationClient,
    decoder: PgOutputDecoder,
    sequence: u64,
    transaction: Option<TransactionMetadata>,
    commit_timestamp_ms: Option<i64>,
    pending_events: TransactionBuffer,
    capture_plan: CapturePlan,
}

/// Emits transactional logical messages that provide safe idle WAL checkpoints.
pub struct LogicalHeartbeatEmitter {
    client: Client,
    connection_task: JoinHandle<()>,
}

/// Describes how a configured capture plan aligns with its publication.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PublicationAlignment {
    pub unnecessary_published_tables: Vec<String>,
}

/// Bundles the events from one committed transaction with its checkpoint.
pub struct CapturedTransaction {
    /// The normalized CDC events to persist atomically.
    pub events: TransactionEvents,
    /// The commit WAL position to acknowledge after durable persistence.
    pub ack_lsn: Lsn,
}

/// Reports whether a bounded transaction read completed, timed out, or ended.
pub enum TransactionRead {
    Transaction(CapturedTransaction),
    TimedOut,
    StreamEnded,
}

impl ReplicationReader {
    /// Connects to PostgreSQL from the start of the replication slot.
    pub async fn connect(source: SourceConfig) -> Result<Self, PostgresError> {
        Self::connect_from(source, None).await
    }

    /// Connects to PostgreSQL from a previously saved LSN when provided.
    pub async fn connect_from(
        source: SourceConfig,
        start_lsn: Option<&str>,
    ) -> Result<Self, PostgresError> {
        Self::connect_from_with_buffer(
            source,
            start_lsn,
            TransactionBufferOptions::unbounded_in_memory(),
        )
        .await
    }

    /// Connects with bounded transaction memory and file-backed staging options.
    pub async fn connect_from_with_buffer(
        source: SourceConfig,
        start_lsn: Option<&str>,
        buffer_options: TransactionBufferOptions,
    ) -> Result<Self, PostgresError> {
        let capture_plan = CapturePlan::all(source.name.clone());
        Self::connect_from_with_buffer_and_plan(source, start_lsn, buffer_options, capture_plan)
            .await
    }

    /// Connects with bounded buffering and a compiled configured-table filter.
    pub async fn connect_from_with_buffer_and_plan(
        source: SourceConfig,
        start_lsn: Option<&str>,
        buffer_options: TransactionBufferOptions,
        capture_plan: CapturePlan,
    ) -> Result<Self, PostgresError> {
        if capture_plan.source_name() != source.name {
            return Err(PostgresError::Configuration(format!(
                "capture plan belongs to source {:?}; expected {:?}",
                capture_plan.source_name(),
                source.name
            )));
        }
        let start_lsn = match start_lsn {
            Some(lsn) => Lsn::from_str(lsn).map_err(|error| {
                PostgresError::StreamState(format!("invalid durable source LSN {lsn:?}: {error}"))
            })?,
            None => Lsn::ZERO,
        };

        let replication_config = ReplicationConfig::new(
            source.host.clone(),
            source.user.clone(),
            source.password.clone(),
            source.database.clone(),
            source.slot.clone(),
            source.publication.clone(),
        )
        .with_port(source.port)
        .with_tls(TlsConfig::disabled())
        .with_start_lsn(start_lsn)
        .with_status_interval(Duration::from_secs(1))
        .with_wakeup_interval(Duration::from_secs(5));

        info!(
            connection = %replication_config.display_connection(),
            slot = %source.slot,
            publication = %source.publication,
            "starting logical replication"
        );

        let client = ReplicationClient::connect(replication_config).await?;
        // The client starts a background worker, so cleanup waits until recv()
        // proves that worker acquired the replication stream.
        let pending_events = TransactionBuffer::new_with_deferred_orphan_cleanup(buffer_options)?;

        Ok(Self {
            source,
            client,
            decoder: PgOutputDecoder::default(),
            sequence: 0,
            transaction: None,
            commit_timestamp_ms: None,
            pending_events,
            capture_plan,
        })
    }

    /// Sets the local sequence number that will be assigned to the next event.
    pub fn set_next_sequence(&mut self, sequence: u64) {
        self.sequence = sequence.saturating_sub(1);
    }

    /// Waits for the next committed PostgreSQL transaction.
    pub async fn next_transaction(&mut self) -> Result<Option<CapturedTransaction>, PostgresError> {
        match self.read_transaction(None).await? {
            TransactionRead::Transaction(transaction) => Ok(Some(transaction)),
            TransactionRead::StreamEnded => Ok(None),
            TransactionRead::TimedOut => unreachable!("unbounded transaction read timed out"),
        }
    }

    /// Waits for a committed transaction until an absolute batching deadline.
    pub async fn next_transaction_until(
        &mut self,
        deadline: Instant,
    ) -> Result<TransactionRead, PostgresError> {
        self.read_transaction(Some(deadline)).await
    }

    async fn read_transaction(
        &mut self,
        deadline: Option<Instant>,
    ) -> Result<TransactionRead, PostgresError> {
        loop {
            let received = match deadline {
                Some(deadline) => match timeout_at(deadline, self.client.recv()).await {
                    Ok(result) => result?,
                    Err(_) => return Ok(TransactionRead::TimedOut),
                },
                None => self.client.recv().await?,
            };
            let event = match received {
                Some(event) => event,
                None => return Ok(TransactionRead::StreamEnded),
            };
            let removed_orphans = self.pending_events.cleanup_orphaned_files()?;
            if removed_orphans > 0 {
                info!(
                    source = %self.source.name,
                    removed = removed_orphans,
                    "removed orphaned transaction staging files"
                );
            }

            match event {
                ReplicationEvent::KeepAlive { wal_end, .. } => {
                    debug!(%wal_end, "received replication keepalive");
                }
                ReplicationEvent::Begin {
                    final_lsn,
                    xid,
                    commit_time_micros,
                } => {
                    if self.transaction.is_some() || !self.pending_events.is_empty() {
                        return Err(PostgresError::StreamState(
                            "received transaction begin while another transaction is pending"
                                .to_owned(),
                        ));
                    }

                    self.transaction = Some(TransactionMetadata {
                        transaction_id: Some(xid as u64),
                        begin_lsn: None,
                        commit_lsn: Some(final_lsn.to_string()),
                    });
                    self.pending_events.begin(xid as u64)?;
                    self.commit_timestamp_ms = Some(pg_time_to_unix_ms(commit_time_micros));

                    debug!(
                        %final_lsn,
                        xid,
                        commit_timestamp_ms = pg_time_to_unix_ms(commit_time_micros),
                        "received transaction begin"
                    );
                }
                ReplicationEvent::Commit {
                    end_lsn,
                    commit_time_micros,
                    ..
                } => {
                    debug!(
                        %end_lsn,
                        commit_timestamp_ms = pg_time_to_unix_ms(commit_time_micros),
                        "received transaction commit"
                    );
                    let events = self.pending_events.finish()?;
                    let stats = events.stats();
                    debug!(
                        event_count = stats.event_count,
                        decoded_bytes = stats.decoded_bytes,
                        staged_bytes = stats.staged_bytes,
                        staged = events.is_staged(),
                        "finished decoded source transaction"
                    );
                    self.transaction = None;
                    self.commit_timestamp_ms = None;
                    return Ok(TransactionRead::Transaction(CapturedTransaction {
                        events,
                        ack_lsn: end_lsn,
                    }));
                }
                ReplicationEvent::XLogData {
                    wal_start, data, ..
                } => match self.decoder.decode(&data)? {
                    PgOutputMessage::Relation(relation) => {
                        debug!(
                            relation_id = relation.id,
                            schema = %relation.namespace,
                            table = %relation.name,
                            "decoded relation metadata"
                        );
                    }
                    PgOutputMessage::Insert(row) => {
                        if self
                            .capture_plan
                            .matches_table(&row.relation.namespace, &row.relation.name)
                        {
                            let event = self.row_change(Operation::Insert, row, wal_start);
                            self.pending_events.push(event)?;
                        }
                    }
                    PgOutputMessage::Update(row) => {
                        if self
                            .capture_plan
                            .matches_table(&row.relation.namespace, &row.relation.name)
                        {
                            let event = self.row_change(Operation::Update, row, wal_start);
                            self.pending_events.push(event)?;
                        }
                    }
                    PgOutputMessage::Delete(row) => {
                        if self
                            .capture_plan
                            .matches_table(&row.relation.namespace, &row.relation.name)
                        {
                            let event = self.row_change(Operation::Delete, row, wal_start);
                            self.pending_events.push(event)?;
                        }
                    }
                    PgOutputMessage::Truncate(relations) => {
                        for relation in relations {
                            if self
                                .capture_plan
                                .matches_table(&relation.namespace, &relation.name)
                            {
                                let event = self.truncate_change(relation, wal_start);
                                self.pending_events.push(event)?;
                            }
                        }
                    }
                    PgOutputMessage::Ignored => {}
                },
                ReplicationEvent::Message { prefix, lsn, .. } => {
                    debug!(%prefix, %lsn, "ignored logical decoding message");
                }
                ReplicationEvent::StoppedAt { reached } => {
                    debug!(%reached, "replication stopped at configured LSN");
                    return Ok(TransactionRead::StreamEnded);
                }
            }
        }
    }

    /// Marks a WAL position as applied for the replication client.
    pub fn ack(&self, lsn: Lsn) {
        self.client.update_applied_lsn(lsn);
    }

    /// Gracefully closes the replication connection.
    pub async fn shutdown(&mut self) -> Result<(), PostgresError> {
        self.client.shutdown().await?;
        Ok(())
    }

    /// Converts one decoded row change into a normalized captured event.
    fn row_change(&mut self, operation: Operation, row: RowChange, wal_start: Lsn) -> ChangeEvent {
        self.sequence += 1;

        ChangeEvent {
            sequence: self.sequence,
            event_id: format!(
                "postgres:{}:{}:{}",
                self.source.database, self.source.slot, wal_start
            ),
            source: SourceMetadata {
                database: self.source.database.clone(),
                slot: self.source.slot.clone(),
                lsn: wal_start.to_string(),
            },
            transaction: self.transaction.clone(),
            schema: row.relation.namespace,
            table: row.relation.name,
            operation,
            key: row.key,
            before: row.old,
            after: row.new,
            commit_timestamp_ms: self.commit_timestamp_ms,
        }
    }

    /// Converts one truncated relation into a normalized captured event.
    fn truncate_change(&mut self, relation: Relation, wal_start: Lsn) -> ChangeEvent {
        self.sequence += 1;

        ChangeEvent {
            sequence: self.sequence,
            event_id: format!(
                "postgres:{}:{}:{}:truncate:{}",
                self.source.database, self.source.slot, wal_start, relation.id
            ),
            source: SourceMetadata {
                database: self.source.database.clone(),
                slot: self.source.slot.clone(),
                lsn: wal_start.to_string(),
            },
            transaction: self.transaction.clone(),
            schema: relation.namespace,
            table: relation.name,
            operation: Operation::Truncate,
            key: None,
            before: None,
            after: None,
            commit_timestamp_ms: self.commit_timestamp_ms,
        }
    }
}

impl PostgresError {
    /// Returns true when retrying later may succeed without operator changes.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Connect(error) => error
                .code()
                .is_none_or(|code| is_retryable_sqlstate(code.code())),
            Self::Replication(error) if error.is_transient() => true,
            Self::Replication(PgWireError::Server(message)) => {
                replication_server_sqlstate(message).is_some_and(is_retryable_sqlstate)
            }
            Self::Configuration(_)
            | Self::StreamState(_)
            | Self::Replication(_)
            | Self::Decode(_)
            | Self::TransactionBuffer(_) => false,
        }
    }

    /// Returns true when capture must stop for operator intervention.
    pub fn is_fatal_capture_error(&self) -> bool {
        !self.is_retryable()
    }
}

impl LogicalHeartbeatEmitter {
    /// Opens one ordinary PostgreSQL connection used only to emit heartbeats.
    pub async fn connect(source: &SourceConfig) -> Result<Self, PostgresError> {
        let (client, connection) =
            tokio_postgres::connect(&source.connection_string(), NoTls).await?;
        let connection_task = tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::warn!(%error, "PostgreSQL heartbeat connection stopped");
            }
        });
        Ok(Self {
            client,
            connection_task,
        })
    }

    /// Writes one transactional logical message and returns its WAL position.
    pub async fn emit(&self, prefix: &str, content: &str) -> Result<String, PostgresError> {
        let row = self
            .client
            .query_one(
                "SELECT pg_logical_emit_message(true, $1::text, $2::text)::text",
                &[&prefix, &content],
            )
            .await?;
        Ok(row.get(0))
    }
}

impl Drop for LogicalHeartbeatEmitter {
    fn drop(&mut self) {
        self.connection_task.abort();
    }
}

/// Checks that PostgreSQL accepts a normal connection for the configured source.
pub async fn validate_source_config(config: &SourceConfig) -> Result<(), PostgresError> {
    let (client, connection) = tokio_postgres::connect(&config.connection_string(), NoTls).await?;

    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::error!(%error, "PostgreSQL connection task failed");
        }
    });

    validate_source_client(&client, config).await
}

/// Validates the source and ensures its publication contains every configured table.
pub async fn validate_source_config_with_plan(
    config: &SourceConfig,
    capture_plan: &CapturePlan,
) -> Result<PublicationAlignment, PostgresError> {
    if capture_plan.source_name() != config.name {
        return Err(PostgresError::Configuration(format!(
            "capture plan belongs to source {:?}; expected {:?}",
            capture_plan.source_name(),
            config.name
        )));
    }

    let (client, connection) = tokio_postgres::connect(&config.connection_string(), NoTls).await?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::error!(%error, "PostgreSQL validation connection task failed");
        }
    });

    validate_source_client(&client, config).await?;
    validate_publication_alignment(&client, config, capture_plan).await
}

async fn validate_source_client(
    client: &Client,
    config: &SourceConfig,
) -> Result<(), PostgresError> {
    let row = client
        .query_one(
            r#"
            SELECT
                current_setting('wal_level'),
                current_database(),
                current_user,
                role.rolsuper,
                role.rolreplication
            FROM pg_roles AS role
            WHERE role.rolname = current_user
            "#,
            &[],
        )
        .await?;

    let wal_level: String = row.get(0);
    let database: String = row.get(1);
    let user: String = row.get(2);
    let is_superuser: bool = row.get(3);
    let can_replicate: bool = row.get(4);

    if wal_level != "logical" {
        return Err(PostgresError::Configuration(format!(
            "wal_level is {wal_level:?}; expected \"logical\""
        )));
    }
    if !is_superuser && !can_replicate {
        return Err(PostgresError::Configuration(format!(
            "role {user:?} does not have PostgreSQL REPLICATION privilege"
        )));
    }

    let publication_exists: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)",
            &[&config.publication],
        )
        .await?
        .get(0);
    if !publication_exists {
        return Err(PostgresError::Configuration(format!(
            "publication {:?} does not exist",
            config.publication
        )));
    }

    let slot = client
        .query_opt(
            r#"
            SELECT slot_type, plugin, database
            FROM pg_replication_slots
            WHERE slot_name = $1
            "#,
            &[&config.slot],
        )
        .await?
        .ok_or_else(|| {
            PostgresError::Configuration(format!(
                "replication slot {:?} does not exist",
                config.slot
            ))
        })?;
    let slot_type: String = slot.get(0);
    let plugin: Option<String> = slot.get(1);
    let slot_database: Option<String> = slot.get(2);

    if slot_type != "logical" {
        return Err(PostgresError::Configuration(format!(
            "replication slot {:?} has type {slot_type:?}; expected \"logical\"",
            config.slot
        )));
    }
    if plugin.as_deref() != Some("pgoutput") {
        return Err(PostgresError::Configuration(format!(
            "replication slot {:?} uses plugin {plugin:?}; expected \"pgoutput\"",
            config.slot
        )));
    }
    if slot_database.as_deref() != Some(config.database.as_str()) {
        return Err(PostgresError::Configuration(format!(
            "replication slot {:?} belongs to database {slot_database:?}; expected {:?}",
            config.slot, config.database
        )));
    }

    info!(
        wal_level,
        database,
        user,
        is_superuser,
        can_replicate,
        publication = %config.publication,
        slot = %config.slot,
        "validated PostgreSQL source config"
    );

    Ok(())
}

async fn validate_publication_alignment(
    client: &Client,
    config: &SourceConfig,
    capture_plan: &CapturePlan,
) -> Result<PublicationAlignment, PostgresError> {
    let published_rows = client
        .query(
            r#"
            SELECT schemaname, tablename
            FROM pg_publication_tables
            WHERE pubname = $1
            ORDER BY schemaname, tablename
            "#,
            &[&config.publication],
        )
        .await?;
    let published_tables = published_rows
        .iter()
        .map(|row| {
            let schema: String = row.get(0);
            let table: String = row.get(1);
            format!("{schema}.{table}")
        })
        .collect::<BTreeSet<_>>();

    let database_rows = client
        .query(
            r#"
            SELECT namespace.nspname, relation.relname
            FROM pg_class AS relation
            JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
            WHERE relation.relkind IN ('r', 'p')
              AND relation.relpersistence <> 't'
              AND namespace.nspname <> 'information_schema'
              AND namespace.nspname NOT LIKE 'pg\_%' ESCAPE '\'
            ORDER BY namespace.nspname, relation.relname
            "#,
            &[],
        )
        .await?;
    let database_tables = database_rows
        .iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect::<Vec<_>>();
    let required_tables = capture_plan.required_tables(
        database_tables
            .iter()
            .map(|(schema, table)| (schema.as_str(), table.as_str())),
    );
    let missing_tables = required_tables
        .difference(&published_tables)
        .cloned()
        .collect::<Vec<_>>();
    if !missing_tables.is_empty() {
        return Err(PostgresError::Configuration(format!(
            "publication {:?} is missing configured capture tables: {}",
            config.publication,
            missing_tables.join(", ")
        )));
    }

    let unnecessary_published_tables = published_tables
        .into_iter()
        .filter(|qualified| {
            qualified
                .split_once('.')
                .is_some_and(|(schema, table)| !capture_plan.matches_table(schema, table))
        })
        .collect();

    Ok(PublicationAlignment {
        unnecessary_published_tables,
    })
}

fn pg_time_to_unix_ms(pg_micros: i64) -> i64 {
    POSTGRES_EPOCH_UNIX_MS + (pg_micros / 1_000)
}

fn is_retryable_sqlstate(code: &str) -> bool {
    code.starts_with("08")
        || code.starts_with("53")
        || matches!(code, "55006" | "57P01" | "57P02" | "57P03")
}

fn replication_server_sqlstate(message: &str) -> Option<&str> {
    let sqlstate = message.rsplit_once("(SQLSTATE ")?.1.strip_suffix(')')?;
    (sqlstate.len() == 5).then_some(sqlstate)
}

#[cfg(test)]
mod tests {
    use std::io;

    use pgwire_replication::error::PgWireError;

    use super::{PostgresError, is_retryable_sqlstate, replication_server_sqlstate};

    #[test]
    fn retries_transient_replication_failures() {
        let io_error = PgWireError::from(io::Error::new(
            io::ErrorKind::ConnectionReset,
            "connection reset",
        ));

        assert!(PostgresError::Replication(io_error).is_retryable());
        assert!(
            PostgresError::Replication(PgWireError::Task("worker stopped".to_owned()))
                .is_retryable()
        );
        assert!(
            PostgresError::Replication(PgWireError::Server(
                "replication slot is active (SQLSTATE 55006)".to_owned()
            ))
            .is_retryable()
        );
    }

    #[test]
    fn stops_for_configuration_and_protocol_failures() {
        assert!(!PostgresError::Configuration("publication is missing".to_owned()).is_retryable());
        assert!(
            !PostgresError::Replication(PgWireError::Auth("bad password".to_owned()))
                .is_retryable()
        );
        assert!(
            !PostgresError::Replication(PgWireError::Protocol("bad frame".to_owned()))
                .is_retryable()
        );
        assert!(
            !PostgresError::Replication(PgWireError::Server(
                "publication does not exist (SQLSTATE 42704)".to_owned()
            ))
            .is_retryable()
        );
    }

    #[test]
    fn classifies_retryable_postgres_sqlstates() {
        assert!(is_retryable_sqlstate("08006"));
        assert!(is_retryable_sqlstate("53300"));
        assert!(is_retryable_sqlstate("57P03"));
        assert!(is_retryable_sqlstate("55006"));
        assert!(!is_retryable_sqlstate("28P01"));
        assert!(!is_retryable_sqlstate("3D000"));
        assert!(!is_retryable_sqlstate("42704"));
    }

    #[test]
    fn extracts_pgwire_server_sqlstate() {
        assert_eq!(
            replication_server_sqlstate("object in use (SQLSTATE 55006)"),
            Some("55006")
        );
        assert_eq!(replication_server_sqlstate("missing code"), None);
        assert_eq!(
            replication_server_sqlstate("malformed (SQLSTATE 123)"),
            None
        );
    }
}
