//! Turns PostgreSQL logical replication messages into committed source transactions.

use std::collections::BTreeSet;
use std::fs::File;
use std::str::FromStr;
use std::time::Duration;

use lightcdc_core::{
    CapturePlan, ChangeEvent, Operation, PostgresTlsMode, SourceConfig, SourceMetadata,
    TransactionMetadata,
};
use lightcdc_storage::{
    SourceIdentity, TransactionBuffer, TransactionBufferError, TransactionBufferOptions,
    TransactionEvents,
};
use pgwire_replication::{
    client::{ReplicationClient, ReplicationEvent},
    config::{ReplicationConfig, TlsConfig},
    error::PgWireError,
    lsn::Lsn,
};
use rustls::pki_types::{CertificateDer, pem::PemObject};
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout_at};
use tokio_postgres::{Client, NoTls};
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::{debug, info};

use crate::decoder::{DecodeError, PgOutputDecoder, PgOutputMessage, Relation, RowChange};

const POSTGRES_EPOCH_UNIX_MS: i64 = 946_684_800_000;
/// Reserved logical-message prefix used only for safe idle checkpoints.
pub const LIGHTCDC_HEARTBEAT_PREFIX: &str = "lightcdc.heartbeat";

/// Represents failures while connecting to or decoding PostgreSQL replication.
#[derive(Debug, Error)]
pub enum PostgresError {
    #[error("failed to connect to PostgreSQL: {0}")]
    Connect(#[from] tokio_postgres::Error),

    #[error("replication protocol error: {0}")]
    Replication(#[from] PgWireError),

    #[error("invalid PostgreSQL source configuration: {0}")]
    Configuration(String),

    #[error(
        "replication slot confirmed_flush_lsn {confirmed_flush_lsn} is ahead of the local durable checkpoint {local_lsn}; PostgreSQL can no longer replay the missing range"
    )]
    ResumeLsnGap {
        local_lsn: String,
        confirmed_flush_lsn: String,
    },

    #[error("invalid replication stream state: {0}")]
    StreamState(String),

    #[error("pgoutput decode error: {0}")]
    Decode(#[from] DecodeError),

    #[error("transaction buffering error: {0}")]
    TransactionBuffer(#[from] TransactionBufferError),

    #[error("unsupported PostgreSQL feature: {0}")]
    UnsupportedFeature(String),
}

/// Reads PostgreSQL logical replication messages and emits committed transactions.
pub struct ReplicationReader {
    /// Connection identity copied into emitted event metadata.
    source: SourceConfig,
    /// pgwire client that owns the replication protocol worker.
    client: ReplicationClient,
    /// Stateful decoder that caches relation metadata by relation id.
    decoder: PgOutputDecoder,
    /// Last locally assigned event sequence.
    sequence: u64,
    /// Latest server WAL end observed on keepalive or data frames.
    latest_wal_end: Lsn,
    /// Metadata for the source transaction currently being decoded.
    transaction: Option<TransactionMetadata>,
    /// Commit timestamp announced for the current source transaction.
    commit_timestamp_ms: Option<i64>,
    /// Events held until PostgreSQL sends the matching commit.
    pending_events: TransactionBuffer,
    /// Union of configured tables allowed to become local events.
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
    /// Published tables that no configured stream currently requires.
    pub unnecessary_published_tables: Vec<String>,
}

/// Describes the validated physical source and its durable slot positions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceValidation {
    /// Physical cluster and database identity that local state must remain bound to.
    pub identity: SourceIdentity,
    /// WAL position PostgreSQL believes the client has durably acknowledged.
    pub confirmed_flush_lsn: Option<String>,
    /// Oldest WAL position still retained for this slot.
    pub restart_lsn: Option<String>,
    /// Current source WAL insertion position observed during validation.
    pub current_wal_lsn: u64,
    /// Alignment between configured stream tables and the publication.
    pub publication: PublicationAlignment,
}

struct ValidatedSource {
    identity: SourceIdentity,
    confirmed_flush_lsn: Option<String>,
    restart_lsn: Option<String>,
    current_wal_lsn: String,
    publish_via_partition_root: bool,
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
    /// A complete committed source transaction ready for durable storage.
    Transaction(CapturedTransaction),
    /// No commit arrived before the caller's batching deadline.
    TimedOut,
    /// The replication worker ended without another transaction.
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

        let tls = match source.tls_mode {
            PostgresTlsMode::Disable => TlsConfig::disabled(),
            PostgresTlsMode::VerifyFull => {
                TlsConfig::verify_full(source.tls_ca_file.as_deref().map(std::path::PathBuf::from))
            }
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
        .with_tls(tls)
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
            latest_wal_end: start_lsn,
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

    /// Returns the latest server WAL end observed by this replication session.
    pub fn latest_wal_end(&self) -> Lsn {
        self.latest_wal_end
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

    /// Drives the replication protocol until a commit, deadline, or stream end.
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
                    self.latest_wal_end = self.latest_wal_end.max(wal_end);
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
                    wal_start,
                    wal_end,
                    data,
                    ..
                } => {
                    self.latest_wal_end = self.latest_wal_end.max(wal_end);
                    match self.decoder.decode(&data)? {
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
                    }
                }
                ReplicationEvent::Message { prefix, lsn, .. } => {
                    if prefix != LIGHTCDC_HEARTBEAT_PREFIX {
                        return Err(PostgresError::UnsupportedFeature(format!(
                            "logical message prefix {prefix:?} at {lsn}; only the reserved \
                             {LIGHTCDC_HEARTBEAT_PREFIX:?} heartbeat is accepted"
                        )));
                    }
                    debug!(%prefix, %lsn, "received LightCDC logical heartbeat");
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
            | Self::ResumeLsnGap { .. }
            | Self::StreamState(_)
            | Self::Replication(_)
            | Self::Decode(_)
            | Self::TransactionBuffer(_)
            | Self::UnsupportedFeature(_) => false,
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
        let (client, connection_task) = connect_sql(source, "heartbeat").await?;
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
    let (client, _connection_task) = connect_sql(config, "validation").await?;

    validate_source_client(&client, config).await.map(|_| ())
}

/// Validates the source and ensures its publication contains every configured table.
pub async fn validate_source_config_with_plan(
    config: &SourceConfig,
    capture_plan: &CapturePlan,
) -> Result<SourceValidation, PostgresError> {
    if capture_plan.source_name() != config.name {
        return Err(PostgresError::Configuration(format!(
            "capture plan belongs to source {:?}; expected {:?}",
            capture_plan.source_name(),
            config.name
        )));
    }

    let (client, _connection_task) = connect_sql(config, "validation").await?;

    let source = validate_source_client(&client, config).await?;
    let publication = validate_publication_alignment(
        &client,
        config,
        capture_plan,
        source.publish_via_partition_root,
    )
    .await?;
    Ok(SourceValidation {
        identity: source.identity,
        confirmed_flush_lsn: source.confirmed_flush_lsn,
        restart_lsn: source.restart_lsn,
        current_wal_lsn: Lsn::from_str(&source.current_wal_lsn)
            .map_err(|error| {
                PostgresError::StreamState(format!(
                    "PostgreSQL returned invalid current WAL LSN {:?}: {error}",
                    source.current_wal_lsn
                ))
            })?
            .as_u64(),
        publication,
    })
}

/// Opens ordinary SQL connections with the same TLS policy as replication.
async fn connect_sql(
    source: &SourceConfig,
    purpose: &'static str,
) -> Result<(Client, JoinHandle<()>), PostgresError> {
    let config = sql_config(source);
    match source.tls_mode {
        PostgresTlsMode::Disable => {
            let (client, connection) = config.connect(NoTls).await?;
            let task = tokio::spawn(async move {
                if let Err(error) = connection.await {
                    tracing::warn!(%error, %purpose, "PostgreSQL connection stopped");
                }
            });
            Ok((client, task))
        }
        PostgresTlsMode::VerifyFull => {
            let _ = rustls::crypto::ring::default_provider().install_default();
            let connector = sql_tls_connector(source)?;
            let (client, connection) = config.connect(connector).await?;
            let task = tokio::spawn(async move {
                if let Err(error) = connection.await {
                    tracing::warn!(%error, %purpose, "PostgreSQL TLS connection stopped");
                }
            });
            Ok((client, task))
        }
    }
}

/// Builds SQL connection settings without reparsing operator-controlled values.
fn sql_config(source: &SourceConfig) -> tokio_postgres::Config {
    let mut config = tokio_postgres::Config::new();
    config
        .host(&source.host)
        .port(source.port)
        .dbname(&source.database)
        .user(&source.user)
        .password(&source.password);
    config
}

fn sql_tls_connector(source: &SourceConfig) -> Result<MakeRustlsConnect, PostgresError> {
    if let Some(path) = &source.tls_ca_file {
        let file = File::open(path).map_err(|error| {
            PostgresError::Configuration(format!(
                "failed to open PostgreSQL CA file {path:?}: {error}"
            ))
        })?;
        let certificates = CertificateDer::pem_reader_iter(file)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| {
                PostgresError::Configuration(format!(
                    "failed to parse PostgreSQL CA file {path:?}: {error}"
                ))
            })?;
        if certificates.is_empty() {
            return Err(PostgresError::Configuration(format!(
                "PostgreSQL CA file {path:?} contains no certificates"
            )));
        }
        let mut roots = rustls::RootCertStore::empty();
        for certificate in certificates {
            roots.add(certificate).map_err(|error| {
                PostgresError::Configuration(format!(
                    "invalid certificate in PostgreSQL CA file {path:?}: {error}"
                ))
            })?;
        }
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        return Ok(MakeRustlsConnect::new(config));
    }

    let (connector, errors) = MakeRustlsConnect::with_native_certs().map_err(|errors| {
        PostgresError::Configuration(format!(
            "could not load platform CA certificates: {} errors",
            errors.len()
        ))
    })?;
    if !errors.is_empty() {
        tracing::warn!(
            errors = errors.len(),
            "some platform CA certificates could not be loaded"
        );
    }
    Ok(connector)
}

/// Checks server settings, role privileges, publication presence, and slot identity.
async fn validate_source_client(
    client: &Client,
    config: &SourceConfig,
) -> Result<ValidatedSource, PostgresError> {
    let row = client
        .query_one(
            r#"
            SELECT
                current_setting('wal_level'),
                current_database(),
                current_user,
                role.rolsuper,
                role.rolreplication,
                pg_current_wal_lsn()::text
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
    let current_wal_lsn: String = row.get(5);

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

    let publication = client
        .query_opt(
            r#"
            SELECT pubinsert, pubupdate, pubdelete, pubtruncate, pubviaroot
            FROM pg_publication
            WHERE pubname = $1
            "#,
            &[&config.publication],
        )
        .await?
        .ok_or_else(|| {
            PostgresError::Configuration(format!(
                "publication {:?} does not exist",
                config.publication
            ))
        })?;
    let published_operations = [
        ("insert", publication.get::<_, bool>(0)),
        ("update", publication.get::<_, bool>(1)),
        ("delete", publication.get::<_, bool>(2)),
        ("truncate", publication.get::<_, bool>(3)),
    ];
    let missing_operations = published_operations
        .into_iter()
        .filter_map(|(name, enabled)| (!enabled).then_some(name))
        .collect::<Vec<_>>();
    if !missing_operations.is_empty() {
        return Err(PostgresError::Configuration(format!(
            "publication {:?} must publish insert, update, delete, and truncate; missing {}",
            config.publication,
            missing_operations.join(", ")
        )));
    }

    let slot = client
        .query_opt(
            r#"
            SELECT
                slot_type,
                plugin,
                database,
                confirmed_flush_lsn::text,
                restart_lsn::text,
                two_phase
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
    let confirmed_flush_lsn: Option<String> = slot.get(3);
    let restart_lsn: Option<String> = slot.get(4);
    let two_phase: bool = slot.get(5);

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
    if two_phase {
        return Err(PostgresError::Configuration(format!(
            "replication slot {:?} has two_phase enabled; prepared transactions are unsupported",
            config.slot
        )));
    }

    let identity = client
        .query_one(
            r#"
            SELECT
                control.system_identifier::text,
                database.oid::text
            FROM pg_control_system() AS control
            JOIN pg_database AS database ON database.datname = current_database()
            "#,
            &[],
        )
        .await?;
    let identity = SourceIdentity {
        system_identifier: identity.get(0),
        database_oid: identity.get(1),
    };

    info!(
        wal_level,
        database,
        user,
        is_superuser,
        can_replicate,
        publication_via_partition_root = publication.get::<_, bool>(4),
        publication = %config.publication,
        slot = %config.slot,
        system_identifier = %identity.system_identifier,
        database_oid = %identity.database_oid,
        confirmed_flush_lsn = ?confirmed_flush_lsn,
        restart_lsn = ?restart_lsn,
        current_wal_lsn,
        "validated PostgreSQL source config"
    );

    Ok(ValidatedSource {
        identity,
        confirmed_flush_lsn,
        restart_lsn,
        current_wal_lsn,
        publish_via_partition_root: publication.get(4),
    })
}

/// Rejects a local checkpoint whose missing WAL has already been acknowledged away.
pub fn validate_resume_lsn(
    local_lsn: Option<&str>,
    confirmed_flush_lsn: Option<&str>,
) -> Result<(), PostgresError> {
    let Some(local_lsn) = local_lsn else {
        return Ok(());
    };
    let Some(confirmed_flush_lsn) = confirmed_flush_lsn else {
        return Ok(());
    };
    let local = Lsn::from_str(local_lsn).map_err(|error| {
        PostgresError::StreamState(format!("invalid durable source LSN {local_lsn:?}: {error}"))
    })?;
    let confirmed = Lsn::from_str(confirmed_flush_lsn).map_err(|error| {
        PostgresError::StreamState(format!(
            "invalid slot confirmed_flush_lsn {confirmed_flush_lsn:?}: {error}"
        ))
    })?;
    if confirmed > local {
        return Err(PostgresError::ResumeLsnGap {
            local_lsn: local_lsn.to_owned(),
            confirmed_flush_lsn: confirmed_flush_lsn.to_owned(),
        });
    }
    Ok(())
}

/// Compares configured table selection with the operator-managed publication.
async fn validate_publication_alignment(
    client: &Client,
    config: &SourceConfig,
    capture_plan: &CapturePlan,
    publish_via_partition_root: bool,
) -> Result<PublicationAlignment, PostgresError> {
    let published_rows = client
        .query(
            r#"
            SELECT
                tables.schemaname,
                tables.tablename,
                relation.prattrs IS NOT NULL AS has_column_filter,
                relation.prqual IS NOT NULL AS has_row_filter
            FROM pg_publication_tables AS tables
            JOIN pg_publication AS publication ON publication.pubname = tables.pubname
            JOIN pg_namespace AS namespace ON namespace.nspname = tables.schemaname
            JOIN pg_class AS class
              ON class.relnamespace = namespace.oid
             AND class.relname = tables.tablename
            LEFT JOIN pg_publication_rel AS relation
              ON relation.prpubid = publication.oid
             AND relation.prrelid = class.oid
            WHERE tables.pubname = $1
            ORDER BY tables.schemaname, tables.tablename
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
    let restricted_tables = published_rows
        .iter()
        .filter_map(|row| {
            let has_column_filter: bool = row.get(2);
            let has_row_filter: bool = row.get(3);
            (has_column_filter || has_row_filter).then(|| {
                let schema: String = row.get(0);
                let table: String = row.get(1);
                format!("{schema}.{table}")
            })
        })
        .collect::<BTreeSet<_>>();

    let database_rows = client
        .query(
            r#"
            SELECT
                namespace.nspname,
                relation.relname,
                relation.relkind::text,
                relation.relispartition,
                relation.relreplident::text,
                relation.relreplident = 'f'
                  OR EXISTS (
                    SELECT 1
                    FROM pg_index AS identity_index
                    WHERE identity_index.indrelid = relation.oid
                      AND identity_index.indisvalid
                      AND (
                        (relation.relreplident = 'd' AND identity_index.indisprimary)
                        OR (relation.relreplident = 'i' AND identity_index.indisreplident)
                      )
                  ) AS has_usable_replica_identity
                , EXISTS (
                    SELECT 1
                    FROM pg_attribute AS generated_column
                    WHERE generated_column.attrelid = relation.oid
                      AND generated_column.attnum > 0
                      AND NOT generated_column.attisdropped
                      AND generated_column.attgenerated <> ''
                  ) AS has_generated_columns
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
        .map(|row| {
            (
                row.get::<_, String>(0),
                row.get::<_, String>(1),
                row.get::<_, String>(2),
                row.get::<_, bool>(3),
                row.get::<_, String>(4),
                row.get::<_, bool>(5),
                row.get::<_, bool>(6),
            )
        })
        .collect::<Vec<_>>();
    let required_tables = capture_plan.required_tables(
        database_tables
            .iter()
            .map(|(schema, table, ..)| (schema.as_str(), table.as_str())),
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
    let restricted_required = required_tables
        .intersection(&restricted_tables)
        .cloned()
        .collect::<Vec<_>>();
    if !restricted_required.is_empty() {
        return Err(PostgresError::Configuration(format!(
            "publication {:?} applies a row or column filter to configured capture tables: {}",
            config.publication,
            restricted_required.join(", ")
        )));
    }
    for (
        schema,
        table,
        relation_kind,
        is_partition,
        replica_identity,
        usable_identity,
        has_generated_columns,
    ) in &database_tables
    {
        let qualified = format!("{schema}.{table}");
        if !required_tables.contains(&qualified) {
            continue;
        }
        if relation_kind == "p" && !publish_via_partition_root {
            return Err(PostgresError::Configuration(format!(
                "configured partitioned table {qualified} requires publication {:?} WITH (publish_via_partition_root = true)",
                config.publication
            )));
        }
        if *is_partition && publish_via_partition_root {
            return Err(PostgresError::Configuration(format!(
                "configured leaf partition {qualified} cannot be captured by name while publication {:?} routes changes through partition roots",
                config.publication
            )));
        }
        if !usable_identity {
            return Err(PostgresError::Configuration(format!(
                "configured table {qualified} has replica identity {replica_identity:?} without a usable identity; configure a primary key, REPLICA IDENTITY USING INDEX, or REPLICA IDENTITY FULL"
            )));
        }
        if *has_generated_columns {
            return Err(PostgresError::Configuration(format!(
                "configured table {qualified} contains generated columns, which PostgreSQL 17 pgoutput does not publish"
            )));
        }
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

/// Converts PostgreSQL's 2000-based microsecond timestamp into Unix milliseconds.
fn pg_time_to_unix_ms(pg_micros: i64) -> i64 {
    POSTGRES_EPOCH_UNIX_MS + (pg_micros / 1_000)
}

/// Classifies SQLSTATE families that can recover without configuration changes.
fn is_retryable_sqlstate(code: &str) -> bool {
    code.starts_with("08")
        || code.starts_with("53")
        || matches!(code, "55006" | "57P01" | "57P02" | "57P03")
}

/// Extracts a SQLSTATE appended to a pgwire replication server error.
fn replication_server_sqlstate(message: &str) -> Option<&str> {
    let sqlstate = message.rsplit_once("(SQLSTATE ")?.1.strip_suffix(')')?;
    (sqlstate.len() == 5).then_some(sqlstate)
}

#[cfg(test)]
mod tests {
    use std::io;

    use lightcdc_core::{PostgresTlsMode, SourceConfig};
    use pgwire_replication::error::PgWireError;
    use tempfile::TempDir;

    use super::{
        PostgresError, is_retryable_sqlstate, replication_server_sqlstate, sql_config,
        sql_tls_connector, validate_resume_lsn,
    };

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
    fn sql_config_preserves_operator_values_without_reparsing() {
        let source = SourceConfig {
            name: "default".to_owned(),
            host: "postgres.internal".to_owned(),
            port: 5432,
            database: "light cdc".to_owned(),
            user: "capture user".to_owned(),
            password: "secret host=attacker password='other'".to_owned(),
            password_env: None,
            password_file: None,
            tls_mode: PostgresTlsMode::VerifyFull,
            tls_ca_file: None,
            publication: "lightcdc_publication".to_owned(),
            slot: "lightcdc_slot".to_owned(),
        };

        let config = sql_config(&source);

        assert_eq!(config.get_user(), Some(source.user.as_str()));
        assert_eq!(config.get_password(), Some(source.password.as_bytes()));
        assert_eq!(config.get_dbname(), Some(source.database.as_str()));
        assert_eq!(config.get_ports(), [source.port]);
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

    #[test]
    fn resume_validation_allows_replay_and_rejects_acknowledged_gap() {
        validate_resume_lsn(Some("0/20"), Some("0/10")).expect("slot can replay local LSN");
        validate_resume_lsn(Some("0/20"), Some("0/20")).expect("positions match");
        validate_resume_lsn(None, Some("0/20")).expect("fresh local store adopts slot position");

        let error = validate_resume_lsn(Some("0/10"), Some("0/20"))
            .expect_err("slot acknowledged beyond local durability");
        assert!(matches!(error, PostgresError::ResumeLsnGap { .. }));
        assert!(error.to_string().contains("can no longer replay"));
    }

    #[test]
    fn rejects_a_custom_ca_file_without_certificates() {
        let temp = TempDir::new().expect("temp dir");
        let ca_path = temp.path().join("empty-ca.pem");
        std::fs::write(&ca_path, "# no certificates\n").expect("write CA fixture");
        let source = SourceConfig {
            name: "default".to_owned(),
            host: "postgres.internal".to_owned(),
            port: 5432,
            database: "lightcdc".to_owned(),
            user: "lightcdc".to_owned(),
            password: "secret".to_owned(),
            password_env: None,
            password_file: None,
            tls_mode: PostgresTlsMode::VerifyFull,
            tls_ca_file: Some(ca_path.display().to_string()),
            publication: "lightcdc_publication".to_owned(),
            slot: "lightcdc_slot".to_owned(),
        };

        let error = match sql_tls_connector(&source) {
            Ok(_) => panic!("empty CA must fail"),
            Err(error) => error,
        };

        assert!(matches!(error, PostgresError::Configuration(_)));
        assert!(error.to_string().contains("contains no certificates"));
    }
}
