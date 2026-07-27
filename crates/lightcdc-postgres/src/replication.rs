use std::str::FromStr;
use std::time::Duration;

use lightcdc_core::{ChangeEvent, Operation, SourceConfig, SourceMetadata, TransactionMetadata};
use pgwire_replication::{
    client::{ReplicationClient, ReplicationEvent},
    config::{ReplicationConfig, TlsConfig},
    lsn::Lsn,
};
use thiserror::Error;
use tokio_postgres::NoTls;
use tracing::{debug, info};

use crate::decoder::{DecodeError, PgOutputDecoder, PgOutputMessage, RowChange};

const POSTGRES_EPOCH_UNIX_MS: i64 = 946_684_800_000;

/// Represents failures while connecting to or decoding PostgreSQL replication.
#[derive(Debug, Error)]
pub enum PostgresError {
    #[error("failed to connect to PostgreSQL: {0}")]
    Connect(#[from] tokio_postgres::Error),

    #[error("replication protocol error: {0}")]
    Replication(String),

    #[error("pgoutput decode error: {0}")]
    Decode(#[from] DecodeError),
}

/// Reads PostgreSQL logical replication messages and emits committed transactions.
pub struct ReplicationReader {
    source: SourceConfig,
    client: ReplicationClient,
    decoder: PgOutputDecoder,
    sequence: u64,
    transaction: Option<TransactionMetadata>,
    commit_timestamp_ms: Option<i64>,
    pending_events: Vec<ChangeEvent>,
}

/// Bundles the events from one committed transaction with its checkpoint.
#[derive(Debug, Clone)]
pub struct CapturedTransaction {
    /// The normalized CDC events to persist atomically.
    pub events: Vec<ChangeEvent>,
    /// The commit WAL position to acknowledge after durable persistence.
    pub ack_lsn: Lsn,
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
        let start_lsn = match start_lsn {
            Some(lsn) => Lsn::from_str(lsn).map_err(replication_error)?,
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

        let client = ReplicationClient::connect(replication_config)
            .await
            .map_err(replication_error)?;

        Ok(Self {
            source,
            client,
            decoder: PgOutputDecoder::default(),
            sequence: 0,
            transaction: None,
            commit_timestamp_ms: None,
            pending_events: Vec::new(),
        })
    }

    /// Sets the local sequence number that will be assigned to the next event.
    pub fn set_next_sequence(&mut self, sequence: u64) {
        self.sequence = sequence.saturating_sub(1);
    }

    /// Waits for the next committed PostgreSQL transaction.
    pub async fn next_transaction(&mut self) -> Result<Option<CapturedTransaction>, PostgresError> {
        loop {
            let event = match self.client.recv().await.map_err(replication_error)? {
                Some(event) => event,
                None => return Ok(None),
            };

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
                        return Err(PostgresError::Replication(
                            "received transaction begin while another transaction is pending"
                                .to_owned(),
                        ));
                    }

                    self.transaction = Some(TransactionMetadata {
                        transaction_id: Some(xid as u64),
                        begin_lsn: None,
                        commit_lsn: Some(final_lsn.to_string()),
                    });
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
                    self.transaction = None;
                    self.commit_timestamp_ms = None;
                    return Ok(Some(CapturedTransaction {
                        events: std::mem::take(&mut self.pending_events),
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
                        let event = self.row_change(Operation::Insert, row, wal_start);
                        self.pending_events.push(event);
                    }
                    PgOutputMessage::Update(row) => {
                        let event = self.row_change(Operation::Update, row, wal_start);
                        self.pending_events.push(event);
                    }
                    PgOutputMessage::Delete(row) => {
                        let event = self.row_change(Operation::Delete, row, wal_start);
                        self.pending_events.push(event);
                    }
                    PgOutputMessage::Truncate(relation_ids) => {
                        debug!(?relation_ids, "decoded truncate message");
                    }
                    PgOutputMessage::Ignored => {}
                },
                ReplicationEvent::Message { prefix, lsn, .. } => {
                    debug!(%prefix, %lsn, "ignored logical decoding message");
                }
                ReplicationEvent::StoppedAt { reached } => {
                    debug!(%reached, "replication stopped at configured LSN");
                    return Ok(None);
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
        self.client.shutdown().await.map_err(replication_error)
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
}

/// Checks that PostgreSQL accepts a normal connection for the configured source.
pub async fn validate_source_config(config: &SourceConfig) -> Result<(), PostgresError> {
    let (client, connection) = tokio_postgres::connect(&config.connection_string(), NoTls).await?;

    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::error!(%error, "PostgreSQL connection task failed");
        }
    });

    let row = client
        .query_one(
            "SELECT current_setting('wal_level'), current_database(), current_user",
            &[],
        )
        .await?;

    let wal_level: String = row.get(0);
    let database: String = row.get(1);
    let user: String = row.get(2);

    info!(
        wal_level,
        database,
        user,
        publication = %config.publication,
        slot = %config.slot,
        "validated PostgreSQL source config"
    );

    Ok(())
}

fn pg_time_to_unix_ms(pg_micros: i64) -> i64 {
    POSTGRES_EPOCH_UNIX_MS + (pg_micros / 1_000)
}

fn replication_error(error: impl ToString) -> PostgresError {
    PostgresError::Replication(error.to_string())
}
