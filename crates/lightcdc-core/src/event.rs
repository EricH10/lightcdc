//! Defines the canonical CDC event model shared across connectors and storage.

use serde::{Deserialize, Serialize};

/// Represents one captured database change in the local event log.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ChangeEvent {
    /// The local monotonically increasing event sequence.
    pub sequence: u64,
    /// A source-derived identifier used to detect duplicate replay.
    pub event_id: String,
    /// The PostgreSQL source metadata for this event.
    pub source: SourceMetadata,
    /// The transaction metadata available for this event.
    pub transaction: Option<TransactionMetadata>,
    /// The database schema that changed.
    pub schema: String,
    /// The table that changed.
    pub table: String,
    /// The kind of row-level change.
    pub operation: Operation,
    /// The key tuple for updates or deletes when PostgreSQL sends it.
    pub key: Option<Vec<u8>>,
    /// The previous row image when PostgreSQL sends it.
    pub before: Option<Vec<u8>>,
    /// The new row image when PostgreSQL sends it.
    pub after: Option<Vec<u8>>,
    /// The commit timestamp in Unix milliseconds when available.
    pub commit_timestamp_ms: Option<i64>,
}

/// Identifies where a change came from in PostgreSQL.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct SourceMetadata {
    /// The source database name.
    pub database: String,
    /// The logical replication slot name.
    pub slot: String,
    /// The WAL location associated with this event.
    pub lsn: String,
}

/// Carries transaction-level metadata for a captured event.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct TransactionMetadata {
    /// The PostgreSQL transaction id when available.
    pub transaction_id: Option<u64>,
    /// The transaction begin LSN when available.
    pub begin_lsn: Option<String>,
    /// The transaction commit LSN when available.
    pub commit_lsn: Option<String>,
}

/// Describes the database operation represented by an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    /// A row was inserted.
    Insert,
    /// A row was updated.
    Update,
    /// A row was deleted.
    Delete,
    /// A table was truncated.
    Truncate,
}
