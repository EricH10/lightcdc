//! Provides durable event-log storage and bounded transaction staging.

/// redb-backed local event log storage.
pub mod log;
mod segments;
/// Bounded in-memory and file-backed source transaction buffering.
pub mod transaction;

pub use log::{
    ConsumerOffset, LogOpenOptions, PersistTransactionOutcome, RedbEventStore, RetentionOutcome,
    RetentionPolicy, SegmentOptions, SourceOffset, SourceTransaction, StorageError, StoreStats,
};
pub use transaction::{
    TransactionBuffer, TransactionBufferError, TransactionBufferOptions, TransactionEventIter,
    TransactionEvents, TransactionStats,
};
