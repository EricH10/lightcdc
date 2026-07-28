/// redb-backed local event log storage.
pub mod log;
/// Bounded in-memory and file-backed source transaction buffering.
pub mod transaction;

pub use log::{
    ConsumerOffset, LogOpenOptions, PersistTransactionOutcome, RedbEventStore, SourceOffset,
    StorageError, StoreStats,
};
pub use transaction::{
    TransactionBuffer, TransactionBufferError, TransactionBufferOptions, TransactionEventIter,
    TransactionEvents, TransactionStats,
};
