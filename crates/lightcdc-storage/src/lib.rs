/// redb-backed local event log storage.
pub mod log;

pub use log::{
    ConsumerOffset, LogOpenOptions, PersistTransactionOutcome, RedbEventStore, SourceOffset,
    StorageError, StoreStats,
};
