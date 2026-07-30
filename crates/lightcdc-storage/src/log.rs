//! Implements the durable redb event log, checkpoints, deduplication, and retention.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use lightcdc_core::ChangeEvent;
use thiserror::Error;

use crate::{TransactionBufferError, TransactionEvents, TransactionStats, segments::SegmentStore};

/// Describes where the redb event log should be opened.
#[derive(Debug, Clone)]
pub struct LogOpenOptions {
    /// The directory that contains the control database and event segments.
    pub data_dir: PathBuf,
    /// The control database filename inside the data directory.
    pub database_file: String,
}

/// Summarizes the rows stored in each LightCDC redb table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreStats {
    /// Retained event payload rows.
    pub event_count: u64,
    /// Deduplication identifiers retained with event segments.
    pub event_id_count: u64,
    /// Durable PostgreSQL source checkpoints.
    pub source_offset_count: u64,
    /// Durable stream consumer checkpoints.
    pub consumer_offset_count: u64,
    /// Event segment files, including the active segment.
    pub segment_count: u64,
    /// Immutable segment files eligible for whole-file retention.
    pub sealed_segment_count: u64,
}

/// Controls when the one active event file is sealed and replaced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentOptions {
    /// Preferred maximum events in one segment.
    pub max_events: u64,
    /// Preferred maximum serialized event bytes in one segment.
    pub max_bytes: u64,
    /// Maximum time a non-empty segment remains active.
    pub max_age: Duration,
    /// Number of sealed redb handles retained for replay.
    pub sealed_cache_capacity: usize,
    /// redb cache limit for the active segment.
    pub active_cache_bytes: usize,
    /// redb cache limit for each cached sealed segment.
    pub sealed_cache_bytes: usize,
}

/// Describes one persisted PostgreSQL source checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceOffset {
    /// Configured source identity.
    pub source_name: String,
    /// Last durably persisted PostgreSQL commit LSN.
    pub lsn: String,
}

/// Describes one persisted stream consumer checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerOffset {
    /// Stream whose filtered event sequence is consumed.
    pub stream_name: String,
    /// Downstream-provided durable consumer identity.
    pub consumer_name: String,
    /// Last sequence acknowledged or explicitly selected by seek.
    pub sequence: u64,
}

/// Configures hard event-log retention limits; either limit may delete an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionPolicy {
    /// Maximum retained event payload count.
    pub max_events: Option<u64>,
    /// Maximum retained event payload age.
    pub max_age: Option<Duration>,
    /// Target maximum events retired per sweep; one whole segment may exceed it.
    pub delete_batch_size: usize,
}

impl RetentionPolicy {
    /// Returns true when at least one retention boundary is configured.
    pub fn is_enabled(self) -> bool {
        self.max_events.is_some() || self.max_age.is_some()
    }
}

/// Summarizes one event-log retention sweep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionOutcome {
    /// Event payload rows removed by the sweep.
    pub deleted_events: u64,
    /// Deduplication identifiers old enough to remove safely.
    pub deleted_event_ids: u64,
    /// Oldest sequence whose payload remains after the sweep.
    pub first_retained_sequence: Option<u64>,
    /// Highest sequence ever assigned, which retention never rewinds.
    pub high_watermark: Option<u64>,
}

/// Describes whether a committed source transaction was newly stored or replayed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistTransactionOutcome {
    /// The source transaction and checkpoint were newly committed.
    Persisted,
    /// Every event id was already durable, while the checkpoint was refreshed.
    AlreadyPersisted,
}

impl Default for LogOpenOptions {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data"),
            database_file: String::from("lightcdc.redb"),
        }
    }
}

impl Default for SegmentOptions {
    fn default() -> Self {
        Self {
            max_events: 1_000_000,
            max_bytes: 256 * 1024 * 1024,
            max_age: Duration::from_secs(15 * 60),
            sealed_cache_capacity: 8,
            active_cache_bytes: 256 * 1024 * 1024,
            sealed_cache_bytes: 16 * 1024 * 1024,
        }
    }
}

impl SegmentOptions {
    pub(crate) fn validate(self) -> Result<(), StorageError> {
        if self.max_events == 0 {
            return Err(StorageError::InvalidSegmentOptions(
                "max_events must be greater than zero".to_owned(),
            ));
        }
        if self.max_bytes == 0 {
            return Err(StorageError::InvalidSegmentOptions(
                "max_bytes must be greater than zero".to_owned(),
            ));
        }
        if self.max_age.is_zero() {
            return Err(StorageError::InvalidSegmentOptions(
                "max_age must be greater than zero".to_owned(),
            ));
        }
        if self.sealed_cache_capacity == 0 {
            return Err(StorageError::InvalidSegmentOptions(
                "sealed_cache_capacity must be greater than zero".to_owned(),
            ));
        }
        if self.active_cache_bytes == 0 || self.sealed_cache_bytes == 0 {
            return Err(StorageError::InvalidSegmentOptions(
                "redb cache limits must be greater than zero".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Presents one ordered event log backed by a control redb and sequence segments.
#[derive(Clone)]
pub struct RedbEventStore {
    inner: Arc<SegmentStore>,
}

impl RedbEventStore {
    /// Opens the store with production-oriented default segment limits.
    pub fn open(options: &LogOpenOptions) -> Result<Self, StorageError> {
        Self::open_with_segment_options(options, SegmentOptions::default())
    }

    /// Opens the store with explicit segment rotation and cache limits.
    pub fn open_with_segment_options(
        options: &LogOpenOptions,
        segment_options: SegmentOptions,
    ) -> Result<Self, StorageError> {
        std::fs::create_dir_all(&options.data_dir)?;
        let control_path = options.data_dir.join(&options.database_file);
        Ok(Self {
            inner: Arc::new(SegmentStore::open(control_path, segment_options)?),
        })
    }

    /// Opens an exact control path with default segment limits.
    pub fn open_path(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let path = path.as_ref();
        let data_dir = path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        let database_file = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                StorageError::InvalidSegmentOptions(format!(
                    "storage path has no UTF-8 filename: {}",
                    path.display()
                ))
            })?
            .to_owned();
        Self::open(&LogOpenOptions {
            data_dir,
            database_file,
        })
    }

    /// Appends one event through the active sequence segment.
    pub fn append_event(&self, event: &ChangeEvent) -> Result<(), StorageError> {
        self.inner.append_event(event)
    }

    /// Atomically persists one source transaction and its commit LSN.
    pub fn persist_transaction(
        &self,
        transaction_events: &[ChangeEvent],
        source_name: &str,
        source_lsn: &str,
    ) -> Result<PersistTransactionOutcome, StorageError> {
        self.persist_transaction_before_commit(transaction_events, source_name, source_lsn, || {
            Ok(())
        })
    }

    /// Atomically persists an in-memory or staged source transaction and its LSN.
    pub fn persist_transaction_events(
        &self,
        transaction_events: &TransactionEvents,
        source_name: &str,
        source_lsn: &str,
    ) -> Result<PersistTransactionOutcome, StorageError> {
        self.persist_transaction_batch(&[transaction_events], source_name, source_lsn)
    }

    /// Atomically persists complete transactions in one active segment commit.
    pub fn persist_transaction_batch(
        &self,
        transaction_events: &[&TransactionEvents],
        source_name: &str,
        source_lsn: &str,
    ) -> Result<PersistTransactionOutcome, StorageError> {
        self.persist_transaction_batch_before_commit(
            transaction_events,
            source_name,
            source_lsn,
            || Ok(()),
        )
    }

    fn persist_transaction_batch_before_commit<F>(
        &self,
        transaction_events: &[&TransactionEvents],
        source_name: &str,
        source_lsn: &str,
        before_commit: F,
    ) -> Result<PersistTransactionOutcome, StorageError>
    where
        F: FnOnce() -> Result<(), StorageError>,
    {
        self.inner.persist_transaction_batch(
            transaction_events,
            source_name,
            source_lsn,
            before_commit,
        )
    }

    fn persist_transaction_before_commit<F>(
        &self,
        transaction_events: &[ChangeEvent],
        source_name: &str,
        source_lsn: &str,
        before_commit: F,
    ) -> Result<PersistTransactionOutcome, StorageError>
    where
        F: FnOnce() -> Result<(), StorageError>,
    {
        let staged_bytes = transaction_events.iter().try_fold(0u64, |total, event| {
            let encoded = serde_json::to_vec(event)?;
            total
                .checked_add(encoded.len() as u64 + size_of::<u64>() as u64)
                .ok_or(StorageError::TransactionBatchByteCountOverflow)
        })?;
        let events = TransactionEvents::InMemory {
            events: transaction_events.to_vec(),
            stats: TransactionStats {
                event_count: transaction_events.len(),
                decoded_bytes: staged_bytes,
                staged_bytes,
            },
        };
        self.inner
            .persist_transaction_batch(&[&events], source_name, source_lsn, before_commit)
    }

    /// Returns the sequence assigned to the next newly captured event.
    pub fn next_sequence(&self) -> Result<u64, StorageError> {
        self.inner.next_sequence()
    }

    /// Returns the highest sequence ever assigned, including expired events.
    pub fn last_sequence(&self) -> Result<Option<u64>, StorageError> {
        self.inner.last_sequence()
    }

    /// Returns the oldest event sequence still retained.
    pub fn first_sequence(&self) -> Result<Option<u64>, StorageError> {
        self.inner.first_sequence()
    }

    /// Replays across as many ordered segment files as the limit requires.
    pub fn replay_from(
        &self,
        sequence: u64,
        limit: usize,
    ) -> Result<Vec<ChangeEvent>, StorageError> {
        self.inner.replay_from(sequence, limit)
    }

    /// Removes whole sealed segment files that exceed retention boundaries.
    pub fn prune_events(
        &self,
        policy: RetentionPolicy,
        now_ms: i64,
    ) -> Result<RetentionOutcome, StorageError> {
        self.inner.prune_events(policy, now_ms)
    }

    /// Returns aggregate control and segment row counts.
    pub fn stats(&self) -> Result<StoreStats, StorageError> {
        self.inner.stats()
    }

    /// Lists the latest durable source checkpoints.
    pub fn source_offsets(&self) -> Result<Vec<SourceOffset>, StorageError> {
        self.inner.source_offsets()
    }

    /// Lists all durable stream-consumer checkpoints.
    pub fn consumer_offsets(&self) -> Result<Vec<ConsumerOffset>, StorageError> {
        self.inner.consumer_offsets()
    }

    /// Persists a source checkpoint in the active segment.
    pub fn set_source_offset(&self, source_name: &str, lsn: &str) -> Result<(), StorageError> {
        self.inner.set_source_offset(source_name, lsn)
    }

    /// Reads the latest source checkpoint from the segment chain.
    pub fn source_offset(&self, source_name: &str) -> Result<Option<String>, StorageError> {
        self.inner.source_offset(source_name)
    }

    /// Sets a consumer offset, allowing an explicit seek in either direction.
    pub fn set_consumer_offset(
        &self,
        stream_name: &str,
        consumer_name: &str,
        last_acknowledged_sequence: u64,
    ) -> Result<(), StorageError> {
        self.inner
            .set_consumer_offset(stream_name, consumer_name, last_acknowledged_sequence)
    }

    /// Advances a consumer offset monotonically.
    pub fn acknowledge_consumer_offset(
        &self,
        stream_name: &str,
        consumer_name: &str,
        acknowledged_sequence: u64,
    ) -> Result<u64, StorageError> {
        self.inner
            .acknowledge_consumer_offset(stream_name, consumer_name, acknowledged_sequence)
    }

    /// Reads one stream-consumer checkpoint.
    pub fn consumer_offset(
        &self,
        stream_name: &str,
        consumer_name: &str,
    ) -> Result<Option<u64>, StorageError> {
        self.inner.consumer_offset(stream_name, consumer_name)
    }
}

/// Represents failures while reading or writing the local event store.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("redb error: {0}")]
    Redb(String),

    #[error("event serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("transaction buffer error: {0}")]
    TransactionBuffer(#[from] TransactionBufferError),

    #[error("duplicate event id: {0}")]
    DuplicateEventId(String),

    #[error("duplicate event sequence: {0}")]
    DuplicateSequence(u64),

    #[error("cannot persist an empty source transaction batch")]
    EmptyTransactionBatch,

    #[error("source transaction batch event count overflowed")]
    TransactionBatchEventCountOverflow,

    #[error("source transaction batch byte count overflowed")]
    TransactionBatchByteCountOverflow,

    #[error(
        "source transaction is only partially persisted: {duplicate_count} of {event_count} events already exist"
    )]
    PartiallyPersistedTransaction {
        duplicate_count: usize,
        event_count: usize,
    },

    #[error("invalid stored consumer offset key: {0:?}")]
    InvalidConsumerOffsetKey(String),

    #[error(
        "event sequence {requested} is no longer retained; first available sequence is {first_available}"
    )]
    SequenceExpired {
        requested: u64,
        first_available: u64,
    },

    #[error("invalid retention policy: {0}")]
    InvalidRetentionPolicy(String),

    #[error("invalid segment options: {0}")]
    InvalidSegmentOptions(String),

    #[error("invalid segment catalog: {0}")]
    InvalidSegmentCatalog(String),

    #[error("invalid durable format marker: {}", .0.display())]
    InvalidFormatMarker(PathBuf),

    #[error("the segment catalog has no active segment")]
    MissingActiveSegment,

    #[error("control store format {found} is unsupported; this binary supports format {supported}")]
    UnsupportedControlFormat { found: u64, supported: u64 },

    #[error(
        "segment {} uses unsupported format {found}; this binary supports format {supported}",
        path.display()
    )]
    UnsupportedSegmentFormat {
        path: PathBuf,
        found: u64,
        supported: u64,
    },
}

impl StorageError {
    /// Returns true when an error means the event was already captured.
    pub fn is_duplicate_event(&self) -> bool {
        matches!(self, Self::DuplicateEventId(_))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use lightcdc_core::{ChangeEvent, Operation, SourceMetadata};
    use tempfile::TempDir;

    use super::{
        LogOpenOptions, PersistTransactionOutcome, RedbEventStore, RetentionPolicy, SegmentOptions,
        StorageError,
    };
    use crate::{TransactionBuffer, TransactionBufferOptions, TransactionEvents};

    #[test]
    fn replay_from_returns_events_in_sequence_order() {
        let temp = TempDir::new().expect("temp dir");
        let store = RedbEventStore::open(&LogOpenOptions {
            data_dir: temp.path().to_path_buf(),
            database_file: "test.redb".to_owned(),
        })
        .expect("open store");

        store
            .append_event(&event(1, "0/1"))
            .expect("append event 1");
        store
            .append_event(&event(2, "0/2"))
            .expect("append event 2");
        store
            .append_event(&event(3, "0/3"))
            .expect("append event 3");

        let replayed = store.replay_from(2, 10).expect("replay events");

        assert_eq!(
            replayed
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
    }

    #[test]
    fn inspection_reports_table_counts_and_offsets() {
        let temp = TempDir::new().expect("temp dir");
        let store = RedbEventStore::open(&LogOpenOptions {
            data_dir: temp.path().to_path_buf(),
            database_file: "test.redb".to_owned(),
        })
        .expect("open store");

        store.append_event(&event(1, "0/1")).expect("append event");
        store
            .set_source_offset("default", "0/1")
            .expect("set source offset");
        store
            .set_consumer_offset("orders", "search-indexer", 1)
            .expect("set consumer offset");

        let stats = store.stats().expect("store stats");
        assert_eq!(stats.event_count, 1);
        assert_eq!(stats.event_id_count, 1);
        assert_eq!(stats.source_offset_count, 1);
        assert_eq!(stats.consumer_offset_count, 1);

        let source_offsets = store.source_offsets().expect("source offsets");
        assert_eq!(source_offsets[0].source_name, "default");
        assert_eq!(source_offsets[0].lsn, "0/1");

        let consumer_offsets = store.consumer_offsets().expect("consumer offsets");
        assert_eq!(consumer_offsets[0].stream_name, "orders");
        assert_eq!(consumer_offsets[0].consumer_name, "search-indexer");
        assert_eq!(consumer_offsets[0].sequence, 1);
    }

    #[test]
    fn replay_from_respects_limit() {
        let temp = TempDir::new().expect("temp dir");
        let store = RedbEventStore::open(&LogOpenOptions {
            data_dir: temp.path().to_path_buf(),
            database_file: "test.redb".to_owned(),
        })
        .expect("open store");

        store
            .append_event(&event(1, "0/1"))
            .expect("append event 1");
        store
            .append_event(&event(2, "0/2"))
            .expect("append event 2");

        let replayed = store.replay_from(1, 1).expect("replay events");

        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].sequence, 1);
    }

    #[test]
    fn consumer_offsets_are_scoped_by_stream() {
        let temp = TempDir::new().expect("temp dir");
        let store = RedbEventStore::open(&LogOpenOptions {
            data_dir: temp.path().to_path_buf(),
            database_file: "test.redb".to_owned(),
        })
        .expect("open store");

        store
            .set_consumer_offset("orders", "search-indexer", 10)
            .expect("set orders offset");
        store
            .set_consumer_offset("customers", "search-indexer", 20)
            .expect("set customers offset");

        assert_eq!(
            store
                .consumer_offset("orders", "search-indexer")
                .expect("get orders offset"),
            Some(10)
        );
        assert_eq!(
            store
                .consumer_offset("customers", "search-indexer")
                .expect("get customers offset"),
            Some(20)
        );
    }

    #[test]
    fn acknowledgements_only_advance_consumer_offsets() {
        let temp = TempDir::new().expect("temp dir");
        let store = RedbEventStore::open(&LogOpenOptions {
            data_dir: temp.path().to_path_buf(),
            database_file: "test.redb".to_owned(),
        })
        .expect("open store");

        assert_eq!(
            store
                .acknowledge_consumer_offset("orders", "search-indexer", 10)
                .expect("ack initial offset"),
            10
        );
        assert_eq!(
            store
                .acknowledge_consumer_offset("orders", "search-indexer", 9)
                .expect("ack stale offset"),
            10
        );
        assert_eq!(
            store
                .acknowledge_consumer_offset("orders", "search-indexer", 11)
                .expect("ack newer offset"),
            11
        );
        assert_eq!(
            store
                .consumer_offset("orders", "search-indexer")
                .expect("get consumer offset"),
            Some(11)
        );
    }

    #[test]
    fn transaction_events_and_source_offset_are_persisted_together() {
        let temp = TempDir::new().expect("temp dir");
        let store = RedbEventStore::open(&LogOpenOptions {
            data_dir: temp.path().to_path_buf(),
            database_file: "test.redb".to_owned(),
        })
        .expect("open store");
        let events = [event(1, "0/1"), event(2, "0/2")];

        let outcome = store
            .persist_transaction(&events, "default", "0/3")
            .expect("persist transaction");

        assert_eq!(outcome, PersistTransactionOutcome::Persisted);
        assert_eq!(
            store.replay_from(1, 10).expect("replay transaction events"),
            events
        );
        assert_eq!(
            store.source_offset("default").expect("source offset"),
            Some("0/3".to_owned())
        );
    }

    #[test]
    fn staged_transaction_is_streamed_into_one_atomic_redb_commit() {
        let temp = TempDir::new().expect("temp dir");
        let store = RedbEventStore::open(&LogOpenOptions {
            data_dir: temp.path().to_path_buf(),
            database_file: "test.redb".to_owned(),
        })
        .expect("open store");
        let options = TransactionBufferOptions::bounded(temp.path(), "default", 1, 1_000_000, 10);
        let mut buffer = TransactionBuffer::new(options).expect("transaction buffer");
        buffer.begin(42).expect("begin transaction");
        buffer.push(event(1, "0/1")).expect("event 1");
        buffer.push(event(2, "0/2")).expect("event 2");
        let events = buffer.finish().expect("finish transaction");
        assert!(events.is_staged());
        let TransactionEvents::Staged(staged) = &events else {
            panic!("expected staged transaction");
        };
        let staging_path = staged.path().to_path_buf();

        let outcome = store
            .persist_transaction_events(&events, "default", "0/3")
            .expect("persist staged transaction");

        assert_eq!(outcome, PersistTransactionOutcome::Persisted);
        assert_eq!(
            store.replay_from(1, 10).expect("replay staged transaction"),
            [event(1, "0/1"), event(2, "0/2")]
        );
        assert_eq!(
            store.source_offset("default").expect("source offset"),
            Some("0/3".to_owned())
        );
        assert!(staging_path.exists());
        drop(events);
        assert!(!staging_path.exists());
    }

    #[test]
    fn source_transaction_batch_uses_one_atomic_commit_and_final_lsn() {
        let temp = TempDir::new().expect("temp dir");
        let store = RedbEventStore::open(&LogOpenOptions {
            data_dir: temp.path().to_path_buf(),
            database_file: "test.redb".to_owned(),
        })
        .expect("open store");

        let mut first_buffer =
            TransactionBuffer::new(TransactionBufferOptions::unbounded_in_memory())
                .expect("first buffer");
        first_buffer.begin(41).expect("begin first transaction");
        first_buffer.push(event(1, "0/1")).expect("first event");
        let first = first_buffer.finish().expect("finish first transaction");

        let staged_options =
            TransactionBufferOptions::bounded(temp.path(), "default", 1, 1_000_000, 10);
        let mut second_buffer = TransactionBuffer::new(staged_options).expect("second buffer");
        second_buffer.begin(42).expect("begin second transaction");
        second_buffer.push(event(2, "0/2")).expect("second event");
        second_buffer.push(event(3, "0/3")).expect("third event");
        let second = second_buffer.finish().expect("finish second transaction");
        assert!(second.is_staged());

        let outcome = store
            .persist_transaction_batch(&[&first, &second], "default", "0/4")
            .expect("persist source transaction batch");

        assert_eq!(outcome, PersistTransactionOutcome::Persisted);
        assert_eq!(
            store.replay_from(1, 10).expect("replay batch"),
            [event(1, "0/1"), event(2, "0/2"), event(3, "0/3")]
        );
        assert_eq!(
            store.source_offset("default").expect("source offset"),
            Some("0/4".to_owned())
        );
    }

    #[test]
    fn empty_source_transaction_batch_is_rejected() {
        let temp = TempDir::new().expect("temp dir");
        let store = RedbEventStore::open(&LogOpenOptions {
            data_dir: temp.path().to_path_buf(),
            database_file: "test.redb".to_owned(),
        })
        .expect("open store");

        let error = store
            .persist_transaction_batch(&[], "default", "0/1")
            .expect_err("empty batch should fail");

        assert!(matches!(error, StorageError::EmptyTransactionBatch));
        assert_eq!(store.source_offset("default").expect("source offset"), None);
    }

    #[test]
    fn eventless_source_transaction_advances_only_the_checkpoint() {
        let temp = TempDir::new().expect("temp dir");
        let store = RedbEventStore::open(&LogOpenOptions {
            data_dir: temp.path().to_path_buf(),
            database_file: "test.redb".to_owned(),
        })
        .expect("open store");
        let mut buffer = TransactionBuffer::new(TransactionBufferOptions::unbounded_in_memory())
            .expect("transaction buffer");
        buffer.begin(42).expect("begin transaction");
        let eventless = buffer.finish().expect("finish eventless transaction");

        let outcome = store
            .persist_transaction_events(&eventless, "default", "0/10")
            .expect("persist checkpoint-only transaction");

        assert_eq!(outcome, PersistTransactionOutcome::Persisted);
        assert!(store.replay_from(1, 10).expect("replay").is_empty());
        assert_eq!(store.last_sequence().expect("high watermark"), None);
        assert_eq!(store.next_sequence().expect("next sequence"), 1);
        assert_eq!(
            store.source_offset("default").expect("source offset"),
            Some("0/10".to_owned())
        );
    }

    #[test]
    fn source_transaction_batch_rolls_back_all_events_and_final_lsn() {
        let temp = TempDir::new().expect("temp dir");
        let options = LogOpenOptions {
            data_dir: temp.path().to_path_buf(),
            database_file: "test.redb".to_owned(),
        };
        let store = RedbEventStore::open(&options).expect("open store");

        let mut first_buffer =
            TransactionBuffer::new(TransactionBufferOptions::unbounded_in_memory())
                .expect("first buffer");
        first_buffer.begin(41).expect("begin first transaction");
        first_buffer.push(event(1, "0/1")).expect("first event");
        let first = first_buffer.finish().expect("finish first transaction");

        let mut second_buffer =
            TransactionBuffer::new(TransactionBufferOptions::unbounded_in_memory())
                .expect("second buffer");
        second_buffer.begin(42).expect("begin second transaction");
        second_buffer.push(event(2, "0/2")).expect("second event");
        let second = second_buffer.finish().expect("finish second transaction");

        let error = store
            .persist_transaction_batch_before_commit(&[&first, &second], "default", "0/3", || {
                Err(StorageError::Redb(
                    "injected group failure before commit".to_owned(),
                ))
            })
            .expect_err("injected persistence failure");
        assert!(matches!(error, StorageError::Redb(_)));
        drop(store);

        let reopened = RedbEventStore::open(&options).expect("reopen store");
        assert!(reopened.replay_from(1, 10).expect("replay").is_empty());
        assert_eq!(
            reopened.source_offset("default").expect("source offset"),
            None
        );
    }

    #[test]
    fn pre_commit_failure_rolls_back_events_ids_and_source_offset() {
        let temp = TempDir::new().expect("temp dir");
        let options = LogOpenOptions {
            data_dir: temp.path().to_path_buf(),
            database_file: "test.redb".to_owned(),
        };
        let store = RedbEventStore::open(&options).expect("open store");
        let events = [event(1, "0/1"), event(2, "0/2")];

        let error = store
            .persist_transaction_before_commit(&events, "default", "0/3", || {
                Err(StorageError::Redb(
                    "injected failure before commit".to_owned(),
                ))
            })
            .expect_err("injected persistence failure");
        assert!(matches!(error, StorageError::Redb(_)));
        drop(store);

        let reopened = RedbEventStore::open(&options).expect("reopen store");
        assert!(
            reopened
                .replay_from(1, 10)
                .expect("replay events")
                .is_empty()
        );
        assert_eq!(
            reopened.source_offset("default").expect("source offset"),
            None
        );
        let stats = reopened.stats().expect("store stats");
        assert_eq!(stats.event_count, 0);
        assert_eq!(stats.event_id_count, 0);
    }

    #[test]
    fn committed_transaction_survives_store_reopen() {
        let temp = TempDir::new().expect("temp dir");
        let options = LogOpenOptions {
            data_dir: temp.path().to_path_buf(),
            database_file: "test.redb".to_owned(),
        };
        let store = RedbEventStore::open(&options).expect("open store");
        let events = [event(1, "0/1"), event(2, "0/2")];
        store
            .persist_transaction(&events, "default", "0/3")
            .expect("persist transaction");
        drop(store);

        let reopened = RedbEventStore::open(&options).expect("reopen store");
        assert_eq!(reopened.replay_from(1, 10).expect("replay events"), events);
        assert_eq!(
            reopened.source_offset("default").expect("source offset"),
            Some("0/3".to_owned())
        );
        let stats = reopened.stats().expect("store stats");
        assert_eq!(stats.event_count, 2);
        assert_eq!(stats.event_id_count, 2);
    }

    #[test]
    fn rejected_partial_transaction_changes_neither_events_nor_source_offset() {
        let temp = TempDir::new().expect("temp dir");
        let store = RedbEventStore::open(&LogOpenOptions {
            data_dir: temp.path().to_path_buf(),
            database_file: "test.redb".to_owned(),
        })
        .expect("open store");
        let existing = event(2, "0/2");
        store
            .append_event(&existing)
            .expect("append existing event");
        store
            .set_source_offset("default", "0/2")
            .expect("set initial source offset");

        let error = store
            .persist_transaction(&[event(1, "0/1"), existing.clone()], "default", "0/3")
            .expect_err("partially persisted transaction should fail");

        assert!(matches!(
            error,
            StorageError::PartiallyPersistedTransaction {
                duplicate_count: 1,
                event_count: 2
            }
        ));
        assert_eq!(
            store.replay_from(1, 10).expect("replay stored events"),
            vec![existing]
        );
        assert_eq!(
            store.source_offset("default").expect("source offset"),
            Some("0/2".to_owned())
        );
    }

    #[test]
    fn fully_replayed_transaction_only_advances_source_offset() {
        let temp = TempDir::new().expect("temp dir");
        let store = RedbEventStore::open(&LogOpenOptions {
            data_dir: temp.path().to_path_buf(),
            database_file: "test.redb".to_owned(),
        })
        .expect("open store");
        let original = [event(1, "0/1"), event(2, "0/2")];
        store
            .persist_transaction(&original, "default", "0/3")
            .expect("persist original transaction");
        let mut replayed = original.clone();
        replayed[0].sequence = 3;
        replayed[1].sequence = 4;

        let outcome = store
            .persist_transaction(&replayed, "default", "0/4")
            .expect("persist replayed transaction");

        assert_eq!(outcome, PersistTransactionOutcome::AlreadyPersisted);
        assert_eq!(
            store.replay_from(1, 10).expect("replay stored events"),
            original
        );
        assert_eq!(
            store.source_offset("default").expect("source offset"),
            Some("0/4".to_owned())
        );
    }

    #[test]
    fn count_retention_prunes_payloads_without_resetting_sequence_numbers() {
        let temp = TempDir::new().expect("temp dir");
        let store = RedbEventStore::open_with_segment_options(
            &LogOpenOptions {
                data_dir: temp.path().to_path_buf(),
                database_file: "test.redb".to_owned(),
            },
            SegmentOptions {
                max_events: 2,
                ..SegmentOptions::default()
            },
        )
        .expect("open store");
        store
            .persist_transaction(&[event(1, "0/1"), event(2, "0/2")], "default", "0/3")
            .expect("persist first batch");
        store
            .persist_transaction(&[event(3, "0/3")], "default", "0/4")
            .expect("persist latest batch");

        let outcome = store
            .prune_events(
                RetentionPolicy {
                    max_events: Some(1),
                    max_age: None,
                    delete_batch_size: 10,
                },
                i64::MAX,
            )
            .expect("prune events");

        assert_eq!(outcome.deleted_events, 2);
        assert_eq!(outcome.deleted_event_ids, 2);
        assert_eq!(outcome.first_retained_sequence, Some(3));
        assert_eq!(outcome.high_watermark, Some(3));
        assert_eq!(store.next_sequence().expect("next sequence"), 4);
        assert!(matches!(
            store.replay_from(1, 10),
            Err(StorageError::SequenceExpired {
                requested: 1,
                first_available: 3
            })
        ));
        assert_eq!(
            store.replay_from(3, 10).expect("retained events"),
            [event(3, "0/3")]
        );
        assert_eq!(store.stats().expect("stats").event_id_count, 1);
        assert!(matches!(
            store.append_event(&event(1, "0/99")),
            Err(StorageError::DuplicateSequence(1))
        ));
    }

    #[test]
    fn age_retention_keeps_latest_checkpoint_ids_for_source_replay() {
        let temp = TempDir::new().expect("temp dir");
        let store = RedbEventStore::open_with_segment_options(
            &LogOpenOptions {
                data_dir: temp.path().to_path_buf(),
                database_file: "test.redb".to_owned(),
            },
            SegmentOptions {
                max_events: 2,
                ..SegmentOptions::default()
            },
        )
        .expect("open store");
        let original = [event(1, "0/1"), event(2, "0/2")];
        store
            .persist_transaction(&original, "default", "0/3")
            .expect("persist checkpoint");

        let outcome = store
            .prune_events(
                RetentionPolicy {
                    max_events: None,
                    max_age: Some(Duration::from_secs(1)),
                    delete_batch_size: 10,
                },
                1_784_862_082_000,
            )
            .expect("prune expired events");

        assert_eq!(outcome.deleted_events, 2);
        assert_eq!(outcome.deleted_event_ids, 2);
        assert_eq!(store.next_sequence().expect("next sequence"), 3);
        assert_eq!(store.stats().expect("stats").event_id_count, 0);

        let mut replayed = original;
        replayed[0].sequence = 3;
        replayed[1].sequence = 4;
        assert_eq!(
            store
                .persist_transaction(&replayed, "default", "0/3")
                .expect("deduplicate replay"),
            PersistTransactionOutcome::AlreadyPersisted
        );
        assert_eq!(store.next_sequence().expect("next sequence"), 3);
    }

    fn event(sequence: u64, lsn: &str) -> ChangeEvent {
        ChangeEvent {
            sequence,
            event_id: format!("postgres:lightcdc:slot:{lsn}"),
            source: SourceMetadata {
                database: "lightcdc".to_owned(),
                slot: "slot".to_owned(),
                lsn: lsn.to_owned(),
            },
            transaction: None,
            schema: "public".to_owned(),
            table: "orders".to_owned(),
            operation: Operation::Insert,
            key: None,
            before: None,
            after: Some(br#"{"id":"1"}"#.to_vec()),
            commit_timestamp_ms: Some(1_784_862_080_756),
        }
    }
}
