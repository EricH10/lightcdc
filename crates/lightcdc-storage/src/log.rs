use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use lightcdc_core::ChangeEvent;
use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use thiserror::Error;

const EVENTS: TableDefinition<u64, &[u8]> = TableDefinition::new("events");
const EVENT_IDS: TableDefinition<&str, u64> = TableDefinition::new("event_ids");
const SOURCE_OFFSETS: TableDefinition<&str, &str> = TableDefinition::new("source_offsets");
const CONSUMER_OFFSETS: TableDefinition<&str, u64> = TableDefinition::new("consumer_offsets");
const CONSUMER_OFFSET_SEPARATOR: char = '\u{1f}';

/// Describes where the redb event log should be opened.
#[derive(Debug, Clone)]
pub struct LogOpenOptions {
    /// The directory that contains the redb database file.
    pub data_dir: PathBuf,
    /// The redb database filename inside the data directory.
    pub database_file: String,
}

/// Summarizes the rows stored in each LightCDC redb table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreStats {
    pub event_count: u64,
    pub event_id_count: u64,
    pub source_offset_count: u64,
    pub consumer_offset_count: u64,
}

/// Describes one persisted PostgreSQL source checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceOffset {
    pub source_name: String,
    pub lsn: String,
}

/// Describes one persisted stream consumer checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerOffset {
    pub stream_name: String,
    pub consumer_name: String,
    pub sequence: u64,
}

/// Describes whether a committed source transaction was newly stored or replayed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistTransactionOutcome {
    Persisted,
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

/// Stores captured events, source offsets, and consumer offsets in redb.
#[derive(Clone)]
pub struct RedbEventStore {
    db: Arc<Database>,
}

impl RedbEventStore {
    /// Opens the event store from configured storage options.
    pub fn open(options: &LogOpenOptions) -> Result<Self, StorageError> {
        fs::create_dir_all(&options.data_dir)?;
        Self::open_path(options.data_dir.join(&options.database_file))
    }

    /// Opens the event store at an exact filesystem path.
    pub fn open_path(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let db = Database::create(path).map_err(redb_error)?;
        let store = Self { db: Arc::new(db) };
        store.migrate()?;
        Ok(store)
    }

    /// Appends one event unless its sequence or source event id is already stored.
    pub fn append_event(&self, event: &ChangeEvent) -> Result<(), StorageError> {
        let payload = serde_json::to_vec(event)?;
        let write = self.db.begin_write().map_err(redb_error)?;

        {
            let mut events = write.open_table(EVENTS).map_err(redb_error)?;
            let mut event_ids = write.open_table(EVENT_IDS).map_err(redb_error)?;

            if events.get(event.sequence).map_err(redb_error)?.is_some() {
                return Err(StorageError::DuplicateSequence(event.sequence));
            }

            if event_ids
                .get(event.event_id.as_str())
                .map_err(redb_error)?
                .is_some()
            {
                return Err(StorageError::DuplicateEventId(event.event_id.clone()));
            }

            events
                .insert(event.sequence, payload.as_slice())
                .map_err(redb_error)?;
            event_ids
                .insert(event.event_id.as_str(), event.sequence)
                .map_err(redb_error)?;
        }

        write.commit().map_err(redb_error)?;
        Ok(())
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
        let payloads = transaction_events
            .iter()
            .map(serde_json::to_vec)
            .collect::<Result<Vec<_>, _>>()?;
        let write = self.db.begin_write().map_err(redb_error)?;

        let outcome = {
            let mut events = write.open_table(EVENTS).map_err(redb_error)?;
            let mut event_ids = write.open_table(EVENT_IDS).map_err(redb_error)?;
            let mut source_offsets = write.open_table(SOURCE_OFFSETS).map_err(redb_error)?;
            let mut duplicate_count = 0;

            for event in transaction_events {
                if event_ids
                    .get(event.event_id.as_str())
                    .map_err(redb_error)?
                    .is_some()
                {
                    duplicate_count += 1;
                    continue;
                }

                if events.get(event.sequence).map_err(redb_error)?.is_some() {
                    return Err(StorageError::DuplicateSequence(event.sequence));
                }
            }

            if duplicate_count > 0 && duplicate_count != transaction_events.len() {
                return Err(StorageError::PartiallyPersistedTransaction {
                    duplicate_count,
                    event_count: transaction_events.len(),
                });
            }

            let outcome =
                if duplicate_count == transaction_events.len() && !transaction_events.is_empty() {
                    PersistTransactionOutcome::AlreadyPersisted
                } else {
                    for (event, payload) in transaction_events.iter().zip(&payloads) {
                        events
                            .insert(event.sequence, payload.as_slice())
                            .map_err(redb_error)?;
                        event_ids
                            .insert(event.event_id.as_str(), event.sequence)
                            .map_err(redb_error)?;
                    }
                    PersistTransactionOutcome::Persisted
                };

            source_offsets
                .insert(source_name, source_lsn)
                .map_err(redb_error)?;
            outcome
        };

        before_commit()?;
        write.commit().map_err(redb_error)?;
        Ok(outcome)
    }

    /// Returns the next sequence number that should be assigned locally.
    pub fn next_sequence(&self) -> Result<u64, StorageError> {
        Ok(self.last_sequence()?.map_or(1, |sequence| sequence + 1))
    }

    /// Returns the highest stored event sequence.
    pub fn last_sequence(&self) -> Result<Option<u64>, StorageError> {
        let read = self.db.begin_read().map_err(redb_error)?;
        let events = read.open_table(EVENTS).map_err(redb_error)?;

        Ok(events
            .last()
            .map_err(redb_error)?
            .map(|(sequence, _payload)| sequence.value()))
    }

    /// Replays stored events from a sequence number, up to the requested limit.
    pub fn replay_from(
        &self,
        sequence: u64,
        limit: usize,
    ) -> Result<Vec<ChangeEvent>, StorageError> {
        let read = self.db.begin_read().map_err(redb_error)?;
        let events = read.open_table(EVENTS).map_err(redb_error)?;
        let mut output = Vec::with_capacity(limit);

        for entry in events.range(sequence..).map_err(redb_error)?.take(limit) {
            let (_sequence, payload) = entry.map_err(redb_error)?;
            output.push(serde_json::from_slice(payload.value())?);
        }

        Ok(output)
    }

    /// Returns row counts for the tables that make up the event store.
    pub fn stats(&self) -> Result<StoreStats, StorageError> {
        let read = self.db.begin_read().map_err(redb_error)?;
        let events = read.open_table(EVENTS).map_err(redb_error)?;
        let event_ids = read.open_table(EVENT_IDS).map_err(redb_error)?;
        let source_offsets = read.open_table(SOURCE_OFFSETS).map_err(redb_error)?;
        let consumer_offsets = read.open_table(CONSUMER_OFFSETS).map_err(redb_error)?;

        Ok(StoreStats {
            event_count: events.len().map_err(redb_error)?,
            event_id_count: event_ids.len().map_err(redb_error)?,
            source_offset_count: source_offsets.len().map_err(redb_error)?,
            consumer_offset_count: consumer_offsets.len().map_err(redb_error)?,
        })
    }

    /// Lists all persisted PostgreSQL source checkpoints.
    pub fn source_offsets(&self) -> Result<Vec<SourceOffset>, StorageError> {
        let read = self.db.begin_read().map_err(redb_error)?;
        let offsets = read.open_table(SOURCE_OFFSETS).map_err(redb_error)?;
        let mut output = Vec::new();

        for entry in offsets.iter().map_err(redb_error)? {
            let (source_name, lsn) = entry.map_err(redb_error)?;
            output.push(SourceOffset {
                source_name: source_name.value().to_owned(),
                lsn: lsn.value().to_owned(),
            });
        }

        Ok(output)
    }

    /// Lists all persisted stream consumer checkpoints.
    pub fn consumer_offsets(&self) -> Result<Vec<ConsumerOffset>, StorageError> {
        let read = self.db.begin_read().map_err(redb_error)?;
        let offsets = read.open_table(CONSUMER_OFFSETS).map_err(redb_error)?;
        let mut output = Vec::new();

        for entry in offsets.iter().map_err(redb_error)? {
            let (key, sequence) = entry.map_err(redb_error)?;
            let key = key.value();
            let (stream_name, consumer_name) = key
                .split_once(CONSUMER_OFFSET_SEPARATOR)
                .ok_or_else(|| StorageError::InvalidConsumerOffsetKey(key.to_owned()))?;

            output.push(ConsumerOffset {
                stream_name: stream_name.to_owned(),
                consumer_name: consumer_name.to_owned(),
                sequence: sequence.value(),
            });
        }

        Ok(output)
    }

    /// Persists the last acknowledged PostgreSQL LSN for a source.
    pub fn set_source_offset(&self, source_name: &str, lsn: &str) -> Result<(), StorageError> {
        let write = self.db.begin_write().map_err(redb_error)?;

        {
            let mut offsets = write.open_table(SOURCE_OFFSETS).map_err(redb_error)?;
            offsets.insert(source_name, lsn).map_err(redb_error)?;
        }

        write.commit().map_err(redb_error)?;
        Ok(())
    }

    /// Reads the last acknowledged PostgreSQL LSN for a source.
    pub fn source_offset(&self, source_name: &str) -> Result<Option<String>, StorageError> {
        let read = self.db.begin_read().map_err(redb_error)?;
        let offsets = read.open_table(SOURCE_OFFSETS).map_err(redb_error)?;

        Ok(offsets
            .get(source_name)
            .map_err(redb_error)?
            .map(|value| value.value().to_owned()))
    }

    /// Sets a stream consumer offset, allowing movement in either direction.
    pub fn set_consumer_offset(
        &self,
        stream_name: &str,
        consumer_name: &str,
        last_acknowledged_sequence: u64,
    ) -> Result<(), StorageError> {
        let key = consumer_offset_key(stream_name, consumer_name);
        let write = self.db.begin_write().map_err(redb_error)?;

        {
            let mut offsets = write.open_table(CONSUMER_OFFSETS).map_err(redb_error)?;
            offsets
                .insert(key.as_str(), last_acknowledged_sequence)
                .map_err(redb_error)?;
        }

        write.commit().map_err(redb_error)?;
        Ok(())
    }

    /// Advances a stream consumer offset without allowing it to move backward.
    pub fn acknowledge_consumer_offset(
        &self,
        stream_name: &str,
        consumer_name: &str,
        acknowledged_sequence: u64,
    ) -> Result<u64, StorageError> {
        let key = consumer_offset_key(stream_name, consumer_name);
        let write = self.db.begin_write().map_err(redb_error)?;

        let persisted_offset = {
            let mut offsets = write.open_table(CONSUMER_OFFSETS).map_err(redb_error)?;
            let current_offset = offsets
                .get(key.as_str())
                .map_err(redb_error)?
                .map(|value| value.value());
            let persisted_offset = current_offset
                .unwrap_or_default()
                .max(acknowledged_sequence);

            if current_offset != Some(persisted_offset) {
                offsets
                    .insert(key.as_str(), persisted_offset)
                    .map_err(redb_error)?;
            }

            persisted_offset
        };
        write.commit().map_err(redb_error)?;
        Ok(persisted_offset)
    }

    /// Reads the last acknowledged event sequence for one stream consumer.
    pub fn consumer_offset(
        &self,
        stream_name: &str,
        consumer_name: &str,
    ) -> Result<Option<u64>, StorageError> {
        let key = consumer_offset_key(stream_name, consumer_name);
        let read = self.db.begin_read().map_err(redb_error)?;
        let offsets = read.open_table(CONSUMER_OFFSETS).map_err(redb_error)?;

        Ok(offsets
            .get(key.as_str())
            .map_err(redb_error)?
            .map(|value| value.value()))
    }

    /// Creates any missing redb tables used by the event store.
    fn migrate(&self) -> Result<(), StorageError> {
        let write = self.db.begin_write().map_err(redb_error)?;

        {
            write.open_table(EVENTS).map_err(redb_error)?;
            write.open_table(EVENT_IDS).map_err(redb_error)?;
            write.open_table(SOURCE_OFFSETS).map_err(redb_error)?;
            write.open_table(CONSUMER_OFFSETS).map_err(redb_error)?;
        }

        write.commit().map_err(redb_error)?;
        Ok(())
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

    #[error("duplicate event id: {0}")]
    DuplicateEventId(String),

    #[error("duplicate event sequence: {0}")]
    DuplicateSequence(u64),

    #[error(
        "source transaction is only partially persisted: {duplicate_count} of {event_count} events already exist"
    )]
    PartiallyPersistedTransaction {
        duplicate_count: usize,
        event_count: usize,
    },

    #[error("invalid stored consumer offset key: {0:?}")]
    InvalidConsumerOffsetKey(String),
}

impl StorageError {
    /// Returns true when an error means the event was already captured.
    pub fn is_duplicate_event(&self) -> bool {
        matches!(self, Self::DuplicateEventId(_))
    }
}

fn redb_error(error: impl ToString) -> StorageError {
    StorageError::Redb(error.to_string())
}

fn consumer_offset_key(stream_name: &str, consumer_name: &str) -> String {
    format!("{stream_name}{CONSUMER_OFFSET_SEPARATOR}{consumer_name}")
}

#[cfg(test)]
mod tests {
    use lightcdc_core::{ChangeEvent, Operation, SourceMetadata};
    use tempfile::TempDir;

    use super::{LogOpenOptions, PersistTransactionOutcome, RedbEventStore, StorageError};

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
