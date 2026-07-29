//! Implements the durable redb event log, checkpoints, deduplication, and retention.

use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use lightcdc_core::ChangeEvent;
use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use thiserror::Error;

use crate::{TransactionBufferError, TransactionEvents};

const EVENTS: TableDefinition<u64, &[u8]> = TableDefinition::new("events");
const EVENT_IDS: TableDefinition<&str, u64> = TableDefinition::new("event_ids");
const SOURCE_OFFSETS: TableDefinition<&str, &str> = TableDefinition::new("source_offsets");
const CONSUMER_OFFSETS: TableDefinition<&str, u64> = TableDefinition::new("consumer_offsets");
const SOURCE_REPLAY_FLOORS: TableDefinition<&str, u64> =
    TableDefinition::new("source_replay_floors");
const METADATA: TableDefinition<&str, u64> = TableDefinition::new("metadata");
const CONSUMER_OFFSET_SEPARATOR: char = '\u{1f}';
const EVENT_HIGH_WATERMARK_KEY: &str = "event_high_watermark";
const RETENTION_FLOOR_KEY: &str = "retention_floor";

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
    /// Retained event payload rows.
    pub event_count: u64,
    /// Deduplication identifiers, including any preserved past retention.
    pub event_id_count: u64,
    /// Durable PostgreSQL source checkpoints.
    pub source_offset_count: u64,
    /// Durable stream consumer checkpoints.
    pub consumer_offset_count: u64,
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
    /// Maximum prefix entries deleted in one redb transaction.
    pub delete_batch_size: usize,
}

impl RetentionPolicy {
    /// Returns true when at least one retention boundary is configured.
    pub fn is_enabled(self) -> bool {
        self.max_events.is_some() || self.max_age.is_some()
    }
}

/// Summarizes one atomic event-log retention sweep.
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
            let mut metadata = write.open_table(METADATA).map_err(redb_error)?;

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
            let high_watermark = metadata
                .get(EVENT_HIGH_WATERMARK_KEY)
                .map_err(redb_error)?
                .map_or(event.sequence, |current| {
                    current.value().max(event.sequence)
                });
            metadata
                .insert(EVENT_HIGH_WATERMARK_KEY, high_watermark)
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

    /// Atomically persists an in-memory or staged source transaction and its LSN.
    pub fn persist_transaction_events(
        &self,
        transaction_events: &TransactionEvents,
        source_name: &str,
        source_lsn: &str,
    ) -> Result<PersistTransactionOutcome, StorageError> {
        self.persist_transaction_batch(&[transaction_events], source_name, source_lsn)
    }

    /// Atomically persists several complete source transactions and their final LSN.
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

    /// Flattens complete source transactions into one atomic redb write.
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
        if transaction_events.is_empty() {
            return Err(StorageError::EmptyTransactionBatch);
        }

        let event_count = transaction_events
            .iter()
            .try_fold(0usize, |total, transaction| {
                total
                    .checked_add(transaction.len())
                    .ok_or(StorageError::TransactionBatchEventCountOverflow)
            })?;
        let event_iterators = transaction_events
            .iter()
            .map(|transaction| transaction.iter())
            .collect::<Result<Vec<_>, _>>()?;
        let events = event_iterators
            .into_iter()
            .flatten()
            .map(|event| event.map_err(StorageError::from));

        self.persist_transaction_iter(event_count, events, source_name, source_lsn, before_commit)
    }

    /// Exposes the exact pre-commit boundary for deterministic rollback testing.
    ///
    /// Production passes a no-op closure. Tests inject an error after every
    /// table write is staged but before redb commits, proving that events,
    /// event IDs, and the source offset roll back together.
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
        self.persist_transaction_iter(
            transaction_events.len(),
            transaction_events.iter().cloned().map(Ok),
            source_name,
            source_lsn,
            before_commit,
        )
    }

    /// Persists streamed events, deduplication IDs, and the source LSN together.
    fn persist_transaction_iter<I, F>(
        &self,
        event_count: usize,
        transaction_events: I,
        source_name: &str,
        source_lsn: &str,
        before_commit: F,
    ) -> Result<PersistTransactionOutcome, StorageError>
    where
        I: IntoIterator<Item = Result<ChangeEvent, StorageError>>,
        F: FnOnce() -> Result<(), StorageError>,
    {
        let write = self.db.begin_write().map_err(redb_error)?;

        let outcome = {
            let mut events = write.open_table(EVENTS).map_err(redb_error)?;
            let mut event_ids = write.open_table(EVENT_IDS).map_err(redb_error)?;
            let mut source_offsets = write.open_table(SOURCE_OFFSETS).map_err(redb_error)?;
            let mut source_replay_floors =
                write.open_table(SOURCE_REPLAY_FLOORS).map_err(redb_error)?;
            let mut metadata = write.open_table(METADATA).map_err(redb_error)?;
            let mut duplicate_count = 0;
            let mut new_count = 0;
            let mut first_new_sequence = None;
            let mut last_new_sequence = None;

            for event in transaction_events {
                let event = event?;
                if event_ids
                    .get(event.event_id.as_str())
                    .map_err(redb_error)?
                    .is_some()
                {
                    duplicate_count += 1;
                    if new_count > 0 {
                        return Err(StorageError::PartiallyPersistedTransaction {
                            duplicate_count,
                            event_count,
                        });
                    }
                    continue;
                }

                if duplicate_count > 0 {
                    return Err(StorageError::PartiallyPersistedTransaction {
                        duplicate_count,
                        event_count,
                    });
                }

                if events.get(event.sequence).map_err(redb_error)?.is_some() {
                    return Err(StorageError::DuplicateSequence(event.sequence));
                }

                let payload = serde_json::to_vec(&event)?;
                events
                    .insert(event.sequence, payload.as_slice())
                    .map_err(redb_error)?;
                event_ids
                    .insert(event.event_id.as_str(), event.sequence)
                    .map_err(redb_error)?;
                first_new_sequence.get_or_insert(event.sequence);
                last_new_sequence = Some(event.sequence);
                new_count += 1;
            }

            let outcome = if duplicate_count == event_count && event_count > 0 {
                PersistTransactionOutcome::AlreadyPersisted
            } else {
                PersistTransactionOutcome::Persisted
            };

            source_offsets
                .insert(source_name, source_lsn)
                .map_err(redb_error)?;
            if let (Some(first_sequence), Some(last_sequence)) =
                (first_new_sequence, last_new_sequence)
            {
                source_replay_floors
                    .insert(source_name, first_sequence)
                    .map_err(redb_error)?;
                let high_watermark = metadata
                    .get(EVENT_HIGH_WATERMARK_KEY)
                    .map_err(redb_error)?
                    .map_or(last_sequence, |current| current.value().max(last_sequence));
                metadata
                    .insert(EVENT_HIGH_WATERMARK_KEY, high_watermark)
                    .map_err(redb_error)?;
            }
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

    /// Returns the highest event sequence ever assigned, including retained-away events.
    pub fn last_sequence(&self) -> Result<Option<u64>, StorageError> {
        let read = self.db.begin_read().map_err(redb_error)?;
        let metadata = read.open_table(METADATA).map_err(redb_error)?;

        Ok(metadata
            .get(EVENT_HIGH_WATERMARK_KEY)
            .map_err(redb_error)?
            .map(|sequence| sequence.value()))
    }

    /// Returns the oldest event sequence whose payload is still retained.
    pub fn first_sequence(&self) -> Result<Option<u64>, StorageError> {
        let read = self.db.begin_read().map_err(redb_error)?;
        let events = read.open_table(EVENTS).map_err(redb_error)?;

        Ok(events
            .first()
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
        let metadata = read.open_table(METADATA).map_err(redb_error)?;
        let mut output = Vec::with_capacity(limit);

        if let Some(first_available) = metadata
            .get(RETENTION_FLOOR_KEY)
            .map_err(redb_error)?
            .map(|sequence| sequence.value())
            && sequence < first_available
        {
            return Err(StorageError::SequenceExpired {
                requested: sequence,
                first_available,
            });
        }

        for entry in events.range(sequence..).map_err(redb_error)?.take(limit) {
            let (_sequence, payload) = entry.map_err(redb_error)?;
            output.push(serde_json::from_slice(payload.value())?);
        }

        Ok(output)
    }

    /// Atomically removes one bounded prefix that exceeds count or age retention.
    pub fn prune_events(
        &self,
        policy: RetentionPolicy,
        now_ms: i64,
    ) -> Result<RetentionOutcome, StorageError> {
        if !policy.is_enabled() {
            return Ok(RetentionOutcome {
                deleted_events: 0,
                deleted_event_ids: 0,
                first_retained_sequence: self.first_sequence()?,
                high_watermark: self.last_sequence()?,
            });
        }
        if policy.delete_batch_size == 0 {
            return Err(StorageError::InvalidRetentionPolicy(
                "delete_batch_size must be greater than zero".to_owned(),
            ));
        }

        let (candidates, replay_floor, first_retained_sequence, high_watermark) = {
            let read = self.db.begin_read().map_err(redb_error)?;
            let replay_floor = {
                let replay_floors = read.open_table(SOURCE_REPLAY_FLOORS).map_err(redb_error)?;
                let mut minimum = None;
                for entry in replay_floors.iter().map_err(redb_error)? {
                    let (_source, floor) = entry.map_err(redb_error)?;
                    minimum =
                        Some(minimum.map_or(floor.value(), |value: u64| value.min(floor.value())));
                }
                minimum
            };
            let events = read.open_table(EVENTS).map_err(redb_error)?;
            let retained_count = events.len().map_err(redb_error)?;
            let count_excess = policy
                .max_events
                .map_or(0, |max_events| retained_count.saturating_sub(max_events));
            let age_cutoff_ms = policy.max_age.map(|max_age| {
                let age_ms = max_age.as_millis().min(i64::MAX as u128) as i64;
                now_ms.saturating_sub(age_ms)
            });
            let mut candidates = Vec::new();

            for (index, entry) in events
                .iter()
                .map_err(redb_error)?
                .take(policy.delete_batch_size)
                .enumerate()
            {
                let (sequence, payload) = entry.map_err(redb_error)?;
                let event: ChangeEvent = serde_json::from_slice(payload.value())?;
                let exceeds_count = (index as u64) < count_excess;
                let exceeds_age = age_cutoff_ms.is_some_and(|cutoff| {
                    event
                        .commit_timestamp_ms
                        .is_some_and(|timestamp| timestamp <= cutoff)
                });
                if !exceeds_count && !exceeds_age {
                    break;
                }
                candidates.push((sequence.value(), event.event_id));
            }
            let first_retained_sequence = events
                .first()
                .map_err(redb_error)?
                .map(|(sequence, _payload)| sequence.value());
            let metadata = read.open_table(METADATA).map_err(redb_error)?;
            let high_watermark = metadata
                .get(EVENT_HIGH_WATERMARK_KEY)
                .map_err(redb_error)?
                .map(|sequence| sequence.value());

            (
                candidates,
                replay_floor,
                first_retained_sequence,
                high_watermark,
            )
        };

        if candidates.is_empty() {
            return Ok(RetentionOutcome {
                deleted_events: 0,
                deleted_event_ids: 0,
                first_retained_sequence,
                high_watermark,
            });
        }

        let write = self.db.begin_write().map_err(redb_error)?;
        let (deleted_event_ids, first_retained_sequence, high_watermark) = {
            let mut events = write.open_table(EVENTS).map_err(redb_error)?;
            let mut event_ids = write.open_table(EVENT_IDS).map_err(redb_error)?;
            let mut deleted_event_ids = 0u64;
            for (sequence, event_id) in &candidates {
                drop(events.remove(*sequence).map_err(redb_error)?);
                // Keep recent deduplication IDs after payload retention so a
                // source transaction replay cannot recreate deleted events.
                if replay_floor.is_some_and(|floor| *sequence < floor)
                    && event_ids
                        .remove(event_id.as_str())
                        .map_err(redb_error)?
                        .is_some()
                {
                    deleted_event_ids += 1;
                }
            }
            let first_retained_sequence = events
                .first()
                .map_err(redb_error)?
                .map(|(sequence, _payload)| sequence.value());
            let mut metadata = write.open_table(METADATA).map_err(redb_error)?;
            let high_watermark = metadata
                .get(EVENT_HIGH_WATERMARK_KEY)
                .map_err(redb_error)?
                .map(|sequence| sequence.value());
            if !candidates.is_empty()
                && let Some(first_available) = first_retained_sequence
                    .or_else(|| high_watermark.map(|sequence| sequence.saturating_add(1)))
            {
                let retention_floor = metadata
                    .get(RETENTION_FLOOR_KEY)
                    .map_err(redb_error)?
                    .map_or(first_available, |current| {
                        current.value().max(first_available)
                    });
                metadata
                    .insert(RETENTION_FLOOR_KEY, retention_floor)
                    .map_err(redb_error)?;
            }

            (deleted_event_ids, first_retained_sequence, high_watermark)
        };
        write.commit().map_err(redb_error)?;

        Ok(RetentionOutcome {
            deleted_events: candidates.len() as u64,
            deleted_event_ids,
            first_retained_sequence,
            high_watermark,
        })
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
            let events = write.open_table(EVENTS).map_err(redb_error)?;
            let existing_high_watermark = events
                .last()
                .map_err(redb_error)?
                .map(|(sequence, _payload)| sequence.value());
            drop(events);
            write.open_table(EVENT_IDS).map_err(redb_error)?;
            write.open_table(SOURCE_OFFSETS).map_err(redb_error)?;
            write.open_table(CONSUMER_OFFSETS).map_err(redb_error)?;
            write.open_table(SOURCE_REPLAY_FLOORS).map_err(redb_error)?;
            let mut metadata = write.open_table(METADATA).map_err(redb_error)?;
            if metadata
                .get(EVENT_HIGH_WATERMARK_KEY)
                .map_err(redb_error)?
                .is_none()
                && let Some(high_watermark) = existing_high_watermark
            {
                metadata
                    .insert(EVENT_HIGH_WATERMARK_KEY, high_watermark)
                    .map_err(redb_error)?;
            }
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
}

impl StorageError {
    /// Returns true when an error means the event was already captured.
    pub fn is_duplicate_event(&self) -> bool {
        matches!(self, Self::DuplicateEventId(_))
    }
}

/// Erases redb's operation-specific error types behind the storage error API.
fn redb_error(error: impl ToString) -> StorageError {
    StorageError::Redb(error.to_string())
}

/// Encodes a stream and consumer pair into one unambiguous redb string key.
fn consumer_offset_key(stream_name: &str, consumer_name: &str) -> String {
    format!("{stream_name}{CONSUMER_OFFSET_SEPARATOR}{consumer_name}")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use lightcdc_core::{ChangeEvent, Operation, SourceMetadata};
    use tempfile::TempDir;

    use super::{
        LogOpenOptions, PersistTransactionOutcome, RedbEventStore, RetentionPolicy, StorageError,
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
        let store = RedbEventStore::open(&LogOpenOptions {
            data_dir: temp.path().to_path_buf(),
            database_file: "test.redb".to_owned(),
        })
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
    }

    #[test]
    fn age_retention_keeps_latest_checkpoint_ids_for_source_replay() {
        let temp = TempDir::new().expect("temp dir");
        let store = RedbEventStore::open(&LogOpenOptions {
            data_dir: temp.path().to_path_buf(),
            database_file: "test.redb".to_owned(),
        })
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
        assert_eq!(outcome.deleted_event_ids, 0);
        assert_eq!(store.next_sequence().expect("next sequence"), 3);
        assert_eq!(store.stats().expect("stats").event_id_count, 2);

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
