//! Implements the one-partition sequence-segment storage engine.
//!
//! This module owns opening, writing, and rotating the active segment. The child
//! modules isolate catalog compatibility, replay, offsets, retention, integrity,
//! and deferred redb close work so each lifecycle can be read independently.

use std::{
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use lightcdc_core::ChangeEvent;
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use crate::log::{PersistTransactionOutcome, SegmentOptions, SourceTransaction, StorageError};

mod catalog;
mod integrity;
mod offsets;
mod reaper;
mod replay;
mod retention;

use catalog::{
    cleanup_interrupted_segment_files, create_segment, encode_event, initialize_or_migrate_control,
    load_segment_descriptor, load_segment_descriptors, migrate_segment_formats, next_segment_id,
    open_database, preflight_segment_formats, read_control_format_marker, read_source_offsets,
    segment_directory, segment_path, validate_segment_catalog, write_control_format_marker,
    write_segment_statistics,
};
use reaper::{DatabaseReaper, SegmentCache, SegmentDatabase};

const EVENTS: TableDefinition<u64, &[u8]> = TableDefinition::new("events");
const REPLAY_IDS: TableDefinition<&str, u64> = TableDefinition::new("event_ids");
const SOURCE_OFFSETS: TableDefinition<&str, &str> = TableDefinition::new("source_offsets");
const SOURCE_REPLAY_FLOORS: TableDefinition<&str, u64> =
    TableDefinition::new("source_replay_floors");
const CONSUMER_OFFSETS: TableDefinition<&str, u64> = TableDefinition::new("consumer_offsets");
const SOURCE_IDENTITIES: TableDefinition<&str, &[u8]> = TableDefinition::new("source_identities");
const METADATA: TableDefinition<&str, u64> = TableDefinition::new("metadata");

const CONTROL_FORMAT_VERSION: u64 = 2;
const SEGMENT_FORMAT_VERSION: u64 = 2;
const EVENT_PAYLOAD_FORMAT_VERSION: u64 = 1;
const CONTROL_FORMAT_KEY: &str = "control_format_version";
const SEGMENT_FORMAT_KEY: &str = "segment_format_version";
const EVENT_PAYLOAD_FORMAT_KEY: &str = "event_payload_format_version";
const SEGMENT_ID_KEY: &str = "segment_id";
const SEGMENT_SEALED_KEY: &str = "segment_sealed";
const SEGMENT_CREATED_AT_MS_KEY: &str = "segment_created_at_ms";
const SEGMENT_EVENT_COUNT_KEY: &str = "segment_event_count";
const SEGMENT_REPLAY_ID_COUNT_KEY: &str = "segment_event_id_count";
const SEGMENT_STORED_BYTES_KEY: &str = "segment_stored_bytes";
const SEGMENT_FIRST_TIMESTAMP_KEY: &str = "segment_first_timestamp_ms";
const SEGMENT_LAST_TIMESTAMP_KEY: &str = "segment_last_timestamp_ms";
const EVENT_HIGH_WATERMARK_KEY: &str = "event_high_watermark";
const RETENTION_FLOOR_KEY: &str = "retention_floor";
const CONSUMER_OFFSET_SEPARATOR: char = '\u{1f}';
const SEGMENT_FILE_PREFIX: &str = "segment-";
const SEGMENT_FILE_SUFFIX: &str = ".redb";
const SEGMENT_TEMP_SUFFIX: &str = ".redb.tmp";
const SEGMENT_DELETING_SUFFIX: &str = ".redb.deleting";
const CONTROL_FORMAT_MARKER_SUFFIX: &str = ".format";
const CONTROL_FORMAT_MARKER_PREFIX: &str = "lightcdc-control-format=";
const SEGMENT_FORMAT_MARKER_PREFIX: &str = "lightcdc-segment-format=";
const EVENT_FORMAT_MARKER_PREFIX: &str = "event-payload-format=";
const TRANSACTION_REPLAY_PREFIX: &str = "lightcdc-tx-v1";
const CONTROL_CACHE_BYTES: usize = 32 * 1024 * 1024;

/// Owns the control database and the ordered set of event segment files.
pub(crate) struct SegmentStore {
    control: Arc<Database>,
    segments_dir: PathBuf,
    options: SegmentOptions,
    state: RwLock<SegmentState>,
    cache: Mutex<SegmentCache>,
    /// Serializes segment rotation, capture commits, and whole-segment retention.
    write_gate: Mutex<()>,
    /// Closes large sealed databases without blocking the capture writer.
    reaper: DatabaseReaper,
}

struct SegmentState {
    descriptors: Vec<SegmentDescriptor>,
    active: Option<SegmentDatabase>,
}

#[derive(Clone, Debug)]
struct SegmentDescriptor {
    id: u64,
    path: PathBuf,
    sealed: bool,
    created_at_ms: i64,
    event_count: u64,
    replay_id_count: u64,
    stored_bytes: u64,
    first_sequence: Option<u64>,
    last_sequence: Option<u64>,
    high_watermark: Option<u64>,
    first_timestamp_ms: Option<i64>,
    last_timestamp_ms: Option<i64>,
}

#[derive(Serialize)]
struct StoredEventRef<'a> {
    version: u64,
    event: &'a ChangeEvent,
}

#[derive(Deserialize)]
struct StoredEvent {
    version: u64,
    event: ChangeEvent,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct SegmentFormatMarker {
    segment: u64,
    event: u64,
}

impl SegmentStore {
    pub(crate) fn open(
        control_path: PathBuf,
        options: SegmentOptions,
    ) -> Result<Self, StorageError> {
        options.validate()?;
        let parent = control_path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)?;
        let segments_dir = segment_directory(&control_path)?;
        let marker_format = read_control_format_marker(&control_path)?;
        if marker_format.is_some() && !control_path.exists() {
            return Err(StorageError::InvalidSegmentCatalog(format!(
                "format marker exists but control database is missing: {}",
                control_path.display()
            )));
        }
        if let Some(found) = marker_format
            && found > CONTROL_FORMAT_VERSION
        {
            return Err(StorageError::UnsupportedControlFormat {
                found,
                supported: CONTROL_FORMAT_VERSION,
            });
        }
        preflight_segment_formats(&segments_dir)?;

        let mut control = open_database(&control_path, CONTROL_CACHE_BYTES, true)?;
        initialize_or_migrate_control(&mut control, &segments_dir, options)?;
        if marker_format != Some(CONTROL_FORMAT_VERSION) {
            write_control_format_marker(&control_path, CONTROL_FORMAT_VERSION)?;
        }
        fs::create_dir_all(&segments_dir)?;
        migrate_segment_formats(&segments_dir, options)?;
        cleanup_interrupted_segment_files(&segments_dir, &control, options)?;
        let control = Arc::new(control);

        let mut descriptors = load_segment_descriptors(&segments_dir, options)?;
        if descriptors.is_empty() {
            let descriptor =
                create_segment(&segments_dir, 1, &[], None, options.active_cache_bytes)?;
            descriptors.push(descriptor);
        }
        validate_segment_catalog(&descriptors)?;

        let reaper = DatabaseReaper::start()?;
        let active_descriptor = descriptors.iter().find(|segment| !segment.sealed).cloned();
        let active = match active_descriptor {
            Some(segment) => Some(reaper.wrap(
                open_database(&segment.path, options.active_cache_bytes, false)?,
                segment.path.clone(),
            )),
            None => {
                let latest = descriptors
                    .last()
                    .expect("a non-empty catalog has a latest segment");
                let latest_db = open_database(&latest.path, options.sealed_cache_bytes, false)?;
                let source_offsets = read_source_offsets(&latest_db)?;
                let next_id = next_segment_id(latest.id)?;
                let descriptor = create_segment(
                    &segments_dir,
                    next_id,
                    &source_offsets,
                    latest.high_watermark,
                    options.active_cache_bytes,
                )?;
                let active = reaper.wrap(
                    open_database(&descriptor.path, options.active_cache_bytes, false)?,
                    descriptor.path.clone(),
                );
                descriptors.push(descriptor);
                Some(active)
            }
        };

        Ok(Self {
            control,
            segments_dir,
            options,
            state: RwLock::new(SegmentState {
                descriptors,
                active,
            }),
            cache: Mutex::new(SegmentCache::new(options.sealed_cache_capacity)),
            write_gate: Mutex::new(()),
            reaper,
        })
    }

    pub(crate) fn append_event(&self, event: &ChangeEvent) -> Result<(), StorageError> {
        let _write = mutex(&self.write_gate);
        if self.event_id_exists(&event.event_id)? {
            return Err(StorageError::DuplicateEventId(event.event_id.clone()));
        }
        if self
            .last_sequence()?
            .is_some_and(|high_watermark| event.sequence <= high_watermark)
            || self.sequence_exists(event.sequence)?
        {
            return Err(StorageError::DuplicateSequence(event.sequence));
        }

        let payload = encode_event(event)?;
        self.rotate_before_write(1, payload.len() as u64, unix_timestamp_ms())?;
        let (active, mut descriptor) = self.active_segment()?;
        let write = active.begin_write().map_err(redb_error)?;
        {
            let mut events = write.open_table(EVENTS).map_err(redb_error)?;
            let mut event_ids = write.open_table(REPLAY_IDS).map_err(redb_error)?;
            let mut metadata = write.open_table(METADATA).map_err(redb_error)?;
            events
                .insert(event.sequence, payload.as_slice())
                .map_err(redb_error)?;
            event_ids
                .insert(event.event_id.as_str(), event.sequence)
                .map_err(redb_error)?;
            descriptor.record_event(event, payload.len() as u64);
            descriptor.replay_id_count = descriptor.replay_id_count.saturating_add(1);
            write_segment_statistics(&mut metadata, &descriptor)?;
        }
        write.commit().map_err(redb_error)?;
        self.replace_descriptor(descriptor.clone());
        self.rotate_after_write(&descriptor, unix_timestamp_ms())?;
        Ok(())
    }

    pub(crate) fn persist_transaction_batch<F>(
        &self,
        transactions: &[SourceTransaction<'_>],
        source_name: &str,
        check_replay: bool,
        before_commit: F,
    ) -> Result<PersistTransactionOutcome, StorageError>
    where
        F: FnOnce() -> Result<(), StorageError>,
    {
        let source_lsn = transactions
            .last()
            .ok_or(StorageError::EmptyTransactionBatch)?
            .source_lsn;
        let event_count = transactions.iter().try_fold(0usize, |total, transaction| {
            total
                .checked_add(transaction.events.len())
                .ok_or(StorageError::TransactionBatchEventCountOverflow)
        })?;
        let estimated_bytes = transactions.iter().try_fold(0u64, |total, transaction| {
            total
                .checked_add(transaction.events.stats().staged_bytes)
                .ok_or(StorageError::TransactionBatchByteCountOverflow)
        })?;

        let _write = mutex(&self.write_gate);
        self.ensure_active_segment()?;
        let durable_lsn = self.source_offset_from_active(source_name)?;
        if durable_lsn
            .as_deref()
            .is_some_and(|durable| lsn_is_at_or_before(source_lsn, durable))
        {
            return Ok(PersistTransactionOutcome::AlreadyPersisted);
        }

        if check_replay && durable_lsn.is_some() {
            let duplicates = self.count_duplicate_transactions(transactions, source_name)?;
            if duplicates.transactions == transactions.len() {
                self.persist_checkpoint_only(source_name, source_lsn)?;
                return Ok(PersistTransactionOutcome::AlreadyPersisted);
            }
            if duplicates.transactions > 0 || duplicates.events > 0 {
                return Err(StorageError::PartiallyPersistedTransaction {
                    duplicate_count: duplicates.events,
                    event_count,
                });
            }
        }

        self.rotate_before_write(event_count as u64, estimated_bytes, unix_timestamp_ms())?;
        let (active, mut descriptor) = self.active_segment()?;
        let write = active.begin_write().map_err(redb_error)?;
        let outcome = {
            let mut events = write.open_table(EVENTS).map_err(redb_error)?;
            let mut replay_ids = write.open_table(REPLAY_IDS).map_err(redb_error)?;
            let mut source_offsets = write.open_table(SOURCE_OFFSETS).map_err(redb_error)?;
            let mut metadata = write.open_table(METADATA).map_err(redb_error)?;

            for transaction in transactions {
                let replay_id = (!transaction.events.is_empty())
                    .then(|| transaction_replay_id(source_name, transaction.source_lsn));
                if let Some(replay_id) = &replay_id
                    && replay_ids
                        .get(replay_id.as_str())
                        .map_err(redb_error)?
                        .is_some()
                {
                    return Err(StorageError::DuplicateTransactionMarker(replay_id.clone()));
                }
                for event in transaction.events.iter()? {
                    let event = event?;
                    if events.get(event.sequence).map_err(redb_error)?.is_some()
                        || descriptor
                            .high_watermark
                            .is_some_and(|high| event.sequence <= high)
                    {
                        return Err(StorageError::DuplicateSequence(event.sequence));
                    }

                    let payload = encode_event(&event)?;
                    events
                        .insert(event.sequence, payload.as_slice())
                        .map_err(redb_error)?;
                    descriptor.record_event(&event, payload.len() as u64);
                }
                if let Some(replay_id) = replay_id {
                    replay_ids
                        .insert(
                            replay_id.as_str(),
                            descriptor.high_watermark.unwrap_or_default(),
                        )
                        .map_err(redb_error)?;
                    descriptor.replay_id_count = descriptor.replay_id_count.saturating_add(1);
                }
            }
            source_offsets
                .insert(source_name, source_lsn)
                .map_err(redb_error)?;
            write_segment_statistics(&mut metadata, &descriptor)?;
            PersistTransactionOutcome::Persisted
        };

        before_commit()?;
        write.commit().map_err(redb_error)?;
        self.replace_descriptor(descriptor.clone());
        self.rotate_after_write(&descriptor, unix_timestamp_ms())?;
        Ok(outcome)
    }

    pub(crate) fn next_sequence(&self) -> Result<u64, StorageError> {
        Ok(self
            .last_sequence()?
            .map_or(1, |sequence| sequence.saturating_add(1)))
    }

    pub(crate) fn last_sequence(&self) -> Result<Option<u64>, StorageError> {
        Ok(read_state(&self.state)
            .descriptors
            .last()
            .and_then(|segment| segment.high_watermark))
    }

    pub(crate) fn first_sequence(&self) -> Result<Option<u64>, StorageError> {
        Ok(read_state(&self.state)
            .descriptors
            .iter()
            .find_map(|segment| segment.first_sequence))
    }

    fn ensure_active_segment(&self) -> Result<(), StorageError> {
        if read_state(&self.state).active.is_some() {
            return Ok(());
        }
        let latest = read_state(&self.state)
            .descriptors
            .last()
            .cloned()
            .ok_or(StorageError::MissingActiveSegment)?;
        let latest_db = self.sealed_database(&latest)?;
        let source_offsets = read_source_offsets(&latest_db)?;
        let next_id = next_segment_id(latest.id)?;
        let next_path = segment_path(&self.segments_dir, next_id);
        let descriptor = if next_path.exists() {
            let descriptor = load_segment_descriptor(&next_path, self.options.active_cache_bytes)?;
            if descriptor.sealed {
                return Err(StorageError::InvalidSegmentCatalog(format!(
                    "newest recovery segment {} is already sealed",
                    descriptor.id
                )));
            }
            descriptor
        } else {
            create_segment(
                &self.segments_dir,
                next_id,
                &source_offsets,
                latest.high_watermark,
                self.options.active_cache_bytes,
            )?
        };
        let database = self.reaper.wrap(
            open_database(&descriptor.path, self.options.active_cache_bytes, false)?,
            descriptor.path.clone(),
        );
        let mut state = write_state(&self.state);
        state.descriptors.push(descriptor);
        state.active = Some(database);
        Ok(())
    }

    fn active_segment(&self) -> Result<(SegmentDatabase, SegmentDescriptor), StorageError> {
        let state = read_state(&self.state);
        let descriptor = state
            .descriptors
            .last()
            .filter(|segment| !segment.sealed)
            .cloned()
            .ok_or(StorageError::MissingActiveSegment)?;
        let database = state
            .active
            .as_ref()
            .cloned()
            .ok_or(StorageError::MissingActiveSegment)?;
        Ok((database, descriptor))
    }

    fn replace_descriptor(&self, descriptor: SegmentDescriptor) {
        let mut state = write_state(&self.state);
        if let Some(existing) = state
            .descriptors
            .iter_mut()
            .find(|existing| existing.id == descriptor.id)
        {
            *existing = descriptor;
        }
    }

    fn rotate_before_write(
        &self,
        incoming_events: u64,
        incoming_bytes: u64,
        now_ms: i64,
    ) -> Result<(), StorageError> {
        self.ensure_active_segment()?;
        let (_database, descriptor) = self.active_segment()?;
        if descriptor.event_count == 0 {
            return Ok(());
        }
        let exceeds_events =
            descriptor.event_count.saturating_add(incoming_events) > self.options.max_events;
        let exceeds_bytes =
            descriptor.stored_bytes.saturating_add(incoming_bytes) > self.options.max_bytes;
        let exceeds_age = now_ms.saturating_sub(descriptor.created_at_ms)
            >= duration_ms_i64(self.options.max_age);
        if exceeds_events || exceeds_bytes || exceeds_age {
            self.rotate_active_segment()?;
        }
        Ok(())
    }

    fn rotate_after_write(
        &self,
        descriptor: &SegmentDescriptor,
        now_ms: i64,
    ) -> Result<(), StorageError> {
        if descriptor.event_count == 0 {
            return Ok(());
        }
        let reached_events = descriptor.event_count >= self.options.max_events;
        let reached_bytes = descriptor.stored_bytes >= self.options.max_bytes;
        let reached_age = now_ms.saturating_sub(descriptor.created_at_ms)
            >= duration_ms_i64(self.options.max_age);
        if reached_events || reached_bytes || reached_age {
            self.rotate_active_segment()?;
        }
        Ok(())
    }

    fn rotate_aged_active(&self, now_ms: i64) -> Result<(), StorageError> {
        self.ensure_active_segment()?;
        let (_database, descriptor) = self.active_segment()?;
        if descriptor.event_count > 0
            && now_ms.saturating_sub(descriptor.created_at_ms)
                >= duration_ms_i64(self.options.max_age)
        {
            self.rotate_active_segment()?;
        }
        Ok(())
    }

    fn rotate_active_segment(&self) -> Result<(), StorageError> {
        let (active, mut descriptor) = self.active_segment()?;
        let source_offsets = read_source_offsets(&active)?;
        let write = active.begin_write().map_err(redb_error)?;
        {
            let mut metadata = write.open_table(METADATA).map_err(redb_error)?;
            metadata.insert(SEGMENT_SEALED_KEY, 1).map_err(redb_error)?;
        }
        write.commit().map_err(redb_error)?;
        descriptor.sealed = true;
        mutex(&self.cache).insert(descriptor.id, Arc::clone(&active));
        {
            let mut state = write_state(&self.state);
            if let Some(existing) = state
                .descriptors
                .iter_mut()
                .find(|existing| existing.id == descriptor.id)
            {
                *existing = descriptor.clone();
            }
            state.active = None;
        }

        let next = create_segment(
            &self.segments_dir,
            next_segment_id(descriptor.id)?,
            &source_offsets,
            descriptor.high_watermark,
            self.options.active_cache_bytes,
        )?;
        let next_database = self.reaper.wrap(
            open_database(&next.path, self.options.active_cache_bytes, false)?,
            next.path.clone(),
        );
        let mut state = write_state(&self.state);
        state.descriptors.push(next);
        state.active = Some(next_database);
        Ok(())
    }

    fn sealed_database(
        &self,
        descriptor: &SegmentDescriptor,
    ) -> Result<SegmentDatabase, StorageError> {
        let mut cache = mutex(&self.cache);
        if let Some(database) = cache.get(descriptor.id) {
            return Ok(database);
        }
        self.reaper.wait_until_closed(&descriptor.path);
        let database = self.reaper.wrap(
            open_database(&descriptor.path, self.options.sealed_cache_bytes, false)?,
            descriptor.path.clone(),
        );
        cache.insert(descriptor.id, Arc::clone(&database));
        Ok(database)
    }
}

impl SegmentDescriptor {
    fn record_event(&mut self, event: &ChangeEvent, payload_bytes: u64) {
        self.event_count = self.event_count.saturating_add(1);
        self.stored_bytes = self.stored_bytes.saturating_add(payload_bytes);
        self.first_sequence.get_or_insert(event.sequence);
        self.last_sequence = Some(event.sequence);
        self.high_watermark = Some(
            self.high_watermark
                .map_or(event.sequence, |current| current.max(event.sequence)),
        );
        if let Some(timestamp) = event.commit_timestamp_ms {
            self.first_timestamp_ms = Some(
                self.first_timestamp_ms
                    .map_or(timestamp, |current| current.min(timestamp)),
            );
            self.last_timestamp_ms = Some(
                self.last_timestamp_ms
                    .map_or(timestamp, |current| current.max(timestamp)),
            );
        }
    }
}

fn consumer_offset_key(stream_name: &str, consumer_name: &str) -> String {
    format!("{stream_name}{CONSUMER_OFFSET_SEPARATOR}{consumer_name}")
}

fn transaction_replay_id(source_name: &str, source_lsn: &str) -> String {
    format!(
        "{TRANSACTION_REPLAY_PREFIX}:{}:{source_name}:{source_lsn}",
        source_name.len()
    )
}

fn lsn_is_at_or_before(candidate: &str, durable: &str) -> bool {
    match (parse_lsn(candidate), parse_lsn(durable)) {
        (Some(candidate), Some(durable)) => candidate <= durable,
        _ => candidate == durable,
    }
}

fn parse_lsn(lsn: &str) -> Option<u64> {
    let (high, low) = lsn.split_once('/')?;
    let high = u64::from_str_radix(high, 16).ok()?;
    let low = u64::from_str_radix(low, 16).ok()?;
    high.checked_shl(32)?.checked_add(low)
}

fn unix_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn duration_ms_i64(duration: Duration) -> i64 {
    duration.as_millis().min(i64::MAX as u128) as i64
}

fn encode_i64(value: i64) -> u64 {
    (value as u64) ^ (1_u64 << 63)
}

fn decode_i64(value: u64) -> i64 {
    (value ^ (1_u64 << 63)) as i64
}

fn redb_error(error: impl ToString) -> StorageError {
    StorageError::Redb(error.to_string())
}

fn mutex<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn read_state(state: &RwLock<SegmentState>) -> RwLockReadGuard<'_, SegmentState> {
    state
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn write_state(state: &RwLock<SegmentState>) -> RwLockWriteGuard<'_, SegmentState> {
    state
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests;
