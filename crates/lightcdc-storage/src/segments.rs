//! Implements the one-partition sequence-segment storage engine.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs,
    io::Write,
    ops::Deref,
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard,
        mpsc::{self, Sender},
    },
    thread::{self, JoinHandle},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use lightcdc_core::ChangeEvent;
use redb::{
    Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition, TableHandle,
};

use crate::log::{
    ConsumerOffset, PersistTransactionOutcome, RetentionOutcome, RetentionPolicy, SegmentOptions,
    SourceIdentity, SourceOffset, SourceTransaction, StorageError, StoreStats,
};

const EVENTS: TableDefinition<u64, &[u8]> = TableDefinition::new("events");
const REPLAY_IDS: TableDefinition<&str, u64> = TableDefinition::new("event_ids");
const SOURCE_OFFSETS: TableDefinition<&str, &str> = TableDefinition::new("source_offsets");
const SOURCE_REPLAY_FLOORS: TableDefinition<&str, u64> =
    TableDefinition::new("source_replay_floors");
const CONSUMER_OFFSETS: TableDefinition<&str, u64> = TableDefinition::new("consumer_offsets");
const SOURCE_IDENTITIES: TableDefinition<&str, &[u8]> = TableDefinition::new("source_identities");
const METADATA: TableDefinition<&str, u64> = TableDefinition::new("metadata");

const CONTROL_FORMAT_VERSION: u64 = 2;
const SEGMENT_FORMAT_VERSION: u64 = 1;
const CONTROL_FORMAT_KEY: &str = "control_format_version";
const SEGMENT_FORMAT_KEY: &str = "segment_format_version";
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

struct SegmentCache {
    capacity: usize,
    order: VecDeque<u64>,
    databases: HashMap<u64, SegmentDatabase>,
}

type SegmentDatabase = Arc<DeferredDatabase>;

/// Defers redb's close-time allocator commit and flush to the reaper thread.
struct DeferredDatabase {
    database: Option<Database>,
    path: PathBuf,
    reaper: Sender<RetiredDatabase>,
    pending_closes: Arc<PendingCloses>,
}

struct DatabaseReaper {
    sender: Option<Sender<RetiredDatabase>>,
    pending_closes: Arc<PendingCloses>,
    thread: Option<JoinHandle<()>>,
}

struct RetiredDatabase {
    path: PathBuf,
    database: Database,
}

#[derive(Default)]
struct PendingCloses {
    paths: Mutex<HashSet<PathBuf>>,
    changed: Condvar,
}

#[derive(Default)]
struct ReplayDuplicateCounts {
    transactions: usize,
    events: usize,
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

        let mut control = open_database(&control_path, CONTROL_CACHE_BYTES, true)?;
        initialize_or_migrate_control(&mut control, &segments_dir, options)?;
        if marker_format != Some(CONTROL_FORMAT_VERSION) {
            write_control_format_marker(&control_path, CONTROL_FORMAT_VERSION)?;
        }
        fs::create_dir_all(&segments_dir)?;
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

        let payload = serde_json::to_vec(event)?;
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
                let replay_id = transaction_replay_id(source_name, transaction.source_lsn);
                if replay_ids
                    .get(replay_id.as_str())
                    .map_err(redb_error)?
                    .is_some()
                {
                    return Err(StorageError::DuplicateTransactionMarker(replay_id));
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

                    let payload = serde_json::to_vec(&event)?;
                    events
                        .insert(event.sequence, payload.as_slice())
                        .map_err(redb_error)?;
                    descriptor.record_event(&event, payload.len() as u64);
                }
                replay_ids
                    .insert(
                        replay_id.as_str(),
                        descriptor.high_watermark.unwrap_or_default(),
                    )
                    .map_err(redb_error)?;
                descriptor.replay_id_count = descriptor.replay_id_count.saturating_add(1);
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

    pub(crate) fn replay_from(
        &self,
        sequence: u64,
        limit: usize,
    ) -> Result<Vec<ChangeEvent>, StorageError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        if let Some(first_available) = self.retention_floor()?
            && sequence < first_available
        {
            return Err(StorageError::SequenceExpired {
                requested: sequence,
                first_available,
            });
        }

        let snapshots = {
            let state = read_state(&self.state);
            let mut snapshots = Vec::new();
            for descriptor in state.descriptors.iter().filter(|descriptor| {
                descriptor
                    .last_sequence
                    .is_some_and(|last| last >= sequence)
            }) {
                let database = if !descriptor.sealed {
                    state
                        .active
                        .as_ref()
                        .cloned()
                        .ok_or(StorageError::MissingActiveSegment)?
                } else {
                    self.sealed_database(descriptor)?
                };
                snapshots.push((descriptor.clone(), database));
            }
            snapshots
        };

        let mut output = Vec::with_capacity(limit);
        let mut next_sequence = sequence;
        for (_descriptor, database) in snapshots {
            let read = database.begin_read().map_err(redb_error)?;
            let events = read.open_table(EVENTS).map_err(redb_error)?;
            for entry in events.range(next_sequence..).map_err(redb_error)? {
                let (stored_sequence, payload) = entry.map_err(redb_error)?;
                let event: ChangeEvent = serde_json::from_slice(payload.value())?;
                next_sequence = stored_sequence.value().saturating_add(1);
                output.push(event);
                if output.len() >= limit {
                    return Ok(output);
                }
            }
        }
        Ok(output)
    }

    pub(crate) fn prune_events(
        &self,
        policy: RetentionPolicy,
        now_ms: i64,
    ) -> Result<RetentionOutcome, StorageError> {
        if !policy.is_enabled() {
            return Ok(RetentionOutcome {
                deleted_events: 0,
                deleted_replay_ids: 0,
                deleted_bytes: 0,
                first_retained_sequence: self.first_sequence()?,
                high_watermark: self.last_sequence()?,
            });
        }
        if policy.delete_batch_size == 0 {
            return Err(StorageError::InvalidRetentionPolicy(
                "delete_batch_size must be greater than zero".to_owned(),
            ));
        }

        let _write = mutex(&self.write_gate);
        self.rotate_aged_active(now_ms)?;
        let descriptors = read_state(&self.state).descriptors.clone();
        let total_events = descriptors
            .iter()
            .map(|segment| segment.event_count)
            .sum::<u64>();
        let total_bytes = descriptors.iter().try_fold(0u64, |total, segment| {
            let bytes = fs::metadata(&segment.path)?.len();
            Ok::<_, StorageError>(total.saturating_add(bytes))
        })?;
        let age_cutoff = policy.max_age.map(|max_age| {
            let max_age_ms = max_age.as_millis().min(i64::MAX as u128) as i64;
            now_ms.saturating_sub(max_age_ms)
        });
        let mut remaining_events = total_events;
        let mut remaining_bytes = total_bytes;
        let mut candidates = Vec::new();
        let mut selected_events = 0u64;

        for descriptor in descriptors.iter().filter(|segment| segment.sealed) {
            let exceeds_count = policy
                .max_events
                .is_some_and(|maximum| remaining_events > maximum);
            let exceeds_bytes = policy
                .max_bytes
                .is_some_and(|maximum| remaining_bytes > maximum);
            let exceeds_age = age_cutoff.is_some_and(|cutoff| {
                descriptor
                    .last_timestamp_ms
                    .is_some_and(|timestamp| timestamp <= cutoff)
            });
            if !exceeds_count && !exceeds_bytes && !exceeds_age {
                break;
            }
            candidates.push(descriptor.clone());
            remaining_events = remaining_events.saturating_sub(descriptor.event_count);
            remaining_bytes = remaining_bytes.saturating_sub(fs::metadata(&descriptor.path)?.len());
            selected_events = selected_events.saturating_add(descriptor.event_count);
            if selected_events >= policy.delete_batch_size as u64 {
                break;
            }
        }

        let mut deleted_events = 0u64;
        let mut deleted_replay_ids = 0u64;
        let mut deleted_bytes = 0u64;
        for candidate in candidates {
            let candidate_bytes = fs::metadata(&candidate.path)?.len();
            self.delete_segment(&candidate)?;
            deleted_events = deleted_events.saturating_add(candidate.event_count);
            deleted_replay_ids = deleted_replay_ids.saturating_add(candidate.replay_id_count);
            deleted_bytes = deleted_bytes.saturating_add(candidate_bytes);
        }

        Ok(RetentionOutcome {
            deleted_events,
            deleted_replay_ids,
            deleted_bytes,
            first_retained_sequence: self.first_sequence()?,
            high_watermark: self.last_sequence()?,
        })
    }

    pub(crate) fn stats(&self) -> Result<StoreStats, StorageError> {
        let state = read_state(&self.state);
        let event_count = state
            .descriptors
            .iter()
            .map(|segment| segment.event_count)
            .sum();
        let replay_id_count = state
            .descriptors
            .iter()
            .map(|segment| segment.replay_id_count)
            .sum();
        let segment_count = state.descriptors.len() as u64;
        let sealed_segment_count = state
            .descriptors
            .iter()
            .filter(|segment| segment.sealed)
            .count() as u64;
        let source_offset_count = match state.active.as_ref() {
            Some(active) => {
                let read = active.begin_read().map_err(redb_error)?;
                let offsets = read.open_table(SOURCE_OFFSETS).map_err(redb_error)?;
                offsets.len().map_err(redb_error)?
            }
            None => 0,
        };
        drop(state);

        let read = self.control.begin_read().map_err(redb_error)?;
        let offsets = read.open_table(CONSUMER_OFFSETS).map_err(redb_error)?;
        let consumer_offset_count = offsets.len().map_err(redb_error)?;
        Ok(StoreStats {
            event_count,
            replay_id_count,
            source_offset_count,
            consumer_offset_count,
            segment_count,
            sealed_segment_count,
        })
    }

    pub(crate) fn source_offsets(&self) -> Result<Vec<SourceOffset>, StorageError> {
        let (active, _descriptor) = self.active_segment()?;
        read_source_offsets(&active)
    }

    pub(crate) fn consumer_offsets(&self) -> Result<Vec<ConsumerOffset>, StorageError> {
        let read = self.control.begin_read().map_err(redb_error)?;
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

    pub(crate) fn set_source_offset(
        &self,
        source_name: &str,
        lsn: &str,
    ) -> Result<(), StorageError> {
        let _write = mutex(&self.write_gate);
        self.ensure_active_segment()?;
        self.persist_checkpoint_only(source_name, lsn)
    }

    pub(crate) fn source_offset(&self, source_name: &str) -> Result<Option<String>, StorageError> {
        let state = read_state(&self.state);
        for descriptor in state.descriptors.iter().rev() {
            let database = if !descriptor.sealed {
                state
                    .active
                    .as_ref()
                    .cloned()
                    .ok_or(StorageError::MissingActiveSegment)?
            } else {
                self.sealed_database(descriptor)?
            };
            let read = database.begin_read().map_err(redb_error)?;
            let offsets = read.open_table(SOURCE_OFFSETS).map_err(redb_error)?;
            if let Some(offset) = offsets.get(source_name).map_err(redb_error)? {
                return Ok(Some(offset.value().to_owned()));
            }
        }
        Ok(None)
    }

    pub(crate) fn bind_source_identity(
        &self,
        source_name: &str,
        identity: &SourceIdentity,
    ) -> Result<(), StorageError> {
        let encoded = serde_json::to_vec(identity)?;
        let write = self.control.begin_write().map_err(redb_error)?;
        {
            let mut identities = write.open_table(SOURCE_IDENTITIES).map_err(redb_error)?;
            if let Some(stored) = identities.get(source_name).map_err(redb_error)? {
                let stored: SourceIdentity = serde_json::from_slice(stored.value())?;
                if stored != *identity {
                    return Err(StorageError::SourceIdentityMismatch {
                        source_name: source_name.to_owned(),
                        expected_system_identifier: stored.system_identifier,
                        expected_database_oid: stored.database_oid,
                        actual_system_identifier: identity.system_identifier.clone(),
                        actual_database_oid: identity.database_oid.clone(),
                    });
                }
            } else {
                identities
                    .insert(source_name, encoded.as_slice())
                    .map_err(redb_error)?;
            }
        }
        write.commit().map_err(redb_error)?;
        Ok(())
    }

    pub(crate) fn source_identity(
        &self,
        source_name: &str,
    ) -> Result<Option<SourceIdentity>, StorageError> {
        let read = self.control.begin_read().map_err(redb_error)?;
        let identities = read.open_table(SOURCE_IDENTITIES).map_err(redb_error)?;
        identities
            .get(source_name)
            .map_err(redb_error)?
            .map(|identity| serde_json::from_slice(identity.value()).map_err(StorageError::from))
            .transpose()
    }

    pub(crate) fn set_consumer_offset(
        &self,
        stream_name: &str,
        consumer_name: &str,
        last_acknowledged_sequence: u64,
    ) -> Result<(), StorageError> {
        let key = consumer_offset_key(stream_name, consumer_name);
        let write = self.control.begin_write().map_err(redb_error)?;
        {
            let mut offsets = write.open_table(CONSUMER_OFFSETS).map_err(redb_error)?;
            offsets
                .insert(key.as_str(), last_acknowledged_sequence)
                .map_err(redb_error)?;
        }
        write.commit().map_err(redb_error)?;
        Ok(())
    }

    pub(crate) fn acknowledge_consumer_offset(
        &self,
        stream_name: &str,
        consumer_name: &str,
        acknowledged_sequence: u64,
    ) -> Result<u64, StorageError> {
        let key = consumer_offset_key(stream_name, consumer_name);
        let write = self.control.begin_write().map_err(redb_error)?;
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

    pub(crate) fn consumer_offset(
        &self,
        stream_name: &str,
        consumer_name: &str,
    ) -> Result<Option<u64>, StorageError> {
        let key = consumer_offset_key(stream_name, consumer_name);
        let read = self.control.begin_read().map_err(redb_error)?;
        let offsets = read.open_table(CONSUMER_OFFSETS).map_err(redb_error)?;
        Ok(offsets
            .get(key.as_str())
            .map_err(redb_error)?
            .map(|value| value.value()))
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

    fn count_duplicate_transactions(
        &self,
        transactions: &[SourceTransaction<'_>],
        source_name: &str,
    ) -> Result<ReplayDuplicateCounts, StorageError> {
        let replay_ids = transactions
            .iter()
            .map(|transaction| transaction_replay_id(source_name, transaction.source_lsn))
            .collect::<Vec<_>>();
        let mut legacy_event_ids = Vec::with_capacity(transactions.len());
        for transaction in transactions {
            let mut candidates = HashMap::<String, usize>::new();
            for event in transaction.events.iter()? {
                let event_id = event?.event_id;
                *candidates.entry(event_id).or_default() += 1;
            }
            legacy_event_ids.push(candidates);
        }

        let mut replayed = vec![false; transactions.len()];
        let mut matched_legacy_ids = vec![HashSet::<String>::new(); transactions.len()];
        let state = read_state(&self.state);
        for descriptor in state.descriptors.iter().rev() {
            let database = if !descriptor.sealed {
                state
                    .active
                    .as_ref()
                    .cloned()
                    .ok_or(StorageError::MissingActiveSegment)?
            } else {
                self.sealed_database(descriptor)?
            };
            let read = database.begin_read().map_err(redb_error)?;
            let replay_table = read.open_table(REPLAY_IDS).map_err(redb_error)?;
            for (index, replay_id) in replay_ids.iter().enumerate() {
                if !replayed[index]
                    && replay_table
                        .get(replay_id.as_str())
                        .map_err(redb_error)?
                        .is_some()
                {
                    replayed[index] = true;
                }
                if replayed[index] {
                    continue;
                }
                for event_id in legacy_event_ids[index].keys() {
                    if !matched_legacy_ids[index].contains(event_id)
                        && replay_table
                            .get(event_id.as_str())
                            .map_err(redb_error)?
                            .is_some()
                    {
                        matched_legacy_ids[index].insert(event_id.clone());
                    }
                }
            }
            drop(replay_table);
            drop(read);
            if replayed.iter().all(|duplicate| *duplicate) {
                break;
            }
        }
        drop(state);

        let mut duplicates = ReplayDuplicateCounts::default();
        for (index, transaction) in transactions.iter().enumerate() {
            if replayed[index] {
                duplicates.transactions = duplicates.transactions.saturating_add(1);
                duplicates.events = duplicates.events.saturating_add(transaction.events.len());
                continue;
            }
            let matched_events = matched_legacy_ids[index]
                .iter()
                .map(|event_id| legacy_event_ids[index].get(event_id).copied().unwrap_or(0))
                .sum::<usize>();
            duplicates.events = duplicates.events.saturating_add(matched_events);
            if !transaction.events.is_empty() && matched_events == transaction.events.len() {
                duplicates.transactions = duplicates.transactions.saturating_add(1);
            }
        }
        Ok(duplicates)
    }

    fn event_id_exists(&self, event_id: &str) -> Result<bool, StorageError> {
        let state = read_state(&self.state);
        for descriptor in state.descriptors.iter().rev() {
            let database = if !descriptor.sealed {
                state
                    .active
                    .as_ref()
                    .cloned()
                    .ok_or(StorageError::MissingActiveSegment)?
            } else {
                self.sealed_database(descriptor)?
            };
            let read = database.begin_read().map_err(redb_error)?;
            let event_ids = read.open_table(REPLAY_IDS).map_err(redb_error)?;
            if event_ids.get(event_id).map_err(redb_error)?.is_some() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn sequence_exists(&self, sequence: u64) -> Result<bool, StorageError> {
        let state = read_state(&self.state);
        for descriptor in &state.descriptors {
            if descriptor
                .first_sequence
                .zip(descriptor.last_sequence)
                .is_some_and(|(first, last)| sequence >= first && sequence <= last)
            {
                let database = if !descriptor.sealed {
                    state
                        .active
                        .as_ref()
                        .cloned()
                        .ok_or(StorageError::MissingActiveSegment)?
                } else {
                    self.sealed_database(descriptor)?
                };
                let read = database.begin_read().map_err(redb_error)?;
                let events = read.open_table(EVENTS).map_err(redb_error)?;
                return Ok(events.get(sequence).map_err(redb_error)?.is_some());
            }
        }
        Ok(false)
    }

    fn source_offset_from_active(&self, source_name: &str) -> Result<Option<String>, StorageError> {
        let (active, _descriptor) = self.active_segment()?;
        let read = active.begin_read().map_err(redb_error)?;
        let offsets = read.open_table(SOURCE_OFFSETS).map_err(redb_error)?;
        Ok(offsets
            .get(source_name)
            .map_err(redb_error)?
            .map(|offset| offset.value().to_owned()))
    }

    fn persist_checkpoint_only(
        &self,
        source_name: &str,
        source_lsn: &str,
    ) -> Result<(), StorageError> {
        let (active, _descriptor) = self.active_segment()?;
        let write = active.begin_write().map_err(redb_error)?;
        {
            let mut offsets = write.open_table(SOURCE_OFFSETS).map_err(redb_error)?;
            offsets
                .insert(source_name, source_lsn)
                .map_err(redb_error)?;
        }
        write.commit().map_err(redb_error)?;
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

    fn retention_floor(&self) -> Result<Option<u64>, StorageError> {
        let read = self.control.begin_read().map_err(redb_error)?;
        let metadata = read.open_table(METADATA).map_err(redb_error)?;
        Ok(metadata
            .get(RETENTION_FLOOR_KEY)
            .map_err(redb_error)?
            .map(|value| value.value()))
    }

    fn delete_segment(&self, descriptor: &SegmentDescriptor) -> Result<(), StorageError> {
        // Keep catalog readers from opening this path between its rename and removal.
        let mut state = write_state(&self.state);
        mutex(&self.cache).remove(descriptor.id);
        let deleting_path = deleting_segment_path(&descriptor.path)?;
        fs::rename(&descriptor.path, &deleting_path)?;
        sync_directory(&self.segments_dir)?;

        let next_first = state
            .descriptors
            .iter()
            .filter(|candidate| candidate.id != descriptor.id)
            .find_map(|candidate| candidate.first_sequence)
            .or_else(|| {
                state
                    .descriptors
                    .last()
                    .and_then(|candidate| candidate.high_watermark)
                    .map(|high| high.saturating_add(1))
            });
        if let Err(error) = self.advance_retention_floor(next_first) {
            let _ = fs::rename(&deleting_path, &descriptor.path);
            return Err(error);
        }

        state
            .descriptors
            .retain(|candidate| candidate.id != descriptor.id);
        drop(state);
        match fs::remove_file(&deleting_path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        sync_directory(&self.segments_dir)?;
        Ok(())
    }

    fn advance_retention_floor(&self, first_available: Option<u64>) -> Result<(), StorageError> {
        let Some(first_available) = first_available else {
            return Ok(());
        };
        let write = self.control.begin_write().map_err(redb_error)?;
        {
            let mut metadata = write.open_table(METADATA).map_err(redb_error)?;
            let floor = metadata
                .get(RETENTION_FLOOR_KEY)
                .map_err(redb_error)?
                .map_or(first_available, |current| {
                    current.value().max(first_available)
                });
            metadata
                .insert(RETENTION_FLOOR_KEY, floor)
                .map_err(redb_error)?;
        }
        write.commit().map_err(redb_error)?;
        Ok(())
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

impl SegmentCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            order: VecDeque::new(),
            databases: HashMap::new(),
        }
    }

    fn get(&mut self, id: u64) -> Option<SegmentDatabase> {
        let database = self.databases.get(&id).cloned()?;
        self.order.retain(|candidate| *candidate != id);
        self.order.push_back(id);
        Some(database)
    }

    fn insert(&mut self, id: u64, database: SegmentDatabase) {
        self.remove(id);
        self.order.push_back(id);
        self.databases.insert(id, database);
        while self.databases.len() > self.capacity {
            if let Some(expired) = self.order.pop_front() {
                self.databases.remove(&expired);
            }
        }
    }

    fn remove(&mut self, id: u64) {
        self.order.retain(|candidate| *candidate != id);
        self.databases.remove(&id);
    }
}

impl Deref for DeferredDatabase {
    type Target = Database;

    fn deref(&self) -> &Self::Target {
        self.database
            .as_ref()
            .expect("segment database remains available until its final handle drops")
    }
}

impl Drop for DeferredDatabase {
    fn drop(&mut self) {
        let Some(database) = self.database.take() else {
            return;
        };
        mutex(&self.pending_closes.paths).insert(self.path.clone());
        if let Err(error) = self.reaper.send(RetiredDatabase {
            path: self.path.clone(),
            database,
        }) {
            drop(error.0.database);
            self.pending_closes.finish(&self.path);
        }
    }
}

impl DatabaseReaper {
    fn start() -> Result<Self, StorageError> {
        let (sender, receiver) = mpsc::channel::<RetiredDatabase>();
        let pending_closes = Arc::new(PendingCloses::default());
        let thread_pending_closes = Arc::clone(&pending_closes);
        let thread = thread::Builder::new()
            .name("lightcdc-segment-reaper".to_owned())
            .spawn(move || {
                while let Ok(retired) = receiver.recv() {
                    drop(retired.database);
                    thread_pending_closes.finish(&retired.path);
                }
            })?;
        Ok(Self {
            sender: Some(sender),
            pending_closes,
            thread: Some(thread),
        })
    }

    fn wrap(&self, database: Database, path: PathBuf) -> SegmentDatabase {
        Arc::new(DeferredDatabase {
            database: Some(database),
            path,
            reaper: self
                .sender
                .as_ref()
                .expect("segment reaper remains available while the store is open")
                .clone(),
            pending_closes: Arc::clone(&self.pending_closes),
        })
    }

    fn wait_until_closed(&self, path: &Path) {
        let mut pending = mutex(&self.pending_closes.paths);
        while pending.contains(path) {
            pending = self
                .pending_closes
                .changed
                .wait(pending)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }
}

impl Drop for DatabaseReaper {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl PendingCloses {
    fn finish(&self, path: &Path) {
        mutex(&self.paths).remove(path);
        self.changed.notify_all();
    }
}

fn initialize_or_migrate_control(
    control: &mut Database,
    segments_dir: &Path,
    options: SegmentOptions,
) -> Result<(), StorageError> {
    let format = read_metadata_value(control, CONTROL_FORMAT_KEY)?;
    match format {
        Some(CONTROL_FORMAT_VERSION) => initialize_control_tables(control),
        Some(1) => migrate_control_v1_to_v2(control),
        Some(found) if found > CONTROL_FORMAT_VERSION => {
            Err(StorageError::UnsupportedControlFormat {
                found,
                supported: CONTROL_FORMAT_VERSION,
            })
        }
        Some(found) => Err(StorageError::UnsupportedControlFormat {
            found,
            supported: CONTROL_FORMAT_VERSION,
        }),
        None if table_exists(control, "events")? => {
            fs::create_dir_all(segments_dir)?;
            migrate_legacy_store(control, segments_dir, options)
        }
        None => initialize_control_tables(control),
    }
}

fn initialize_control_tables(control: &Database) -> Result<(), StorageError> {
    let write = control.begin_write().map_err(redb_error)?;
    {
        write.open_table(CONSUMER_OFFSETS).map_err(redb_error)?;
        write.open_table(SOURCE_IDENTITIES).map_err(redb_error)?;
        let mut metadata = write.open_table(METADATA).map_err(redb_error)?;
        metadata
            .insert(CONTROL_FORMAT_KEY, CONTROL_FORMAT_VERSION)
            .map_err(redb_error)?;
    }
    write.commit().map_err(redb_error)?;
    Ok(())
}

fn migrate_control_v1_to_v2(control: &Database) -> Result<(), StorageError> {
    let write = control.begin_write().map_err(redb_error)?;
    {
        write.open_table(CONSUMER_OFFSETS).map_err(redb_error)?;
        write.open_table(SOURCE_IDENTITIES).map_err(redb_error)?;
        let mut metadata = write.open_table(METADATA).map_err(redb_error)?;
        metadata
            .insert(CONTROL_FORMAT_KEY, CONTROL_FORMAT_VERSION)
            .map_err(redb_error)?;
    }
    write.commit().map_err(redb_error)?;
    Ok(())
}

fn migrate_legacy_store(
    control: &mut Database,
    segments_dir: &Path,
    options: SegmentOptions,
) -> Result<(), StorageError> {
    let target = segment_path(segments_dir, 1);
    if !target.exists() {
        copy_legacy_segment(control, segments_dir, options)?;
    } else {
        let descriptor = load_segment_descriptor(&target, options.sealed_cache_bytes)?;
        if descriptor.id != 1 {
            return Err(StorageError::InvalidSegmentCatalog(
                "legacy migration segment has an unexpected id".to_owned(),
            ));
        }
    }

    let write = control.begin_write().map_err(redb_error)?;
    {
        write.open_table(CONSUMER_OFFSETS).map_err(redb_error)?;
        write.open_table(SOURCE_IDENTITIES).map_err(redb_error)?;
        let mut metadata = write.open_table(METADATA).map_err(redb_error)?;
        metadata
            .insert(CONTROL_FORMAT_KEY, CONTROL_FORMAT_VERSION)
            .map_err(redb_error)?;
        drop(metadata);
        write.delete_table(EVENTS).map_err(redb_error)?;
        write.delete_table(REPLAY_IDS).map_err(redb_error)?;
        write.delete_table(SOURCE_OFFSETS).map_err(redb_error)?;
        write
            .delete_table(SOURCE_REPLAY_FLOORS)
            .map_err(redb_error)?;
    }
    write.commit().map_err(redb_error)?;
    control.compact().map_err(redb_error)?;
    Ok(())
}

fn copy_legacy_segment(
    legacy: &Database,
    segments_dir: &Path,
    options: SegmentOptions,
) -> Result<(), StorageError> {
    let temp = temporary_segment_path(segments_dir, 1);
    if temp.exists() {
        fs::remove_file(&temp)?;
    }
    let segment = open_database(&temp, options.active_cache_bytes, true)?;
    let created_at_ms = unix_timestamp_ms();
    let mut descriptor = SegmentDescriptor {
        id: 1,
        path: segment_path(segments_dir, 1),
        sealed: false,
        created_at_ms,
        event_count: 0,
        replay_id_count: 0,
        stored_bytes: 0,
        first_sequence: None,
        last_sequence: None,
        high_watermark: None,
        first_timestamp_ms: None,
        last_timestamp_ms: None,
    };

    let legacy_read = legacy.begin_read().map_err(redb_error)?;
    let legacy_events = legacy_read.open_table(EVENTS).map_err(redb_error)?;
    let legacy_event_ids = legacy_read.open_table(REPLAY_IDS).map_err(redb_error)?;
    let legacy_offsets = legacy_read.open_table(SOURCE_OFFSETS).map_err(redb_error)?;
    let legacy_replay_floors = legacy_read
        .open_table(SOURCE_REPLAY_FLOORS)
        .map_err(redb_error)?;
    let legacy_metadata = legacy_read.open_table(METADATA).map_err(redb_error)?;

    let write = segment.begin_write().map_err(redb_error)?;
    {
        let mut events = write.open_table(EVENTS).map_err(redb_error)?;
        let mut event_ids = write.open_table(REPLAY_IDS).map_err(redb_error)?;
        let mut offsets = write.open_table(SOURCE_OFFSETS).map_err(redb_error)?;
        let mut replay_floors = write.open_table(SOURCE_REPLAY_FLOORS).map_err(redb_error)?;
        let mut metadata = write.open_table(METADATA).map_err(redb_error)?;

        for entry in legacy_events.iter().map_err(redb_error)? {
            let (sequence, payload) = entry.map_err(redb_error)?;
            let event: ChangeEvent = serde_json::from_slice(payload.value())?;
            events
                .insert(sequence.value(), payload.value())
                .map_err(redb_error)?;
            descriptor.record_event(&event, payload.value().len() as u64);
        }
        for entry in legacy_event_ids.iter().map_err(redb_error)? {
            let (event_id, sequence) = entry.map_err(redb_error)?;
            event_ids
                .insert(event_id.value(), sequence.value())
                .map_err(redb_error)?;
        }
        for entry in legacy_offsets.iter().map_err(redb_error)? {
            let (source, lsn) = entry.map_err(redb_error)?;
            offsets
                .insert(source.value(), lsn.value())
                .map_err(redb_error)?;
        }
        for entry in legacy_replay_floors.iter().map_err(redb_error)? {
            let (source, sequence) = entry.map_err(redb_error)?;
            replay_floors
                .insert(source.value(), sequence.value())
                .map_err(redb_error)?;
        }
        descriptor.replay_id_count = legacy_event_ids.len().map_err(redb_error)?;
        descriptor.high_watermark = legacy_metadata
            .get(EVENT_HIGH_WATERMARK_KEY)
            .map_err(redb_error)?
            .map(|value| value.value())
            .or(descriptor.last_sequence);
        initialize_segment_metadata(&mut metadata, &descriptor)?;
    }
    write.commit().map_err(redb_error)?;
    drop(segment);
    fs::rename(temp, descriptor.path)?;
    sync_directory(segments_dir)?;
    Ok(())
}

fn create_segment(
    segments_dir: &Path,
    id: u64,
    source_offsets: &[SourceOffset],
    high_watermark: Option<u64>,
    cache_bytes: usize,
) -> Result<SegmentDescriptor, StorageError> {
    let path = segment_path(segments_dir, id);
    if path.exists() {
        return Err(StorageError::InvalidSegmentCatalog(format!(
            "segment file already exists: {}",
            path.display()
        )));
    }
    let temp = temporary_segment_path(segments_dir, id);
    if temp.exists() {
        fs::remove_file(&temp)?;
    }
    let database = open_database(&temp, cache_bytes, true)?;
    let descriptor = SegmentDescriptor {
        id,
        path: path.clone(),
        sealed: false,
        created_at_ms: unix_timestamp_ms(),
        event_count: 0,
        replay_id_count: 0,
        stored_bytes: 0,
        first_sequence: None,
        last_sequence: None,
        high_watermark,
        first_timestamp_ms: None,
        last_timestamp_ms: None,
    };
    let write = database.begin_write().map_err(redb_error)?;
    {
        write.open_table(EVENTS).map_err(redb_error)?;
        write.open_table(REPLAY_IDS).map_err(redb_error)?;
        let mut offsets = write.open_table(SOURCE_OFFSETS).map_err(redb_error)?;
        write.open_table(SOURCE_REPLAY_FLOORS).map_err(redb_error)?;
        let mut metadata = write.open_table(METADATA).map_err(redb_error)?;
        for offset in source_offsets {
            offsets
                .insert(offset.source_name.as_str(), offset.lsn.as_str())
                .map_err(redb_error)?;
        }
        initialize_segment_metadata(&mut metadata, &descriptor)?;
    }
    write.commit().map_err(redb_error)?;
    drop(database);
    fs::rename(temp, path)?;
    sync_directory(segments_dir)?;
    Ok(descriptor)
}

fn initialize_segment_metadata(
    metadata: &mut redb::Table<'_, &str, u64>,
    descriptor: &SegmentDescriptor,
) -> Result<(), StorageError> {
    metadata
        .insert(SEGMENT_FORMAT_KEY, SEGMENT_FORMAT_VERSION)
        .map_err(redb_error)?;
    metadata
        .insert(SEGMENT_ID_KEY, descriptor.id)
        .map_err(redb_error)?;
    metadata
        .insert(SEGMENT_SEALED_KEY, u64::from(descriptor.sealed))
        .map_err(redb_error)?;
    metadata
        .insert(
            SEGMENT_CREATED_AT_MS_KEY,
            encode_i64(descriptor.created_at_ms),
        )
        .map_err(redb_error)?;
    write_segment_statistics(metadata, descriptor)
}

fn write_segment_statistics(
    metadata: &mut redb::Table<'_, &str, u64>,
    descriptor: &SegmentDescriptor,
) -> Result<(), StorageError> {
    metadata
        .insert(SEGMENT_EVENT_COUNT_KEY, descriptor.event_count)
        .map_err(redb_error)?;
    metadata
        .insert(SEGMENT_REPLAY_ID_COUNT_KEY, descriptor.replay_id_count)
        .map_err(redb_error)?;
    metadata
        .insert(SEGMENT_STORED_BYTES_KEY, descriptor.stored_bytes)
        .map_err(redb_error)?;
    insert_optional_metadata(
        metadata,
        EVENT_HIGH_WATERMARK_KEY,
        descriptor.high_watermark,
    )?;
    insert_optional_metadata(
        metadata,
        SEGMENT_FIRST_TIMESTAMP_KEY,
        descriptor.first_timestamp_ms.map(encode_i64),
    )?;
    insert_optional_metadata(
        metadata,
        SEGMENT_LAST_TIMESTAMP_KEY,
        descriptor.last_timestamp_ms.map(encode_i64),
    )?;
    Ok(())
}

fn insert_optional_metadata(
    metadata: &mut redb::Table<'_, &str, u64>,
    key: &str,
    value: Option<u64>,
) -> Result<(), StorageError> {
    match value {
        Some(value) => {
            metadata.insert(key, value).map_err(redb_error)?;
        }
        None => {
            drop(metadata.remove(key).map_err(redb_error)?);
        }
    }
    Ok(())
}

fn load_segment_descriptors(
    segments_dir: &Path,
    options: SegmentOptions,
) -> Result<Vec<SegmentDescriptor>, StorageError> {
    let mut descriptors = Vec::new();
    for entry in fs::read_dir(segments_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && parse_segment_id(&path).is_some() {
            descriptors.push(load_segment_descriptor(&path, options.sealed_cache_bytes)?);
        }
    }
    descriptors.sort_by_key(|segment| segment.id);
    Ok(descriptors)
}

fn load_segment_descriptor(
    path: &Path,
    cache_bytes: usize,
) -> Result<SegmentDescriptor, StorageError> {
    let database = open_database(path, cache_bytes, false)?;
    let read = database.begin_read().map_err(redb_error)?;
    let events = read.open_table(EVENTS).map_err(redb_error)?;
    let event_ids = read.open_table(REPLAY_IDS).map_err(redb_error)?;
    let metadata = read.open_table(METADATA).map_err(redb_error)?;
    let format = required_metadata(&metadata, SEGMENT_FORMAT_KEY)?;
    if format != SEGMENT_FORMAT_VERSION {
        return Err(StorageError::UnsupportedSegmentFormat {
            path: path.to_path_buf(),
            found: format,
            supported: SEGMENT_FORMAT_VERSION,
        });
    }
    let id = required_metadata(&metadata, SEGMENT_ID_KEY)?;
    if parse_segment_id(path) != Some(id) {
        return Err(StorageError::InvalidSegmentCatalog(format!(
            "segment id {id} does not match filename {}",
            path.display()
        )));
    }
    Ok(SegmentDescriptor {
        id,
        path: path.to_path_buf(),
        sealed: required_metadata(&metadata, SEGMENT_SEALED_KEY)? != 0,
        created_at_ms: decode_i64(required_metadata(&metadata, SEGMENT_CREATED_AT_MS_KEY)?),
        event_count: events.len().map_err(redb_error)?,
        replay_id_count: event_ids.len().map_err(redb_error)?,
        stored_bytes: optional_metadata(&metadata, SEGMENT_STORED_BYTES_KEY)?.unwrap_or(0),
        first_sequence: events
            .first()
            .map_err(redb_error)?
            .map(|(sequence, _)| sequence.value()),
        last_sequence: events
            .last()
            .map_err(redb_error)?
            .map(|(sequence, _)| sequence.value()),
        high_watermark: optional_metadata(&metadata, EVENT_HIGH_WATERMARK_KEY)?,
        first_timestamp_ms: optional_metadata(&metadata, SEGMENT_FIRST_TIMESTAMP_KEY)?
            .map(decode_i64),
        last_timestamp_ms: optional_metadata(&metadata, SEGMENT_LAST_TIMESTAMP_KEY)?
            .map(decode_i64),
    })
}

fn validate_segment_catalog(descriptors: &[SegmentDescriptor]) -> Result<(), StorageError> {
    let mut ids = HashSet::new();
    let mut active_count = 0usize;
    let mut previous_last = None;
    for (index, descriptor) in descriptors.iter().enumerate() {
        if !ids.insert(descriptor.id) {
            return Err(StorageError::InvalidSegmentCatalog(format!(
                "segment id {} appears more than once",
                descriptor.id
            )));
        }
        if !descriptor.sealed {
            active_count += 1;
            if index + 1 != descriptors.len() {
                return Err(StorageError::InvalidSegmentCatalog(
                    "only the newest segment may be active".to_owned(),
                ));
            }
        }
        if let (Some(previous), Some(first)) = (previous_last, descriptor.first_sequence)
            && first <= previous
        {
            return Err(StorageError::InvalidSegmentCatalog(format!(
                "segment {} overlaps the preceding sequence range",
                descriptor.id
            )));
        }
        previous_last = descriptor.last_sequence.or(previous_last);
    }
    if active_count > 1 {
        return Err(StorageError::InvalidSegmentCatalog(
            "more than one segment is active".to_owned(),
        ));
    }
    Ok(())
}

fn read_source_offsets(database: &Database) -> Result<Vec<SourceOffset>, StorageError> {
    let read = database.begin_read().map_err(redb_error)?;
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

fn open_database(path: &Path, cache_bytes: usize, create: bool) -> Result<Database, StorageError> {
    let mut builder = Database::builder();
    builder.set_cache_size(cache_bytes);
    if create {
        builder.create(path).map_err(redb_error)
    } else {
        builder.open(path).map_err(redb_error)
    }
}

fn read_metadata_value(database: &Database, key: &str) -> Result<Option<u64>, StorageError> {
    if !table_exists(database, "metadata")? {
        return Ok(None);
    }
    let read = database.begin_read().map_err(redb_error)?;
    let metadata = read.open_table(METADATA).map_err(redb_error)?;
    Ok(metadata
        .get(key)
        .map_err(redb_error)?
        .map(|value| value.value()))
}

fn table_exists(database: &Database, name: &str) -> Result<bool, StorageError> {
    let read = database.begin_read().map_err(redb_error)?;
    Ok(read
        .list_tables()
        .map_err(redb_error)?
        .any(|table| table.name() == name))
}

fn required_metadata(
    metadata: &impl ReadableTable<&'static str, u64>,
    key: &'static str,
) -> Result<u64, StorageError> {
    optional_metadata(metadata, key)?.ok_or_else(|| {
        StorageError::InvalidSegmentCatalog(format!("segment metadata key {key:?} is missing"))
    })
}

fn optional_metadata(
    metadata: &impl ReadableTable<&'static str, u64>,
    key: &'static str,
) -> Result<Option<u64>, StorageError> {
    Ok(metadata
        .get(key)
        .map_err(redb_error)?
        .map(|value| value.value()))
}

fn cleanup_interrupted_segment_files(
    segments_dir: &Path,
    control: &Database,
    options: SegmentOptions,
) -> Result<(), StorageError> {
    let retention_floor = read_metadata_value(control, RETENTION_FLOOR_KEY)?;
    for entry in fs::read_dir(segments_dir)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.starts_with(SEGMENT_FILE_PREFIX) && name.ends_with(SEGMENT_TEMP_SUFFIX) {
            fs::remove_file(path)?;
            continue;
        }
        if let Some(id) = parse_interrupted_segment_id(name, SEGMENT_DELETING_SUFFIX) {
            let database = open_database(&path, options.sealed_cache_bytes, false)?;
            let read = database.begin_read().map_err(redb_error)?;
            let events = read.open_table(EVENTS).map_err(redb_error)?;
            let last_sequence = events
                .last()
                .map_err(redb_error)?
                .map(|(sequence, _)| sequence.value());
            drop(events);
            drop(read);
            drop(database);
            if retention_floor.is_some_and(|floor| {
                last_sequence.is_none_or(|last_sequence| last_sequence < floor)
            }) {
                fs::remove_file(path)?;
            } else {
                let restored = segment_path(segments_dir, id);
                if restored.exists() {
                    fs::remove_file(path)?;
                } else {
                    fs::rename(path, restored)?;
                }
            }
        }
    }
    Ok(())
}

fn segment_directory(control_path: &Path) -> Result<PathBuf, StorageError> {
    let file_name = control_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            StorageError::InvalidSegmentCatalog(format!(
                "control database path has no UTF-8 filename: {}",
                control_path.display()
            ))
        })?;
    Ok(control_path.with_file_name(format!("{file_name}.segments")))
}

fn control_format_marker_path(control_path: &Path) -> Result<PathBuf, StorageError> {
    let file_name = control_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            StorageError::InvalidSegmentCatalog(format!(
                "control database path has no UTF-8 filename: {}",
                control_path.display()
            ))
        })?;
    Ok(control_path.with_file_name(format!("{file_name}{CONTROL_FORMAT_MARKER_SUFFIX}")))
}

fn read_control_format_marker(control_path: &Path) -> Result<Option<u64>, StorageError> {
    let marker = control_format_marker_path(control_path)?;
    let raw = match fs::read_to_string(&marker) {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let version = raw
        .trim()
        .strip_prefix(CONTROL_FORMAT_MARKER_PREFIX)
        .and_then(|version| version.parse::<u64>().ok())
        .ok_or(StorageError::InvalidFormatMarker(marker))?;
    Ok(Some(version))
}

fn write_control_format_marker(control_path: &Path, version: u64) -> Result<(), StorageError> {
    let marker = control_format_marker_path(control_path)?;
    let temporary = marker.with_extension("format.tmp");
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(format!("{CONTROL_FORMAT_MARKER_PREFIX}{version}\n").as_bytes())?;
    file.sync_all()?;
    drop(file);
    fs::rename(temporary, marker)?;
    if let Some(parent) = control_path.parent() {
        sync_directory(parent)?;
    }
    Ok(())
}

fn segment_path(segments_dir: &Path, id: u64) -> PathBuf {
    segments_dir.join(format!(
        "{SEGMENT_FILE_PREFIX}{id:020}{SEGMENT_FILE_SUFFIX}"
    ))
}

fn temporary_segment_path(segments_dir: &Path, id: u64) -> PathBuf {
    segments_dir.join(format!(
        "{SEGMENT_FILE_PREFIX}{id:020}{SEGMENT_TEMP_SUFFIX}"
    ))
}

fn deleting_segment_path(path: &Path) -> Result<PathBuf, StorageError> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            StorageError::InvalidSegmentCatalog(format!(
                "segment path has no UTF-8 filename: {}",
                path.display()
            ))
        })?;
    let stem = name.strip_suffix(SEGMENT_FILE_SUFFIX).ok_or_else(|| {
        StorageError::InvalidSegmentCatalog(format!(
            "segment path has an invalid suffix: {}",
            path.display()
        ))
    })?;
    Ok(path.with_file_name(format!("{stem}{SEGMENT_DELETING_SUFFIX}")))
}

fn parse_segment_id(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    name.strip_prefix(SEGMENT_FILE_PREFIX)?
        .strip_suffix(SEGMENT_FILE_SUFFIX)?
        .parse()
        .ok()
}

fn next_segment_id(id: u64) -> Result<u64, StorageError> {
    id.checked_add(1).ok_or_else(|| {
        StorageError::InvalidSegmentCatalog("segment id space is exhausted".to_owned())
    })
}

fn parse_interrupted_segment_id(name: &str, suffix: &str) -> Option<u64> {
    name.strip_prefix(SEGMENT_FILE_PREFIX)?
        .strip_suffix(suffix)?
        .parse()
        .ok()
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<(), StorageError> {
    fs::File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<(), StorageError> {
    Ok(())
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
mod tests {
    use lightcdc_core::{Operation, SourceMetadata};
    use tempfile::TempDir;

    use super::*;
    use crate::{TransactionEvents, TransactionStats};

    #[test]
    fn replay_crosses_segments_and_survives_reopen() {
        let temp = TempDir::new().expect("temp dir");
        let control_path = temp.path().join("events.redb");
        let options = test_segment_options(2);
        let store =
            SegmentStore::open(control_path.clone(), options).expect("open segmented store");

        for sequence in 1..=5 {
            store
                .append_event(&event(sequence))
                .expect("append segmented event");
        }

        assert_eq!(
            store
                .replay_from(2, 4)
                .expect("replay across segments")
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            [2, 3, 4, 5]
        );
        let stats = store.stats().expect("segmented stats");
        assert_eq!(stats.segment_count, 3);
        assert_eq!(stats.sealed_segment_count, 2);
        drop(store);

        let reopened = SegmentStore::open(control_path, options).expect("reopen segmented store");
        assert_eq!(
            reopened
                .replay_from(1, 10)
                .expect("replay reopened segments")
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            [1, 2, 3, 4, 5]
        );
        assert_eq!(reopened.next_sequence().expect("next sequence"), 6);
    }

    #[test]
    fn oversized_source_transaction_stays_in_one_segment() {
        let temp = TempDir::new().expect("temp dir");
        let store = SegmentStore::open(temp.path().join("events.redb"), test_segment_options(2))
            .expect("open segmented store");
        let events = (1..=3).map(event).collect::<Vec<_>>();
        let transaction = TransactionEvents::InMemory {
            events,
            stats: TransactionStats {
                event_count: 3,
                decoded_bytes: 300,
                staged_bytes: 300,
            },
        };

        store
            .persist_transaction_batch(
                &[SourceTransaction {
                    events: &transaction,
                    source_lsn: "0/10",
                }],
                "default",
                true,
                || Ok(()),
            )
            .expect("persist oversized transaction");

        let state = read_state(&store.state);
        assert_eq!(state.descriptors.len(), 2);
        assert!(state.descriptors[0].sealed);
        assert_eq!(state.descriptors[0].event_count, 3);
        assert_eq!(state.descriptors[0].first_sequence, Some(1));
        assert_eq!(state.descriptors[0].last_sequence, Some(3));
        assert_eq!(state.descriptors[1].event_count, 0);
        assert!(!state.descriptors[1].sealed);
    }

    #[test]
    fn serialized_byte_limit_rotates_without_splitting_an_event() {
        let temp = TempDir::new().expect("temp dir");
        let mut options = test_segment_options(100);
        options.max_bytes = 1;
        let store = SegmentStore::open(temp.path().join("events.redb"), options)
            .expect("open segmented store");

        store
            .append_event(&event(1))
            .expect("append oversized event");

        let state = read_state(&store.state);
        assert_eq!(state.descriptors.len(), 2);
        assert!(state.descriptors[0].sealed);
        assert_eq!(state.descriptors[0].event_count, 1);
        assert_eq!(state.descriptors[0].first_sequence, Some(1));
        assert_eq!(state.descriptors[0].last_sequence, Some(1));
        assert!(!state.descriptors[1].sealed);
        assert_eq!(state.descriptors[1].event_count, 0);
    }

    #[test]
    fn elapsed_age_seals_a_nonempty_active_segment() {
        let temp = TempDir::new().expect("temp dir");
        let store = SegmentStore::open(temp.path().join("events.redb"), test_segment_options(100))
            .expect("open segmented store");
        store.append_event(&event(1)).expect("append event");
        let created_at_ms = read_state(&store.state).descriptors[0].created_at_ms;

        store
            .rotate_aged_active(created_at_ms + 60_000)
            .expect("rotate aged segment");

        let state = read_state(&store.state);
        assert_eq!(state.descriptors.len(), 2);
        assert!(state.descriptors[0].sealed);
        assert_eq!(state.descriptors[0].event_count, 1);
        assert!(!state.descriptors[1].sealed);
        assert_eq!(state.descriptors[1].event_count, 0);
    }

    #[test]
    fn legacy_single_file_store_migrates_into_a_segment() {
        let temp = TempDir::new().expect("temp dir");
        let control_path = temp.path().join("events.redb");
        create_legacy_store(&control_path);

        let store = SegmentStore::open(control_path.clone(), test_segment_options(10))
            .expect("migrate legacy store");

        assert_eq!(
            store.replay_from(1, 10).expect("replay migrated events"),
            [event(1)]
        );
        assert_eq!(
            store.source_offset("default").expect("source offset"),
            Some("0/1".to_owned())
        );
        assert_eq!(
            store
                .consumer_offset("orders", "search")
                .expect("consumer offset"),
            Some(1)
        );
        assert_eq!(store.stats().expect("stats").segment_count, 1);
        assert!(!table_exists(&store.control, "events").expect("list control tables"));
        drop(store);

        let reopened = SegmentStore::open(control_path, test_segment_options(10))
            .expect("reopen migrated store");
        assert_eq!(reopened.replay_from(1, 10).expect("replay"), [event(1)]);
    }

    #[test]
    fn newer_control_format_is_rejected_without_mutation() {
        let temp = TempDir::new().expect("temp dir");
        let control_path = temp.path().join("events.redb");
        let database = Database::create(&control_path).expect("create future control");
        let write = database.begin_write().expect("begin future format");
        {
            let mut metadata = write.open_table(METADATA).expect("metadata");
            metadata
                .insert(CONTROL_FORMAT_KEY, CONTROL_FORMAT_VERSION + 1)
                .expect("future format");
        }
        write.commit().expect("commit future format");
        drop(database);
        write_control_format_marker(&control_path, CONTROL_FORMAT_VERSION + 1)
            .expect("write future format marker");
        let before = fs::read(&control_path).expect("read control before rejection");

        let error = match SegmentStore::open(control_path.clone(), test_segment_options(10)) {
            Ok(_) => panic!("future format must be rejected"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            StorageError::UnsupportedControlFormat {
                found: 3,
                supported: 2
            }
        ));
        assert_eq!(
            fs::read(&control_path).expect("read control after rejection"),
            before
        );
        assert!(
            !segment_directory(&control_path)
                .expect("segment directory")
                .exists()
        );
    }

    #[test]
    fn version_one_control_store_migrates_source_identity_table() {
        let temp = TempDir::new().expect("temp dir");
        let control_path = temp.path().join("events.redb");
        let database = Database::create(&control_path).expect("create v1 control");
        let write = database.begin_write().expect("begin v1 control");
        {
            write
                .open_table(CONSUMER_OFFSETS)
                .expect("consumer offsets");
            let mut metadata = write.open_table(METADATA).expect("metadata");
            metadata.insert(CONTROL_FORMAT_KEY, 1).expect("v1 format");
        }
        write.commit().expect("commit v1 control");
        drop(database);
        write_control_format_marker(&control_path, 1).expect("write v1 marker");

        let store = SegmentStore::open(control_path.clone(), test_segment_options(10))
            .expect("migrate v1 control");
        store
            .bind_source_identity(
                "default",
                &SourceIdentity {
                    system_identifier: "7412345678901234567".to_owned(),
                    database_oid: "16384".to_owned(),
                },
            )
            .expect("write identity after migration");

        assert_eq!(
            read_control_format_marker(&control_path).expect("read marker"),
            Some(CONTROL_FORMAT_VERSION)
        );
    }

    #[test]
    fn interrupted_uncommitted_segment_deletion_is_rolled_back() {
        let temp = TempDir::new().expect("temp dir");
        let control_path = temp.path().join("events.redb");
        let options = test_segment_options(1);
        let store =
            SegmentStore::open(control_path.clone(), options).expect("open segmented store");
        store.append_event(&event(1)).expect("append first segment");
        let first_path = read_state(&store.state).descriptors[0].path.clone();
        drop(store);

        let deleting = deleting_segment_path(&first_path).expect("deleting path");
        fs::rename(&first_path, &deleting).expect("simulate interrupted deletion");

        let reopened =
            SegmentStore::open(control_path, options).expect("recover interrupted deletion");
        assert_eq!(
            reopened.replay_from(1, 1).expect("replay restored"),
            [event(1)]
        );
        assert!(first_path.exists());
        assert!(!deleting.exists());
    }

    fn create_legacy_store(path: &Path) {
        let database = Database::create(path).expect("create legacy database");
        let write = database.begin_write().expect("begin legacy write");
        {
            let payload = serde_json::to_vec(&event(1)).expect("serialize legacy event");
            let mut events = write.open_table(EVENTS).expect("legacy events");
            events.insert(1, payload.as_slice()).expect("legacy event");
            let mut event_ids = write.open_table(REPLAY_IDS).expect("legacy ids");
            event_ids.insert("event-1", 1).expect("legacy id");
            let mut source_offsets = write
                .open_table(SOURCE_OFFSETS)
                .expect("legacy source offsets");
            source_offsets
                .insert("default", "0/1")
                .expect("legacy source offset");
            write
                .open_table(SOURCE_REPLAY_FLOORS)
                .expect("legacy replay floors");
            let mut consumer_offsets = write
                .open_table(CONSUMER_OFFSETS)
                .expect("legacy consumer offsets");
            consumer_offsets
                .insert(consumer_offset_key("orders", "search").as_str(), 1)
                .expect("legacy consumer offset");
            let mut metadata = write.open_table(METADATA).expect("legacy metadata");
            metadata
                .insert(EVENT_HIGH_WATERMARK_KEY, 1)
                .expect("legacy high watermark");
        }
        write.commit().expect("commit legacy store");
    }

    fn test_segment_options(max_events: u64) -> SegmentOptions {
        SegmentOptions {
            max_events,
            max_bytes: 1024 * 1024,
            max_age: Duration::from_secs(60),
            sealed_cache_capacity: 2,
            active_cache_bytes: 1024 * 1024,
            sealed_cache_bytes: 1024 * 1024,
        }
    }

    fn event(sequence: u64) -> ChangeEvent {
        ChangeEvent {
            sequence,
            event_id: format!("event-{sequence}"),
            source: SourceMetadata {
                database: "lightcdc".to_owned(),
                slot: "slot".to_owned(),
                lsn: format!("0/{sequence:X}"),
            },
            transaction: None,
            schema: "public".to_owned(),
            table: "orders".to_owned(),
            operation: Operation::Insert,
            key: None,
            before: None,
            after: Some(format!(r#"{{"id":{sequence}}}"#).into_bytes()),
            commit_timestamp_ms: Some(1_784_862_080_000 + sequence as i64),
        }
    }
}
