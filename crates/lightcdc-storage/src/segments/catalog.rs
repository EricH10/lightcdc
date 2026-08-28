//! Opens, creates, validates, and migrates the durable segment-file catalog.

use std::{
    collections::HashSet,
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use lightcdc_core::ChangeEvent;
use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableHandle};

use crate::log::{SegmentOptions, SourceOffset, StorageError};

use super::{
    CONSUMER_OFFSETS, CONTROL_FORMAT_KEY, CONTROL_FORMAT_MARKER_PREFIX,
    CONTROL_FORMAT_MARKER_SUFFIX, CONTROL_FORMAT_VERSION, EVENT_FORMAT_MARKER_PREFIX,
    EVENT_HIGH_WATERMARK_KEY, EVENT_PAYLOAD_FORMAT_KEY, EVENT_PAYLOAD_FORMAT_VERSION, EVENTS,
    METADATA, REPLAY_IDS, RETENTION_FLOOR_KEY, SEGMENT_CREATED_AT_MS_KEY, SEGMENT_DELETING_SUFFIX,
    SEGMENT_EVENT_COUNT_KEY, SEGMENT_FILE_PREFIX, SEGMENT_FILE_SUFFIX, SEGMENT_FIRST_TIMESTAMP_KEY,
    SEGMENT_FORMAT_KEY, SEGMENT_FORMAT_MARKER_PREFIX, SEGMENT_FORMAT_VERSION, SEGMENT_ID_KEY,
    SEGMENT_LAST_TIMESTAMP_KEY, SEGMENT_REPLAY_ID_COUNT_KEY, SEGMENT_SEALED_KEY,
    SEGMENT_STORED_BYTES_KEY, SEGMENT_TEMP_SUFFIX, SOURCE_IDENTITIES, SOURCE_OFFSETS,
    SOURCE_REPLAY_FLOORS, SegmentDescriptor, SegmentFormatMarker, StoredEvent, StoredEventRef,
    decode_i64, encode_i64, redb_error, unix_timestamp_ms,
};

pub(super) fn initialize_or_migrate_control(
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
            let encoded = encode_event(&event)?;
            events
                .insert(sequence.value(), encoded.as_slice())
                .map_err(redb_error)?;
            descriptor.record_event(&event, encoded.len() as u64);
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

pub(super) fn create_segment(
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
    write_segment_format_marker(
        &descriptor.path,
        SEGMENT_FORMAT_VERSION,
        EVENT_PAYLOAD_FORMAT_VERSION,
    )?;
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
        .insert(EVENT_PAYLOAD_FORMAT_KEY, EVENT_PAYLOAD_FORMAT_VERSION)
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

pub(super) fn write_segment_statistics(
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

/// Rejects unknown segment and event formats before opening any redb file.
pub(super) fn preflight_segment_formats(segments_dir: &Path) -> Result<(), StorageError> {
    if !segments_dir.exists() {
        return Ok(());
    }
    for path in segment_paths(segments_dir)? {
        if let Some(marker) = read_segment_format_marker(&path)? {
            if !matches!(marker.segment, SEGMENT_FORMAT_VERSION | 1) {
                return Err(StorageError::UnsupportedSegmentFormat {
                    path,
                    found: marker.segment,
                    supported: SEGMENT_FORMAT_VERSION,
                });
            }
            if marker.segment == SEGMENT_FORMAT_VERSION
                && marker.event != EVENT_PAYLOAD_FORMAT_VERSION
            {
                return Err(StorageError::UnsupportedEventPayloadFormat {
                    found: marker.event,
                    supported: EVENT_PAYLOAD_FORMAT_VERSION,
                });
            }
        }
    }
    Ok(())
}

/// Atomically wraps legacy bare event JSON in the current payload envelope.
pub(super) fn migrate_segment_formats(
    segments_dir: &Path,
    options: SegmentOptions,
) -> Result<(), StorageError> {
    let current = SegmentFormatMarker {
        segment: SEGMENT_FORMAT_VERSION,
        event: EVENT_PAYLOAD_FORMAT_VERSION,
    };
    for path in segment_paths(segments_dir)? {
        let marker = read_segment_format_marker(&path)?;
        let database = open_database(&path, options.active_cache_bytes, false)?;
        match read_metadata_value(&database, SEGMENT_FORMAT_KEY)? {
            Some(1) => migrate_segment_v1_to_v2(&database)?,
            Some(SEGMENT_FORMAT_VERSION) => {
                let payload_format = read_metadata_value(&database, EVENT_PAYLOAD_FORMAT_KEY)?
                    .ok_or_else(|| {
                        StorageError::InvalidSegmentCatalog(format!(
                            "segment {} has no event payload format",
                            path.display()
                        ))
                    })?;
                if payload_format != EVENT_PAYLOAD_FORMAT_VERSION {
                    return Err(StorageError::UnsupportedEventPayloadFormat {
                        found: payload_format,
                        supported: EVENT_PAYLOAD_FORMAT_VERSION,
                    });
                }
            }
            Some(found) => {
                return Err(StorageError::UnsupportedSegmentFormat {
                    path,
                    found,
                    supported: SEGMENT_FORMAT_VERSION,
                });
            }
            None => {
                return Err(StorageError::InvalidSegmentCatalog(format!(
                    "segment {} has no format version",
                    path.display()
                )));
            }
        }
        drop(database);
        if marker != Some(current) {
            write_segment_format_marker(
                &path,
                SEGMENT_FORMAT_VERSION,
                EVENT_PAYLOAD_FORMAT_VERSION,
            )?;
        }
    }
    Ok(())
}

fn migrate_segment_v1_to_v2(database: &Database) -> Result<(), StorageError> {
    let read = database.begin_read().map_err(redb_error)?;
    let legacy_events = read.open_table(EVENTS).map_err(redb_error)?;
    let write = database.begin_write().map_err(redb_error)?;
    let mut stored_bytes = 0u64;
    {
        let mut events = write.open_table(EVENTS).map_err(redb_error)?;
        for entry in legacy_events.iter().map_err(redb_error)? {
            let (sequence, payload) = entry.map_err(redb_error)?;
            let event: ChangeEvent = serde_json::from_slice(payload.value())?;
            let encoded = encode_event(&event)?;
            stored_bytes = stored_bytes.saturating_add(encoded.len() as u64);
            events
                .insert(sequence.value(), encoded.as_slice())
                .map_err(redb_error)?;
        }
        let mut metadata = write.open_table(METADATA).map_err(redb_error)?;
        metadata
            .insert(SEGMENT_FORMAT_KEY, SEGMENT_FORMAT_VERSION)
            .map_err(redb_error)?;
        metadata
            .insert(EVENT_PAYLOAD_FORMAT_KEY, EVENT_PAYLOAD_FORMAT_VERSION)
            .map_err(redb_error)?;
        metadata
            .insert(SEGMENT_STORED_BYTES_KEY, stored_bytes)
            .map_err(redb_error)?;
    }
    write.commit().map_err(redb_error)?;
    Ok(())
}

fn segment_paths(segments_dir: &Path) -> Result<Vec<PathBuf>, StorageError> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(segments_dir)? {
        let path = entry?.path();
        if path.is_file() && parse_segment_id(&path).is_some() {
            paths.push(path);
        }
    }
    paths.sort_by_key(|path| parse_segment_id(path));
    Ok(paths)
}

pub(super) fn load_segment_descriptors(
    segments_dir: &Path,
    options: SegmentOptions,
) -> Result<Vec<SegmentDescriptor>, StorageError> {
    let mut descriptors = Vec::new();
    for path in segment_paths(segments_dir)? {
        descriptors.push(load_segment_descriptor(&path, options.sealed_cache_bytes)?);
    }
    descriptors.sort_by_key(|segment| segment.id);
    Ok(descriptors)
}

pub(super) fn load_segment_descriptor(
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
    let event_payload_format = required_metadata(&metadata, EVENT_PAYLOAD_FORMAT_KEY)?;
    if event_payload_format != EVENT_PAYLOAD_FORMAT_VERSION {
        return Err(StorageError::UnsupportedEventPayloadFormat {
            found: event_payload_format,
            supported: EVENT_PAYLOAD_FORMAT_VERSION,
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

pub(super) fn validate_segment_catalog(
    descriptors: &[SegmentDescriptor],
) -> Result<(), StorageError> {
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

pub(super) fn read_source_offsets(database: &Database) -> Result<Vec<SourceOffset>, StorageError> {
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

pub(super) fn encode_event(event: &ChangeEvent) -> Result<Vec<u8>, StorageError> {
    Ok(serde_json::to_vec(&StoredEventRef {
        version: EVENT_PAYLOAD_FORMAT_VERSION,
        event,
    })?)
}

pub(super) fn decode_event(payload: &[u8]) -> Result<ChangeEvent, StorageError> {
    let stored: StoredEvent = serde_json::from_slice(payload)?;
    if stored.version != EVENT_PAYLOAD_FORMAT_VERSION {
        return Err(StorageError::UnsupportedEventPayloadFormat {
            found: stored.version,
            supported: EVENT_PAYLOAD_FORMAT_VERSION,
        });
    }
    Ok(stored.event)
}

pub(super) fn open_database(
    path: &Path,
    cache_bytes: usize,
    create: bool,
) -> Result<Database, StorageError> {
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

pub(super) fn table_exists(database: &Database, name: &str) -> Result<bool, StorageError> {
    let read = database.begin_read().map_err(redb_error)?;
    Ok(read
        .list_tables()
        .map_err(redb_error)?
        .any(|table| table.name() == name))
}

pub(super) fn required_metadata(
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

pub(super) fn cleanup_interrupted_segment_files(
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
                let original = segment_path(segments_dir, id);
                match fs::remove_file(segment_format_marker_path(&original)?) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
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

pub(super) fn segment_directory(control_path: &Path) -> Result<PathBuf, StorageError> {
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

pub(super) fn read_control_format_marker(control_path: &Path) -> Result<Option<u64>, StorageError> {
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

pub(super) fn write_control_format_marker(
    control_path: &Path,
    version: u64,
) -> Result<(), StorageError> {
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

pub(super) fn segment_format_marker_path(segment_path: &Path) -> Result<PathBuf, StorageError> {
    let name = segment_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            StorageError::InvalidSegmentCatalog(format!(
                "segment path has no UTF-8 filename: {}",
                segment_path.display()
            ))
        })?;
    Ok(segment_path.with_file_name(format!("{name}.format")))
}

fn read_segment_format_marker(
    segment_path: &Path,
) -> Result<Option<SegmentFormatMarker>, StorageError> {
    let marker = segment_format_marker_path(segment_path)?;
    let raw = match fs::read_to_string(&marker) {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut fields = raw.trim().split(',');
    let segment = fields
        .next()
        .and_then(|field| field.strip_prefix(SEGMENT_FORMAT_MARKER_PREFIX))
        .and_then(|version| version.parse::<u64>().ok());
    let event = fields
        .next()
        .and_then(|field| field.strip_prefix(EVENT_FORMAT_MARKER_PREFIX))
        .and_then(|version| version.parse::<u64>().ok());
    if fields.next().is_some() || segment.is_none() || event.is_none() {
        return Err(StorageError::InvalidFormatMarker(marker));
    }
    Ok(Some(SegmentFormatMarker {
        segment: segment.expect("segment marker was validated"),
        event: event.expect("event marker was validated"),
    }))
}

pub(super) fn write_segment_format_marker(
    segment_path: &Path,
    segment_version: u64,
    event_version: u64,
) -> Result<(), StorageError> {
    let marker = segment_format_marker_path(segment_path)?;
    let temporary = marker.with_extension("format.tmp");
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(
        format!(
            "{SEGMENT_FORMAT_MARKER_PREFIX}{segment_version},\
             {EVENT_FORMAT_MARKER_PREFIX}{event_version}\n"
        )
        .as_bytes(),
    )?;
    file.sync_all()?;
    drop(file);
    fs::rename(temporary, marker)?;
    if let Some(parent) = segment_path.parent() {
        sync_directory(parent)?;
    }
    Ok(())
}

pub(super) fn segment_path(segments_dir: &Path, id: u64) -> PathBuf {
    segments_dir.join(format!(
        "{SEGMENT_FILE_PREFIX}{id:020}{SEGMENT_FILE_SUFFIX}"
    ))
}

fn temporary_segment_path(segments_dir: &Path, id: u64) -> PathBuf {
    segments_dir.join(format!(
        "{SEGMENT_FILE_PREFIX}{id:020}{SEGMENT_TEMP_SUFFIX}"
    ))
}

pub(super) fn deleting_segment_path(path: &Path) -> Result<PathBuf, StorageError> {
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

pub(super) fn next_segment_id(id: u64) -> Result<u64, StorageError> {
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
pub(super) fn sync_directory(path: &Path) -> Result<(), StorageError> {
    fs::File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
pub(super) fn sync_directory(_path: &Path) -> Result<(), StorageError> {
    Ok(())
}
