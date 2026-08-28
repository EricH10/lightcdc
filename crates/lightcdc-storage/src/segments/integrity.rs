//! Traverses segment and control data to verify persisted metadata and ordering.

use std::fs;

use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata};

use crate::log::{IntegrityReport, SourceIdentity, StorageError, StoreStats};

use super::catalog::{decode_event, required_metadata};
use super::{
    CONSUMER_OFFSETS, EVENTS, METADATA, REPLAY_IDS, SEGMENT_EVENT_COUNT_KEY,
    SEGMENT_REPLAY_ID_COUNT_KEY, SEGMENT_STORED_BYTES_KEY, SOURCE_IDENTITIES, SOURCE_OFFSETS,
    SegmentStore, read_state, redb_error,
};

impl SegmentStore {
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

    pub(crate) fn verify_integrity(&self) -> Result<IntegrityReport, StorageError> {
        let snapshots = {
            let state = read_state(&self.state);
            let mut snapshots = Vec::with_capacity(state.descriptors.len());
            for descriptor in &state.descriptors {
                let database = if descriptor.sealed {
                    self.sealed_database(descriptor)?
                } else {
                    state
                        .active
                        .as_ref()
                        .cloned()
                        .ok_or(StorageError::MissingActiveSegment)?
                };
                snapshots.push((descriptor.clone(), database));
            }
            snapshots
        };

        let mut event_count = 0u64;
        let mut replay_id_count = 0u64;
        let mut segment_file_bytes = 0u64;
        let mut first_sequence: Option<u64> = None;
        let mut previous_sequence: Option<u64> = None;

        for (descriptor, database) in snapshots {
            segment_file_bytes =
                segment_file_bytes.saturating_add(fs::metadata(&descriptor.path)?.len());
            let read = database.begin_read().map_err(redb_error)?;
            let events = read.open_table(EVENTS).map_err(redb_error)?;
            let replay_ids = read.open_table(REPLAY_IDS).map_err(redb_error)?;
            let source_offsets = read.open_table(SOURCE_OFFSETS).map_err(redb_error)?;
            let metadata = read.open_table(METADATA).map_err(redb_error)?;

            let actual_event_count = events.len().map_err(redb_error)?;
            let actual_replay_id_count = replay_ids.len().map_err(redb_error)?;
            validate_integrity_value(
                descriptor.id,
                "event count",
                required_metadata(&metadata, SEGMENT_EVENT_COUNT_KEY)?,
                actual_event_count,
            )?;
            validate_integrity_value(
                descriptor.id,
                "replay marker count",
                required_metadata(&metadata, SEGMENT_REPLAY_ID_COUNT_KEY)?,
                actual_replay_id_count,
            )?;

            let mut segment_payload_bytes = 0u64;
            let mut segment_first = None;
            let mut segment_last = None;
            for entry in events.iter().map_err(redb_error)? {
                let (stored_sequence, payload) = entry.map_err(redb_error)?;
                let stored_sequence = stored_sequence.value();
                let event = decode_event(payload.value())?;
                if event.sequence != stored_sequence {
                    return Err(StorageError::Integrity(format!(
                        "segment {} key sequence {stored_sequence} does not match payload sequence {}",
                        descriptor.id, event.sequence
                    )));
                }
                if let Some(previous) = previous_sequence
                    && stored_sequence != previous.saturating_add(1)
                {
                    return Err(StorageError::Integrity(format!(
                        "retained event sequence jumps from {previous} to {stored_sequence}"
                    )));
                }
                first_sequence.get_or_insert(stored_sequence);
                segment_first.get_or_insert(stored_sequence);
                segment_last = Some(stored_sequence);
                previous_sequence = Some(stored_sequence);
                segment_payload_bytes =
                    segment_payload_bytes.saturating_add(payload.value().len() as u64);
            }
            if descriptor.first_sequence != segment_first
                || descriptor.last_sequence != segment_last
            {
                return Err(StorageError::Integrity(format!(
                    "segment {} sequence range metadata does not match its event table",
                    descriptor.id
                )));
            }
            validate_integrity_value(
                descriptor.id,
                "stored payload bytes",
                required_metadata(&metadata, SEGMENT_STORED_BYTES_KEY)?,
                segment_payload_bytes,
            )?;

            for entry in replay_ids.iter().map_err(redb_error)? {
                let (replay_id, _) = entry.map_err(redb_error)?;
                if replay_id.value().is_empty() {
                    return Err(StorageError::Integrity(format!(
                        "segment {} contains an empty replay marker",
                        descriptor.id
                    )));
                }
            }
            for entry in source_offsets.iter().map_err(redb_error)? {
                let (source, lsn) = entry.map_err(redb_error)?;
                if source.value().is_empty() || lsn.value().is_empty() {
                    return Err(StorageError::Integrity(format!(
                        "segment {} contains an empty source checkpoint",
                        descriptor.id
                    )));
                }
            }
            event_count = event_count.saturating_add(actual_event_count);
            replay_id_count = replay_id_count.saturating_add(actual_replay_id_count);
        }

        // These readers validate durable control-table encodings as well as redb pages.
        let _ = self.consumer_offsets()?;
        let control_read = self.control.begin_read().map_err(redb_error)?;
        let identities = control_read
            .open_table(SOURCE_IDENTITIES)
            .map_err(redb_error)?;
        for entry in identities.iter().map_err(redb_error)? {
            let (source, encoded) = entry.map_err(redb_error)?;
            if source.value().is_empty() {
                return Err(StorageError::Integrity(
                    "control store contains an empty source identity key".to_owned(),
                ));
            }
            let identity: SourceIdentity = serde_json::from_slice(encoded.value())?;
            if identity.system_identifier.is_empty() || identity.database_oid.is_empty() {
                return Err(StorageError::Integrity(format!(
                    "source {:?} has an incomplete physical identity",
                    source.value()
                )));
            }
        }

        Ok(IntegrityReport {
            segment_count: self.stats()?.segment_count,
            event_count,
            replay_id_count,
            segment_file_bytes,
            first_sequence,
            high_watermark: self.last_sequence()?,
        })
    }
}

fn validate_integrity_value(
    segment_id: u64,
    name: &str,
    recorded: u64,
    actual: u64,
) -> Result<(), StorageError> {
    if recorded != actual {
        return Err(StorageError::Integrity(format!(
            "segment {segment_id} {name} metadata is {recorded}, actual value is {actual}"
        )));
    }
    Ok(())
}
