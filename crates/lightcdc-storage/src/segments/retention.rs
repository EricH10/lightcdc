//! Applies retention by retiring complete sealed segment files.

use std::fs;

use redb::{ReadableDatabase, ReadableTable};

use crate::log::{RetentionOutcome, RetentionPolicy, StorageError};

use super::catalog::{deleting_segment_path, segment_format_marker_path, sync_directory};
use super::{
    METADATA, RETENTION_FLOOR_KEY, SegmentDescriptor, SegmentStore, mutex, read_state, redb_error,
    write_state,
};

impl SegmentStore {
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

    pub(super) fn retention_floor(&self) -> Result<Option<u64>, StorageError> {
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
        match fs::remove_file(segment_format_marker_path(&descriptor.path)?) {
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
