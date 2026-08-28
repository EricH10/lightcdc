//! Persists source checkpoints, source identities, and named consumer offsets.

use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata};

use crate::log::{ConsumerOffset, SourceIdentity, SourceOffset, StorageError};

use super::catalog::read_source_offsets;
use super::{
    CONSUMER_OFFSET_SEPARATOR, CONSUMER_OFFSETS, SOURCE_IDENTITIES, SOURCE_OFFSETS, SegmentStore,
    consumer_offset_key, mutex, read_state, redb_error,
};

impl SegmentStore {
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
        self.set_consumer_offset_bounded(
            stream_name,
            consumer_name,
            last_acknowledged_sequence,
            usize::MAX,
        )
    }

    pub(crate) fn set_consumer_offset_bounded(
        &self,
        stream_name: &str,
        consumer_name: &str,
        last_acknowledged_sequence: u64,
        maximum_consumers: usize,
    ) -> Result<(), StorageError> {
        let key = consumer_offset_key(stream_name, consumer_name);
        let write = self.control.begin_write().map_err(redb_error)?;
        {
            let mut offsets = write.open_table(CONSUMER_OFFSETS).map_err(redb_error)?;
            if offsets.get(key.as_str()).map_err(redb_error)?.is_none()
                && offsets.len().map_err(redb_error)? >= maximum_consumers as u64
            {
                return Err(StorageError::ConsumerLimitReached {
                    maximum: maximum_consumers,
                });
            }
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
        self.acknowledge_consumer_offset_bounded(
            stream_name,
            consumer_name,
            acknowledged_sequence,
            usize::MAX,
        )
    }

    pub(crate) fn acknowledge_consumer_offset_bounded(
        &self,
        stream_name: &str,
        consumer_name: &str,
        acknowledged_sequence: u64,
        maximum_consumers: usize,
    ) -> Result<u64, StorageError> {
        let key = consumer_offset_key(stream_name, consumer_name);
        let write = self.control.begin_write().map_err(redb_error)?;
        let persisted_offset = {
            let mut offsets = write.open_table(CONSUMER_OFFSETS).map_err(redb_error)?;
            let current_offset = offsets
                .get(key.as_str())
                .map_err(redb_error)?
                .map(|value| value.value());
            if current_offset.is_none()
                && offsets.len().map_err(redb_error)? >= maximum_consumers as u64
            {
                return Err(StorageError::ConsumerLimitReached {
                    maximum: maximum_consumers,
                });
            }
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

    pub(super) fn source_offset_from_active(
        &self,
        source_name: &str,
    ) -> Result<Option<String>, StorageError> {
        let (active, _descriptor) = self.active_segment()?;
        let read = active.begin_read().map_err(redb_error)?;
        let offsets = read.open_table(SOURCE_OFFSETS).map_err(redb_error)?;
        Ok(offsets
            .get(source_name)
            .map_err(redb_error)?
            .map(|offset| offset.value().to_owned()))
    }

    pub(super) fn persist_checkpoint_only(
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
}
