//! Reads ordered events across segments and detects replayed source transactions.

use std::collections::{HashMap, HashSet};

use lightcdc_core::ChangeEvent;
use redb::ReadableDatabase;

use crate::log::{SourceTransaction, StorageError};

use super::catalog::decode_event;
use super::{EVENTS, REPLAY_IDS, SegmentStore, read_state, redb_error, transaction_replay_id};

#[derive(Default)]
pub(super) struct ReplayDuplicateCounts {
    pub(super) transactions: usize,
    pub(super) events: usize,
}

impl SegmentStore {
    pub(crate) fn replay_from(
        &self,
        sequence: u64,
        limit: usize,
        max_bytes: u64,
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
        let mut output_bytes = 0u64;
        for (_descriptor, database) in snapshots {
            let read = database.begin_read().map_err(redb_error)?;
            let events = read.open_table(EVENTS).map_err(redb_error)?;
            for entry in events.range(next_sequence..).map_err(redb_error)? {
                let (stored_sequence, payload) = entry.map_err(redb_error)?;
                let payload_bytes = payload.value().len() as u64;
                if !output.is_empty() && output_bytes.saturating_add(payload_bytes) > max_bytes {
                    return Ok(output);
                }
                let event = decode_event(payload.value())?;
                next_sequence = stored_sequence.value().saturating_add(1);
                output_bytes = output_bytes.saturating_add(payload_bytes);
                output.push(event);
                if output.len() >= limit {
                    return Ok(output);
                }
            }
        }
        Ok(output)
    }

    pub(super) fn count_duplicate_transactions(
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

    pub(super) fn event_id_exists(&self, event_id: &str) -> Result<bool, StorageError> {
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

    pub(super) fn sequence_exists(&self, sequence: u64) -> Result<bool, StorageError> {
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
}
