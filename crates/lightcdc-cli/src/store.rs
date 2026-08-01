//! Opens the configured local redb event store for CLI commands.

use std::{path::PathBuf, time::Duration};

use anyhow::Context;
use lightcdc_core::Config;
use lightcdc_runtime::StorageResourceLimits;
use lightcdc_storage::{LogOpenOptions, RedbEventStore, SegmentOptions};

/// Builds storage open options from the runtime config.
pub(crate) fn storage_options(config: &Config) -> LogOpenOptions {
    LogOpenOptions {
        data_dir: PathBuf::from(&config.runtime.data_dir),
        database_file: config.runtime.storage_file.clone(),
    }
}

/// Builds hard disk-growth limits enforced immediately before capture commits.
pub(crate) fn storage_resource_limits(config: &Config) -> StorageResourceLimits {
    StorageResourceLimits {
        data_dir: PathBuf::from(&config.runtime.data_dir),
        max_storage_bytes: config.runtime.max_storage_bytes,
        min_free_disk_bytes: config.runtime.min_free_disk_bytes,
    }
}

/// Builds sequence-segment rotation limits from runtime configuration.
pub(crate) fn segment_options(config: &Config) -> SegmentOptions {
    SegmentOptions {
        max_events: config.runtime.segment_max_events,
        max_bytes: config.runtime.segment_max_bytes,
        max_age: Duration::from_secs(config.runtime.segment_max_age_seconds),
        ..SegmentOptions::default()
    }
}

/// Opens the configured redb event store.
pub(crate) fn open_event_store(config: &Config) -> anyhow::Result<RedbEventStore> {
    let storage = storage_options(config);
    RedbEventStore::open_with_segment_options(&storage, segment_options(config))
        .context("failed to open local segmented redb event store")
}
