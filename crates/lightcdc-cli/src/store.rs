//! Opens the configured local redb event store for CLI commands.

use std::path::PathBuf;

use anyhow::Context;
use lightcdc_core::Config;
use lightcdc_storage::{LogOpenOptions, RedbEventStore};

/// Builds storage open options from the runtime config.
pub(crate) fn storage_options(config: &Config) -> LogOpenOptions {
    LogOpenOptions {
        data_dir: PathBuf::from(&config.runtime.data_dir),
        database_file: config.runtime.storage_file.clone(),
    }
}

/// Opens the configured redb event store.
pub(crate) fn open_event_store(config: &Config) -> anyhow::Result<RedbEventStore> {
    let storage = storage_options(config);
    RedbEventStore::open(&storage).context("failed to open local redb event store")
}
