//! Builds and runs the PostgreSQL capture subsystem.

use std::{net::SocketAddr, path::PathBuf, time::Duration};

use anyhow::Context;
use lightcdc_api::EventNotifier;
use lightcdc_core::{CapturePlan, Config};
use lightcdc_storage::{RedbEventStore, TransactionBufferOptions};
use tracing::{info, warn};

use self::{
    metrics::CaptureMetrics,
    supervisor::{
        capture_batch_limits, capture_heartbeat_interval, capture_retention, run_retention_sweeps,
        supervise_capture,
    },
    writer::{CaptureBatchLimits, CaptureStorageWriter},
};
use crate::{
    cli::CaptureOptions,
    logging::init_logging,
    store::{open_event_store, storage_options},
};

mod metrics;
mod pipeline;
mod supervisor;
mod writer;

/// Borrows immutable capture dependencies shared by the supervision functions.
struct CaptureContext<'a> {
    config: &'a Config,
    store: &'a RedbEventStore,
    storage_writer: &'a CaptureStorageWriter,
    options: &'a CaptureOptions,
    metrics: &'a Option<CaptureMetrics>,
    event_notifier: &'a Option<EventNotifier>,
    source_name: &'a str,
    capture_plan: &'a CapturePlan,
    heartbeat_interval: Duration,
    batch_limits: CaptureBatchLimits,
    transaction_buffer_options: TransactionBufferOptions,
}

/// Runs capture only and writes changes into the local event store.
pub(crate) async fn capture(config_path: PathBuf, options: CaptureOptions) -> anyhow::Result<()> {
    let config = Config::from_path(&config_path)
        .with_context(|| format!("could not load config from {}", config_path.display()))?;

    init_logging(&config.logging.level)?;

    info!(
        config = %config_path.display(),
        source = %config.source.redacted_connection_string(),
        "loaded lightcdc config"
    );

    let store = open_event_store(&config)?;
    capture_with_store(config, store, options, None).await
}

/// Runs capture and the gRPC server in one process sharing one event store.
pub(crate) async fn run(
    config_path: PathBuf,
    addr: SocketAddr,
    options: CaptureOptions,
) -> anyhow::Result<()> {
    let config = Config::from_path(&config_path)
        .with_context(|| format!("could not load config from {}", config_path.display()))?;

    init_logging(&config.logging.level)?;

    info!(
        config = %config_path.display(),
        source = %config.source.redacted_connection_string(),
        addr = %addr,
        "loaded lightcdc config"
    );

    let store = open_event_store(&config)?;
    let server_config = config.clone();
    let server_store = store.clone();
    let event_notifier = EventNotifier::new();
    let server_notifier = event_notifier.clone();
    let mut server = tokio::spawn(async move {
        lightcdc_api::serve_with_notifier(addr, server_config, server_store, server_notifier).await
    });
    let capture = capture_with_store(config, store, options, Some(event_notifier));
    tokio::pin!(capture);

    tokio::select! {
        result = &mut capture => {
            server.abort();
            match server.await {
                Err(error) if error.is_cancelled() => {}
                Err(error) => return Err(error).context("gRPC server task failed"),
                Ok(Err(error)) => return Err(error).context("gRPC server failed"),
                Ok(Ok(())) => {}
            }
            result
        }
        server_result = &mut server => {
            match server_result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(error).context("gRPC server failed"),
                Err(error) => Err(error).context("gRPC server task failed"),
            }
        }
    }
}

/// Captures PostgreSQL changes into an already-opened event store.
async fn capture_with_store(
    config: Config,
    store: RedbEventStore,
    options: CaptureOptions,
    event_notifier: Option<EventNotifier>,
) -> anyhow::Result<()> {
    let storage = storage_options(&config);
    let source_name = config.source.name.clone();
    let capture_plan = config
        .capture_plan()
        .context("failed to compile configured stream table selection")?;
    let heartbeat_interval = capture_heartbeat_interval(&config)?;
    let batch_limits = capture_batch_limits(&config)?;
    let retention = capture_retention(&config)?;
    let transaction_buffer_options = TransactionBufferOptions::bounded(
        storage.data_dir.join("staging"),
        &source_name,
        config.runtime.transaction_memory_threshold_bytes,
        config.runtime.max_transaction_bytes,
        config.runtime.max_transaction_events,
    );
    let metrics = options
        .metrics_file
        .as_deref()
        .map(|path| CaptureMetrics::start(path, options.metrics_interval))
        .transpose()
        .context("failed to start capture metrics writer")?;

    info!(
        data_dir = %storage.data_dir.display(),
        database_file = %storage.database_file,
        transaction_memory_threshold_bytes = config.runtime.transaction_memory_threshold_bytes,
        max_transaction_bytes = config.runtime.max_transaction_bytes,
        max_transaction_events = config.runtime.max_transaction_events,
        capture_batch_max_transactions = batch_limits.max_transactions,
        capture_batch_max_events = batch_limits.max_events,
        capture_batch_max_bytes = batch_limits.max_bytes,
        capture_batch_max_delay_ms = batch_limits.max_delay.as_millis(),
        segment_max_events = config.runtime.segment_max_events,
        segment_max_bytes = config.runtime.segment_max_bytes,
        segment_max_age_seconds = config.runtime.segment_max_age_seconds,
        retention_max_events = ?retention.and_then(|retention| retention.policy.max_events),
        retention_max_age_seconds = ?retention
            .and_then(|retention| retention.policy.max_age)
            .map(|max_age| max_age.as_secs()),
        heartbeat_interval_ms = heartbeat_interval.as_millis(),
        event_output = ?options.output,
        metrics_file = ?options.metrics_file,
        "opened local event store"
    );

    warn!(
        stop_after_events = ?options.stop_after_events,
        "capture is running; insert, update, or delete rows in the published tables"
    );

    let storage_writer = CaptureStorageWriter::start(store.clone(), source_name.clone())?;
    let retention_task = retention
        .map(|retention| tokio::spawn(run_retention_sweeps(storage_writer.handle(), retention)));
    let capture_result = supervise_capture(CaptureContext {
        config: &config,
        store: &store,
        storage_writer: &storage_writer,
        options: &options,
        metrics: &metrics,
        event_notifier: &event_notifier,
        source_name: &source_name,
        capture_plan: &capture_plan,
        heartbeat_interval,
        batch_limits,
        transaction_buffer_options,
    })
    .await;
    if let Some(retention_task) = retention_task {
        retention_task.abort();
        if let Err(error) = retention_task.await
            && !error.is_cancelled()
        {
            warn!(%error, "retention task stopped unexpectedly");
        }
    }
    capture_result
}
