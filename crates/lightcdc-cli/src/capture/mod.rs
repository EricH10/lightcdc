//! Builds and runs the PostgreSQL capture subsystem.

use std::{net::SocketAddr, path::PathBuf, time::Duration};

use anyhow::Context;
use lightcdc_api::EventNotifier;
use lightcdc_core::{CapturePlan, Config};
use lightcdc_runtime::{
    CaptureBatchLimits, CaptureStorageWriter, RuntimeState, RuntimeStateHandle,
    runtime_state_channel, shutdown_channel,
};
use lightcdc_storage::{RedbEventStore, TransactionBufferOptions};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use self::{
    metrics::CaptureMetrics,
    supervisor::{
        capture_batch_limits, capture_heartbeat_interval, capture_retention, run_retention_sweeps,
        supervise_capture,
    },
};
use crate::{
    cli::CaptureOptions,
    logging::init_logging,
    store::{open_event_store, storage_options, storage_resource_limits},
};

mod metrics;
mod pipeline;
mod supervisor;

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
    state: &'a RuntimeStateHandle,
}

/// Aborts a spawned helper when its owning capture future is cancelled.
struct AbortTask<T> {
    task: Option<JoinHandle<T>>,
}

impl<T> AbortTask<T> {
    fn new(task: JoinHandle<T>) -> Self {
        Self { task: Some(task) }
    }

    async fn abort_and_wait(mut self) -> Result<T, tokio::task::JoinError> {
        let task = self.task.take().expect("abort task is present");
        task.abort();
        task.await
    }
}

impl<T> Drop for AbortTask<T> {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

/// Runs capture only and writes changes into the local event store.
pub(crate) async fn capture(config_path: PathBuf, options: CaptureOptions) -> anyhow::Result<()> {
    let mut config = Config::from_path(&config_path)
        .with_context(|| format!("could not load config from {}", config_path.display()))?;
    config
        .resolve_source_password()
        .map_err(anyhow::Error::msg)
        .context("could not resolve PostgreSQL password")?;

    init_logging(&config.logging.level)?;

    info!(
        config = %config_path.display(),
        source = %config.source.redacted_connection_string(),
        "loaded lightcdc config"
    );

    let shutdown_timeout = shutdown_timeout(&config)?;
    let store = open_event_store(&config)?;
    let storage_writer = CaptureStorageWriter::start_with_limits(
        store.clone(),
        config.source.name.clone(),
        Some(storage_resource_limits(&config)),
    )?;
    let (state, _state_rx) = runtime_state_channel();
    let outcome = {
        let capture = capture_with_store(config, store, options, None, &storage_writer, &state);
        tokio::pin!(capture);
        tokio::select! {
            result = &mut capture => result,
            () = process_shutdown_signal() => {
                state.transition(RuntimeState::Draining, Some("process signal".to_owned()));
                info!("capture is draining after shutdown signal");
                Ok(())
            }
        }
    };
    close_storage_writer(storage_writer, shutdown_timeout).await?;
    if let Err(error) = &outcome {
        state.transition(RuntimeState::Failed, Some(error.to_string()));
    }
    outcome
}

/// Runs capture and the gRPC server in one process sharing one event store.
pub(crate) async fn run(
    config_path: PathBuf,
    addr: SocketAddr,
    options: CaptureOptions,
) -> anyhow::Result<()> {
    let mut config = Config::from_path(&config_path)
        .with_context(|| format!("could not load config from {}", config_path.display()))?;
    config
        .resolve_source_password()
        .map_err(anyhow::Error::msg)
        .context("could not resolve PostgreSQL password")?;

    init_logging(&config.logging.level)?;

    info!(
        config = %config_path.display(),
        source = %config.source.redacted_connection_string(),
        addr = %addr,
        "loaded lightcdc config"
    );

    let shutdown_timeout = shutdown_timeout(&config)?;
    let store = open_event_store(&config)?;
    let server_config = config.clone();
    let server_store = store.clone();
    let event_notifier = EventNotifier::new();
    let server_notifier = event_notifier.clone();
    let source_name = config.source.name.clone();
    let storage_writer = CaptureStorageWriter::start_with_limits(
        store.clone(),
        source_name.clone(),
        Some(storage_resource_limits(&config)),
    )?;
    let server_storage = storage_writer.handle();
    let (state, state_rx) = runtime_state_channel();
    let (shutdown, shutdown_rx) = shutdown_channel();
    let mut server = tokio::spawn(async move {
        lightcdc_api::serve_with_runtime(
            addr,
            server_config,
            server_store,
            server_notifier,
            server_storage,
            state_rx,
            shutdown_rx,
        )
        .await
    });
    enum RunExit {
        Capture(anyhow::Result<()>),
        Server(Result<anyhow::Result<()>, tokio::task::JoinError>),
        Signal,
    }

    let exit = {
        let capture = capture_with_store(
            config,
            store,
            options,
            Some(event_notifier),
            &storage_writer,
            &state,
        );
        tokio::pin!(capture);
        tokio::select! {
            result = &mut capture => {
                RunExit::Capture(result)
            }
            server_result = &mut server => {
                RunExit::Server(server_result)
            }
            () = process_shutdown_signal() => RunExit::Signal,
        }
    };

    let outcome = match exit {
        RunExit::Capture(result) => {
            if let Err(error) = &result {
                state.transition(RuntimeState::Failed, Some(error.to_string()));
            } else {
                state.transition(RuntimeState::Draining, Some("capture completed".to_owned()));
            }
            shutdown.trigger();
            await_server_shutdown(&mut server, shutdown_timeout).await?;
            result
        }
        RunExit::Server(result) => {
            let error = match result {
                Ok(Ok(())) => anyhow::anyhow!("gRPC server stopped unexpectedly"),
                Ok(Err(error)) => error.context("gRPC server failed"),
                Err(error) => anyhow::Error::new(error).context("gRPC server task failed"),
            };
            state.transition(RuntimeState::Failed, Some(error.to_string()));
            shutdown.trigger();
            Err(error)
        }
        RunExit::Signal => {
            state.transition(RuntimeState::Draining, Some("process signal".to_owned()));
            info!("runtime is draining after shutdown signal");
            shutdown.trigger();
            await_server_shutdown(&mut server, shutdown_timeout).await?;
            Ok(())
        }
    };
    close_storage_writer(storage_writer, shutdown_timeout).await?;
    outcome
}

/// Captures PostgreSQL changes into an already-opened event store.
async fn capture_with_store(
    config: Config,
    store: RedbEventStore,
    options: CaptureOptions,
    event_notifier: Option<EventNotifier>,
    storage_writer: &CaptureStorageWriter,
    state: &RuntimeStateHandle,
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
    )
    .with_min_free_disk_bytes(config.runtime.min_free_disk_bytes);
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
        max_storage_bytes = config.runtime.max_storage_bytes,
        min_free_disk_bytes = config.runtime.min_free_disk_bytes,
        capture_batch_max_transactions = batch_limits.max_transactions,
        capture_batch_max_events = batch_limits.max_events,
        capture_batch_max_bytes = batch_limits.max_bytes,
        capture_batch_max_delay_ms = batch_limits.max_delay.as_millis(),
        segment_max_events = config.runtime.segment_max_events,
        segment_max_bytes = config.runtime.segment_max_bytes,
        segment_max_age_seconds = config.runtime.segment_max_age_seconds,
        retention_max_events = ?retention.and_then(|retention| retention.policy.max_events),
        retention_max_bytes = ?retention.and_then(|retention| retention.policy.max_bytes),
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

    state.transition(RuntimeState::Starting, None);
    let capture_context = CaptureContext {
        config: &config,
        store: &store,
        storage_writer,
        options: &options,
        metrics: &metrics,
        event_notifier: &event_notifier,
        source_name: &source_name,
        capture_plan: &capture_plan,
        heartbeat_interval,
        batch_limits,
        transaction_buffer_options,
        state,
    };
    if let Some(retention) = retention {
        let capture = supervise_capture(capture_context);
        let sweeps = run_retention_sweeps(storage_writer.handle(), retention);
        tokio::pin!(capture);
        tokio::pin!(sweeps);
        tokio::select! {
            result = &mut capture => result,
            result = &mut sweeps => result.context(
                "retention stopped capture before disk growth could continue unchecked"
            ),
        }
    } else {
        supervise_capture(capture_context).await
    }
}

/// Converts the configured graceful shutdown timeout into a validated duration.
pub(crate) fn shutdown_timeout(config: &Config) -> anyhow::Result<Duration> {
    if config.runtime.shutdown_timeout_ms == 0 {
        anyhow::bail!("runtime.shutdown_timeout_ms must be greater than zero");
    }
    Ok(Duration::from_millis(config.runtime.shutdown_timeout_ms))
}

/// Listens for SIGINT on every platform and SIGTERM on Unix.
pub(crate) async fn process_shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Waits for tonic's graceful drain and aborts it after the configured limit.
pub(crate) async fn await_server_shutdown(
    server: &mut JoinHandle<anyhow::Result<()>>,
    timeout: Duration,
) -> anyhow::Result<()> {
    match tokio::time::timeout(timeout, &mut *server).await {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(error))) => Err(error).context("gRPC server failed while draining"),
        Ok(Err(error)) => Err(error).context("gRPC server task failed while draining"),
        Err(_) => {
            server.abort();
            error!(
                timeout_ms = timeout.as_millis(),
                "gRPC drain timed out; aborting server"
            );
            Ok(())
        }
    }
}

/// Closes the command channel and waits a bounded time for the redb thread.
pub(crate) async fn close_storage_writer(
    writer: CaptureStorageWriter,
    timeout: Duration,
) -> anyhow::Result<()> {
    let (finished, receiver) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("lightcdc-writer-shutdown".to_owned())
        .spawn(move || {
            drop(writer);
            let _ = finished.send(());
        })
        .context("start storage shutdown coordinator")?;

    tokio::time::timeout(timeout, receiver)
        .await
        .map_err(|_| anyhow::anyhow!("storage writer did not stop within {timeout:?}"))?
        .context("storage shutdown coordinator stopped unexpectedly")
}
