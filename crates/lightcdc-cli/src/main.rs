use std::{
    net::SocketAddr,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, anyhow};
use clap::{Parser, Subcommand, ValueEnum};
use lightcdc_api::EventNotifier;
use lightcdc_core::{CapturePlan, ChangeEvent, Config, Operation, SourceConfig, StreamConfig};
use lightcdc_postgres::{
    CapturedTransaction, LogicalHeartbeatEmitter, PostgresError, ReplicationReader, TransactionRead,
};
use lightcdc_storage::{
    LogOpenOptions, PersistTransactionOutcome, RedbEventStore, RetentionPolicy,
    TransactionBufferOptions,
};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use crate::capture_metrics::CaptureMetrics;
use crate::capture_writer::{
    CaptureBatch, CaptureBatchLimits, CaptureStorageHandle, CaptureStorageWriter,
    PendingCaptureWrite, StorageCompletion,
};

mod capture_metrics;
mod capture_writer;

const RECONNECT_INITIAL_DELAY_MS: u64 = 250;
const RECONNECT_MAX_DELAY_MS: u64 = 15_000;
const HEARTBEAT_PREFIX: &str = "lightcdc.heartbeat";

/// Parses the top-level lightcdc command line.
#[derive(Debug, Parser)]
#[command(name = "lightcdc")]
#[command(about = "Lightweight PostgreSQL CDC runtime")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// Lists the CLI commands supported by lightcdc.
#[derive(Debug, Subcommand)]
enum Command {
    Capture {
        #[arg(short, long, default_value = "lightcdc.example.toml")]
        config: PathBuf,

        #[arg(long)]
        max_events: Option<usize>,

        #[arg(long, value_enum, default_value_t = CaptureOutput::Json)]
        output: CaptureOutput,

        #[arg(long)]
        metrics_file: Option<PathBuf>,

        #[arg(long, default_value_t = 1)]
        metrics_interval_seconds: u64,
    },

    Run {
        #[arg(short, long, default_value = "lightcdc.example.toml")]
        config: PathBuf,

        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: SocketAddr,

        #[arg(long)]
        max_events: Option<usize>,

        #[arg(long, value_enum, default_value_t = CaptureOutput::Json)]
        output: CaptureOutput,

        #[arg(long)]
        metrics_file: Option<PathBuf>,

        #[arg(long, default_value_t = 1)]
        metrics_interval_seconds: u64,
    },

    Replay {
        #[arg(short, long, default_value = "lightcdc.example.toml")]
        config: PathBuf,

        #[arg(long)]
        stream: Option<String>,

        #[arg(long, default_value_t = 1)]
        from: u64,

        #[arg(long, default_value_t = 100)]
        limit: usize,

        #[arg(long)]
        pretty: bool,
    },

    Inspect {
        #[arg(short, long, default_value = "lightcdc.example.toml")]
        config: PathBuf,

        #[arg(long, default_value_t = 20)]
        limit: usize,

        #[arg(long)]
        sequence: Option<u64>,
    },

    Serve {
        #[arg(short, long, default_value = "lightcdc.example.toml")]
        config: PathBuf,

        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: SocketAddr,
    },
}

/// Controls whether capture writes each durable event to standard output.
#[derive(Clone, Copy, Debug, ValueEnum)]
enum CaptureOutput {
    Json,
    None,
}

/// Holds optional capture behavior used by bounded runs and benchmarks.
struct CaptureOptions {
    max_events: Option<usize>,
    output: CaptureOutput,
    metrics_file: Option<PathBuf>,
    metrics_interval: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PersistCaptureBatchOutcome {
    Persisted(usize),
    Replayed,
}

enum CaptureProgress {
    Transaction(Result<TransactionRead, PostgresError>),
    Storage(anyhow::Result<StorageCompletion>),
}

struct CapturePipeline {
    batch: CaptureBatch,
    pending_write: Option<PendingCaptureWrite>,
    replay_reconciled: bool,
}

impl CapturePipeline {
    fn new(replay_reconciled: bool) -> Self {
        Self {
            batch: CaptureBatch::default(),
            pending_write: None,
            replay_reconciled,
        }
    }

    fn buffered_event_count(&self, captured: usize) -> usize {
        captured
            .saturating_add(self.pending_event_count())
            .saturating_add(self.batch.event_count)
    }

    fn pending_event_count(&self) -> usize {
        self.pending_write
            .as_ref()
            .map_or(0, |pending_write| pending_write.event_count)
    }

    fn read_deadline(&self, limits: CaptureBatchLimits) -> Option<tokio::time::Instant> {
        if self.batch.is_empty() || !self.replay_reconciled || self.pending_write.is_some() {
            None
        } else {
            Some(self.batch.deadline(limits))
        }
    }
}

#[derive(Clone, Copy)]
struct CaptureRetention {
    policy: RetentionPolicy,
    check_interval: Duration,
}

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

enum CaptureSessionExit {
    LimitReached,
    Disconnected(String),
}

/// Parses CLI arguments and dispatches to the requested command.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Capture {
            config,
            max_events,
            output,
            metrics_file,
            metrics_interval_seconds,
        } => {
            capture(
                config,
                CaptureOptions {
                    max_events,
                    output,
                    metrics_file,
                    metrics_interval: Duration::from_secs(metrics_interval_seconds),
                },
            )
            .await
        }
        Command::Run {
            config,
            addr,
            max_events,
            output,
            metrics_file,
            metrics_interval_seconds,
        } => {
            run(
                config,
                addr,
                CaptureOptions {
                    max_events,
                    output,
                    metrics_file,
                    metrics_interval: Duration::from_secs(metrics_interval_seconds),
                },
            )
            .await
        }
        Command::Replay {
            config,
            stream,
            from,
            limit,
            pretty,
        } => replay(config, stream, from, limit, pretty).await,
        Command::Inspect {
            config,
            limit,
            sequence,
        } => inspect(config, limit, sequence),
        Command::Serve { config, addr } => serve(config, addr).await,
    }
}

/// Runs capture only and writes changes into the local event store.
async fn capture(config_path: PathBuf, options: CaptureOptions) -> anyhow::Result<()> {
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
async fn run(
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
        max_events = ?options.max_events,
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

async fn run_retention_sweeps(storage: CaptureStorageHandle, retention: CaptureRetention) {
    let mut interval = tokio::time::interval(retention.check_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await;

    loop {
        interval.tick().await;
        match storage
            .prune(retention.policy, unix_timestamp_ms_i64())
            .await
        {
            Ok(outcome) if outcome.deleted_events > 0 => {
                info!(
                    deleted_events = outcome.deleted_events,
                    deleted_event_ids = outcome.deleted_event_ids,
                    first_retained_sequence = ?outcome.first_retained_sequence,
                    high_watermark = ?outcome.high_watermark,
                    "pruned retained events"
                );
            }
            Ok(_) => {}
            Err(error) => {
                warn!(%error, "event retention sweep failed; retrying");
            }
        }
    }
}

async fn supervise_capture(context: CaptureContext<'_>) -> anyhow::Result<()> {
    let mut captured = 0usize;
    let mut retry_attempt = 0u32;
    let mut reconnect_count = 0u64;

    validate_source_until_ready(&context, &mut retry_attempt, &mut reconnect_count).await?;
    let heartbeat_task = tokio::spawn(run_logical_heartbeats(
        context.config.source.clone(),
        context.heartbeat_interval,
    ));
    let capture_result = supervise_capture_sessions(
        &context,
        &mut captured,
        &mut retry_attempt,
        &mut reconnect_count,
    )
    .await;
    heartbeat_task.abort();
    if let Err(error) = heartbeat_task.await
        && !error.is_cancelled()
    {
        warn!(%error, "logical heartbeat task stopped unexpectedly");
    }
    capture_result
}

async fn supervise_capture_sessions(
    context: &CaptureContext<'_>,
    captured: &mut usize,
    retry_attempt: &mut u32,
    reconnect_count: &mut u64,
) -> anyhow::Result<()> {
    loop {
        if capture_limit_reached(context.options, *captured) {
            return Ok(());
        }

        let (mut reader, replay_reconciled) =
            connect_capture_reader(context, retry_attempt, reconnect_count)
                .await
                .context("failed to connect PostgreSQL capture")?;

        let session_result =
            run_capture_session(context, &mut reader, captured, replay_reconciled).await;
        shutdown_capture_reader(&mut reader).await;

        match session_result? {
            CaptureSessionExit::LimitReached => return Ok(()),
            CaptureSessionExit::Disconnected(reason) => {
                wait_before_session_reconnect(context, retry_attempt, reconnect_count, &reason)
                    .await;
            }
        }
    }
}

async fn validate_source_until_ready(
    context: &CaptureContext<'_>,
    retry_attempt: &mut u32,
    reconnect_count: &mut u64,
) -> anyhow::Result<()> {
    loop {
        match lightcdc_postgres::validate_source_config_with_plan(
            &context.config.source,
            context.capture_plan,
        )
        .await
        {
            Ok(alignment) => {
                if !alignment.unnecessary_published_tables.is_empty() {
                    warn!(
                        tables = ?alignment.unnecessary_published_tables,
                        "publication contains tables that no configured stream consumes"
                    );
                }
                return Ok(());
            }
            Err(error) if error.is_retryable() => {
                record_capture_reconnect(context.metrics);
                wait_before_reconnect(*retry_attempt, *reconnect_count, &error).await;
                advance_reconnect_state(retry_attempt, reconnect_count);
            }
            Err(error) => {
                return Err(error)
                    .context("PostgreSQL source configuration requires operator action");
            }
        }
    }
}

async fn connect_capture_reader(
    context: &CaptureContext<'_>,
    retry_attempt: &mut u32,
    reconnect_count: &mut u64,
) -> anyhow::Result<(ReplicationReader, bool)> {
    loop {
        let next_sequence = context
            .store
            .next_sequence()
            .context("failed to read next event sequence from redb")?;
        let source_offset = context
            .store
            .source_offset(context.source_name)
            .context("failed to read source offset from redb")?;

        match ReplicationReader::connect_from_with_buffer_and_plan(
            context.config.source.clone(),
            source_offset.as_deref(),
            context.transaction_buffer_options.clone(),
            context.capture_plan.clone(),
        )
        .await
        {
            Ok(mut reader) => {
                reader.set_next_sequence(next_sequence);
                *retry_attempt = 0;
                info!(
                    reconnect_count = *reconnect_count,
                    next_sequence,
                    source_offset = ?source_offset,
                    "PostgreSQL capture connection is ready"
                );
                return Ok((reader, source_offset.is_none()));
            }
            Err(error) if error.is_fatal_capture_error() => {
                return Err(error).context("failed to initialize transaction buffering");
            }
            Err(error) => {
                record_capture_reconnect(context.metrics);
                wait_before_reconnect(*retry_attempt, *reconnect_count, &error).await;
                advance_reconnect_state(retry_attempt, reconnect_count);
            }
        }
    }
}

async fn run_capture_session(
    context: &CaptureContext<'_>,
    reader: &mut ReplicationReader,
    captured: &mut usize,
    replay_reconciled: bool,
) -> anyhow::Result<CaptureSessionExit> {
    let mut pipeline = CapturePipeline::new(replay_reconciled);

    loop {
        if capture_limit_reached(context.options, *captured) {
            return Ok(CaptureSessionExit::LimitReached);
        }

        if capture_limit_includes_pending_write(context.options, *captured, &pipeline) {
            complete_pending_capture_write(context, reader, &mut pipeline, captured)
                .await?
                .expect("the pending event count requires a pending write");
            continue;
        }

        match wait_for_capture_progress(context, reader, &mut pipeline).await {
            CaptureProgress::Storage(completion) => {
                let replayed = complete_pipelined_capture_write(
                    context,
                    reader,
                    &mut pipeline,
                    captured,
                    completion?,
                )?;
                if replayed {
                    return Ok(reconnect_after_replayed_batch("pipelined"));
                }
            }
            CaptureProgress::Transaction(Ok(TransactionRead::Transaction(transaction))) => {
                if let Some(exit) = buffer_captured_transaction(
                    context,
                    reader,
                    &mut pipeline,
                    captured,
                    transaction,
                )
                .await?
                {
                    return Ok(exit);
                }
            }
            CaptureProgress::Transaction(Ok(TransactionRead::TimedOut)) => {
                if let Some(exit) =
                    flush_expired_capture_batch(context, reader, &mut pipeline, captured).await?
                {
                    return Ok(exit);
                }
            }
            CaptureProgress::Transaction(Ok(TransactionRead::StreamEnded)) => {
                return finish_capture_stream(context, reader, &mut pipeline, captured).await;
            }
            CaptureProgress::Transaction(Err(error)) => {
                return finish_capture_read_error(context, reader, &mut pipeline, captured, error)
                    .await;
            }
        }
    }
}

async fn wait_for_capture_progress(
    context: &CaptureContext<'_>,
    reader: &mut ReplicationReader,
    pipeline: &mut CapturePipeline,
) -> CaptureProgress {
    let deadline = pipeline.read_deadline(context.batch_limits);
    if let Some(pending_write) = pipeline.pending_write.as_mut() {
        tokio::select! {
            biased;
            completion = &mut pending_write.response => {
                CaptureProgress::Storage(
                    completion.context("dedicated redb writer stopped before returning a batch")
                )
            }
            transaction = next_capture_transaction(reader, deadline) => {
                CaptureProgress::Transaction(transaction)
            }
        }
    } else {
        CaptureProgress::Transaction(next_capture_transaction(reader, deadline).await)
    }
}

async fn buffer_captured_transaction(
    context: &CaptureContext<'_>,
    reader: &mut ReplicationReader,
    pipeline: &mut CapturePipeline,
    captured: &mut usize,
    transaction: CapturedTransaction,
) -> anyhow::Result<Option<CaptureSessionExit>> {
    if !pipeline.replay_reconciled {
        pipeline.batch.push(transaction);
        submit_capture_batch(context, pipeline).await?;
        complete_pending_capture_write(context, reader, pipeline, captured)
            .await?
            .expect("the submitted recovery batch has a pending write");
        return Ok(None);
    }

    if pipeline
        .batch
        .would_exceed(&transaction, context.batch_limits)
    {
        if complete_pending_capture_write(context, reader, pipeline, captured).await?
            == Some(PersistCaptureBatchOutcome::Replayed)
        {
            return Ok(Some(reconnect_after_replayed_batch("queued")));
        }
        submit_capture_batch(context, pipeline).await?;
    }

    pipeline.batch.push(transaction);
    let max_events_reached = context
        .options
        .max_events
        .is_some_and(|max| pipeline.buffered_event_count(*captured) >= max);
    if pipeline.batch.reached_limit(context.batch_limits) || max_events_reached {
        if complete_pending_capture_write(context, reader, pipeline, captured).await?
            == Some(PersistCaptureBatchOutcome::Replayed)
        {
            return Ok(Some(reconnect_after_replayed_batch("queued")));
        }
        if !capture_limit_reached(context.options, *captured) {
            submit_capture_batch(context, pipeline).await?;
        }
    }

    Ok(None)
}

async fn flush_expired_capture_batch(
    context: &CaptureContext<'_>,
    reader: &mut ReplicationReader,
    pipeline: &mut CapturePipeline,
    captured: &mut usize,
) -> anyhow::Result<Option<CaptureSessionExit>> {
    if complete_pending_capture_write(context, reader, pipeline, captured).await?
        == Some(PersistCaptureBatchOutcome::Replayed)
    {
        return Ok(Some(reconnect_after_replayed_batch("queued")));
    }

    submit_capture_batch(context, pipeline).await?;
    Ok(None)
}

async fn finish_capture_stream(
    context: &CaptureContext<'_>,
    reader: &mut ReplicationReader,
    pipeline: &mut CapturePipeline,
    captured: &mut usize,
) -> anyhow::Result<CaptureSessionExit> {
    let replayed = complete_pending_capture_write(context, reader, pipeline, captured).await?
        == Some(PersistCaptureBatchOutcome::Replayed);

    if !pipeline.batch.is_empty() {
        if replayed {
            return Ok(reconnect_after_replayed_batch("queued"));
        }
        submit_and_complete_capture_batch(context, reader, pipeline, captured).await?;
    }

    Ok(CaptureSessionExit::Disconnected(
        "replication stream ended".to_owned(),
    ))
}

async fn finish_capture_read_error(
    context: &CaptureContext<'_>,
    reader: &mut ReplicationReader,
    pipeline: &mut CapturePipeline,
    captured: &mut usize,
    error: PostgresError,
) -> anyhow::Result<CaptureSessionExit> {
    let replayed = complete_pending_capture_write(context, reader, pipeline, captured).await?
        == Some(PersistCaptureBatchOutcome::Replayed);

    if !replayed && !pipeline.batch.is_empty() {
        submit_and_complete_capture_batch(context, reader, pipeline, captured).await?;
    }

    if error.is_fatal_capture_error() {
        return Err(error).context("capture stopped without acknowledging the source transaction");
    }

    Ok(CaptureSessionExit::Disconnected(error.to_string()))
}

async fn submit_capture_batch(
    context: &CaptureContext<'_>,
    pipeline: &mut CapturePipeline,
) -> anyhow::Result<()> {
    debug_assert!(pipeline.pending_write.is_none());
    debug_assert!(!pipeline.batch.is_empty());
    pipeline.pending_write = Some(
        context
            .storage_writer
            .submit(
                std::mem::take(&mut pipeline.batch),
                context.metrics.is_some(),
            )
            .await?,
    );
    Ok(())
}

async fn submit_and_complete_capture_batch(
    context: &CaptureContext<'_>,
    reader: &mut ReplicationReader,
    pipeline: &mut CapturePipeline,
    captured: &mut usize,
) -> anyhow::Result<PersistCaptureBatchOutcome> {
    submit_capture_batch(context, pipeline).await?;
    complete_pending_capture_write(context, reader, pipeline, captured)
        .await?
        .context("the submitted capture batch has no pending write")
}

async fn complete_pending_capture_write(
    context: &CaptureContext<'_>,
    reader: &mut ReplicationReader,
    pipeline: &mut CapturePipeline,
    captured: &mut usize,
) -> anyhow::Result<Option<PersistCaptureBatchOutcome>> {
    let Some(pending_write) = pipeline.pending_write.take() else {
        return Ok(None);
    };
    let completion = pending_write
        .response
        .await
        .context("dedicated redb writer stopped before returning a batch")?;
    complete_and_apply_capture_write(context, reader, pipeline, captured, completion).map(Some)
}

fn complete_pipelined_capture_write(
    context: &CaptureContext<'_>,
    reader: &mut ReplicationReader,
    pipeline: &mut CapturePipeline,
    captured: &mut usize,
    completion: StorageCompletion,
) -> anyhow::Result<bool> {
    pipeline.pending_write.take();
    Ok(
        complete_and_apply_capture_write(context, reader, pipeline, captured, completion)?
            == PersistCaptureBatchOutcome::Replayed,
    )
}

fn complete_and_apply_capture_write(
    context: &CaptureContext<'_>,
    reader: &mut ReplicationReader,
    pipeline: &mut CapturePipeline,
    captured: &mut usize,
    completion: StorageCompletion,
) -> anyhow::Result<PersistCaptureBatchOutcome> {
    let outcome = complete_capture_write(
        completion,
        reader,
        context.store,
        context.options,
        context.metrics.as_ref(),
        context.event_notifier.as_ref(),
    )?;
    apply_capture_batch_outcome(outcome, captured, &mut pipeline.replay_reconciled);
    Ok(outcome)
}

fn capture_limit_includes_pending_write(
    options: &CaptureOptions,
    captured: usize,
    pipeline: &CapturePipeline,
) -> bool {
    options
        .max_events
        .is_some_and(|max| captured.saturating_add(pipeline.pending_event_count()) >= max)
}

fn reconnect_after_replayed_batch(batch_state: &str) -> CaptureSessionExit {
    CaptureSessionExit::Disconnected(format!(
        "replayed {batch_state} batch required sequence reconciliation"
    ))
}

async fn shutdown_capture_reader(reader: &mut ReplicationReader) {
    if let Err(error) = reader.shutdown().await {
        warn!(%error, "failed to close PostgreSQL capture session");
    }
}

async fn wait_before_session_reconnect(
    context: &CaptureContext<'_>,
    retry_attempt: &mut u32,
    reconnect_count: &mut u64,
    reason: &str,
) {
    record_capture_reconnect(context.metrics);
    let delay = reconnect_delay(*retry_attempt);
    warn!(
        %reason,
        reconnect_count = *reconnect_count,
        retry_in_ms = delay.as_millis(),
        "PostgreSQL capture disconnected; retrying from the durable source LSN"
    );
    tokio::time::sleep(delay).await;
    advance_reconnect_state(retry_attempt, reconnect_count);
}

fn record_capture_reconnect(metrics: &Option<CaptureMetrics>) {
    if let Some(metrics) = metrics {
        metrics.record_reconnect();
    }
}

fn advance_reconnect_state(retry_attempt: &mut u32, reconnect_count: &mut u64) {
    *retry_attempt = retry_attempt.saturating_add(1);
    *reconnect_count = reconnect_count.saturating_add(1);
}

fn capture_limit_reached(options: &CaptureOptions, captured: usize) -> bool {
    options.max_events.is_some_and(|max| captured >= max)
}

fn capture_batch_limits(config: &Config) -> anyhow::Result<CaptureBatchLimits> {
    let runtime = &config.runtime;
    if runtime.capture_batch_max_transactions == 0 {
        return Err(anyhow!(
            "runtime.capture_batch_max_transactions must be greater than zero"
        ));
    }
    if runtime.capture_batch_max_events == 0 {
        return Err(anyhow!(
            "runtime.capture_batch_max_events must be greater than zero"
        ));
    }
    if runtime.capture_batch_max_bytes == 0 {
        return Err(anyhow!(
            "runtime.capture_batch_max_bytes must be greater than zero"
        ));
    }
    if runtime.capture_batch_max_delay_ms == 0 {
        return Err(anyhow!(
            "runtime.capture_batch_max_delay_ms must be greater than zero"
        ));
    }

    Ok(CaptureBatchLimits {
        max_transactions: runtime.capture_batch_max_transactions,
        max_events: runtime.capture_batch_max_events,
        max_bytes: runtime.capture_batch_max_bytes,
        max_delay: Duration::from_millis(runtime.capture_batch_max_delay_ms),
    })
}

fn capture_heartbeat_interval(config: &Config) -> anyhow::Result<Duration> {
    if config.runtime.heartbeat_interval_ms == 0 {
        return Err(anyhow!(
            "runtime.heartbeat_interval_ms must be greater than zero"
        ));
    }
    Ok(Duration::from_millis(config.runtime.heartbeat_interval_ms))
}

async fn run_logical_heartbeats(source: SourceConfig, interval: Duration) {
    let content = format!("source={};slot={}", source.name, source.slot);
    loop {
        match LogicalHeartbeatEmitter::connect(&source).await {
            Ok(emitter) => {
                let mut ticker = tokio::time::interval(interval);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                ticker.tick().await;
                loop {
                    ticker.tick().await;
                    match emitter.emit(HEARTBEAT_PREFIX, &content).await {
                        Ok(lsn) => {
                            tracing::debug!(%lsn, "emitted PostgreSQL logical heartbeat");
                        }
                        Err(error) => {
                            warn!(%error, "logical heartbeat connection failed; reconnecting");
                            break;
                        }
                    }
                }
            }
            Err(error) => {
                warn!(%error, "failed to connect PostgreSQL logical heartbeat; retrying");
            }
        }
        tokio::time::sleep(Duration::from_millis(RECONNECT_INITIAL_DELAY_MS)).await;
    }
}

fn capture_retention(config: &Config) -> anyhow::Result<Option<CaptureRetention>> {
    let runtime = &config.runtime;
    if runtime.retention_max_events.is_none() && runtime.retention_max_age_seconds.is_none() {
        return Ok(None);
    }
    if runtime.retention_max_events == Some(0) {
        return Err(anyhow!(
            "runtime.retention_max_events must be greater than zero"
        ));
    }
    if runtime.retention_max_age_seconds == Some(0) {
        return Err(anyhow!(
            "runtime.retention_max_age_seconds must be greater than zero"
        ));
    }
    if runtime.retention_check_interval_ms == 0 {
        return Err(anyhow!(
            "runtime.retention_check_interval_ms must be greater than zero"
        ));
    }
    if runtime.retention_delete_batch_size == 0 {
        return Err(anyhow!(
            "runtime.retention_delete_batch_size must be greater than zero"
        ));
    }

    Ok(Some(CaptureRetention {
        policy: RetentionPolicy {
            max_events: runtime.retention_max_events,
            max_age: runtime.retention_max_age_seconds.map(Duration::from_secs),
            delete_batch_size: runtime.retention_delete_batch_size,
        },
        check_interval: Duration::from_millis(runtime.retention_check_interval_ms),
    }))
}

fn unix_timestamp_ms_i64() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

async fn next_capture_transaction(
    reader: &mut ReplicationReader,
    deadline: Option<tokio::time::Instant>,
) -> Result<TransactionRead, PostgresError> {
    match deadline {
        Some(deadline) => reader.next_transaction_until(deadline).await,
        None => reader
            .next_transaction()
            .await
            .map(|transaction| match transaction {
                Some(transaction) => TransactionRead::Transaction(transaction),
                None => TransactionRead::StreamEnded,
            }),
    }
}

fn complete_capture_write(
    completion: StorageCompletion,
    reader: &mut ReplicationReader,
    store: &RedbEventStore,
    options: &CaptureOptions,
    metrics: Option<&CaptureMetrics>,
    event_notifier: Option<&EventNotifier>,
) -> anyhow::Result<PersistCaptureBatchOutcome> {
    let batch = completion.batch;
    let transaction_count = batch.transactions.len();
    let ack_lsn = batch
        .transactions
        .last()
        .expect("capture never persists an empty batch")
        .ack_lsn;
    let persist_outcome = completion
        .result
        .context("failed to persist captured transaction batch to redb")?;

    let outcome = match persist_outcome {
        PersistTransactionOutcome::Persisted => {
            if batch.event_count > 0
                && let Some(event_notifier) = event_notifier
            {
                event_notifier.notify();
            }
            if let (Some(metrics), Some(persist_latency)) = (metrics, completion.persist_latency) {
                metrics.record_persisted(
                    transaction_count,
                    batch.event_count,
                    batch.decoded_bytes,
                    batch.staged_bytes,
                    persist_latency,
                    batch.staged_transaction_count,
                );
            }
            if matches!(options.output, CaptureOutput::Json) {
                for transaction in &batch.transactions {
                    for event in transaction
                        .events
                        .iter()
                        .context("failed to open captured transaction events")?
                    {
                        let event = event.context("failed to read captured transaction event")?;
                        println!("{}", event_to_json(&event, false)?);
                    }
                }
            }

            PersistCaptureBatchOutcome::Persisted(batch.event_count)
        }
        PersistTransactionOutcome::AlreadyPersisted => {
            warn!(
                transaction_count,
                event_count = batch.event_count,
                decoded_bytes = batch.decoded_bytes,
                staged_bytes = batch.staged_bytes,
                %ack_lsn,
                "skipping replayed transaction batch already present in redb"
            );
            reader.set_next_sequence(
                store
                    .next_sequence()
                    .context("failed to reset sequence after transaction replay")?,
            );
            PersistCaptureBatchOutcome::Replayed
        }
    };

    reader.ack(ack_lsn);
    Ok(outcome)
}

fn apply_capture_batch_outcome(
    outcome: PersistCaptureBatchOutcome,
    captured: &mut usize,
    replay_reconciled: &mut bool,
) {
    match outcome {
        PersistCaptureBatchOutcome::Persisted(event_count) => {
            *captured = captured.saturating_add(event_count);
            *replay_reconciled = true;
        }
        PersistCaptureBatchOutcome::Replayed => *replay_reconciled = false,
    }
}

/// Logs a failed connection attempt and waits using capped exponential backoff.
async fn wait_before_reconnect(
    retry_attempt: u32,
    reconnect_count: u64,
    error: &impl std::fmt::Display,
) {
    let delay = reconnect_delay(retry_attempt);
    warn!(
        %error,
        reconnect_count,
        retry_in_ms = delay.as_millis(),
        "PostgreSQL capture is unavailable; retrying from the durable source LSN"
    );
    tokio::time::sleep(delay).await;
}

/// Returns capped exponential retry delay with up to 25 percent positive jitter.
fn reconnect_delay(attempt: u32) -> Duration {
    let exponent = attempt.min(16);
    let base_ms = RECONNECT_INITIAL_DELAY_MS
        .saturating_mul(1_u64 << exponent)
        .min(RECONNECT_MAX_DELAY_MS);
    let jitter_window = (base_ms / 4).max(1);
    let jitter_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64
        % jitter_window;

    Duration::from_millis((base_ms + jitter_ms).min(RECONNECT_MAX_DELAY_MS))
}

/// Replays stored events to stdout, optionally filtering by stream.
async fn replay(
    config_path: PathBuf,
    stream: Option<String>,
    from: u64,
    limit: usize,
    pretty: bool,
) -> anyhow::Result<()> {
    let config = Config::from_path(&config_path)
        .with_context(|| format!("could not load config from {}", config_path.display()))?;

    init_logging(&config.logging.level)?;

    let stream = stream
        .as_deref()
        .map(|name| {
            config
                .stream(name)
                .with_context(|| format!("stream {name:?} is not defined in config"))
        })
        .transpose()?;
    let storage = storage_options(&config);
    let store = RedbEventStore::open(&storage).context("failed to open local redb event store")?;
    let events = replay_from_store(&store, from, limit, stream)
        .with_context(|| format!("failed to replay events from sequence {from}"))?;

    for event in events {
        println!("{}", event_to_json(&event, pretty)?);
    }

    Ok(())
}

/// Reads events from storage while applying optional stream filtering.
fn replay_from_store(
    store: &RedbEventStore,
    from: u64,
    limit: usize,
    stream: Option<&StreamConfig>,
) -> anyhow::Result<Vec<ChangeEvent>> {
    if limit == 0 {
        return Ok(Vec::new());
    }

    let mut next_sequence = from;
    let mut output = Vec::with_capacity(limit);
    const BATCH_SIZE: usize = 256;

    while output.len() < limit {
        let batch = store.replay_from(next_sequence, BATCH_SIZE)?;
        if batch.is_empty() {
            break;
        }

        for event in batch {
            next_sequence = event.sequence + 1;
            if stream.is_none_or(|stream| stream.matches_event(&event)) {
                output.push(event);
                if output.len() >= limit {
                    break;
                }
            }
        }
    }

    Ok(output)
}

/// Builds storage open options from the runtime config.
fn storage_options(config: &Config) -> LogOpenOptions {
    LogOpenOptions {
        data_dir: PathBuf::from(&config.runtime.data_dir),
        database_file: config.runtime.storage_file.clone(),
    }
}

/// Opens the configured redb event store.
fn open_event_store(config: &Config) -> anyhow::Result<RedbEventStore> {
    let storage = storage_options(config);
    RedbEventStore::open(&storage).context("failed to open local redb event store")
}

/// Prints a human-readable snapshot of the configured redb event store.
fn inspect(
    config_path: PathBuf,
    limit: usize,
    selected_sequence: Option<u64>,
) -> anyhow::Result<()> {
    let config = Config::from_path(&config_path)
        .with_context(|| format!("could not load config from {}", config_path.display()))?;
    let storage = storage_options(&config);
    let database_path = storage.data_dir.join(&storage.database_file);
    let store = RedbEventStore::open(&storage).with_context(|| {
        format!(
            "failed to open {}; stop any other lightcdc process using this redb file",
            database_path.display()
        )
    })?;
    let stats = store.stats().context("failed to read redb table counts")?;
    let source_offsets = store
        .source_offsets()
        .context("failed to read source offsets")?;
    let consumer_offsets = store
        .consumer_offsets()
        .context("failed to read consumer offsets")?;
    let last_sequence = store
        .last_sequence()
        .context("failed to read last event sequence")?;
    let first_sequence = store
        .first_sequence()
        .context("failed to read first event sequence")?;
    let file_size = std::fs::metadata(&database_path)
        .map(|metadata| metadata.len())
        .unwrap_or(0);

    println!("LightCDC redb inspector");
    println!("Database       {}", database_path.display());
    println!("File size      {}", human_bytes(file_size));
    println!(
        "Event range    {}",
        match (first_sequence, last_sequence) {
            (Some(first), Some(last)) => format!("{first}..={last}"),
            _ => "empty".to_owned(),
        }
    );

    print_table(
        "Tables",
        &["NAME", "ROWS"],
        &[
            vec!["events".to_owned(), stats.event_count.to_string()],
            vec!["event_ids".to_owned(), stats.event_id_count.to_string()],
            vec![
                "source_offsets".to_owned(),
                stats.source_offset_count.to_string(),
            ],
            vec![
                "consumer_offsets".to_owned(),
                stats.consumer_offset_count.to_string(),
            ],
        ],
        &[24, 12],
    );

    let source_rows = source_offsets
        .into_iter()
        .map(|offset| vec![offset.source_name, offset.lsn])
        .collect::<Vec<_>>();
    print_table(
        "Source offsets",
        &["SOURCE", "LSN"],
        &source_rows,
        &[24, 24],
    );

    let consumer_rows = consumer_offsets
        .into_iter()
        .map(|offset| {
            let lag = last_sequence.unwrap_or(0).saturating_sub(offset.sequence);
            vec![
                offset.stream_name,
                offset.consumer_name,
                offset.sequence.to_string(),
                lag.to_string(),
            ]
        })
        .collect::<Vec<_>>();
    print_table(
        "Consumer offsets",
        &["STREAM", "CONSUMER", "OFFSET", "LAG"],
        &consumer_rows,
        &[24, 28, 12, 12],
    );

    let recent_events = match (last_sequence, limit) {
        (Some(last), limit) if limit > 0 => {
            let from = last.saturating_sub(limit.saturating_sub(1) as u64);
            store
                .replay_from(from, limit)
                .context("failed to read recent events")?
        }
        _ => Vec::new(),
    };
    let event_rows = recent_events
        .iter()
        .map(|event| {
            vec![
                event.sequence.to_string(),
                operation_name(event.operation).to_owned(),
                format!("{}.{}", event.schema, event.table),
                event.source.lsn.clone(),
                event.event_id.clone(),
            ]
        })
        .collect::<Vec<_>>();
    print_table(
        "Recent events",
        &["SEQ", "OPERATION", "RELATION", "LSN", "EVENT ID"],
        &event_rows,
        &[12, 12, 32, 20, 44],
    );

    if let Some(sequence) = selected_sequence {
        let event = store
            .replay_from(sequence, 1)
            .with_context(|| format!("failed to read event sequence {sequence}"))?
            .into_iter()
            .find(|event| event.sequence == sequence)
            .with_context(|| format!("event sequence {sequence} is not stored"))?;

        println!("\nEvent {sequence}");
        println!("{}", event_to_json(&event, true)?);
    }

    Ok(())
}

/// Serves the gRPC API against an existing event store when capture is not running.
async fn serve(config_path: PathBuf, addr: SocketAddr) -> anyhow::Result<()> {
    let config = Config::from_path(&config_path)
        .with_context(|| format!("could not load config from {}", config_path.display()))?;

    init_logging(&config.logging.level)?;

    let store = open_event_store(&config)?;

    lightcdc_api::serve(addr, config, store).await?;
    Ok(())
}

/// Initializes tracing from the environment or configured log level.
fn init_logging(level: &str) -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new(level))?;

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .try_init()
        .map_err(|error| anyhow!("failed to initialize tracing subscriber: {error}"))
}

/// Converts a change event into CLI JSON output.
fn event_to_json(event: &ChangeEvent, pretty: bool) -> anyhow::Result<String> {
    let value = serde_json::json!({
        "sequence": event.sequence,
        "event_id": event.event_id,
        "source": event.source,
        "transaction": event.transaction,
        "schema": event.schema,
        "table": event.table,
        "operation": event.operation,
        "key": optional_json_bytes(event.key.as_deref())?,
        "before": optional_json_bytes(event.before.as_deref())?,
        "after": optional_json_bytes(event.after.as_deref())?,
        "commit_timestamp_ms": event.commit_timestamp_ms,
    });

    if pretty {
        Ok(serde_json::to_string_pretty(&value)?)
    } else {
        Ok(serde_json::to_string(&value)?)
    }
}

/// Parses optional JSON row bytes into JSON values.
fn optional_json_bytes(bytes: Option<&[u8]>) -> anyhow::Result<Option<serde_json::Value>> {
    bytes
        .map(|bytes| serde_json::from_slice(bytes).context("event row payload was not JSON"))
        .transpose()
}

/// Prints a compact ASCII table with bounded column widths.
fn print_table(title: &str, headers: &[&str], rows: &[Vec<String>], max_widths: &[usize]) {
    let widths = headers
        .iter()
        .enumerate()
        .map(|(column, header)| {
            rows.iter()
                .filter_map(|row| row.get(column))
                .map(|value| value.chars().count())
                .chain(std::iter::once(header.len()))
                .max()
                .unwrap_or(header.len())
                .min(max_widths[column])
        })
        .collect::<Vec<_>>();

    println!("\n{title}");
    print_table_row(
        &headers.iter().map(ToString::to_string).collect::<Vec<_>>(),
        &widths,
    );
    println!(
        "{}",
        widths
            .iter()
            .map(|width| "-".repeat(*width))
            .collect::<Vec<_>>()
            .join("  ")
    );

    if rows.is_empty() {
        println!("(empty)");
        return;
    }

    for row in rows {
        print_table_row(row, &widths);
    }
}

/// Prints one row from a compact ASCII table.
fn print_table_row(row: &[String], widths: &[usize]) {
    let cells = row
        .iter()
        .zip(widths)
        .map(|(value, width)| {
            let value = truncate(value, *width);
            format!("{value:width$}")
        })
        .collect::<Vec<_>>();
    println!("{}", cells.join("  "));
}

/// Truncates a display value without splitting a Unicode character.
fn truncate(value: &str, width: usize) -> String {
    if value.chars().count() <= width {
        return value.to_owned();
    }

    if width <= 3 {
        return ".".repeat(width);
    }

    let mut output = value.chars().take(width - 3).collect::<String>();
    output.push_str("...");
    output
}

/// Formats a byte count for interactive output.
fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;

    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }

    if unit == 0 {
        format!("{bytes} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

/// Returns the stable display name for an event operation.
fn operation_name(operation: Operation) -> &'static str {
    match operation {
        Operation::Insert => "insert",
        Operation::Update => "update",
        Operation::Delete => "delete",
        Operation::Truncate => "truncate",
    }
}

#[cfg(test)]
mod tests {
    use lightcdc_core::{Operation, SourceMetadata};
    use lightcdc_postgres::CapturedTransaction;
    use lightcdc_storage::{TransactionEvents, TransactionStats};
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn stream_replay_filters_by_configured_table() {
        let temp = TempDir::new().expect("temp dir");
        let store = RedbEventStore::open(&LogOpenOptions {
            data_dir: temp.path().to_path_buf(),
            database_file: "test.redb".to_owned(),
        })
        .expect("open store");
        store
            .append_event(&event(1, "public", "orders"))
            .expect("append orders event");
        store
            .append_event(&event(2, "public", "customers"))
            .expect("append customers event");
        store
            .append_event(&event(3, "public", "orders"))
            .expect("append second orders event");

        let stream = StreamConfig {
            name: "orders".to_owned(),
            source: "default".to_owned(),
            tables: vec!["public.orders".to_owned()],
        };

        let events = replay_from_store(&store, 1, 10, Some(&stream)).expect("replay events");

        assert_eq!(
            events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
    }

    #[test]
    fn stream_replay_limit_counts_emitted_events() {
        let temp = TempDir::new().expect("temp dir");
        let store = RedbEventStore::open(&LogOpenOptions {
            data_dir: temp.path().to_path_buf(),
            database_file: "test.redb".to_owned(),
        })
        .expect("open store");
        store
            .append_event(&event(1, "public", "customers"))
            .expect("append customers event");
        store
            .append_event(&event(2, "public", "orders"))
            .expect("append orders event");
        store
            .append_event(&event(3, "public", "orders"))
            .expect("append second orders event");

        let stream = StreamConfig {
            name: "orders".to_owned(),
            source: "default".to_owned(),
            tables: vec!["public.orders".to_owned()],
        };

        let events = replay_from_store(&store, 1, 1, Some(&stream)).expect("replay events");

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].sequence, 2);
    }

    #[test]
    fn reconnect_backoff_grows_and_is_capped() {
        let first = reconnect_delay(0);
        let second = reconnect_delay(1);
        let capped = reconnect_delay(30);

        assert!(first >= Duration::from_millis(RECONNECT_INITIAL_DELAY_MS));
        assert!(first < Duration::from_millis(RECONNECT_INITIAL_DELAY_MS * 2));
        assert!(second >= Duration::from_millis(RECONNECT_INITIAL_DELAY_MS * 2));
        assert!(second < Duration::from_millis(RECONNECT_INITIAL_DELAY_MS * 4));
        assert_eq!(capped, Duration::from_millis(RECONNECT_MAX_DELAY_MS));
    }

    #[test]
    fn capture_batch_flushes_before_adding_a_transaction_that_exceeds_limits() {
        let limits = CaptureBatchLimits {
            max_transactions: 10,
            max_events: 3,
            max_bytes: 1_000,
            max_delay: Duration::from_millis(5),
        };
        let mut batch = CaptureBatch::default();
        batch.push(captured_transaction(1, 2, 200));
        let next = captured_transaction(3, 2, 200);

        assert!(!batch.reached_limit(limits));
        assert!(batch.would_exceed(&next, limits));
    }

    #[test]
    fn capture_batch_accepts_one_source_transaction_larger_than_group_limits() {
        let limits = CaptureBatchLimits {
            max_transactions: 10,
            max_events: 3,
            max_bytes: 100,
            max_delay: Duration::from_millis(5),
        };
        let transaction = captured_transaction(1, 5, 500);
        let mut batch = CaptureBatch::default();

        assert!(!batch.would_exceed(&transaction, limits));
        batch.push(transaction);
        assert!(batch.reached_limit(limits));
    }

    #[test]
    fn capture_keeps_reading_past_the_batch_deadline_while_a_write_is_in_flight() {
        let limits = CaptureBatchLimits {
            max_transactions: 10,
            max_events: 10,
            max_bytes: 1_000,
            max_delay: Duration::from_millis(5),
        };
        let mut pipeline = CapturePipeline::new(true);
        pipeline.batch.push(captured_transaction(1, 1, 100));

        assert!(pipeline.read_deadline(limits).is_some());

        let (_sender, response) = tokio::sync::oneshot::channel::<StorageCompletion>();
        pipeline.pending_write = Some(PendingCaptureWrite {
            response,
            event_count: 2,
        });

        assert!(pipeline.read_deadline(limits).is_none());
        assert_eq!(pipeline.buffered_event_count(3), 6);
    }

    #[test]
    fn capture_pipeline_tracks_replay_reconciliation() {
        let mut pipeline = CapturePipeline::new(true);
        let mut captured = 3;

        apply_capture_batch_outcome(
            PersistCaptureBatchOutcome::Replayed,
            &mut captured,
            &mut pipeline.replay_reconciled,
        );

        assert_eq!(captured, 3);
        assert!(!pipeline.replay_reconciled);

        apply_capture_batch_outcome(
            PersistCaptureBatchOutcome::Persisted(2),
            &mut captured,
            &mut pipeline.replay_reconciled,
        );

        assert_eq!(captured, 5);
        assert!(pipeline.replay_reconciled);
    }

    fn captured_transaction(
        first_sequence: u64,
        event_count: usize,
        decoded_bytes: u64,
    ) -> CapturedTransaction {
        let events = (0..event_count)
            .map(|index| event(first_sequence + index as u64, "public", "orders"))
            .collect();
        CapturedTransaction {
            events: TransactionEvents::InMemory {
                events,
                stats: TransactionStats {
                    event_count,
                    decoded_bytes,
                    staged_bytes: decoded_bytes,
                },
            },
            ack_lsn: format!("0/{first_sequence:X}").parse().expect("test LSN"),
        }
    }

    fn event(sequence: u64, schema: &str, table: &str) -> ChangeEvent {
        ChangeEvent {
            sequence,
            event_id: format!("event-{sequence}"),
            source: SourceMetadata {
                database: "lightcdc".to_owned(),
                slot: "slot".to_owned(),
                lsn: format!("0/{sequence:X}"),
            },
            transaction: None,
            schema: schema.to_owned(),
            table: table.to_owned(),
            operation: Operation::Insert,
            key: None,
            before: None,
            after: Some(br#"{"id":"1"}"#.to_vec()),
            commit_timestamp_ms: None,
        }
    }
}
