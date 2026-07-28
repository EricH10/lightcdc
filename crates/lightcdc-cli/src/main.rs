use std::{
    net::SocketAddr,
    path::PathBuf,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, anyhow};
use clap::{Parser, Subcommand, ValueEnum};
use lightcdc_core::{ChangeEvent, Config, Operation, StreamConfig};
use lightcdc_postgres::ReplicationReader;
use lightcdc_storage::{
    LogOpenOptions, PersistTransactionOutcome, RedbEventStore, TransactionBufferOptions,
};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use crate::capture_metrics::CaptureMetrics;

mod capture_metrics;

const RECONNECT_INITIAL_DELAY_MS: u64 = 250;
const RECONNECT_MAX_DELAY_MS: u64 = 15_000;

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
    capture_with_store(config, store, options).await
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
    let mut server =
        tokio::spawn(async move { lightcdc_api::serve(addr, server_config, server_store).await });
    let capture = capture_with_store(config, store, options);
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
) -> anyhow::Result<()> {
    let storage = storage_options(&config);
    let source_name = config.source.name.clone();
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
        event_output = ?options.output,
        metrics_file = ?options.metrics_file,
        "opened local event store"
    );

    warn!(
        max_events = ?options.max_events,
        "capture is running; insert, update, or delete rows in the published tables"
    );

    let mut captured = 0usize;
    let mut source_validated = false;
    let mut retry_attempt = 0u32;
    let mut reconnect_count = 0u64;

    loop {
        if options.max_events.is_some_and(|max| captured >= max) {
            return Ok(());
        }

        if !source_validated {
            match lightcdc_postgres::validate_source_config(&config.source).await {
                Ok(()) => source_validated = true,
                Err(error) if error.is_retryable() => {
                    if let Some(metrics) = &metrics {
                        metrics.record_reconnect();
                    }
                    wait_before_reconnect(retry_attempt, reconnect_count, &error).await;
                    retry_attempt = retry_attempt.saturating_add(1);
                    reconnect_count = reconnect_count.saturating_add(1);
                    continue;
                }
                Err(error) => {
                    return Err(error)
                        .context("PostgreSQL source configuration requires operator action");
                }
            }
        }

        let next_sequence = store
            .next_sequence()
            .context("failed to read next event sequence from redb")?;
        let source_offset = store
            .source_offset(&source_name)
            .context("failed to read source offset from redb")?;

        let mut reader = match ReplicationReader::connect_from_with_buffer(
            config.source.clone(),
            source_offset.as_deref(),
            transaction_buffer_options.clone(),
        )
        .await
        {
            Ok(reader) => reader,
            Err(error) if error.is_fatal_capture_error() => {
                return Err(error).context("failed to initialize transaction buffering");
            }
            Err(error) => {
                if let Some(metrics) = &metrics {
                    metrics.record_reconnect();
                }
                wait_before_reconnect(retry_attempt, reconnect_count, &error).await;
                retry_attempt = retry_attempt.saturating_add(1);
                reconnect_count = reconnect_count.saturating_add(1);
                continue;
            }
        };

        reader.set_next_sequence(next_sequence);
        retry_attempt = 0;
        info!(
            reconnect_count,
            next_sequence,
            source_offset = ?source_offset,
            "PostgreSQL capture connection is ready"
        );

        let disconnect_reason = loop {
            if options.max_events.is_some_and(|max| captured >= max) {
                reader.shutdown().await?;
                return Ok(());
            }

            let transaction = match reader.next_transaction().await {
                Ok(Some(transaction)) => transaction,
                Ok(None) => break "replication stream ended".to_owned(),
                Err(error) if error.is_fatal_capture_error() => {
                    if let Err(shutdown_error) = reader.shutdown().await {
                        warn!(%shutdown_error, "failed to close PostgreSQL capture after transaction buffer failure");
                    }
                    return Err(error)
                        .context("capture stopped without acknowledging the source transaction");
                }
                Err(error) => break error.to_string(),
            };
            let ack_lsn = transaction.ack_lsn;
            let transaction_stats = transaction.events.stats();
            let transaction_was_staged = transaction.events.is_staged();
            let persist_started = metrics.as_ref().map(|_| Instant::now());

            let persist_outcome = store
                .persist_transaction_events(&transaction.events, &source_name, &ack_lsn.to_string())
                .context("failed to persist captured transaction to redb")?;
            let persist_latency = persist_started.map(|started| started.elapsed());

            match persist_outcome {
                PersistTransactionOutcome::Persisted => {
                    if let (Some(metrics), Some(persist_latency)) = (&metrics, persist_latency) {
                        metrics.record_persisted(
                            transaction_stats.event_count,
                            transaction_stats.decoded_bytes,
                            transaction_stats.staged_bytes,
                            persist_latency,
                            transaction_was_staged,
                        );
                    }
                    if matches!(options.output, CaptureOutput::Json) {
                        for event in transaction
                            .events
                            .iter()
                            .context("failed to open captured transaction events")?
                        {
                            let event =
                                event.context("failed to read captured transaction event")?;
                            println!("{}", event_to_json(&event, false)?);
                        }
                    }
                    captured += transaction_stats.event_count;
                }
                PersistTransactionOutcome::AlreadyPersisted => {
                    warn!(
                        event_count = transaction_stats.event_count,
                        decoded_bytes = transaction_stats.decoded_bytes,
                        staged_bytes = transaction_stats.staged_bytes,
                        %ack_lsn,
                        "skipping replayed transaction already present in redb"
                    );
                    reader.set_next_sequence(
                        store
                            .next_sequence()
                            .context("failed to reset sequence after transaction replay")?,
                    );
                }
            }

            reader.ack(ack_lsn);
        };

        if let Err(error) = reader.shutdown().await {
            warn!(%error, "failed to close disconnected PostgreSQL capture session");
        }

        if let Some(metrics) = &metrics {
            metrics.record_reconnect();
        }
        let delay = reconnect_delay(retry_attempt);
        warn!(
            reason = %disconnect_reason,
            reconnect_count,
            retry_in_ms = delay.as_millis(),
            "PostgreSQL capture disconnected; retrying from the durable source LSN"
        );
        tokio::time::sleep(delay).await;
        retry_attempt = retry_attempt.saturating_add(1);
        reconnect_count = reconnect_count.saturating_add(1);
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
        .replay_from(0, 1)
        .context("failed to read first event sequence")?
        .first()
        .map(|event| event.sequence);
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
