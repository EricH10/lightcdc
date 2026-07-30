//! Measures consumer throughput and delivery latency through the LightCDC gRPC API.

use std::{
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use clap::{Parser, ValueEnum};
use hdrhistogram::Histogram;
use lightcdc_api::proto::{
    AckRequest, ChangeEvent, SeekPosition, SeekRequest, SubscribeRequest,
    light_cdc_client::LightCdcClient,
};
use serde_json::json;
use tokio::time::{interval, sleep};

/// Measures gRPC delivery, acknowledgement, and end-to-end event latency.
#[derive(Debug, Parser)]
#[command(about = "Record lightcdc consumer load-test metrics as JSON Lines")]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    endpoint: String,

    #[arg(long, default_value = "benchmark")]
    stream: String,

    #[arg(long, default_value = "benchmark-consumer")]
    consumer: String,

    #[arg(long, value_enum, default_value_t = SeekStart::Latest)]
    seek: SeekStart,

    #[arg(long, default_value_t = 60)]
    duration_seconds: u64,

    #[arg(long, default_value_t = 1)]
    report_interval_seconds: u64,

    #[arg(long, default_value_t = 5_000)]
    ack_every: u64,

    #[arg(long)]
    metrics_file: PathBuf,
}

/// Selects the consumer offset used before the benchmark starts.
#[derive(Clone, Copy, Debug, ValueEnum)]
enum SeekStart {
    Keep,
    Earliest,
    Latest,
}

/// Tracks lifetime counters and resettable interval latency histograms.
struct ConsumerStats {
    started: Instant,
    last_report: Instant,
    events_total: u64,
    payload_bytes_total: u64,
    acknowledgements_total: u64,
    interval_events: u64,
    interval_payload_bytes: u64,
    last_sequence: u64,
    end_to_end_latency: Histogram<u64>,
    acknowledgement_latency: Histogram<u64>,
    missing_commit_timestamps: u64,
    clock_skew_samples: u64,
}

impl ConsumerStats {
    /// Creates empty counters and HDR histograms for one benchmark run.
    fn new() -> Result<Self> {
        let now = Instant::now();
        Ok(Self {
            started: now,
            last_report: now,
            events_total: 0,
            payload_bytes_total: 0,
            acknowledgements_total: 0,
            interval_events: 0,
            interval_payload_bytes: 0,
            last_sequence: 0,
            end_to_end_latency: Histogram::new(3)?,
            acknowledgement_latency: Histogram::new(3)?,
            missing_commit_timestamps: 0,
            clock_skew_samples: 0,
        })
    }

    /// Records payload size and source-commit-to-consumer latency for one event.
    fn record_event(&mut self, event: &ChangeEvent) {
        let payload_bytes = event.key.as_ref().map_or(0, Vec::len)
            + event.before.as_ref().map_or(0, Vec::len)
            + event.after.as_ref().map_or(0, Vec::len);
        self.events_total += 1;
        self.payload_bytes_total += payload_bytes as u64;
        self.interval_events += 1;
        self.interval_payload_bytes += payload_bytes as u64;
        self.last_sequence = event.sequence;

        match event.commit_timestamp_ms {
            Some(commit_timestamp_ms) => {
                let now_ms = unix_timestamp_ms() as i128;
                let latency_ms = now_ms - commit_timestamp_ms as i128;
                if latency_ms < 0 {
                    self.clock_skew_samples += 1;
                } else {
                    let latency_ns = (latency_ms as u128)
                        .saturating_mul(1_000_000)
                        .min(u64::MAX as u128) as u64;
                    let _ = self.end_to_end_latency.record(latency_ns.max(1));
                }
            }
            None => self.missing_commit_timestamps += 1,
        }
    }

    /// Records one acknowledgement round-trip.
    fn record_acknowledgement(&mut self, latency: Duration) {
        self.acknowledgements_total += 1;
        let _ = self
            .acknowledgement_latency
            .record(duration_ns(latency).max(1));
    }

    /// Writes one JSON Lines snapshot and resets interval-only measurements.
    fn write_report(&mut self, writer: &mut BufWriter<File>, final_report: bool) -> Result<()> {
        let now = Instant::now();
        let interval_seconds = now
            .duration_since(self.last_report)
            .as_secs_f64()
            .max(0.000_001);
        let report = json!({
            "timestamp_ms": unix_timestamp_ms(),
            "elapsed_seconds": now.duration_since(self.started).as_secs_f64(),
            "interval_seconds": interval_seconds,
            "final": final_report,
            "events_per_second": self.interval_events as f64 / interval_seconds,
            "payload_bytes_per_second": self.interval_payload_bytes as f64 / interval_seconds,
            "events_total": self.events_total,
            "payload_bytes_total": self.payload_bytes_total,
            "acknowledgements_total": self.acknowledgements_total,
            "last_sequence": self.last_sequence,
            "missing_commit_timestamps_total": self.missing_commit_timestamps,
            "clock_skew_samples_total": self.clock_skew_samples,
            "end_to_end_latency_ms": latency_report(&self.end_to_end_latency),
            "acknowledgement_latency_ms": latency_report(&self.acknowledgement_latency),
        });

        serde_json::to_writer(&mut *writer, &report)?;
        writer.write_all(b"\n")?;
        writer.flush()?;
        self.interval_events = 0;
        self.interval_payload_bytes = 0;
        self.end_to_end_latency.reset();
        self.acknowledgement_latency.reset();
        self.last_report = now;
        Ok(())
    }
}

/// Runs a timed subscription and periodically records consumer metrics.
#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.duration_seconds == 0 {
        bail!("duration-seconds must be greater than zero");
    }
    if args.report_interval_seconds == 0 {
        bail!("report-interval-seconds must be greater than zero");
    }
    if args.ack_every == 0 {
        bail!("ack-every must be greater than zero");
    }

    let mut writer = open_metrics_file(&args.metrics_file)?;
    let mut client = connect_with_retry(&args.endpoint).await?;
    let mut ack_client = client.clone();
    seek_consumer(&mut client, &args).await?;
    let mut events = client
        .subscribe(SubscribeRequest {
            stream: args.stream.clone(),
            consumer: args.consumer.clone(),
            limit: 0,
        })
        .await
        .context("subscribe to benchmark stream")?
        .into_inner();

    let mut stats = ConsumerStats::new()?;
    let mut report_tick = interval(Duration::from_secs(args.report_interval_seconds));
    report_tick.tick().await;
    let deadline = sleep(Duration::from_secs(args.duration_seconds));
    tokio::pin!(deadline);
    let mut unacknowledged = 0_u64;

    loop {
        tokio::select! {
            _ = &mut deadline => break,
            _ = report_tick.tick() => stats.write_report(&mut writer, false)?,
            event = events.message() => {
                let Some(event) = event.context("receive benchmark event")? else {
                    break;
                };
                stats.record_event(&event);
                unacknowledged += 1;

                if unacknowledged >= args.ack_every {
                    let latency = acknowledge(
                        &mut ack_client,
                        &args.stream,
                        &args.consumer,
                        event.sequence,
                    )
                    .await?;
                    stats.record_acknowledgement(latency);
                    unacknowledged = 0;
                }
            }
        }
    }

    if unacknowledged > 0 && stats.last_sequence > 0 {
        let latency = acknowledge(
            &mut ack_client,
            &args.stream,
            &args.consumer,
            stats.last_sequence,
        )
        .await?;
        stats.record_acknowledgement(latency);
    }
    stats.write_report(&mut writer, true)?;
    Ok(())
}

/// Retries startup connection briefly so benchmark processes can launch together.
async fn connect_with_retry(endpoint: &str) -> Result<LightCdcClient<tonic::transport::Channel>> {
    let started = Instant::now();
    loop {
        match LightCdcClient::connect(endpoint.to_owned()).await {
            Ok(client) => return Ok(client),
            Err(error) if started.elapsed() < Duration::from_secs(15) => {
                eprintln!("waiting for lightcdc at {endpoint}: {error}");
                sleep(Duration::from_millis(100)).await;
            }
            Err(error) => return Err(error).with_context(|| format!("connect to {endpoint}")),
        }
    }
}

/// Applies the requested initial consumer offset before subscribing.
async fn seek_consumer(
    client: &mut LightCdcClient<tonic::transport::Channel>,
    args: &Args,
) -> Result<()> {
    let position = match args.seek {
        SeekStart::Keep => return Ok(()),
        SeekStart::Earliest => SeekPosition::Earliest,
        SeekStart::Latest => SeekPosition::Latest,
    };
    client
        .seek(SeekRequest {
            stream: args.stream.clone(),
            consumer: args.consumer.clone(),
            position: position as i32,
            sequence: 0,
        })
        .await
        .context("seek benchmark consumer")?;
    Ok(())
}

/// Acknowledges one sequence and returns the gRPC round-trip duration.
async fn acknowledge(
    client: &mut LightCdcClient<tonic::transport::Channel>,
    stream: &str,
    consumer: &str,
    sequence: u64,
) -> Result<Duration> {
    let started = Instant::now();
    client
        .ack(AckRequest {
            stream: stream.to_owned(),
            consumer: consumer.to_owned(),
            sequence,
        })
        .await
        .with_context(|| format!("acknowledge sequence {sequence}"))?;
    Ok(started.elapsed())
}

/// Creates the metrics directory and buffered JSON Lines output file.
fn open_metrics_file(path: &Path) -> Result<BufWriter<File>> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("create metrics directory {}", parent.display()))?;
    }
    let file = File::create(path)
        .with_context(|| format!("create consumer metrics file {}", path.display()))?;
    Ok(BufWriter::new(file))
}

/// Converts a nanosecond HDR histogram into millisecond quantiles.
fn latency_report(histogram: &Histogram<u64>) -> serde_json::Value {
    let milliseconds = |quantile| histogram.value_at_quantile(quantile) as f64 / 1_000_000.0;
    json!({
        "samples": histogram.len(),
        "p50": milliseconds(0.50),
        "p95": milliseconds(0.95),
        "p99": milliseconds(0.99),
        "max": histogram.max() as f64 / 1_000_000.0,
    })
}

/// Converts a duration into a saturating nanosecond sample.
fn duration_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

/// Returns a saturating Unix timestamp for metrics output.
fn unix_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
