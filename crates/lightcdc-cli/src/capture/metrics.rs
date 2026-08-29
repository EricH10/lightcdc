//! Records opt-in capture throughput metrics without blocking the capture path.

use std::{
    fs::{self, File},
    io::{self, BufWriter, Write},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc::{Receiver, RecvTimeoutError, SyncSender, TrySendError, sync_channel},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use hdrhistogram::Histogram;
use serde_json::json;

const SAMPLE_CHANNEL_CAPACITY: usize = 8_192;

/// Sends storage-group capture samples to a dedicated aggregation thread.
pub(super) struct CaptureMetrics {
    sender: Option<SyncSender<CaptureSample>>,
    dropped_samples: Arc<AtomicU64>,
    worker: Option<JoinHandle<io::Result<()>>>,
    exit: Mutex<Option<tokio::sync::oneshot::Receiver<Option<io::ErrorKind>>>>,
}

/// Represents one lightweight observation sent to the aggregation thread.
#[derive(Debug)]
enum CaptureSample {
    Persisted {
        transaction_count: u64,
        event_count: u64,
        decoded_bytes: u64,
        staged_bytes: u64,
        persist_latency_ns: u64,
        staged_transaction_count: u64,
    },
    Reconnect,
}

/// Accumulates process-lifetime counters for each report.
#[derive(Default)]
struct Totals {
    storage_commits: u64,
    transactions: u64,
    events: u64,
    decoded_bytes: u64,
    staged_bytes: u64,
    staged_transactions: u64,
    reconnects: u64,
}

/// Accumulates counters that reset after each reporting interval.
#[derive(Default)]
struct IntervalTotals {
    storage_commits: u64,
    transactions: u64,
    events: u64,
    decoded_bytes: u64,
    staged_bytes: u64,
}

impl CaptureMetrics {
    /// Starts an opt-in metrics writer. No metrics object exists in normal runs.
    pub(super) fn start(path: &Path, report_interval: Duration) -> io::Result<Self> {
        if report_interval.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "capture metrics interval must be greater than zero",
            ));
        }
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent)?;
        }

        let file = File::create(path)?;
        let (sender, receiver) = sync_channel(SAMPLE_CHANNEL_CAPACITY);
        let dropped_samples = Arc::new(AtomicU64::new(0));
        let worker_dropped_samples = Arc::clone(&dropped_samples);
        let (exit_sender, exit_receiver) = tokio::sync::oneshot::channel();
        let worker = thread::Builder::new()
            .name("lightcdc-capture-metrics".to_owned())
            .spawn(move || {
                let result =
                    aggregate_metrics(receiver, file, report_interval, worker_dropped_samples);
                let _ = exit_sender.send(result.as_ref().err().map(|error| error.kind()));
                result
            })?;

        Ok(Self {
            sender: Some(sender),
            dropped_samples,
            worker: Some(worker),
            exit: Mutex::new(Some(exit_receiver)),
        })
    }

    /// Resolves when the aggregation thread exits, returning its I/O result so
    /// a metrics-file write failure can fail the capture run instead of being
    /// reported only on drop.
    pub(super) async fn stopped(&self) -> io::Result<()> {
        let receiver = self.exit.lock().expect("metrics exit lock").take();
        let Some(receiver) = receiver else {
            return Ok(());
        };
        match receiver.await {
            Ok(Some(kind)) => Err(io::Error::new(kind, "capture metrics writer failed")),
            Ok(None) => Err(io::Error::other("capture metrics writer stopped unexpectedly")),
            Err(_) => Err(io::Error::other("capture metrics writer panicked")),
        }
    }

    /// Records one durable source transaction group without blocking capture.
    pub(super) fn record_persisted(
        &self,
        transaction_count: usize,
        event_count: usize,
        decoded_bytes: u64,
        staged_bytes: u64,
        persist_latency: Duration,
        staged_transaction_count: usize,
    ) {
        self.try_send(CaptureSample::Persisted {
            transaction_count: transaction_count as u64,
            event_count: event_count as u64,
            decoded_bytes,
            staged_bytes,
            persist_latency_ns: duration_ns(persist_latency),
            staged_transaction_count: staged_transaction_count as u64,
        });
    }

    /// Records one retryable capture disconnection.
    pub(super) fn record_reconnect(&self) {
        self.try_send(CaptureSample::Reconnect);
    }

    /// Drops a sample instead of adding latency when the metrics worker falls behind.
    fn try_send(&self, sample: CaptureSample) {
        let Some(sender) = &self.sender else {
            return;
        };
        if matches!(
            sender.try_send(sample),
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_))
        ) {
            self.dropped_samples.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl Drop for CaptureMetrics {
    fn drop(&mut self) {
        // Closing the sender lets the worker flush its final report before join.
        self.sender.take();
        if let Some(worker) = self.worker.take() {
            match worker.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => eprintln!("capture metrics writer failed: {error}"),
                Err(_) => eprintln!("capture metrics writer panicked"),
            }
        }
    }
}

/// Owns metric aggregation and periodically writes one JSON Lines report.
fn aggregate_metrics(
    receiver: Receiver<CaptureSample>,
    file: File,
    report_interval: Duration,
    dropped_samples: Arc<AtomicU64>,
) -> io::Result<()> {
    let mut writer = BufWriter::new(file);
    let started = Instant::now();
    let mut last_report = started;
    let mut next_report = started + report_interval;
    let mut totals = Totals::default();
    let mut interval = IntervalTotals::default();
    let mut persist_latency = Histogram::<u64>::new(3).map_err(io::Error::other)?;

    loop {
        let now = Instant::now();
        let wait = next_report.saturating_duration_since(now);
        match receiver.recv_timeout(wait) {
            Ok(sample) => record_sample(sample, &mut totals, &mut interval, &mut persist_latency),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                if interval.storage_commits > 0 || totals.reconnects > 0 {
                    write_report(
                        &mut writer,
                        started,
                        last_report,
                        &totals,
                        &interval,
                        &persist_latency,
                        dropped_samples.load(Ordering::Relaxed),
                    )?;
                }
                return writer.flush();
            }
        }

        let now = Instant::now();
        if now >= next_report {
            write_report(
                &mut writer,
                started,
                last_report,
                &totals,
                &interval,
                &persist_latency,
                dropped_samples.load(Ordering::Relaxed),
            )?;
            interval = IntervalTotals::default();
            persist_latency.reset();
            last_report = now;
            next_report = now + report_interval;
        }
    }
}

/// Applies one sample to lifetime totals, interval totals, and latency history.
fn record_sample(
    sample: CaptureSample,
    totals: &mut Totals,
    interval: &mut IntervalTotals,
    persist_latency: &mut Histogram<u64>,
) {
    match sample {
        CaptureSample::Persisted {
            transaction_count,
            event_count,
            decoded_bytes,
            staged_bytes,
            persist_latency_ns,
            staged_transaction_count,
        } => {
            totals.storage_commits += 1;
            totals.transactions += transaction_count;
            totals.events += event_count;
            totals.decoded_bytes += decoded_bytes;
            totals.staged_bytes += staged_bytes;
            totals.staged_transactions += staged_transaction_count;
            interval.storage_commits += 1;
            interval.transactions += transaction_count;
            interval.events += event_count;
            interval.decoded_bytes += decoded_bytes;
            interval.staged_bytes += staged_bytes;
            let _ = persist_latency.record(persist_latency_ns.max(1));
        }
        CaptureSample::Reconnect => totals.reconnects += 1,
    }
}

#[allow(clippy::too_many_arguments)]
/// Serializes the current throughput, totals, and latency quantiles.
fn write_report(
    writer: &mut BufWriter<File>,
    started: Instant,
    last_report: Instant,
    totals: &Totals,
    interval: &IntervalTotals,
    persist_latency: &Histogram<u64>,
    dropped_samples: u64,
) -> io::Result<()> {
    let now = Instant::now();
    let interval_seconds = now.duration_since(last_report).as_secs_f64().max(0.000_001);
    let latency_ms = |quantile| persist_latency.value_at_quantile(quantile) as f64 / 1_000_000.0;
    let per_commit = |value| {
        if interval.storage_commits == 0 {
            0.0
        } else {
            value as f64 / interval.storage_commits as f64
        }
    };
    let report = json!({
        "timestamp_ms": unix_timestamp_ms(),
        "elapsed_seconds": now.duration_since(started).as_secs_f64(),
        "interval_seconds": interval_seconds,
        "events_per_second": interval.events as f64 / interval_seconds,
        "transactions_per_second": interval.transactions as f64 / interval_seconds,
        "storage_commits_per_second": interval.storage_commits as f64 / interval_seconds,
        "events_per_storage_commit": per_commit(interval.events),
        "transactions_per_storage_commit": per_commit(interval.transactions),
        "decoded_bytes_per_second": interval.decoded_bytes as f64 / interval_seconds,
        "staged_bytes_per_second": interval.staged_bytes as f64 / interval_seconds,
        "events_total": totals.events,
        "transactions_total": totals.transactions,
        "storage_commits_total": totals.storage_commits,
        "decoded_bytes_total": totals.decoded_bytes,
        "staged_bytes_total": totals.staged_bytes,
        "staged_transactions_total": totals.staged_transactions,
        "reconnects_total": totals.reconnects,
        "dropped_samples_total": dropped_samples,
        "persist_latency_ms": {
            "p50": latency_ms(0.50),
            "p95": latency_ms(0.95),
            "p99": latency_ms(0.99),
            "max": persist_latency.max() as f64 / 1_000_000.0,
        }
    });

    serde_json::to_writer(&mut *writer, &report)?;
    writer.write_all(b"\n")?;
    writer.flush()
}

/// Converts a duration to the histogram's bounded nanosecond representation.
fn duration_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

/// Returns a saturating Unix timestamp for serialized metric reports.
fn unix_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use std::{fs, thread};

    use tempfile::TempDir;

    use super::*;

    #[test]
    fn writes_transaction_metrics_as_json_lines() {
        let temp = TempDir::new().expect("temp dir");
        let path = temp.path().join("capture.jsonl");
        {
            let metrics = CaptureMetrics::start(&path, Duration::from_millis(10)).expect("metrics");
            metrics.record_persisted(4, 25, 1_000, 1_500, Duration::from_millis(2), 2);
            metrics.record_reconnect();
            thread::sleep(Duration::from_millis(20));
        }

        let reports = fs::read_to_string(path).expect("metrics output");
        let report: serde_json::Value =
            serde_json::from_str(reports.lines().next().expect("report line"))
                .expect("JSON report");
        assert_eq!(report["events_total"], 25);
        assert_eq!(report["transactions_total"], 4);
        assert_eq!(report["storage_commits_total"], 1);
        assert_eq!(report["events_per_storage_commit"], 25.0);
        assert_eq!(report["transactions_per_storage_commit"], 4.0);
        assert_eq!(report["staged_transactions_total"], 2);
        assert_eq!(report["reconnects_total"], 1);
        assert_eq!(report["dropped_samples_total"], 0);
    }

    #[test]
    fn stopped_reports_metrics_write_failure() {
        // /dev/full accepts open but fails every write with ENOSPC, which makes
        // the aggregation thread exit with a storage-full error deterministically.
        let metrics =
            CaptureMetrics::start(Path::new("/dev/full"), Duration::from_millis(10))
                .expect("metrics");
        metrics.record_persisted(1, 1, 1, 1, Duration::from_millis(1), 1);
        thread::sleep(Duration::from_millis(50));

        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let result = runtime.block_on(metrics.stopped());
        assert!(result.is_err(), "expected a metrics write failure");
    }
}
