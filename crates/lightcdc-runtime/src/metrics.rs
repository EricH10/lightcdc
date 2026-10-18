//! Exposes fixed-cardinality production metrics and samples durable storage off Tokio.

use std::{
    fmt::Write as _,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
    task::{Context as TaskContext, Poll},
    thread::{self, JoinHandle},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use axum::{Router, http::header, response::IntoResponse, routing::get, serve::Listener};
use lightcdc_storage::{RedbEventStore, RetentionOutcome};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore},
};

use crate::{ShutdownReceiver, directory_size, state::RuntimeState};

const COMMIT_LATENCY_BUCKET_NS: [u64; 14] = [
    1_000_000,
    2_500_000,
    5_000_000,
    10_000_000,
    25_000_000,
    50_000_000,
    100_000_000,
    250_000_000,
    500_000_000,
    1_000_000_000,
    2_500_000_000,
    5_000_000_000,
    10_000_000_000,
    u64::MAX,
];

/// Holds process-lifetime counters and gauges behind cheap relaxed atomics.
#[derive(Clone, Debug)]
pub struct ProductionMetrics {
    inner: Arc<MetricsInner>,
}

#[derive(Debug)]
struct MetricsInner {
    process_start_time_seconds: u64,
    runtime_state: AtomicU8,
    capture_transactions_total: AtomicU64,
    capture_events_total: AtomicU64,
    capture_decoded_bytes_total: AtomicU64,
    capture_staged_bytes_total: AtomicU64,
    capture_staged_transactions_total: AtomicU64,
    storage_commits_total: AtomicU64,
    storage_commit_duration_ns_sum: AtomicU64,
    storage_commit_duration_buckets: [AtomicU64; COMMIT_LATENCY_BUCKET_NS.len()],
    source_reconnects_total: AtomicU64,
    durable_source_lsn: AtomicU64,
    source_wal_end: AtomicU64,
    retention_sweeps_total: AtomicU64,
    retention_deleted_events_total: AtomicU64,
    retention_deleted_bytes_total: AtomicU64,
    retention_errors_total: AtomicU64,
    retained_events: AtomicU64,
    first_retained_sequence: AtomicU64,
    event_high_watermark: AtomicU64,
    consumer_offsets: AtomicU64,
    consumer_lag_max: AtomicU64,
    durable_consumer_limit_rejections_total: AtomicU64,
    active_subscriptions: AtomicU64,
    subscription_limit_rejections_total: AtomicU64,
    subscription_backpressure_total: AtomicU64,
    delivered_events_total: AtomicU64,
    acknowledgements_total: AtomicU64,
    seeks_total: AtomicU64,
    replay_errors_total: AtomicU64,
    active_api_connections: AtomicU64,
    api_connection_limit_rejections_total: AtomicU64,
    active_metrics_connections: AtomicU64,
    metrics_connection_limit_rejections_total: AtomicU64,
    storage_bytes: AtomicU64,
    staging_bytes: AtomicU64,
    filesystem_available_bytes: AtomicU64,
    filesystem_total_bytes: AtomicU64,
    storage_samples_total: AtomicU64,
    storage_sample_errors_total: AtomicU64,
    storage_sample_ok: AtomicBool,
    storage_healthy: AtomicBool,
    max_storage_bytes: u64,
    min_free_disk_bytes: u64,
    max_active_subscriptions: u64,
    max_durable_consumers: u64,
    max_api_connections: u64,
    max_metrics_connections: u64,
}

/// Rejects metrics sockets above a fixed process-wide connection ceiling.
struct LimitedMetricsListener {
    listener: TcpListener,
    permits: Arc<Semaphore>,
    metrics: ProductionMetrics,
}

/// Returns one metrics connection permit when Axum closes the socket.
struct LimitedMetricsConnection {
    stream: TcpStream,
    _permit: OwnedSemaphorePermit,
    metrics: ProductionMetrics,
}

impl Drop for LimitedMetricsConnection {
    fn drop(&mut self) {
        self.metrics.record_metrics_connection_finished();
    }
}

impl AsyncRead for LimitedMetricsConnection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(context, buffer)
    }
}

impl AsyncWrite for LimitedMetricsConnection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffer: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        Pin::new(&mut self.stream).poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.stream).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.stream).poll_shutdown(context)
    }
}

impl Listener for LimitedMetricsListener {
    type Io = LimitedMetricsConnection;
    type Addr = std::net::SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            let (stream, address) = Listener::accept(&mut self.listener).await;
            match Arc::clone(&self.permits).try_acquire_owned() {
                Ok(permit) => {
                    self.metrics.record_metrics_connection_started();
                    return (
                        LimitedMetricsConnection {
                            stream,
                            _permit: permit,
                            metrics: self.metrics.clone(),
                        },
                        address,
                    );
                }
                Err(_) => self.metrics.record_metrics_connection_limit_rejection(),
            }
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.listener.local_addr()
    }
}

/// Periodically reads redb metadata and filesystem usage on one owned OS thread.
pub struct StorageMetricsSampler {
    stop: Arc<(Mutex<bool>, Condvar)>,
    thread: Option<JoinHandle<()>>,
}

struct StorageSample {
    storage_bytes: u64,
    staging_bytes: u64,
    filesystem_available_bytes: u64,
    filesystem_total_bytes: u64,
    retained_events: u64,
    first_retained_sequence: u64,
    event_high_watermark: u64,
    consumer_offsets: u64,
    consumer_lag_max: u64,
    durable_source_lsn: u64,
}

impl ProductionMetrics {
    /// Creates one fixed-cardinality metric set for the single-node runtime.
    pub fn new(
        max_storage_bytes: u64,
        min_free_disk_bytes: u64,
        max_active_subscriptions: usize,
        max_durable_consumers: usize,
        max_api_connections: usize,
        max_metrics_connections: usize,
    ) -> Self {
        Self {
            inner: Arc::new(MetricsInner {
                process_start_time_seconds: unix_timestamp_seconds(),
                runtime_state: AtomicU8::new(runtime_state_value(RuntimeState::Starting)),
                capture_transactions_total: AtomicU64::new(0),
                capture_events_total: AtomicU64::new(0),
                capture_decoded_bytes_total: AtomicU64::new(0),
                capture_staged_bytes_total: AtomicU64::new(0),
                capture_staged_transactions_total: AtomicU64::new(0),
                storage_commits_total: AtomicU64::new(0),
                storage_commit_duration_ns_sum: AtomicU64::new(0),
                storage_commit_duration_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
                source_reconnects_total: AtomicU64::new(0),
                durable_source_lsn: AtomicU64::new(0),
                source_wal_end: AtomicU64::new(0),
                retention_sweeps_total: AtomicU64::new(0),
                retention_deleted_events_total: AtomicU64::new(0),
                retention_deleted_bytes_total: AtomicU64::new(0),
                retention_errors_total: AtomicU64::new(0),
                retained_events: AtomicU64::new(0),
                first_retained_sequence: AtomicU64::new(0),
                event_high_watermark: AtomicU64::new(0),
                consumer_offsets: AtomicU64::new(0),
                consumer_lag_max: AtomicU64::new(0),
                durable_consumer_limit_rejections_total: AtomicU64::new(0),
                active_subscriptions: AtomicU64::new(0),
                subscription_limit_rejections_total: AtomicU64::new(0),
                subscription_backpressure_total: AtomicU64::new(0),
                delivered_events_total: AtomicU64::new(0),
                acknowledgements_total: AtomicU64::new(0),
                seeks_total: AtomicU64::new(0),
                replay_errors_total: AtomicU64::new(0),
                active_api_connections: AtomicU64::new(0),
                api_connection_limit_rejections_total: AtomicU64::new(0),
                active_metrics_connections: AtomicU64::new(0),
                metrics_connection_limit_rejections_total: AtomicU64::new(0),
                storage_bytes: AtomicU64::new(0),
                staging_bytes: AtomicU64::new(0),
                filesystem_available_bytes: AtomicU64::new(0),
                filesystem_total_bytes: AtomicU64::new(0),
                storage_samples_total: AtomicU64::new(0),
                storage_sample_errors_total: AtomicU64::new(0),
                storage_sample_ok: AtomicBool::new(false),
                storage_healthy: AtomicBool::new(true),
                max_storage_bytes,
                min_free_disk_bytes,
                max_active_subscriptions: max_active_subscriptions as u64,
                max_durable_consumers: max_durable_consumers as u64,
                max_api_connections: max_api_connections as u64,
                max_metrics_connections: max_metrics_connections as u64,
            }),
        }
    }

    /// Records a lifecycle transition for health and one-hot state metrics.
    pub fn record_runtime_state(&self, state: RuntimeState) {
        self.inner
            .runtime_state
            .store(runtime_state_value(state), Ordering::Relaxed);
    }

    /// Records one newly durable group of complete PostgreSQL transactions.
    #[allow(clippy::too_many_arguments)]
    pub fn record_capture_persisted(
        &self,
        transaction_count: usize,
        event_count: usize,
        decoded_bytes: u64,
        staged_bytes: u64,
        staged_transaction_count: usize,
        persist_latency: Duration,
        durable_lsn: u64,
    ) {
        add(
            &self.inner.capture_transactions_total,
            transaction_count as u64,
        );
        add(&self.inner.capture_events_total, event_count as u64);
        add(&self.inner.capture_decoded_bytes_total, decoded_bytes);
        add(&self.inner.capture_staged_bytes_total, staged_bytes);
        add(
            &self.inner.capture_staged_transactions_total,
            staged_transaction_count as u64,
        );
        add(&self.inner.storage_commits_total, 1);
        let latency_ns = persist_latency.as_nanos().min(u64::MAX as u128) as u64;
        add(&self.inner.storage_commit_duration_ns_sum, latency_ns);
        let bucket = COMMIT_LATENCY_BUCKET_NS
            .iter()
            .position(|upper| latency_ns <= *upper)
            .unwrap_or(COMMIT_LATENCY_BUCKET_NS.len() - 1);
        add(&self.inner.storage_commit_duration_buckets[bucket], 1);
        self.record_durable_source_lsn(durable_lsn);
    }

    pub fn record_durable_source_lsn(&self, lsn: u64) {
        self.inner
            .durable_source_lsn
            .fetch_max(lsn, Ordering::Relaxed);
    }

    pub fn record_source_wal_end(&self, lsn: u64) {
        self.inner.source_wal_end.fetch_max(lsn, Ordering::Relaxed);
    }

    pub fn record_source_reconnect(&self) {
        add(&self.inner.source_reconnects_total, 1);
    }

    pub fn record_retention(&self, outcome: RetentionOutcome) {
        add(&self.inner.retention_sweeps_total, 1);
        add(
            &self.inner.retention_deleted_events_total,
            outcome.deleted_events,
        );
        add(
            &self.inner.retention_deleted_bytes_total,
            outcome.deleted_bytes,
        );
        self.inner.first_retained_sequence.store(
            outcome.first_retained_sequence.unwrap_or_default(),
            Ordering::Relaxed,
        );
        self.inner.event_high_watermark.store(
            outcome.high_watermark.unwrap_or_default(),
            Ordering::Relaxed,
        );
    }

    pub fn record_retention_error(&self) {
        add(&self.inner.retention_errors_total, 1);
        self.record_storage_error();
    }

    pub fn record_storage_error(&self) {
        self.inner.storage_healthy.store(false, Ordering::Relaxed);
    }

    pub fn record_subscription_started(&self) {
        add(&self.inner.active_subscriptions, 1);
    }

    pub fn record_subscription_finished(&self) {
        subtract(&self.inner.active_subscriptions, 1);
    }

    pub fn record_subscription_limit_rejection(&self) {
        add(&self.inner.subscription_limit_rejections_total, 1);
    }

    pub fn record_subscription_backpressure(&self) {
        add(&self.inner.subscription_backpressure_total, 1);
    }

    pub fn record_delivery(&self) {
        add(&self.inner.delivered_events_total, 1);
    }

    pub fn record_acknowledgement(&self) {
        add(&self.inner.acknowledgements_total, 1);
    }

    /// Records a rejected attempt to create another durable consumer identity.
    pub fn record_durable_consumer_limit_rejection(&self) {
        self.inner
            .durable_consumer_limit_rejections_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_seek(&self) {
        add(&self.inner.seeks_total, 1);
    }

    pub fn record_replay_error(&self) {
        add(&self.inner.replay_errors_total, 1);
    }

    pub fn record_api_connection_started(&self) {
        add(&self.inner.active_api_connections, 1);
    }

    pub fn record_api_connection_finished(&self) {
        subtract(&self.inner.active_api_connections, 1);
    }

    pub fn record_api_connection_limit_rejection(&self) {
        add(&self.inner.api_connection_limit_rejections_total, 1);
    }

    fn record_metrics_connection_started(&self) {
        add(&self.inner.active_metrics_connections, 1);
    }

    fn record_metrics_connection_finished(&self) {
        subtract(&self.inner.active_metrics_connections, 1);
    }

    fn record_metrics_connection_limit_rejection(&self) {
        add(&self.inner.metrics_connection_limit_rejections_total, 1);
    }

    /// Returns whether sampled storage remains writable and unfailed.
    pub fn storage_ready(&self) -> bool {
        self.inner.storage_sample_ok.load(Ordering::Relaxed)
            && self.inner.storage_healthy.load(Ordering::Relaxed)
            && self.inner.storage_bytes.load(Ordering::Relaxed) < self.inner.max_storage_bytes
            && self
                .inner
                .filesystem_available_bytes
                .load(Ordering::Relaxed)
                > self.inner.min_free_disk_bytes
    }

    /// Renders Prometheus text only when a scraper requests it.
    pub fn render(&self) -> String {
        let mut output = String::with_capacity(8 * 1024);
        let state = self.inner.runtime_state.load(Ordering::Relaxed);
        metric(
            &mut output,
            "lightcdc_process_start_time_seconds",
            "gauge",
            "Unix timestamp when this process metric set was created.",
            self.inner.process_start_time_seconds,
        );
        writeln!(
            output,
            "# HELP lightcdc_build_info Static LightCDC build information.\n# TYPE lightcdc_build_info gauge\nlightcdc_build_info{{version=\"{}\"}} 1",
            env!("CARGO_PKG_VERSION")
        )
        .expect("write metric text");
        writeln!(
            output,
            "# HELP lightcdc_runtime_state Current runtime state as one-hot gauges.\n# TYPE lightcdc_runtime_state gauge"
        )
        .expect("write metric text");
        for (name, value) in [
            ("starting", 0),
            ("capturing", 1),
            ("retrying", 2),
            ("degraded", 3),
            ("draining", 4),
            ("failed", 5),
        ] {
            writeln!(
                output,
                "lightcdc_runtime_state{{state=\"{name}\"}} {}",
                u8::from(state == value)
            )
            .expect("write metric text");
        }
        gauge(
            &mut output,
            "lightcdc_terminal_state",
            "Whether the runtime entered its terminal failed state.",
            u8::from(state == runtime_state_value(RuntimeState::Failed)),
        );
        counter(
            &mut output,
            "lightcdc_capture_transactions_total",
            "Durable PostgreSQL source transactions.",
            load(&self.inner.capture_transactions_total),
        );
        counter(
            &mut output,
            "lightcdc_capture_events_total",
            "Durable consumer events.",
            load(&self.inner.capture_events_total),
        );
        counter(
            &mut output,
            "lightcdc_capture_decoded_bytes_total",
            "Decoded source bytes made durable.",
            load(&self.inner.capture_decoded_bytes_total),
        );
        counter(
            &mut output,
            "lightcdc_capture_staged_bytes_total",
            "Bytes represented by disk-staged transactions.",
            load(&self.inner.capture_staged_bytes_total),
        );
        counter(
            &mut output,
            "lightcdc_capture_staged_transactions_total",
            "Transactions spilled to staging files.",
            load(&self.inner.capture_staged_transactions_total),
        );
        render_commit_histogram(&mut output, &self.inner);
        counter(
            &mut output,
            "lightcdc_source_reconnects_total",
            "PostgreSQL reconnect delays entered.",
            load(&self.inner.source_reconnects_total),
        );
        let durable_lsn = load(&self.inner.durable_source_lsn);
        let wal_end = load(&self.inner.source_wal_end);
        gauge(
            &mut output,
            "lightcdc_durable_source_lsn_bytes",
            "Durable PostgreSQL source LSN as a byte position.",
            durable_lsn,
        );
        gauge(
            &mut output,
            "lightcdc_source_wal_end_bytes",
            "Latest observed PostgreSQL WAL end byte position.",
            wal_end,
        );
        gauge(
            &mut output,
            "lightcdc_source_wal_lag_bytes",
            "Observed WAL end minus the durable source LSN.",
            wal_end.saturating_sub(durable_lsn),
        );
        counter(
            &mut output,
            "lightcdc_retention_sweeps_total",
            "Completed retention sweeps.",
            load(&self.inner.retention_sweeps_total),
        );
        counter(
            &mut output,
            "lightcdc_retention_deleted_events_total",
            "Event payloads removed by retention.",
            load(&self.inner.retention_deleted_events_total),
        );
        counter(
            &mut output,
            "lightcdc_retention_deleted_bytes_total",
            "Segment bytes removed by retention.",
            load(&self.inner.retention_deleted_bytes_total),
        );
        counter(
            &mut output,
            "lightcdc_retention_errors_total",
            "Retention sweeps that failed.",
            load(&self.inner.retention_errors_total),
        );
        let first = load(&self.inner.first_retained_sequence);
        let high = load(&self.inner.event_high_watermark);
        gauge(
            &mut output,
            "lightcdc_retained_events",
            "Retained durable event payloads.",
            load(&self.inner.retained_events),
        );
        gauge(
            &mut output,
            "lightcdc_first_retained_sequence",
            "Oldest retained event sequence, or zero when empty.",
            first,
        );
        gauge(
            &mut output,
            "lightcdc_event_high_watermark",
            "Highest durable event sequence, or zero when empty.",
            high,
        );
        gauge(
            &mut output,
            "lightcdc_retained_sequence_span",
            "Global sequence distance represented by retained payloads.",
            high.saturating_sub(first)
                .saturating_add(u64::from(high > 0)),
        );
        gauge(
            &mut output,
            "lightcdc_consumer_offsets",
            "Named durable consumer offsets.",
            load(&self.inner.consumer_offsets),
        );
        gauge(
            &mut output,
            "lightcdc_consumer_lag_max_sequences",
            "Maximum global sequence distance from a consumer offset to the event high-water mark.",
            load(&self.inner.consumer_lag_max),
        );
        gauge(
            &mut output,
            "lightcdc_max_durable_consumers",
            "Configured durable consumer identity limit.",
            self.inner.max_durable_consumers,
        );
        counter(
            &mut output,
            "lightcdc_durable_consumer_limit_rejections_total",
            "New durable consumer identities rejected by the configured limit.",
            load(&self.inner.durable_consumer_limit_rejections_total),
        );
        gauge(
            &mut output,
            "lightcdc_active_subscriptions",
            "Current active gRPC subscriptions.",
            load(&self.inner.active_subscriptions),
        );
        gauge(
            &mut output,
            "lightcdc_max_active_subscriptions",
            "Configured active subscription limit.",
            self.inner.max_active_subscriptions,
        );
        counter(
            &mut output,
            "lightcdc_subscription_limit_rejections_total",
            "Subscriptions rejected by the active limit.",
            load(&self.inner.subscription_limit_rejections_total),
        );
        counter(
            &mut output,
            "lightcdc_subscription_backpressure_total",
            "Deliveries that found a full consumer queue.",
            load(&self.inner.subscription_backpressure_total),
        );
        counter(
            &mut output,
            "lightcdc_delivered_events_total",
            "Events made available to gRPC consumers.",
            load(&self.inner.delivered_events_total),
        );
        counter(
            &mut output,
            "lightcdc_acknowledgements_total",
            "Successful consumer acknowledgement calls.",
            load(&self.inner.acknowledgements_total),
        );
        counter(
            &mut output,
            "lightcdc_seeks_total",
            "Successful administrative consumer seeks.",
            load(&self.inner.seeks_total),
        );
        counter(
            &mut output,
            "lightcdc_replay_errors_total",
            "Consumer replay reads that failed.",
            load(&self.inner.replay_errors_total),
        );
        gauge(
            &mut output,
            "lightcdc_active_api_connections",
            "Current accepted gRPC TCP connections.",
            load(&self.inner.active_api_connections),
        );
        gauge(
            &mut output,
            "lightcdc_max_api_connections",
            "Configured gRPC TCP connection limit.",
            self.inner.max_api_connections,
        );
        counter(
            &mut output,
            "lightcdc_api_connection_limit_rejections_total",
            "gRPC connections rejected by the process limit.",
            load(&self.inner.api_connection_limit_rejections_total),
        );
        gauge(
            &mut output,
            "lightcdc_active_metrics_connections",
            "Current accepted Prometheus TCP connections.",
            load(&self.inner.active_metrics_connections),
        );
        gauge(
            &mut output,
            "lightcdc_max_metrics_connections",
            "Configured Prometheus TCP connection limit.",
            self.inner.max_metrics_connections,
        );
        counter(
            &mut output,
            "lightcdc_metrics_connection_limit_rejections_total",
            "Prometheus connections rejected by the process limit.",
            load(&self.inner.metrics_connection_limit_rejections_total),
        );
        gauge(
            &mut output,
            "lightcdc_storage_bytes",
            "Bytes allocated below the LightCDC data directory.",
            load(&self.inner.storage_bytes),
        );
        gauge(
            &mut output,
            "lightcdc_staging_bytes",
            "Bytes currently allocated to transaction staging.",
            load(&self.inner.staging_bytes),
        );
        gauge(
            &mut output,
            "lightcdc_max_storage_bytes",
            "Configured hard data-directory ceiling.",
            self.inner.max_storage_bytes,
        );
        gauge(
            &mut output,
            "lightcdc_filesystem_available_bytes",
            "Bytes available on the data filesystem.",
            load(&self.inner.filesystem_available_bytes),
        );
        gauge(
            &mut output,
            "lightcdc_filesystem_total_bytes",
            "Total bytes on the data filesystem.",
            load(&self.inner.filesystem_total_bytes),
        );
        gauge(
            &mut output,
            "lightcdc_min_free_disk_bytes",
            "Configured free-space reserve.",
            self.inner.min_free_disk_bytes,
        );
        counter(
            &mut output,
            "lightcdc_storage_samples_total",
            "Successful durable-store and filesystem samples.",
            load(&self.inner.storage_samples_total),
        );
        counter(
            &mut output,
            "lightcdc_storage_sample_errors_total",
            "Durable-store or filesystem samples that failed.",
            load(&self.inner.storage_sample_errors_total),
        );
        gauge(
            &mut output,
            "lightcdc_storage_sample_ok",
            "Whether the latest storage sample succeeded.",
            u8::from(self.inner.storage_sample_ok.load(Ordering::Relaxed)),
        );
        gauge(
            &mut output,
            "lightcdc_storage_ready",
            "Whether storage is sampled, healthy, below its ceiling, and above its reserve.",
            u8::from(self.storage_ready()),
        );
        output
    }

    fn apply_storage_sample(&self, sample: StorageSample) {
        self.inner
            .storage_bytes
            .store(sample.storage_bytes, Ordering::Relaxed);
        self.inner
            .staging_bytes
            .store(sample.staging_bytes, Ordering::Relaxed);
        self.inner
            .filesystem_available_bytes
            .store(sample.filesystem_available_bytes, Ordering::Relaxed);
        self.inner
            .filesystem_total_bytes
            .store(sample.filesystem_total_bytes, Ordering::Relaxed);
        self.inner
            .retained_events
            .store(sample.retained_events, Ordering::Relaxed);
        self.inner
            .first_retained_sequence
            .store(sample.first_retained_sequence, Ordering::Relaxed);
        self.inner
            .event_high_watermark
            .store(sample.event_high_watermark, Ordering::Relaxed);
        self.inner
            .consumer_offsets
            .store(sample.consumer_offsets, Ordering::Relaxed);
        self.inner
            .consumer_lag_max
            .store(sample.consumer_lag_max, Ordering::Relaxed);
        self.record_durable_source_lsn(sample.durable_source_lsn);
        self.inner.storage_sample_ok.store(true, Ordering::Relaxed);
        self.inner.storage_healthy.store(true, Ordering::Relaxed);
        add(&self.inner.storage_samples_total, 1);
    }

    fn record_storage_sample_error(&self) {
        self.inner.storage_sample_ok.store(false, Ordering::Relaxed);
        add(&self.inner.storage_sample_errors_total, 1);
    }
}

impl StorageMetricsSampler {
    /// Samples once during startup, then continues on a dedicated thread.
    pub fn start(
        metrics: ProductionMetrics,
        store: RedbEventStore,
        data_dir: PathBuf,
        source_name: String,
        interval: Duration,
    ) -> anyhow::Result<Self> {
        if interval.is_zero() {
            anyhow::bail!("storage metrics sample interval must be greater than zero");
        }
        let initial = sample_storage(&store, &data_dir, &source_name)
            .context("take initial production storage metrics sample")?;
        metrics.apply_storage_sample(initial);

        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let thread_stop = Arc::clone(&stop);
        let thread = thread::Builder::new()
            .name("lightcdc-storage-metrics".to_owned())
            .spawn(move || {
                let (lock, wake) = &*thread_stop;
                loop {
                    let stopped = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                    let (stopped, _) = wake
                        .wait_timeout_while(stopped, interval, |stopped| !*stopped)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    if *stopped {
                        return;
                    }
                    drop(stopped);

                    match sample_storage(&store, &data_dir, &source_name) {
                        Ok(sample) => metrics.apply_storage_sample(sample),
                        Err(error) => {
                            metrics.record_storage_sample_error();
                            tracing::warn!(%error, "production storage metrics sample failed");
                        }
                    }
                }
            })
            .context("start production storage metrics thread")?;

        Ok(Self {
            stop,
            thread: Some(thread),
        })
    }
}

impl Drop for StorageMetricsSampler {
    fn drop(&mut self) {
        let (lock, wake) = &*self.stop;
        *lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
        wake.notify_all();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Binds the listener before spawning so startup cannot hide an address failure.
pub async fn bind_metrics_listener(address: std::net::SocketAddr) -> anyhow::Result<TcpListener> {
    TcpListener::bind(address)
        .await
        .with_context(|| format!("bind production metrics listener {address}"))
}

/// Serves Prometheus text until coordinated process shutdown begins.
pub async fn serve_metrics(
    listener: TcpListener,
    metrics: ProductionMetrics,
    max_connections: usize,
    mut shutdown: ShutdownReceiver,
) -> anyhow::Result<()> {
    if max_connections == 0 {
        anyhow::bail!("metrics connection limit must be greater than zero");
    }
    let address = listener
        .local_addr()
        .context("read metrics listener address")?;
    let app_metrics = metrics.clone();
    let app = Router::new().route(
        "/metrics",
        get(move || {
            let metrics = app_metrics.clone();
            async move {
                (
                    [
                        (
                            header::CONTENT_TYPE,
                            "text/plain; version=0.0.4; charset=utf-8",
                        ),
                        (header::CACHE_CONTROL, "no-store"),
                    ],
                    metrics.render(),
                )
                    .into_response()
            }
        }),
    );
    tracing::info!(%address, "starting production metrics server");
    axum::serve(
        LimitedMetricsListener {
            listener,
            permits: Arc::new(Semaphore::new(max_connections)),
            metrics,
        },
        app,
    )
    .with_graceful_shutdown(async move { shutdown.cancelled().await })
    .await
    .context("serve production metrics endpoint")
}

fn sample_storage(
    store: &RedbEventStore,
    data_dir: &Path,
    source_name: &str,
) -> anyhow::Result<StorageSample> {
    let stats = store.stats().context("read durable store statistics")?;
    let first = store
        .first_sequence()
        .context("read first retained sequence")?;
    let high = store
        .last_sequence()
        .context("read event high-water mark")?;
    let offsets = store.consumer_offsets().context("read consumer offsets")?;
    let high_value = high.unwrap_or_default();
    let consumer_lag_max = offsets
        .iter()
        .map(|offset| high_value.saturating_sub(offset.sequence))
        .max()
        .unwrap_or_default();
    let durable_source_lsn = store
        .source_offset(source_name)
        .context("read durable source LSN")?
        .as_deref()
        .map(parse_lsn)
        .transpose()?
        .unwrap_or_default();
    let staging_dir = data_dir.join("staging");
    Ok(StorageSample {
        storage_bytes: directory_size(data_dir).context("measure data directory")?,
        staging_bytes: if staging_dir.exists() {
            directory_size(&staging_dir).context("measure staging directory")?
        } else {
            0
        },
        filesystem_available_bytes: fs2::available_space(data_dir)
            .context("measure available filesystem bytes")?,
        filesystem_total_bytes: fs2::total_space(data_dir)
            .context("measure total filesystem bytes")?,
        retained_events: stats.event_count,
        first_retained_sequence: first.unwrap_or_default(),
        event_high_watermark: high_value,
        consumer_offsets: offsets.len() as u64,
        consumer_lag_max,
        durable_source_lsn,
    })
}

fn parse_lsn(lsn: &str) -> anyhow::Result<u64> {
    let (high, low) = lsn
        .split_once('/')
        .with_context(|| format!("durable LSN {lsn:?} has no separator"))?;
    let high = u64::from_str_radix(high, 16)
        .with_context(|| format!("durable LSN {lsn:?} has an invalid high half"))?;
    let low = u64::from_str_radix(low, 16)
        .with_context(|| format!("durable LSN {lsn:?} has an invalid low half"))?;
    Ok((high << 32) | low)
}

fn render_commit_histogram(output: &mut String, metrics: &MetricsInner) {
    writeln!(output, "# HELP lightcdc_storage_commit_duration_seconds Durable redb capture-commit latency.\n# TYPE lightcdc_storage_commit_duration_seconds histogram").expect("write metric text");
    let mut cumulative = 0u64;
    for (index, upper_ns) in COMMIT_LATENCY_BUCKET_NS.iter().enumerate() {
        cumulative =
            cumulative.saturating_add(load(&metrics.storage_commit_duration_buckets[index]));
        if *upper_ns == u64::MAX {
            writeln!(
                output,
                "lightcdc_storage_commit_duration_seconds_bucket{{le=\"+Inf\"}} {cumulative}"
            )
            .expect("write metric text");
        } else {
            writeln!(
                output,
                "lightcdc_storage_commit_duration_seconds_bucket{{le=\"{}\"}} {cumulative}",
                *upper_ns as f64 / 1_000_000_000.0
            )
            .expect("write metric text");
        }
    }
    let count = load(&metrics.storage_commits_total);
    let sum = load(&metrics.storage_commit_duration_ns_sum) as f64 / 1_000_000_000.0;
    writeln!(output, "lightcdc_storage_commit_duration_seconds_sum {sum}\nlightcdc_storage_commit_duration_seconds_count {count}").expect("write metric text");
}

fn metric(output: &mut String, name: &str, kind: &str, help: &str, value: impl std::fmt::Display) {
    writeln!(
        output,
        "# HELP {name} {help}\n# TYPE {name} {kind}\n{name} {value}"
    )
    .expect("write metric text");
}

fn counter(output: &mut String, name: &str, help: &str, value: impl std::fmt::Display) {
    metric(output, name, "counter", help, value);
}

fn gauge(output: &mut String, name: &str, help: &str, value: impl std::fmt::Display) {
    metric(output, name, "gauge", help, value);
}

fn add(counter: &AtomicU64, value: u64) {
    counter.fetch_add(value, Ordering::Relaxed);
}

fn subtract(counter: &AtomicU64, value: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(value))
    });
}

fn load(value: &AtomicU64) -> u64 {
    value.load(Ordering::Relaxed)
}

fn runtime_state_value(state: RuntimeState) -> u8 {
    match state {
        RuntimeState::Starting => 0,
        RuntimeState::Capturing => 1,
        RuntimeState::Retrying => 2,
        RuntimeState::Degraded => 3,
        RuntimeState::Draining => 4,
        RuntimeState::Failed => 5,
    }
}

fn unix_timestamp_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use lightcdc_storage::LogOpenOptions;
    use tempfile::TempDir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use crate::shutdown_channel;

    #[test]
    fn renders_fixed_metrics_and_commit_histogram() {
        let metrics = ProductionMetrics::new(1_000_000, 1_000, 32, 10_000, 16, 8);
        metrics.record_runtime_state(RuntimeState::Capturing);
        metrics.record_source_wal_end(500);
        metrics.record_capture_persisted(4, 25, 1_000, 500, 2, Duration::from_millis(7), 400);

        let rendered = metrics.render();

        assert!(rendered.contains("lightcdc_capture_events_total 25"));
        assert!(rendered.contains("lightcdc_source_wal_lag_bytes 100"));
        assert!(
            rendered.contains("lightcdc_storage_commit_duration_seconds_bucket{le=\"0.01\"} 1")
        );
        assert!(rendered.contains("lightcdc_runtime_state{state=\"capturing\"} 1"));
    }

    #[test]
    fn successful_storage_sample_clears_a_prior_storage_error() {
        let metrics = ProductionMetrics::new(u64::MAX, 0, 32, 10_000, 16, 8);
        metrics.record_storage_error();
        assert!(
            !metrics.storage_ready(),
            "a recorded storage error must latch storage unhealthy"
        );

        metrics.apply_storage_sample(StorageSample {
            storage_bytes: 0,
            staging_bytes: 0,
            filesystem_available_bytes: u64::MAX,
            filesystem_total_bytes: u64::MAX,
            retained_events: 0,
            first_retained_sequence: 0,
            event_high_watermark: 0,
            consumer_offsets: 0,
            consumer_lag_max: 0,
            durable_source_lsn: 0,
        });

        assert!(
            metrics.storage_ready(),
            "a healthy storage sample must clear a prior storage error"
        );
    }

    #[test]
    fn parses_postgres_lsn_into_a_byte_position() {
        assert_eq!(parse_lsn("1/00000010").expect("LSN"), (1_u64 << 32) + 16);
    }

    #[tokio::test]
    async fn serves_prometheus_text_and_drains_on_shutdown() {
        let metrics = ProductionMetrics::new(1_000_000, 1_000, 32, 10_000, 16, 8);
        let listener = bind_metrics_listener("127.0.0.1:0".parse().expect("address"))
            .await
            .expect("listener");
        let address = listener.local_addr().expect("listener address");
        let (shutdown, shutdown_rx) = shutdown_channel();
        let server = tokio::spawn(serve_metrics(listener, metrics, 8, shutdown_rx));

        let mut client = tokio::net::TcpStream::connect(address)
            .await
            .expect("connect metrics");
        client
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .expect("write request");
        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .await
            .expect("read response");
        let response = String::from_utf8(response).expect("UTF-8 response");

        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("content-type: text/plain; version=0.0.4"));
        assert!(response.contains("lightcdc_capture_events_total 0"));

        shutdown.trigger();
        server.await.expect("metrics task").expect("metrics server");
    }

    #[tokio::test]
    async fn metrics_listener_rejects_excess_connections_and_recovers() {
        let metrics = ProductionMetrics::new(1_000_000, 1_000, 32, 10_000, 16, 1);
        let listener = bind_metrics_listener("127.0.0.1:0".parse().expect("address"))
            .await
            .expect("listener");
        let address = listener.local_addr().expect("listener address");
        let mut listener = LimitedMetricsListener {
            listener,
            permits: Arc::new(Semaphore::new(1)),
            metrics: metrics.clone(),
        };

        let _first_client = TcpStream::connect(address).await.expect("first client");
        let (first_server, _) = listener.accept().await;
        let excess_client = TcpStream::connect(address).await.expect("excess client");
        let accept = tokio::spawn(async move { listener.accept().await });
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(
            metrics
                .render()
                .contains("lightcdc_metrics_connection_limit_rejections_total 1")
        );

        drop(excess_client);
        drop(first_server);
        let _next_client = TcpStream::connect(address).await.expect("next client");
        let (next_server, _) = tokio::time::timeout(Duration::from_secs(1), accept)
            .await
            .expect("listener should recover")
            .expect("accept task");
        assert!(
            metrics
                .render()
                .contains("lightcdc_active_metrics_connections 1")
        );
        drop(next_server);
        assert!(
            metrics
                .render()
                .contains("lightcdc_active_metrics_connections 0")
        );
    }

    #[test]
    fn initial_storage_sample_makes_readiness_meaningful() {
        let temp = TempDir::new().expect("temp dir");
        let store = RedbEventStore::open(&LogOpenOptions {
            data_dir: temp.path().to_path_buf(),
            database_file: "events.redb".to_owned(),
        })
        .expect("store");
        let metrics = ProductionMetrics::new(u64::MAX, 0, 32, 10_000, 16, 8);
        let sampler = StorageMetricsSampler::start(
            metrics.clone(),
            store,
            temp.path().to_path_buf(),
            "default".to_owned(),
            Duration::from_secs(60),
        )
        .expect("sampler");

        assert!(metrics.storage_ready());
        assert!(
            metrics
                .render()
                .contains("lightcdc_storage_samples_total 1")
        );
        drop(sampler);
    }
}
