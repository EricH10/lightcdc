//! Owns source validation, reconnects, heartbeats, retention, and session lifetime.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, anyhow};
use lightcdc_core::{Config, SourceConfig};
use lightcdc_postgres::{LIGHTCDC_HEARTBEAT_PREFIX, LogicalHeartbeatEmitter, ReplicationReader};
use lightcdc_runtime::{CaptureBatchLimits, CaptureStorageHandle, ProductionMetrics, RuntimeState};
use lightcdc_storage::RetentionPolicy;
use tracing::{info, warn};

use super::{
    AbortTask, CaptureContext,
    pipeline::{CaptureSessionExit, requested_event_count_reached, run_capture_session},
};

const RECONNECT_INITIAL_DELAY_MS: u64 = 250;
const RECONNECT_MAX_DELAY_MS: u64 = 15_000;

/// Combines a retention policy with its background sweep interval.
#[derive(Clone, Copy)]
pub(super) struct CaptureRetention {
    /// Count and age boundaries applied to retained event payloads.
    pub(super) policy: RetentionPolicy,
    /// Frequency at which the runtime asks storage to enforce the policy.
    check_interval: Duration,
}

/// Periodically queues bounded retention work on the shared storage writer.
pub(super) async fn run_retention_sweeps(
    storage: CaptureStorageHandle,
    retention: CaptureRetention,
    metrics: ProductionMetrics,
) -> anyhow::Result<()> {
    let mut interval = tokio::time::interval(retention.check_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await;

    loop {
        interval.tick().await;
        let outcome = match storage
            .prune(retention.policy, unix_timestamp_ms_i64())
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                metrics.record_retention_error();
                return Err(error).context("event retention sweep failed");
            }
        };
        metrics.record_retention(outcome);
        if outcome.deleted_events > 0 {
            info!(
                deleted_events = outcome.deleted_events,
                deleted_replay_ids = outcome.deleted_replay_ids,
                deleted_bytes = outcome.deleted_bytes,
                first_retained_sequence = ?outcome.first_retained_sequence,
                high_watermark = ?outcome.high_watermark,
                "pruned retained events"
            );
        }
    }
}

/// Validates the source, owns the heartbeat task, and supervises capture sessions.
pub(super) async fn supervise_capture(context: CaptureContext<'_>) -> anyhow::Result<()> {
    let mut captured = 0usize;
    let mut retry_attempt = 0u32;
    let mut reconnect_count = 0u64;

    validate_source_until_ready(&context, &mut retry_attempt, &mut reconnect_count).await?;
    let mut heartbeat_task = AbortTask::new(tokio::spawn(run_logical_heartbeats(
        context.config.source.clone(),
        context.heartbeat_interval,
    )));
    tokio::select! {
        capture_result = supervise_capture_sessions(
            &context,
            &mut captured,
            &mut retry_attempt,
            &mut reconnect_count,
        ) => {
            if let Err(error) = heartbeat_task.abort_and_wait().await
                && !error.is_cancelled()
            {
                warn!(%error, "logical heartbeat task stopped unexpectedly");
            }
            capture_result
        }
        heartbeat_result = heartbeat_task.wait() => {
            match heartbeat_result {
                Ok(Ok(())) => Err(anyhow!("logical heartbeat task stopped unexpectedly")),
                Ok(Err(error)) => Err(error).context("logical heartbeat task failed"),
                Err(error) => Err(error).context("logical heartbeat task panicked"),
            }
        }
    }
}

/// Reconnects replication sessions until capture completes or a fatal error occurs.
async fn supervise_capture_sessions(
    context: &CaptureContext<'_>,
    captured: &mut usize,
    retry_attempt: &mut u32,
    reconnect_count: &mut u64,
) -> anyhow::Result<()> {
    loop {
        if requested_event_count_reached(context.options, *captured) {
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
            CaptureSessionExit::RequestedEventCountReached => return Ok(()),
            CaptureSessionExit::Disconnected(reason) => {
                context
                    .state
                    .transition(RuntimeState::Retrying, Some(reason.clone()));
                wait_before_session_reconnect(context, retry_attempt, reconnect_count, &reason)
                    .await;
            }
        }
    }
}

/// Retries transient validation failures while surfacing configuration failures.
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
            Ok(validation) => {
                context
                    .production_metrics
                    .record_source_wal_end(validation.current_wal_lsn);
                let local_lsn = context
                    .store
                    .source_offset(context.source_name)
                    .context("failed to read local checkpoint during source validation")?;
                lightcdc_postgres::validate_resume_lsn(
                    local_lsn.as_deref(),
                    validation.confirmed_flush_lsn.as_deref(),
                )
                .context("PostgreSQL replication slot cannot resume local durable state")?;
                context
                    .store
                    .bind_source_identity(context.source_name, &validation.identity)
                    .context("PostgreSQL source identity does not match local durable state")?;

                if !validation
                    .publication
                    .unnecessary_published_tables
                    .is_empty()
                {
                    warn!(
                        tables = ?validation.publication.unnecessary_published_tables,
                        "publication contains tables that no configured stream consumes"
                    );
                }
                context.state.transition(RuntimeState::Starting, None);
                return Ok(());
            }
            Err(error) if error.is_retryable() => {
                context
                    .state
                    .transition(RuntimeState::Retrying, Some(error.to_string()));
                record_capture_reconnect(context);
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

/// Reopens replication from the durable redb LSN and local sequence.
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
                context.state.transition(RuntimeState::Capturing, None);
                return Ok((reader, source_offset.is_none()));
            }
            Err(error) if error.is_fatal_capture_error() => {
                return Err(error).context("failed to initialize transaction buffering");
            }
            Err(error) => {
                context
                    .state
                    .transition(RuntimeState::Retrying, Some(error.to_string()));
                record_capture_reconnect(context);
                wait_before_reconnect(*retry_attempt, *reconnect_count, &error).await;
                advance_reconnect_state(retry_attempt, reconnect_count);
            }
        }
    }
}

/// Attempts graceful replication shutdown without masking the session result.
async fn shutdown_capture_reader(reader: &mut ReplicationReader) {
    if let Err(error) = reader.shutdown().await {
        warn!(%error, "failed to close PostgreSQL capture session");
    }
}

/// Records and delays a reconnect after an established session disconnects.
async fn wait_before_session_reconnect(
    context: &CaptureContext<'_>,
    retry_attempt: &mut u32,
    reconnect_count: &mut u64,
    reason: &str,
) {
    record_capture_reconnect(context);
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

/// Records a reconnect only when opt-in metrics are active.
fn record_capture_reconnect(context: &CaptureContext<'_>) {
    context.production_metrics.record_source_reconnect();
    if let Some(metrics) = context.metrics {
        metrics.record_reconnect();
    }
}

/// Advances saturating retry counters after one backoff delay.
fn advance_reconnect_state(retry_attempt: &mut u32, reconnect_count: &mut u64) {
    *retry_attempt = retry_attempt.saturating_add(1);
    *reconnect_count = reconnect_count.saturating_add(1);
}

/// Validates group-commit settings and converts them to runtime limits.
pub(super) fn capture_batch_limits(config: &Config) -> anyhow::Result<CaptureBatchLimits> {
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

/// Validates and converts the logical heartbeat interval.
pub(super) fn capture_heartbeat_interval(config: &Config) -> anyhow::Result<Duration> {
    if config.runtime.heartbeat_interval_ms == 0 {
        return Err(anyhow!(
            "runtime.heartbeat_interval_ms must be greater than zero"
        ));
    }
    Ok(Duration::from_millis(config.runtime.heartbeat_interval_ms))
}

/// Emits transactional logical messages and reconnects its ordinary SQL client.
async fn run_logical_heartbeats(source: SourceConfig, interval: Duration) -> anyhow::Result<()> {
    let content = format!("source={};slot={}", source.name, source.slot);
    loop {
        match LogicalHeartbeatEmitter::connect(&source).await {
            Ok(emitter) => {
                let mut ticker = tokio::time::interval(interval);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                // Tokio's first interval tick is immediate; consume it so the
                // configured idle period elapses before emitting a heartbeat.
                ticker.tick().await;
                loop {
                    ticker.tick().await;
                    match emitter.emit(LIGHTCDC_HEARTBEAT_PREFIX, &content).await {
                        Ok(lsn) => {
                            tracing::debug!(%lsn, "emitted PostgreSQL logical heartbeat");
                        }
                        Err(error) => {
                            if error.is_fatal_capture_error() {
                                return Err(error.into());
                            }
                            warn!(%error, "logical heartbeat connection failed; reconnecting");
                            break;
                        }
                    }
                }
            }
            Err(error) => {
                if error.is_fatal_capture_error() {
                    return Err(error.into());
                }
                warn!(%error, "failed to connect PostgreSQL logical heartbeat; retrying");
            }
        }
        tokio::time::sleep(Duration::from_millis(RECONNECT_INITIAL_DELAY_MS)).await;
    }
}

/// Builds an optional validated retention schedule from runtime settings.
pub(super) fn capture_retention(config: &Config) -> anyhow::Result<Option<CaptureRetention>> {
    let runtime = &config.runtime;
    if runtime.retention_max_events.is_none()
        && runtime.retention_max_bytes.is_none()
        && runtime.retention_max_age_seconds.is_none()
    {
        return Ok(None);
    }
    if runtime.retention_max_events == Some(0) {
        return Err(anyhow!(
            "runtime.retention_max_events must be greater than zero"
        ));
    }
    if runtime.retention_max_bytes == Some(0) {
        anyhow::bail!("runtime.retention_max_bytes must be greater than zero");
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
            max_bytes: runtime.retention_max_bytes,
            max_age: runtime.retention_max_age_seconds.map(Duration::from_secs),
            delete_batch_size: runtime.retention_delete_batch_size,
        },
        check_interval: Duration::from_millis(runtime.retention_check_interval_ms),
    }))
}

/// Returns a saturating Unix timestamp for age-based retention.
fn unix_timestamp_ms_i64() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
