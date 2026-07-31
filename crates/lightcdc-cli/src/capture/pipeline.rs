//! Pipelines PostgreSQL transaction reads with one in-flight durable redb write.

use anyhow::Context;
use lightcdc_postgres::{CapturedTransaction, PostgresError, ReplicationReader, TransactionRead};
use lightcdc_runtime::{CaptureBatch, CaptureBatchLimits, PendingCaptureWrite, StorageCompletion};
use lightcdc_storage::PersistTransactionOutcome;
use tracing::warn;

use super::CaptureContext;
use crate::{
    cli::{CaptureOptions, CaptureOutput},
    display::event_to_json,
};

/// Distinguishes a newly persisted batch from a safely detected source replay.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PersistCaptureBatchOutcome {
    /// A new durable commit containing this many consumer events.
    Persisted(usize),
    /// A source transaction redelivered after its earlier durable commit.
    Replayed,
}

/// Represents whichever side of the capture/storage pipeline makes progress first.
enum CaptureProgress {
    /// A PostgreSQL transaction result, timeout, stream end, or read error.
    Transaction(Result<TransactionRead, PostgresError>),
    /// The result of the one in-flight redb batch.
    Storage(anyhow::Result<StorageCompletion>),
}

/// Tracks the queued batch, in-flight redb write, and replay recovery state.
struct CapturePipeline {
    /// Complete source transactions waiting to be submitted.
    batch: CaptureBatch,
    /// The single batch currently owned by the storage thread.
    pending_write: Option<PendingCaptureWrite>,
    /// Whether sequence assignment is known to follow durable redb state.
    replay_reconciled: bool,
}

impl CapturePipeline {
    /// Creates an empty pipeline with the connector's initial replay state.
    fn new(replay_reconciled: bool) -> Self {
        Self {
            batch: CaptureBatch::default(),
            pending_write: None,
            replay_reconciled,
        }
    }

    /// Counts durable, in-flight, and queued events toward a bounded run.
    fn buffered_event_count(&self, captured: usize) -> usize {
        captured
            .saturating_add(self.pending_event_count())
            .saturating_add(self.batch.event_count)
    }

    /// Returns the event count owned by the in-flight redb write.
    fn pending_event_count(&self) -> usize {
        self.pending_write
            .as_ref()
            .map_or(0, |pending_write| pending_write.event_count)
    }

    /// Enables the batch deadline only when waiting can safely trigger a flush.
    fn read_deadline(&self, limits: CaptureBatchLimits) -> Option<tokio::time::Instant> {
        if self.batch.is_empty() || !self.replay_reconciled || self.pending_write.is_some() {
            None
        } else {
            Some(self.batch.deadline(limits))
        }
    }
}

/// Tells the supervisor whether a session completed by request or needs reconnecting.
pub(super) enum CaptureSessionExit {
    /// The optional bounded-run event target has completed durably.
    RequestedEventCountReached,
    /// The session should reconnect from the durable source checkpoint.
    Disconnected(String),
}

/// Pipelines committed source transactions with one in-flight redb write.
pub(super) async fn run_capture_session(
    context: &CaptureContext<'_>,
    reader: &mut ReplicationReader,
    captured: &mut usize,
    replay_reconciled: bool,
) -> anyhow::Result<CaptureSessionExit> {
    let mut pipeline = CapturePipeline::new(replay_reconciled);

    loop {
        if requested_event_count_reached(context.options, *captured) {
            return Ok(CaptureSessionExit::RequestedEventCountReached);
        }

        if pending_write_reaches_requested_event_count(context.options, *captured, &pipeline) {
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

/// Waits for either another PostgreSQL transaction or the pending storage result.
async fn wait_for_capture_progress(
    context: &CaptureContext<'_>,
    reader: &mut ReplicationReader,
    pipeline: &mut CapturePipeline,
) -> CaptureProgress {
    let deadline = pipeline.read_deadline(context.batch_limits);
    if let Some(pending_write) = pipeline.pending_write.as_mut() {
        // Apply durable storage results before reading more WAL so replay and
        // sequence state cannot lag behind a completed write.
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

/// Adds one whole source transaction and flushes when a batch boundary is reached.
async fn buffer_captured_transaction(
    context: &CaptureContext<'_>,
    reader: &mut ReplicationReader,
    pipeline: &mut CapturePipeline,
    captured: &mut usize,
    transaction: CapturedTransaction,
) -> anyhow::Result<Option<CaptureSessionExit>> {
    if !pipeline.replay_reconciled {
        // PostgreSQL may redeliver the last durable transaction after reconnect.
        // Persist one batch synchronously so duplicate detection can reset the
        // local sequence before capture pipelines more WAL.
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
    let requested_event_count_buffered = context
        .options
        .stop_after_events
        .is_some_and(|max| pipeline.buffered_event_count(*captured) >= max);
    if pipeline.batch.reached_limit(context.batch_limits) || requested_event_count_buffered {
        if complete_pending_capture_write(context, reader, pipeline, captured).await?
            == Some(PersistCaptureBatchOutcome::Replayed)
        {
            return Ok(Some(reconnect_after_replayed_batch("queued")));
        }
        // An earlier in-flight batch can satisfy a bounded run while this batch
        // is queued. Leave the queued transaction unacknowledged for the next run.
        if !requested_event_count_reached(context.options, *captured) {
            submit_capture_batch(context, pipeline).await?;
        }
    }

    Ok(None)
}

/// Flushes a batch whose maximum group-commit delay has elapsed.
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

/// Drains durable and queued work before reconnecting an ended replication stream.
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

/// Preserves completed work and classifies a replication read failure.
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

/// Moves the queued batch into the dedicated writer without awaiting its commit.
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
                !pipeline.replay_reconciled,
            )
            .await?,
    );
    Ok(())
}

/// Submits the queued batch and waits for its durable result.
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

/// Awaits and applies the in-flight write when one exists.
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

/// Applies a storage result already received by `tokio::select!`.
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

/// Finalizes one storage completion and updates pipeline accounting.
fn complete_and_apply_capture_write(
    context: &CaptureContext<'_>,
    reader: &mut ReplicationReader,
    pipeline: &mut CapturePipeline,
    captured: &mut usize,
    completion: StorageCompletion,
) -> anyhow::Result<PersistCaptureBatchOutcome> {
    let outcome = complete_capture_write(completion, reader, context)?;
    apply_capture_batch_outcome(outcome, captured, &mut pipeline.replay_reconciled);
    Ok(outcome)
}

/// Checks whether the in-flight durable write will finish a bounded run.
fn pending_write_reaches_requested_event_count(
    options: &CaptureOptions,
    captured: usize,
    pipeline: &CapturePipeline,
) -> bool {
    options
        .stop_after_events
        .is_some_and(|max| captured.saturating_add(pipeline.pending_event_count()) >= max)
}

/// Requests reconnect so sequence assignment restarts from durable storage.
fn reconnect_after_replayed_batch(batch_state: &str) -> CaptureSessionExit {
    CaptureSessionExit::Disconnected(format!(
        "replayed {batch_state} batch required sequence reconciliation"
    ))
}

/// Returns true when a user-requested bounded run has durably completed.
pub(super) fn requested_event_count_reached(options: &CaptureOptions, captured: usize) -> bool {
    options
        .stop_after_events
        .is_some_and(|target| captured >= target)
}

/// Adapts bounded and unbounded reader calls into one progress enum.
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

/// Publishes side effects and acknowledges PostgreSQL only after redb succeeds.
fn complete_capture_write(
    completion: StorageCompletion,
    reader: &mut ReplicationReader,
    context: &CaptureContext<'_>,
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
                && let Some(event_notifier) = context.event_notifier
            {
                event_notifier.notify();
            }
            if let (Some(metrics), Some(persist_latency)) =
                (context.metrics, completion.persist_latency)
            {
                metrics.record_persisted(
                    transaction_count,
                    batch.event_count,
                    batch.decoded_bytes,
                    batch.staged_bytes,
                    persist_latency,
                    batch.staged_transaction_count,
                );
            }
            if matches!(context.options.output, CaptureOutput::Json) {
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
                context
                    .store
                    .next_sequence()
                    .context("failed to reset sequence after transaction replay")?,
            );
            PersistCaptureBatchOutcome::Replayed
        }
    };

    reader.ack(ack_lsn);
    Ok(outcome)
}

/// Updates bounded-run counts and replay reconciliation after storage completes.
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

#[cfg(test)]
mod tests {
    use lightcdc_core::{ChangeEvent, Operation, SourceMetadata};
    use lightcdc_postgres::CapturedTransaction;
    use lightcdc_storage::{TransactionEvents, TransactionStats};

    use super::*;

    #[test]
    fn capture_batch_flushes_before_adding_a_transaction_that_exceeds_limits() {
        let limits = CaptureBatchLimits {
            max_transactions: 10,
            max_events: 3,
            max_bytes: 1_000,
            max_delay: std::time::Duration::from_millis(5),
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
            max_delay: std::time::Duration::from_millis(5),
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
            max_delay: std::time::Duration::from_millis(5),
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
