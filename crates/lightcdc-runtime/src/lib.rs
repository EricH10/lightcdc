//! Serializes synchronous redb writes from capture and API callers on one thread.

mod metrics;
mod state;

pub use metrics::{ProductionMetrics, StorageMetricsSampler, bind_metrics_listener, serve_metrics};
pub use state::{
    RuntimeState, RuntimeStateHandle, RuntimeStateReceiver, ShutdownHandle, ShutdownReceiver,
    runtime_state_channel, runtime_state_channel_with_metrics, shutdown_channel,
};

use std::{
    fs,
    path::{Path, PathBuf},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use anyhow::{Context, anyhow};
use lightcdc_core::ChangeEvent;
use lightcdc_postgres::CapturedTransaction;
use lightcdc_storage::{
    PersistTransactionOutcome, RedbEventStore, RetentionOutcome, RetentionPolicy,
    SourceTransaction, StorageError,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::time::Instant as TokioInstant;

const STORAGE_COMMAND_CAPACITY: usize = 1;
const REDB_WRITE_HEADROOM_BYTES: u64 = 16 * 1024 * 1024;

/// Bounds durable storage growth while reserving room for recovery operations.
#[derive(Clone, Debug)]
pub struct StorageResourceLimits {
    /// Directory containing the control database, segments, and staging files.
    pub data_dir: PathBuf,
    /// Maximum logical bytes allowed below `data_dir` before another batch.
    pub max_storage_bytes: u64,
    /// Filesystem bytes that capture must leave unused.
    pub min_free_disk_bytes: u64,
}

/// Bounds how many complete source transactions share one redb commit.
#[derive(Clone, Copy, Debug)]
pub struct CaptureBatchLimits {
    /// Maximum grouped source transactions.
    pub max_transactions: usize,
    /// Soft grouped event boundary.
    pub max_events: usize,
    /// Soft grouped decoded-byte boundary.
    pub max_bytes: u64,
    /// Maximum wait measured from the first grouped transaction.
    pub max_delay: Duration,
}

/// Accumulates complete source transactions and their resource totals.
#[derive(Default)]
pub struct CaptureBatch {
    /// Whole source transactions preserved in commit order.
    pub transactions: Vec<CapturedTransaction>,
    /// Total consumer events across the grouped transactions.
    pub event_count: usize,
    /// Total estimated decoded bytes across the group.
    pub decoded_bytes: u64,
    /// Total staged representation bytes across the group.
    pub staged_bytes: u64,
    /// Transactions whose events currently live in staging files.
    pub staged_transaction_count: usize,
    /// Time the first transaction entered this group.
    started_at: Option<TokioInstant>,
}

impl CaptureBatch {
    /// Adds a whole source transaction and updates aggregate accounting.
    pub fn push(&mut self, transaction: CapturedTransaction) {
        let stats = transaction.events.stats();
        if self.transactions.is_empty() {
            self.started_at = Some(TokioInstant::now());
        }
        self.event_count = self.event_count.saturating_add(stats.event_count);
        self.decoded_bytes = self.decoded_bytes.saturating_add(stats.decoded_bytes);
        self.staged_bytes = self.staged_bytes.saturating_add(stats.staged_bytes);
        self.staged_transaction_count += usize::from(transaction.events.is_staged());
        self.transactions.push(transaction);
    }

    /// Returns true when the batch contains no source transactions.
    pub fn is_empty(&self) -> bool {
        self.transactions.is_empty()
    }

    /// Checks whether adding a transaction would exceed a non-empty batch's limits.
    ///
    /// An empty batch always accepts one transaction so a source transaction is
    /// never split merely to satisfy redb group-commit limits.
    pub fn would_exceed(
        &self,
        transaction: &CapturedTransaction,
        limits: CaptureBatchLimits,
    ) -> bool {
        if self.is_empty() {
            return false;
        }
        let stats = transaction.events.stats();
        self.transactions.len().saturating_add(1) > limits.max_transactions
            || self.event_count.saturating_add(stats.event_count) > limits.max_events
            || self.decoded_bytes.saturating_add(stats.decoded_bytes) > limits.max_bytes
    }

    /// Returns true when the current batch has met any flush boundary.
    pub fn reached_limit(&self, limits: CaptureBatchLimits) -> bool {
        self.transactions.len() >= limits.max_transactions
            || self.event_count >= limits.max_events
            || self.decoded_bytes >= limits.max_bytes
    }

    /// Returns when the oldest transaction in this batch must be flushed.
    pub fn deadline(&self, limits: CaptureBatchLimits) -> TokioInstant {
        self.started_at
            .expect("non-empty capture batch has a start time")
            + limits.max_delay
    }
}

/// Owns the lifetime of the dedicated redb writer thread.
pub struct CaptureStorageWriter {
    handle: Option<CaptureStorageHandle>,
    thread: Option<JoinHandle<()>>,
}

/// Sends serialized storage commands to the dedicated writer.
#[derive(Clone)]
pub struct CaptureStorageHandle {
    sender: mpsc::Sender<StorageCommand>,
}

/// Owns a fixed set of OS threads used for synchronous replay reads.
pub struct StorageReaderPool {
    handle: Option<StorageReaderHandle>,
    workers: Vec<JoinHandle<()>>,
}

/// Submits bounded storage reads without blocking a Tokio worker.
#[derive(Clone)]
pub struct StorageReaderHandle {
    sender: flume::Sender<StorageReadCommand>,
    replay_permits: std::sync::Arc<Semaphore>,
}

/// Keeps one byte-bounded replay response inside the pool's aggregate limit.
pub struct ReplayBatch {
    events: Vec<ChangeEvent>,
    _permit: OwnedSemaphorePermit,
}

impl ReplayBatch {
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

impl IntoIterator for ReplayBatch {
    type Item = ChangeEvent;
    type IntoIter = std::vec::IntoIter<ChangeEvent>;

    fn into_iter(self) -> Self::IntoIter {
        self.events.into_iter()
    }
}

/// Tracks one submitted batch until its redb result is available.
pub struct PendingCaptureWrite {
    pub response: oneshot::Receiver<StorageCompletion>,
    pub event_count: usize,
}

/// Returns both the original batch and its storage result to the async pipeline.
pub struct StorageCompletion {
    pub batch: CaptureBatch,
    pub result: Result<PersistTransactionOutcome, StorageError>,
    pub persist_latency: Option<Duration>,
}

/// Carries all state required to persist and report one capture batch.
struct PersistCommand {
    batch: CaptureBatch,
    measure_latency: bool,
    check_replay: bool,
    #[cfg(test)]
    delay_before_persist: Option<Duration>,
    response: oneshot::Sender<StorageCompletion>,
}

/// Enumerates writes serialized against the same segmented event store.
enum StorageCommand {
    Persist(PersistCommand),
    Prune {
        policy: RetentionPolicy,
        now_ms: i64,
        response: oneshot::Sender<Result<RetentionOutcome, StorageError>>,
    },
    AcknowledgeConsumer {
        stream_name: String,
        consumer_name: String,
        sequence: u64,
        maximum_consumers: usize,
        response: oneshot::Sender<Result<u64, StorageError>>,
    },
    SetConsumerOffset {
        stream_name: String,
        consumer_name: String,
        offset: u64,
        maximum_consumers: usize,
        response: oneshot::Sender<Result<(), StorageError>>,
    },
}

enum StorageReadCommand {
    ReplayFrom {
        sequence: u64,
        max_events: usize,
        max_bytes: u64,
        response: oneshot::Sender<Result<Vec<ChangeEvent>, StorageError>>,
    },
    FirstSequence {
        response: oneshot::Sender<Result<Option<u64>, StorageError>>,
    },
    LastSequence {
        response: oneshot::Sender<Result<Option<u64>, StorageError>>,
    },
    ConsumerOffset {
        stream_name: String,
        consumer_name: String,
        response: oneshot::Sender<Result<Option<u64>, StorageError>>,
    },
    Shutdown,
}

impl StorageReaderPool {
    /// Starts a fixed reader pool behind one bounded Tokio command queue.
    pub fn start(
        store: RedbEventStore,
        worker_count: usize,
        queue_capacity: usize,
    ) -> anyhow::Result<Self> {
        if worker_count == 0 || queue_capacity == 0 {
            return Err(anyhow!(
                "storage reader workers and queue capacity must be nonzero"
            ));
        }
        let (sender, receiver) = flume::bounded(queue_capacity);
        let replay_permits = std::sync::Arc::new(Semaphore::new(worker_count));
        let mut workers = Vec::with_capacity(worker_count);
        for index in 0..worker_count {
            let store = store.clone();
            let receiver = receiver.clone();
            workers.push(
                thread::Builder::new()
                    .name(format!("lightcdc-redb-reader-{index}"))
                    .spawn(move || {
                        loop {
                            let command = receiver.recv();
                            let Ok(command) = command else {
                                break;
                            };
                            if matches!(command, StorageReadCommand::Shutdown) {
                                break;
                            }
                            execute_storage_read(&store, command);
                        }
                    })
                    .context("failed to start redb reader thread")?,
            );
        }
        Ok(Self {
            handle: Some(StorageReaderHandle {
                sender,
                replay_permits,
            }),
            workers,
        })
    }

    /// Clones the lightweight asynchronous command handle.
    pub fn handle(&self) -> StorageReaderHandle {
        self.handle
            .as_ref()
            .expect("storage reader pool is running")
            .clone()
    }
}

impl StorageReaderHandle {
    pub async fn replay_from(
        &self,
        sequence: u64,
        max_events: usize,
        max_bytes: u64,
    ) -> anyhow::Result<ReplayBatch> {
        let permit = std::sync::Arc::clone(&self.replay_permits)
            .acquire_owned()
            .await
            .context("storage reader replay permits closed")?;
        let (response, receiver) = oneshot::channel();
        self.sender
            .send_async(StorageReadCommand::ReplayFrom {
                sequence,
                max_events,
                max_bytes,
                response,
            })
            .await
            .map_err(|_| anyhow!("storage reader pool stopped before accepting replay"))?;
        let events = receiver
            .await
            .context("storage reader pool stopped before returning replay")?
            .context("failed to replay events from redb")?;
        Ok(ReplayBatch {
            events,
            _permit: permit,
        })
    }

    pub async fn first_sequence(&self) -> anyhow::Result<Option<u64>> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send_async(StorageReadCommand::FirstSequence { response })
            .await
            .map_err(|_| anyhow!("storage reader pool stopped before accepting first sequence"))?;
        receiver
            .await
            .context("storage reader pool stopped before returning first sequence")?
            .context("failed to read first sequence from redb")
    }

    pub async fn last_sequence(&self) -> anyhow::Result<Option<u64>> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send_async(StorageReadCommand::LastSequence { response })
            .await
            .map_err(|_| anyhow!("storage reader pool stopped before accepting last sequence"))?;
        receiver
            .await
            .context("storage reader pool stopped before returning last sequence")?
            .context("failed to read last sequence from redb")
    }

    pub async fn consumer_offset(
        &self,
        stream_name: String,
        consumer_name: String,
    ) -> anyhow::Result<Option<u64>> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send_async(StorageReadCommand::ConsumerOffset {
                stream_name,
                consumer_name,
                response,
            })
            .await
            .map_err(|_| anyhow!("storage reader pool stopped before accepting offset read"))?;
        receiver
            .await
            .context("storage reader pool stopped before returning offset")?
            .context("failed to read consumer offset from redb")
    }
}

fn execute_storage_read(store: &RedbEventStore, command: StorageReadCommand) {
    match command {
        StorageReadCommand::ReplayFrom {
            sequence,
            max_events,
            max_bytes,
            response,
        } => {
            let _ = response.send(store.replay_from_bounded(sequence, max_events, max_bytes));
        }
        StorageReadCommand::FirstSequence { response } => {
            let _ = response.send(store.first_sequence());
        }
        StorageReadCommand::LastSequence { response } => {
            let _ = response.send(store.last_sequence());
        }
        StorageReadCommand::ConsumerOffset {
            stream_name,
            consumer_name,
            response,
        } => {
            let _ = response.send(store.consumer_offset(&stream_name, &consumer_name));
        }
        StorageReadCommand::Shutdown => unreachable!("shutdown is handled by the worker loop"),
    }
}

impl CaptureStorageWriter {
    /// Starts the long-lived OS thread that owns synchronous storage work.
    pub fn start(store: RedbEventStore, source_name: String) -> anyhow::Result<Self> {
        Self::start_with_limits(store, source_name, None)
    }

    /// Starts the writer with hard data-directory and free-space limits.
    pub fn start_with_limits(
        store: RedbEventStore,
        source_name: String,
        resource_limits: Option<StorageResourceLimits>,
    ) -> anyhow::Result<Self> {
        let (sender, mut receiver) = mpsc::channel::<StorageCommand>(STORAGE_COMMAND_CAPACITY);
        let thread = thread::Builder::new()
            .name("lightcdc-redb-writer".to_owned())
            .spawn(move || {
                // redb commits are synchronous, so one OS thread owns and
                // serializes capture and retention writes off Tokio's workers.
                while let Some(command) = receiver.blocking_recv() {
                    match command {
                        StorageCommand::Persist(command) => {
                            persist_capture_batch(
                                &store,
                                &source_name,
                                resource_limits.as_ref(),
                                command,
                            );
                        }
                        StorageCommand::Prune {
                            policy,
                            now_ms,
                            response,
                        } => {
                            let _ = response.send(store.prune_events(policy, now_ms));
                        }
                        StorageCommand::AcknowledgeConsumer {
                            stream_name,
                            consumer_name,
                            sequence,
                            maximum_consumers,
                            response,
                        } => {
                            let _ = response.send(store.acknowledge_consumer_offset_bounded(
                                &stream_name,
                                &consumer_name,
                                sequence,
                                maximum_consumers,
                            ));
                        }
                        StorageCommand::SetConsumerOffset {
                            stream_name,
                            consumer_name,
                            offset,
                            maximum_consumers,
                            response,
                        } => {
                            let _ = response.send(store.set_consumer_offset_bounded(
                                &stream_name,
                                &consumer_name,
                                offset,
                                maximum_consumers,
                            ));
                        }
                    }
                }
            })
            .context("failed to start dedicated redb writer thread")?;

        let handle = CaptureStorageHandle {
            sender: sender.clone(),
        };
        Ok(Self {
            handle: Some(handle),
            thread: Some(thread),
        })
    }

    /// Clones a lightweight command handle for retention or other producers.
    pub fn handle(&self) -> CaptureStorageHandle {
        self.handle
            .as_ref()
            .expect("redb writer thread is running")
            .clone()
    }

    /// Queues one capture batch and returns a receiver for its eventual result.
    pub async fn submit(
        &self,
        batch: CaptureBatch,
        measure_latency: bool,
        check_replay: bool,
    ) -> anyhow::Result<PendingCaptureWrite> {
        if batch.is_empty() {
            return Err(anyhow!("cannot submit an empty capture batch"));
        }

        let event_count = batch.event_count;
        let (response, response_receiver) = oneshot::channel();
        self.handle
            .as_ref()
            .context("redb writer thread is shutting down")?
            .sender
            .send(StorageCommand::Persist(PersistCommand {
                batch,
                measure_latency,
                check_replay,
                #[cfg(test)]
                delay_before_persist: None,
                response,
            }))
            .await
            .map_err(|_| anyhow!("redb writer thread stopped before accepting a batch"))?;

        Ok(PendingCaptureWrite {
            response: response_receiver,
            event_count,
        })
    }

    #[cfg(test)]
    async fn submit_with_delay(
        &self,
        batch: CaptureBatch,
        delay_before_persist: Duration,
    ) -> anyhow::Result<PendingCaptureWrite> {
        if batch.is_empty() {
            return Err(anyhow!("cannot submit an empty capture batch"));
        }

        let event_count = batch.event_count;
        let (response, response_receiver) = oneshot::channel();
        self.handle
            .as_ref()
            .context("redb writer thread is shutting down")?
            .sender
            .send(StorageCommand::Persist(PersistCommand {
                batch,
                measure_latency: false,
                check_replay: false,
                delay_before_persist: Some(delay_before_persist),
                response,
            }))
            .await
            .map_err(|_| anyhow!("redb writer thread stopped before accepting a batch"))?;

        Ok(PendingCaptureWrite {
            response: response_receiver,
            event_count,
        })
    }
}

impl CaptureStorageHandle {
    /// Queues one retention sweep behind any capture write already in progress.
    pub async fn prune(
        &self,
        policy: RetentionPolicy,
        now_ms: i64,
    ) -> anyhow::Result<RetentionOutcome> {
        let (response, response_receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::Prune {
                policy,
                now_ms,
                response,
            })
            .await
            .map_err(|_| anyhow!("redb writer thread stopped before accepting retention work"))?;
        response_receiver
            .await
            .context("redb writer thread stopped before returning retention results")?
            .context("failed to prune retained events from redb")
    }

    /// Serializes a cumulative consumer acknowledgement with capture writes.
    pub async fn acknowledge_consumer_offset(
        &self,
        stream_name: String,
        consumer_name: String,
        sequence: u64,
        maximum_consumers: usize,
    ) -> anyhow::Result<u64> {
        let (response, response_receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::AcknowledgeConsumer {
                stream_name,
                consumer_name,
                sequence,
                maximum_consumers,
                response,
            })
            .await
            .map_err(|_| {
                anyhow!("redb writer thread stopped before accepting an acknowledgement")
            })?;
        response_receiver
            .await
            .context("redb writer thread stopped before returning an acknowledgement")?
            .context("failed to acknowledge the consumer offset in redb")
    }

    /// Serializes an explicit consumer seek with capture writes.
    pub async fn set_consumer_offset(
        &self,
        stream_name: String,
        consumer_name: String,
        offset: u64,
        maximum_consumers: usize,
    ) -> anyhow::Result<()> {
        let (response, response_receiver) = oneshot::channel();
        self.sender
            .send(StorageCommand::SetConsumerOffset {
                stream_name,
                consumer_name,
                offset,
                maximum_consumers,
                response,
            })
            .await
            .map_err(|_| anyhow!("redb writer thread stopped before accepting a consumer seek"))?;
        response_receiver
            .await
            .context("redb writer thread stopped before returning a consumer seek")?
            .context("failed to set the consumer offset in redb")
    }
}

/// Persists a batch and sends its ownership and result back to the async caller.
fn persist_capture_batch(
    store: &RedbEventStore,
    source_name: &str,
    resource_limits: Option<&StorageResourceLimits>,
    command: PersistCommand,
) {
    #[cfg(test)]
    if let Some(delay) = command.delay_before_persist {
        thread::sleep(delay);
    }
    let source_lsns = command
        .batch
        .transactions
        .iter()
        .map(|transaction| transaction.ack_lsn.to_string())
        .collect::<Vec<_>>();
    let transactions = command
        .batch
        .transactions
        .iter()
        .zip(&source_lsns)
        .map(|(transaction, source_lsn)| SourceTransaction {
            events: &transaction.events,
            source_lsn,
        })
        .collect::<Vec<_>>();
    let persist_started = command.measure_latency.then(Instant::now);
    let result = ensure_storage_capacity(resource_limits, &command.batch).and_then(|()| {
        if command.check_replay {
            store.persist_transaction_batch(&transactions, source_name)
        } else {
            store.persist_reconciled_transaction_batch(&transactions, source_name)
        }
    });
    let persist_latency = persist_started.map(|persist_started| persist_started.elapsed());
    let _ = command.response.send(StorageCompletion {
        batch: command.batch,
        result,
        persist_latency,
    });
}

fn ensure_storage_capacity(
    limits: Option<&StorageResourceLimits>,
    batch: &CaptureBatch,
) -> Result<(), StorageError> {
    let Some(limits) = limits else {
        return Ok(());
    };
    let current_bytes = directory_size(&limits.data_dir)?;
    // redb uses copy-on-write pages. Reserve the encoded payload plus fixed
    // transaction headroom so the check does not assume payload bytes are the
    // only temporary space needed by a commit.
    let required_bytes = batch.staged_bytes.saturating_add(REDB_WRITE_HEADROOM_BYTES);
    if current_bytes.saturating_add(required_bytes) > limits.max_storage_bytes {
        return Err(StorageError::ResourceLimit(format!(
            "data directory uses {current_bytes} bytes and the next batch reserves {required_bytes}, exceeding max_storage_bytes {}",
            limits.max_storage_bytes
        )));
    }

    let available_bytes = fs2::available_space(&limits.data_dir)?;
    if available_bytes < limits.min_free_disk_bytes.saturating_add(required_bytes) {
        return Err(StorageError::ResourceLimit(format!(
            "filesystem has {available_bytes} bytes available; the next batch needs {required_bytes} bytes while preserving min_free_disk_bytes {}",
            limits.min_free_disk_bytes
        )));
    }
    Ok(())
}

pub(crate) fn directory_size(path: &Path) -> Result<u64, StorageError> {
    let mut total = 0u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            total = total.saturating_add(directory_size(&entry.path())?);
        } else if metadata.is_file() {
            total = total.saturating_add(metadata.len());
        }
    }
    Ok(total)
}

impl Drop for CaptureStorageWriter {
    fn drop(&mut self) {
        // Drop every sender before joining so blocking_recv can observe closure.
        self.handle.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for StorageReaderPool {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            for _ in 0..self.workers.len() {
                let _ = handle.sender.send(StorageReadCommand::Shutdown);
            }
        }
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use lightcdc_core::{ChangeEvent, Operation, SourceMetadata};
    use lightcdc_storage::{LogOpenOptions, SegmentOptions, TransactionEvents, TransactionStats};
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn dedicated_writer_persists_a_batch_and_returns_its_events() {
        let temp = TempDir::new().expect("temp dir");
        let store = RedbEventStore::open_with_segment_options(
            &LogOpenOptions {
                data_dir: temp.path().to_path_buf(),
                database_file: "writer.redb".to_owned(),
            },
            SegmentOptions {
                max_events: 2,
                ..SegmentOptions::default()
            },
        )
        .expect("open store");
        let writer =
            CaptureStorageWriter::start(store.clone(), "default".to_owned()).expect("start writer");
        let mut batch = CaptureBatch::default();
        batch.push(transaction(1, "0/1"));
        batch.push(transaction(2, "0/2"));

        let pending = writer
            .submit(batch, true, true)
            .await
            .expect("submit batch");
        assert_eq!(pending.event_count, 2);
        let completion = pending.response.await.expect("writer response");

        assert_eq!(
            completion.result.expect("persist batch"),
            PersistTransactionOutcome::Persisted
        );
        assert!(completion.persist_latency.is_some());
        assert_eq!(completion.batch.event_count, 2);
        assert_eq!(
            store.replay_from(1, 10).expect("replay"),
            [event(1), event(2)]
        );
        assert_eq!(
            store.source_offset("default").expect("source offset"),
            Some("0/2".to_owned())
        );
        assert_eq!(store.stats().expect("store stats").replay_id_count, 2);
    }

    #[tokio::test]
    async fn dedicated_writer_rejects_an_empty_batch() {
        let temp = TempDir::new().expect("temp dir");
        let store = RedbEventStore::open(&LogOpenOptions {
            data_dir: temp.path().to_path_buf(),
            database_file: "writer.redb".to_owned(),
        })
        .expect("open store");
        let writer =
            CaptureStorageWriter::start(store, "default".to_owned()).expect("start writer");

        let error = match writer.submit(CaptureBatch::default(), false, false).await {
            Ok(_) => panic!("empty batch must fail"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("empty capture batch"));
    }

    #[tokio::test]
    async fn dedicated_writer_rejects_a_batch_before_exceeding_storage_limit() {
        let temp = TempDir::new().expect("temp dir");
        let store = RedbEventStore::open(&LogOpenOptions {
            data_dir: temp.path().to_path_buf(),
            database_file: "writer.redb".to_owned(),
        })
        .expect("open store");
        let writer = CaptureStorageWriter::start_with_limits(
            store.clone(),
            "default".to_owned(),
            Some(StorageResourceLimits {
                data_dir: temp.path().to_path_buf(),
                max_storage_bytes: 1,
                min_free_disk_bytes: 1,
            }),
        )
        .expect("start writer");
        let mut batch = CaptureBatch::default();
        batch.push(transaction(1, "0/1"));

        let completion = writer
            .submit(batch, false, true)
            .await
            .expect("submit batch")
            .response
            .await
            .expect("writer response");

        assert!(matches!(
            completion.result,
            Err(StorageError::ResourceLimit(_))
        ));
        assert_eq!(store.last_sequence().expect("last sequence"), None);
        assert_eq!(store.source_offset("default").expect("source offset"), None);
    }

    #[tokio::test]
    async fn dedicated_writer_serializes_retention_after_capture_writes() {
        let temp = TempDir::new().expect("temp dir");
        let store = RedbEventStore::open_with_segment_options(
            &LogOpenOptions {
                data_dir: temp.path().to_path_buf(),
                database_file: "writer.redb".to_owned(),
            },
            SegmentOptions {
                max_events: 2,
                ..SegmentOptions::default()
            },
        )
        .expect("open store");
        let writer =
            CaptureStorageWriter::start(store.clone(), "default".to_owned()).expect("start writer");
        let mut first_batch = CaptureBatch::default();
        first_batch.push(transaction(1, "0/1"));
        first_batch.push(transaction(2, "0/2"));
        writer
            .submit(first_batch, false, false)
            .await
            .expect("submit first batch")
            .response
            .await
            .expect("first completion")
            .result
            .expect("persist first batch");
        let mut latest_batch = CaptureBatch::default();
        latest_batch.push(transaction(3, "0/3"));
        writer
            .submit(latest_batch, false, false)
            .await
            .expect("submit latest batch")
            .response
            .await
            .expect("latest completion")
            .result
            .expect("persist latest batch");

        let outcome = writer
            .handle()
            .prune(
                RetentionPolicy {
                    max_events: Some(1),
                    max_bytes: None,
                    max_age: None,
                    delete_batch_size: 10,
                },
                i64::MAX,
            )
            .await
            .expect("prune retained events");

        assert_eq!(outcome.deleted_events, 2);
        assert_eq!(
            store.replay_from(3, 10).expect("retained events"),
            [event(3)]
        );
    }

    #[tokio::test]
    async fn dedicated_writer_does_not_block_the_tokio_runtime() {
        let temp = TempDir::new().expect("temp dir");
        let store = RedbEventStore::open(&LogOpenOptions {
            data_dir: temp.path().to_path_buf(),
            database_file: "writer.redb".to_owned(),
        })
        .expect("open store");
        let writer =
            CaptureStorageWriter::start(store, "default".to_owned()).expect("start writer");
        let mut batch = CaptureBatch::default();
        batch.push(transaction(1, "0/1"));
        let mut pending = writer
            .submit_with_delay(batch, Duration::from_millis(50))
            .await
            .expect("submit delayed batch");

        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut pending.response)
                .await
                .is_err(),
            "the Tokio timer should fire while the storage thread is blocked"
        );
        let completion = pending.response.await.expect("writer response");
        assert_eq!(
            completion.result.expect("persist batch"),
            PersistTransactionOutcome::Persisted
        );
    }

    #[tokio::test]
    async fn consumer_offset_mutations_share_the_storage_writer() {
        let temp = TempDir::new().expect("temp dir");
        let store = RedbEventStore::open(&LogOpenOptions {
            data_dir: temp.path().to_path_buf(),
            database_file: "writer.redb".to_owned(),
        })
        .expect("open store");
        let writer =
            CaptureStorageWriter::start(store.clone(), "default".to_owned()).expect("start writer");
        let handle = writer.handle();

        handle
            .set_consumer_offset("orders".to_owned(), "search".to_owned(), 5, 1)
            .await
            .expect("seek consumer");
        let limit_error = handle
            .set_consumer_offset("orders".to_owned(), "analytics".to_owned(), 5, 1)
            .await
            .expect_err("second durable consumer must be rejected");
        assert!(matches!(
            limit_error.downcast_ref::<StorageError>(),
            Some(StorageError::ConsumerLimitReached { maximum: 1 })
        ));
        assert_eq!(
            handle
                .acknowledge_consumer_offset("orders".to_owned(), "search".to_owned(), 3, 1)
                .await
                .expect("monotonic acknowledgement"),
            5
        );
        assert_eq!(
            handle
                .acknowledge_consumer_offset("orders".to_owned(), "search".to_owned(), 8, 1)
                .await
                .expect("advance acknowledgement"),
            8
        );
        assert_eq!(
            store
                .consumer_offset("orders", "search")
                .expect("consumer offset"),
            Some(8)
        );
    }

    #[tokio::test]
    async fn bounded_reader_pool_serves_replay_and_shuts_down_with_a_stale_handle() {
        let temp = TempDir::new().expect("temp dir");
        let store = RedbEventStore::open(&LogOpenOptions {
            data_dir: temp.path().to_path_buf(),
            database_file: "reader.redb".to_owned(),
        })
        .expect("open store");
        store.append_event(&event(1)).expect("append event");
        store
            .set_consumer_offset("orders", "search", 1)
            .expect("set offset");
        let pool = StorageReaderPool::start(store, 1, 4).expect("reader pool");
        let handle = pool.handle();

        let first_batch = handle
            .replay_from(1, 10, 1024 * 1024)
            .await
            .expect("first replay");
        assert!(
            tokio::time::timeout(
                Duration::from_millis(10),
                handle.replay_from(1, 10, 1024 * 1024),
            )
            .await
            .is_err(),
            "a second replay waits while the only aggregate-memory permit is held"
        );
        drop(first_batch);
        assert_eq!(
            handle
                .replay_from(1, 10, 1024 * 1024)
                .await
                .expect("replay")
                .into_iter()
                .collect::<Vec<_>>(),
            [event(1)]
        );
        assert_eq!(handle.first_sequence().await.expect("first"), Some(1));
        assert_eq!(handle.last_sequence().await.expect("last"), Some(1));
        assert_eq!(
            handle
                .consumer_offset("orders".to_owned(), "search".to_owned())
                .await
                .expect("offset"),
            Some(1)
        );

        drop(pool);
        assert!(handle.last_sequence().await.is_err());
    }

    fn transaction(sequence: u64, ack_lsn: &str) -> CapturedTransaction {
        CapturedTransaction {
            events: TransactionEvents::InMemory {
                events: vec![event(sequence)],
                stats: TransactionStats {
                    event_count: 1,
                    decoded_bytes: 100,
                    staged_bytes: 0,
                },
            },
            ack_lsn: ack_lsn.parse().expect("test LSN"),
        }
    }

    fn event(sequence: u64) -> ChangeEvent {
        ChangeEvent {
            sequence,
            event_id: format!("event-{sequence}"),
            source: SourceMetadata {
                database: "lightcdc".to_owned(),
                slot: "slot".to_owned(),
                lsn: format!("0/{sequence:X}"),
            },
            transaction: None,
            schema: "public".to_owned(),
            table: "orders".to_owned(),
            operation: Operation::Insert,
            key: None,
            before: None,
            after: None,
            commit_timestamp_ms: None,
        }
    }
}
