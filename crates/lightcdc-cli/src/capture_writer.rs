use std::{
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use anyhow::{Context, anyhow};
use lightcdc_postgres::CapturedTransaction;
use lightcdc_storage::{
    PersistTransactionOutcome, RedbEventStore, RetentionOutcome, RetentionPolicy, StorageError,
};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant as TokioInstant;

const STORAGE_COMMAND_CAPACITY: usize = 1;

#[derive(Clone, Copy, Debug)]
pub(crate) struct CaptureBatchLimits {
    pub(crate) max_transactions: usize,
    pub(crate) max_events: usize,
    pub(crate) max_bytes: u64,
    pub(crate) max_delay: Duration,
}

#[derive(Default)]
pub(crate) struct CaptureBatch {
    pub(crate) transactions: Vec<CapturedTransaction>,
    pub(crate) event_count: usize,
    pub(crate) decoded_bytes: u64,
    pub(crate) staged_bytes: u64,
    pub(crate) staged_transaction_count: usize,
    started_at: Option<TokioInstant>,
}

impl CaptureBatch {
    pub(crate) fn push(&mut self, transaction: CapturedTransaction) {
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

    pub(crate) fn is_empty(&self) -> bool {
        self.transactions.is_empty()
    }

    pub(crate) fn would_exceed(
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

    pub(crate) fn reached_limit(&self, limits: CaptureBatchLimits) -> bool {
        self.transactions.len() >= limits.max_transactions
            || self.event_count >= limits.max_events
            || self.decoded_bytes >= limits.max_bytes
    }

    pub(crate) fn deadline(&self, limits: CaptureBatchLimits) -> TokioInstant {
        self.started_at
            .expect("non-empty capture batch has a start time")
            + limits.max_delay
    }
}

pub(crate) struct CaptureStorageWriter {
    handle: Option<CaptureStorageHandle>,
    thread: Option<JoinHandle<()>>,
}

#[derive(Clone)]
pub(crate) struct CaptureStorageHandle {
    sender: mpsc::Sender<StorageCommand>,
}

pub(crate) struct PendingCaptureWrite {
    pub(crate) response: oneshot::Receiver<StorageCompletion>,
    pub(crate) event_count: usize,
}

pub(crate) struct StorageCompletion {
    pub(crate) batch: CaptureBatch,
    pub(crate) result: Result<PersistTransactionOutcome, StorageError>,
    pub(crate) persist_latency: Option<Duration>,
}

struct PersistCommand {
    batch: CaptureBatch,
    measure_latency: bool,
    #[cfg(test)]
    delay_before_persist: Option<Duration>,
    response: oneshot::Sender<StorageCompletion>,
}

enum StorageCommand {
    Persist(PersistCommand),
    Prune {
        policy: RetentionPolicy,
        now_ms: i64,
        response: oneshot::Sender<Result<RetentionOutcome, StorageError>>,
    },
}

impl CaptureStorageWriter {
    pub(crate) fn start(store: RedbEventStore, source_name: String) -> anyhow::Result<Self> {
        let (sender, mut receiver) = mpsc::channel::<StorageCommand>(STORAGE_COMMAND_CAPACITY);
        let thread = thread::Builder::new()
            .name("lightcdc-redb-writer".to_owned())
            .spawn(move || {
                while let Some(command) = receiver.blocking_recv() {
                    match command {
                        StorageCommand::Persist(command) => {
                            persist_capture_batch(&store, &source_name, command);
                        }
                        StorageCommand::Prune {
                            policy,
                            now_ms,
                            response,
                        } => {
                            let _ = response.send(store.prune_events(policy, now_ms));
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

    pub(crate) fn handle(&self) -> CaptureStorageHandle {
        self.handle
            .as_ref()
            .expect("redb writer thread is running")
            .clone()
    }

    pub(crate) async fn submit(
        &self,
        batch: CaptureBatch,
        measure_latency: bool,
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
    pub(crate) async fn prune(
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
}

fn persist_capture_batch(store: &RedbEventStore, source_name: &str, command: PersistCommand) {
    #[cfg(test)]
    if let Some(delay) = command.delay_before_persist {
        thread::sleep(delay);
    }
    let ack_lsn = command
        .batch
        .transactions
        .last()
        .expect("storage writer never receives an empty batch")
        .ack_lsn;
    let transaction_events = command
        .batch
        .transactions
        .iter()
        .map(|transaction| &transaction.events)
        .collect::<Vec<_>>();
    let persist_started = command.measure_latency.then(Instant::now);
    let result =
        store.persist_transaction_batch(&transaction_events, source_name, &ack_lsn.to_string());
    let persist_latency = persist_started.map(|persist_started| persist_started.elapsed());
    let _ = command.response.send(StorageCompletion {
        batch: command.batch,
        result,
        persist_latency,
    });
}

impl Drop for CaptureStorageWriter {
    fn drop(&mut self) {
        self.handle.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use lightcdc_core::{ChangeEvent, Operation, SourceMetadata};
    use lightcdc_storage::{LogOpenOptions, TransactionEvents, TransactionStats};
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn dedicated_writer_persists_a_batch_and_returns_its_events() {
        let temp = TempDir::new().expect("temp dir");
        let store = RedbEventStore::open(&LogOpenOptions {
            data_dir: temp.path().to_path_buf(),
            database_file: "writer.redb".to_owned(),
        })
        .expect("open store");
        let writer =
            CaptureStorageWriter::start(store.clone(), "default".to_owned()).expect("start writer");
        let mut batch = CaptureBatch::default();
        batch.push(transaction(1, "0/1"));
        batch.push(transaction(2, "0/2"));

        let pending = writer.submit(batch, true).await.expect("submit batch");
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

        let error = match writer.submit(CaptureBatch::default(), false).await {
            Ok(_) => panic!("empty batch must fail"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("empty capture batch"));
    }

    #[tokio::test]
    async fn dedicated_writer_serializes_retention_after_capture_writes() {
        let temp = TempDir::new().expect("temp dir");
        let store = RedbEventStore::open(&LogOpenOptions {
            data_dir: temp.path().to_path_buf(),
            database_file: "writer.redb".to_owned(),
        })
        .expect("open store");
        let writer =
            CaptureStorageWriter::start(store.clone(), "default".to_owned()).expect("start writer");
        let mut first_batch = CaptureBatch::default();
        first_batch.push(transaction(1, "0/1"));
        first_batch.push(transaction(2, "0/2"));
        writer
            .submit(first_batch, false)
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
            .submit(latest_batch, false)
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
