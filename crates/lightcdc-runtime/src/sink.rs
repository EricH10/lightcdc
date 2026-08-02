//! Runs destination adapters against durable stream offsets inside the main process.

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use anyhow::{Context, anyhow};
use lightcdc_core::{ChangeEvent, StreamConfig};
use lightcdc_storage::RedbEventStore;
use tokio::task::JoinSet;
use tracing::{info, warn};

use crate::{
    CaptureStorageHandle, EventNotifier, ShutdownReceiver, StorageReaderHandle, StorageReaderPool,
};

/// Classifies whether a sink batch should retry or stop for operator action.
#[derive(Debug)]
pub enum SinkDeliveryError {
    Retryable(anyhow::Error),
    Terminal(anyhow::Error),
}

/// Destination-specific code called by the shared delivery and offset runtime.
pub trait Sink: Send + 'static {
    fn deliver<'a>(
        &'a mut self,
        events: &'a [ChangeEvent],
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkDeliveryError>> + Send + 'a>>;
}

/// Generic delivery, batching, and retry settings for one configured sink.
#[derive(Clone, Debug)]
pub struct SinkWorkerConfig {
    pub name: String,
    pub stream: StreamConfig,
    pub batch_max_events: usize,
    pub batch_max_bytes: u64,
    pub retry_initial: Duration,
    pub retry_max: Duration,
    pub maximum_consumers: usize,
}

/// Pairs generic worker settings with one destination implementation.
pub struct SinkRegistration {
    pub config: SinkWorkerConfig,
    pub sink: Box<dyn Sink>,
}

/// Owns all configured sink workers and their shared redb reader pool.
pub struct SinkRuntime {
    tasks: JoinSet<(String, anyhow::Result<()>)>,
    _readers: Option<Arc<StorageReaderPool>>,
}

impl SinkRuntime {
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        store: RedbEventStore,
        reader_threads: usize,
        reader_queue_capacity: usize,
        registrations: Vec<SinkRegistration>,
        storage_writer: CaptureStorageHandle,
        notifier: EventNotifier,
        shutdown: ShutdownReceiver,
    ) -> anyhow::Result<Self> {
        if registrations.is_empty() {
            return Ok(Self {
                tasks: JoinSet::new(),
                _readers: None,
            });
        }
        let readers = Arc::new(StorageReaderPool::start(
            store,
            reader_threads,
            reader_queue_capacity,
        )?);
        let reader = readers.handle();
        let mut tasks = JoinSet::new();
        for registration in registrations {
            let name = registration.config.name.clone();
            let reader = reader.clone();
            let writer = storage_writer.clone();
            let notifier = notifier.clone();
            let shutdown = shutdown.clone();
            tasks.spawn(async move {
                let result = run_sink_worker(
                    registration.config,
                    registration.sink,
                    reader,
                    writer,
                    notifier,
                    shutdown,
                )
                .await;
                (name, result)
            });
        }
        Ok(Self {
            tasks,
            _readers: Some(readers),
        })
    }

    /// Reports the first sink that exits before coordinated shutdown.
    pub async fn stopped(&mut self) -> anyhow::Error {
        if self.tasks.is_empty() {
            return std::future::pending().await;
        }
        match self.tasks.join_next().await {
            Some(Ok((name, Ok(())))) => anyhow!("sink {name:?} stopped unexpectedly"),
            Some(Ok((name, Err(error)))) => error.context(format!("sink {name:?} failed")),
            Some(Err(error)) => anyhow::Error::new(error).context("sink task failed"),
            None => anyhow!("all sink tasks stopped unexpectedly"),
        }
    }

    /// Waits for every sink to observe the shared shutdown signal.
    pub async fn shutdown(mut self, timeout: Duration) -> anyhow::Result<()> {
        let wait = async {
            while let Some(result) = self.tasks.join_next().await {
                let (name, outcome) = result.context("sink task failed while draining")?;
                outcome.with_context(|| format!("sink {name:?} failed while draining"))?;
            }
            Ok(())
        };
        tokio::time::timeout(timeout, wait)
            .await
            .map_err(|_| anyhow!("sink workers did not stop within {timeout:?}"))?
    }
}

async fn run_sink_worker(
    config: SinkWorkerConfig,
    mut sink: Box<dyn Sink>,
    reader: StorageReaderHandle,
    writer: CaptureStorageHandle,
    notifier: EventNotifier,
    mut shutdown: ShutdownReceiver,
) -> anyhow::Result<()> {
    let consumer = format!("sink:{}", config.name);
    let stored_offset = reader
        .consumer_offset(config.stream.name.clone(), consumer.clone())
        .await?;
    let mut offset = match stored_offset {
        Some(offset) => offset,
        None => reader
            .first_sequence()
            .await?
            .unwrap_or(1)
            .saturating_sub(1),
    };
    let mut notifications = notifier.subscribe();
    info!(
        sink = %config.name,
        stream = %config.stream.name,
        consumer = %consumer,
        offset,
        "in-process sink is ready"
    );

    loop {
        if shutdown.is_triggered() {
            return Ok(());
        }
        let replay = reader
            .replay_from(
                offset.saturating_add(1),
                config.batch_max_events,
                config.batch_max_bytes,
            )
            .await?;
        if replay.is_empty() {
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                changed = notifications.changed() => {
                    changed.context("event notification channel closed")?;
                }
            }
            continue;
        }

        let events = replay.into_iter().collect::<Vec<_>>();
        let scanned_offset = events.last().expect("nonempty replay batch").sequence;
        let delivery = events
            .iter()
            .filter(|event| config.stream.matches_event(event))
            .cloned()
            .collect::<Vec<_>>();
        if !delivery.is_empty()
            && !deliver_with_retry(&config, sink.as_mut(), &delivery, &mut shutdown).await?
        {
            return Ok(());
        }
        writer
            .acknowledge_consumer_offset(
                config.stream.name.clone(),
                consumer.clone(),
                scanned_offset,
                config.maximum_consumers,
            )
            .await?;
        offset = scanned_offset;
    }
}

async fn deliver_with_retry(
    config: &SinkWorkerConfig,
    sink: &mut dyn Sink,
    events: &[ChangeEvent],
    shutdown: &mut ShutdownReceiver,
) -> anyhow::Result<bool> {
    let mut delay = config.retry_initial;
    loop {
        match sink.deliver(events).await {
            Ok(()) => return Ok(true),
            Err(SinkDeliveryError::Terminal(error)) => return Err(error),
            Err(SinkDeliveryError::Retryable(error)) => {
                warn!(
                    sink = %config.name,
                    stream = %config.stream.name,
                    event_count = events.len(),
                    retry_in_ms = delay.as_millis(),
                    %error,
                    "sink delivery failed; retrying the unacknowledged batch"
                );
            }
        }
        tokio::select! {
            _ = shutdown.cancelled() => return Ok(false),
            () = tokio::time::sleep(delay) => {}
        }
        delay = delay.saturating_mul(2).min(config.retry_max);
    }
}

#[cfg(test)]
mod tests {
    use std::{future::Future, pin::Pin, time::Duration};

    use lightcdc_core::{ChangeEvent, Operation, SourceMetadata, StreamConfig};
    use lightcdc_storage::{LogOpenOptions, RedbEventStore};
    use tempfile::TempDir;
    use tokio::sync::mpsc;

    use super::{Sink, SinkDeliveryError, SinkRegistration, SinkRuntime, SinkWorkerConfig};
    use crate::{CaptureStorageWriter, EventNotifier, shutdown_channel};

    struct RecordingSink {
        batches: mpsc::UnboundedSender<Vec<u64>>,
    }

    impl Sink for RecordingSink {
        fn deliver<'a>(
            &'a mut self,
            events: &'a [ChangeEvent],
        ) -> Pin<Box<dyn Future<Output = Result<(), SinkDeliveryError>> + Send + 'a>> {
            let sequences = events.iter().map(|event| event.sequence).collect();
            let batches = self.batches.clone();
            Box::pin(async move {
                batches
                    .send(sequences)
                    .expect("test batch receiver remains open");
                Ok(())
            })
        }
    }

    struct RetryingSink {
        attempts: mpsc::UnboundedSender<()>,
    }

    impl Sink for RetryingSink {
        fn deliver<'a>(
            &'a mut self,
            _events: &'a [ChangeEvent],
        ) -> Pin<Box<dyn Future<Output = Result<(), SinkDeliveryError>> + Send + 'a>> {
            let attempts = self.attempts.clone();
            Box::pin(async move {
                attempts
                    .send(())
                    .expect("test attempt receiver remains open");
                Err(SinkDeliveryError::Retryable(anyhow::anyhow!(
                    "Redis is offline"
                )))
            })
        }
    }

    #[tokio::test]
    async fn successful_delivery_advances_the_durable_sink_offset() {
        let (temp, store, writer) = store_with_events(2);
        let (batches_tx, mut batches_rx) = mpsc::unbounded_channel();
        let (shutdown, shutdown_rx) = shutdown_channel();
        let runtime = SinkRuntime::start(
            store.clone(),
            1,
            8,
            vec![registration(Box::new(RecordingSink {
                batches: batches_tx,
            }))],
            writer.handle(),
            EventNotifier::new(),
            shutdown_rx,
        )
        .expect("sink runtime");

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), batches_rx.recv())
                .await
                .expect("delivery timeout")
                .expect("delivery batch"),
            [1, 2]
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            while store
                .consumer_offset("orders", "sink:cache")
                .expect("sink offset")
                != Some(2)
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("offset timeout");

        shutdown.trigger();
        runtime
            .shutdown(Duration::from_secs(1))
            .await
            .expect("sink shutdown");
        drop(writer);
        drop(store);
        drop(temp);
    }

    #[tokio::test]
    async fn shutdown_during_retry_does_not_advance_the_sink_offset() {
        let (temp, store, writer) = store_with_events(1);
        let (attempts_tx, mut attempts_rx) = mpsc::unbounded_channel();
        let (shutdown, shutdown_rx) = shutdown_channel();
        let runtime = SinkRuntime::start(
            store.clone(),
            1,
            8,
            vec![registration(Box::new(RetryingSink {
                attempts: attempts_tx,
            }))],
            writer.handle(),
            EventNotifier::new(),
            shutdown_rx,
        )
        .expect("sink runtime");

        tokio::time::timeout(Duration::from_secs(1), attempts_rx.recv())
            .await
            .expect("delivery timeout")
            .expect("delivery attempt");
        shutdown.trigger();
        runtime
            .shutdown(Duration::from_secs(1))
            .await
            .expect("sink shutdown");

        assert_eq!(
            store
                .consumer_offset("orders", "sink:cache")
                .expect("sink offset"),
            None
        );
        drop(writer);
        drop(store);
        drop(temp);
    }

    fn registration(sink: Box<dyn Sink>) -> SinkRegistration {
        SinkRegistration {
            config: SinkWorkerConfig {
                name: "cache".to_owned(),
                stream: StreamConfig {
                    name: "orders".to_owned(),
                    source: "default".to_owned(),
                    tables: vec!["public.orders".to_owned()],
                },
                batch_max_events: 10,
                batch_max_bytes: 1024 * 1024,
                retry_initial: Duration::from_secs(30),
                retry_max: Duration::from_secs(30),
                maximum_consumers: 10,
            },
            sink,
        }
    }

    fn store_with_events(count: u64) -> (TempDir, RedbEventStore, CaptureStorageWriter) {
        let temp = TempDir::new().expect("temp dir");
        let store = RedbEventStore::open(&LogOpenOptions {
            data_dir: temp.path().to_path_buf(),
            database_file: "sink.redb".to_owned(),
        })
        .expect("open store");
        for sequence in 1..=count {
            store.append_event(&event(sequence)).expect("append event");
        }
        let writer =
            CaptureStorageWriter::start(store.clone(), "default".to_owned()).expect("start writer");
        (temp, store, writer)
    }

    fn event(sequence: u64) -> ChangeEvent {
        ChangeEvent {
            sequence,
            event_id: format!("event-{sequence}"),
            source: SourceMetadata {
                database: "postgres".to_owned(),
                slot: "lightcdc".to_owned(),
                lsn: format!("0/{sequence:X}"),
            },
            transaction: None,
            schema: "public".to_owned(),
            table: "orders".to_owned(),
            operation: Operation::Insert,
            key: None,
            before: None,
            after: Some(format!(r#"{{"id":{sequence}}}"#).into_bytes()),
            commit_timestamp_ms: None,
        }
    }
}
