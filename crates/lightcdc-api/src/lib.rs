//! Serves durable redb events through the generated LightCDC gRPC contract.

use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use lightcdc_core::{ChangeEvent as CoreChangeEvent, Config, Operation, StreamConfig};
use lightcdc_runtime::{
    CaptureStorageHandle, CaptureStorageWriter, RuntimeState, RuntimeStateReceiver, ShutdownHandle,
    ShutdownReceiver, runtime_state_channel, shutdown_channel,
};
use lightcdc_storage::{RedbEventStore, StorageError};
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, transport::Server};
use tonic_health::ServingStatus;
use tracing::{info, warn};

/// Contains Rust types generated from the lightcdc protobuf contract.
pub mod proto {
    tonic::include_proto!("lightcdc.v1");
}

use proto::{
    AckRequest, AckResponse, ChangeEvent, SeekPosition, SeekRequest, SeekResponse,
    SubscribeRequest,
    light_cdc_server::{LightCdc, LightCdcServer},
};

/// Implements the lightcdc gRPC service against a local event store.
#[derive(Clone)]
pub struct LightCdcService {
    /// Immutable stream definitions shared with spawned subscription tasks.
    config: Arc<Config>,
    /// Durable log shared by RPC handlers and long-lived subscription tasks.
    store: Arc<RedbEventStore>,
    /// Queues consumer offset mutations behind capture and retention writes.
    storage_writer: CaptureStorageHandle,
    /// Keeps the writer thread alive when the service was constructed standalone.
    _storage_writer_owner: Option<Arc<CaptureStorageWriter>>,
    /// Stops long-lived subscription workers during graceful server shutdown.
    shutdown: ShutdownReceiver,
    /// Keeps standalone shutdown channels open when no external owner exists.
    _shutdown_owner: Option<ShutdownHandle>,
    event_notifier: EventNotifier,
    /// Prevents two workers from advancing the same consumer concurrently.
    active_subscriptions: Arc<Mutex<HashSet<SubscriptionKey>>>,
    /// Bounds acknowledgements to sequences this process actually delivered.
    delivery_high_watermarks: Arc<Mutex<HashMap<SubscriptionKey, u64>>>,
}

/// Identifies one consumer independently within one configured stream.
type SubscriptionKey = (String, String);

/// Wakes live subscribers after capture durably commits new events.
#[derive(Clone, Debug)]
pub struct EventNotifier {
    sender: watch::Sender<()>,
}

impl EventNotifier {
    /// Creates an independent event notification channel.
    pub fn new() -> Self {
        let (sender, _receiver) = watch::channel(());
        Self { sender }
    }

    /// Signals that subscribers should check the durable event log again.
    pub fn notify(&self) {
        self.sender.send_replace(());
    }

    /// Creates a receiver that wakes when capture commits another batch.
    fn subscribe(&self) -> watch::Receiver<()> {
        self.sender.subscribe()
    }
}

impl Default for EventNotifier {
    fn default() -> Self {
        Self::new()
    }
}

/// Removes a subscription identity from the active set when its task exits.
struct ActiveSubscription {
    key: SubscriptionKey,
    active: Arc<Mutex<HashSet<SubscriptionKey>>>,
}

impl Drop for ActiveSubscription {
    fn drop(&mut self) {
        active_subscriptions(&self.active).remove(&self.key);
    }
}

impl LightCdcService {
    /// Creates a gRPC service from configuration and a shared event store.
    pub fn new(config: Config, store: RedbEventStore) -> Self {
        Self::new_with_notifier(config, store, EventNotifier::new())
    }

    /// Creates a standalone service that owns its storage writer thread.
    pub fn new_with_notifier(
        config: Config,
        store: RedbEventStore,
        event_notifier: EventNotifier,
    ) -> Self {
        let (shutdown_owner, shutdown) = shutdown_channel();
        let storage_writer_owner = Arc::new(
            CaptureStorageWriter::start(store.clone(), config.source.name.clone())
                .expect("failed to start the gRPC storage writer"),
        );
        let storage_writer = storage_writer_owner.handle();
        Self::from_parts(
            config,
            store,
            event_notifier,
            storage_writer,
            Some(storage_writer_owner),
            shutdown,
            Some(shutdown_owner),
        )
    }

    /// Creates a service sharing an existing capture storage writer.
    pub fn new_with_storage_writer(
        config: Config,
        store: RedbEventStore,
        event_notifier: EventNotifier,
        storage_writer: CaptureStorageHandle,
    ) -> Self {
        let (shutdown_owner, shutdown) = shutdown_channel();
        Self::from_parts(
            config,
            store,
            event_notifier,
            storage_writer,
            None,
            shutdown,
            Some(shutdown_owner),
        )
    }

    /// Creates a service sharing capture storage and coordinated shutdown.
    pub fn new_with_runtime(
        config: Config,
        store: RedbEventStore,
        event_notifier: EventNotifier,
        storage_writer: CaptureStorageHandle,
        shutdown: ShutdownReceiver,
    ) -> Self {
        Self::from_parts(
            config,
            store,
            event_notifier,
            storage_writer,
            None,
            shutdown,
            None,
        )
    }

    fn from_parts(
        config: Config,
        store: RedbEventStore,
        event_notifier: EventNotifier,
        storage_writer: CaptureStorageHandle,
        storage_writer_owner: Option<Arc<CaptureStorageWriter>>,
        shutdown: ShutdownReceiver,
        shutdown_owner: Option<ShutdownHandle>,
    ) -> Self {
        Self {
            config: Arc::new(config),
            store: Arc::new(store),
            storage_writer,
            _storage_writer_owner: storage_writer_owner,
            shutdown,
            _shutdown_owner: shutdown_owner,
            event_notifier,
            active_subscriptions: Arc::new(Mutex::new(HashSet::new())),
            delivery_high_watermarks: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

/// Runs the lightcdc gRPC server until it is stopped.
pub async fn serve(
    addr: SocketAddr,
    config: Config,
    store: RedbEventStore,
) -> Result<(), tonic::transport::Error> {
    let service = LightCdcService::new(config, store);
    let (state, state_rx) = runtime_state_channel();
    state.transition(RuntimeState::Capturing, None);
    let (_shutdown, shutdown_rx) = shutdown_channel();
    serve_service(addr, service, state_rx, shutdown_rx).await
}

/// Runs the gRPC server with notifications from an in-process capture loop.
pub async fn serve_with_notifier(
    addr: SocketAddr,
    config: Config,
    store: RedbEventStore,
    event_notifier: EventNotifier,
    storage_writer: CaptureStorageHandle,
) -> Result<(), tonic::transport::Error> {
    let service =
        LightCdcService::new_with_storage_writer(config, store, event_notifier, storage_writer);
    let (state, state_rx) = runtime_state_channel();
    state.transition(RuntimeState::Capturing, None);
    let (_shutdown, shutdown_rx) = shutdown_channel();
    serve_service(addr, service, state_rx, shutdown_rx).await
}

/// Runs gRPC with externally coordinated runtime state and graceful shutdown.
pub async fn serve_with_runtime(
    addr: SocketAddr,
    config: Config,
    store: RedbEventStore,
    event_notifier: EventNotifier,
    storage_writer: CaptureStorageHandle,
    state: RuntimeStateReceiver,
    shutdown: ShutdownReceiver,
) -> Result<(), tonic::transport::Error> {
    let service = LightCdcService::new_with_runtime(
        config,
        store,
        event_notifier,
        storage_writer,
        shutdown.clone(),
    );
    serve_service(addr, service, state, shutdown).await
}

/// Registers an already-built service with tonic and listens on the address.
async fn serve_service(
    addr: SocketAddr,
    service: LightCdcService,
    state: RuntimeStateReceiver,
    mut shutdown: ShutdownReceiver,
) -> Result<(), tonic::transport::Error> {
    info!(%addr, "starting lightcdc gRPC server");
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    let health_task = tokio::spawn(report_health(health_reporter, state, shutdown.clone()));
    let result = Server::builder()
        .add_service(health_service)
        .add_service(LightCdcServer::new(service))
        .serve_with_shutdown(addr, async move { shutdown.cancelled().await })
        .await;
    health_task.abort();
    let _ = health_task.await;
    result
}

const LIVENESS_SERVICE: &str = "lightcdc.liveness";
const READINESS_SERVICE: &str = "lightcdc.readiness";

/// Maps runtime lifecycle transitions onto the standard gRPC health protocol.
async fn report_health(
    reporter: tonic_health::server::HealthReporter,
    mut state: RuntimeStateReceiver,
    mut shutdown: ShutdownReceiver,
) {
    reporter
        .set_service_status(LIVENESS_SERVICE, ServingStatus::Serving)
        .await;
    loop {
        let status = state.current();
        let serving = if status.state == RuntimeState::Capturing {
            ServingStatus::Serving
        } else {
            ServingStatus::NotServing
        };
        reporter
            .set_service_status(READINESS_SERVICE, serving)
            .await;
        reporter
            .set_service_status("lightcdc.v1.LightCdc", serving)
            .await;

        tokio::select! {
            _ = shutdown.cancelled() => {
                reporter
                    .set_service_status(READINESS_SERVICE, ServingStatus::NotServing)
                    .await;
                reporter
                    .set_service_status("lightcdc.v1.LightCdc", ServingStatus::NotServing)
                    .await;
                return;
            }
            changed = state.changed() => {
                if changed.is_err() {
                    return;
                }
            }
        }
    }
}

#[tonic::async_trait]
impl LightCdc for LightCdcService {
    type SubscribeStream = ReceiverStream<Result<ChangeEvent, Status>>;

    /// Streams matching events for a configured stream and consumer.
    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let request = request.into_inner();
        let consumer = validate_consumer(
            &request.consumer,
            self.config.runtime.max_consumer_name_bytes,
        )?;
        let stream = self.stream(&request.stream)?;
        let subscription = self.claim_subscription(&stream.name, consumer)?;
        let stored_offset = self
            .store
            .consumer_offset(&stream.name, consumer)
            .map_err(internal)?;
        let start_offset = match stored_offset {
            Some(offset) => offset,
            None => earliest_retained_offset(&self.store).map_err(internal)?,
        };
        let limit = request.limit as usize;
        let (tx, rx) = mpsc::channel(self.config.runtime.channel_capacity);
        let store = Arc::clone(&self.store);
        let mut event_notifications = self.event_notifier.subscribe();
        let delivery_key = (stream.name.clone(), consumer.to_owned());
        let delivery_high_watermarks = Arc::clone(&self.delivery_high_watermarks);
        let mut shutdown = self.shutdown.clone();
        let max_outbound_event_bytes = self.config.runtime.max_outbound_event_bytes;

        // This worker owns cloned state because it can outlive the subscribe RPC.
        tokio::spawn(async move {
            let _subscription = subscription;
            let mut next_sequence = start_offset + 1;
            let mut emitted = 0usize;

            loop {
                if shutdown.is_triggered() {
                    return;
                }
                let batch = match store.replay_from(next_sequence, 256) {
                    Ok(batch) => batch,
                    Err(error) => {
                        let _ = tx.send(Err(replay_status(error))).await;
                        return;
                    }
                };

                if batch.is_empty() {
                    tokio::select! {
                        _ = tx.closed() => return,
                        _ = shutdown.cancelled() => return,
                        result = event_notifications.changed() => {
                            if result.is_err() {
                                return;
                            }
                        }
                    }
                    continue;
                }

                for event in batch {
                    next_sequence = event.sequence + 1;
                    if !stream.matches_event(&event) {
                        continue;
                    }
                    if event_encoded_size(&event) > max_outbound_event_bytes {
                        let _ = tx
                            .send(Err(Status::resource_exhausted(
                                "event exceeds configured outbound size limit",
                            )))
                            .await;
                        return;
                    }

                    let permit = tokio::select! {
                        result = tx.reserve() => match result {
                            Ok(permit) => permit,
                            Err(_) => return,
                        },
                        _ = shutdown.cancelled() => return,
                    };
                    record_delivery(&delivery_high_watermarks, &delivery_key, event.sequence);
                    permit.send(Ok(event.into()));
                    emitted += 1;

                    if limit > 0 && emitted >= limit {
                        return;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    /// Saves the last event sequence successfully handled by a consumer.
    async fn ack(&self, request: Request<AckRequest>) -> Result<Response<AckResponse>, Status> {
        let request = request.into_inner();
        let consumer = validate_consumer(
            &request.consumer,
            self.config.runtime.max_consumer_name_bytes,
        )?;
        let stream = self.stream(&request.stream)?;
        let key = (stream.name.clone(), consumer.to_owned());
        let highest_delivered = delivery_high_watermarks(&self.delivery_high_watermarks)
            .get(&key)
            .copied()
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "consumer {consumer:?} has not been delivered an event from stream {:?}",
                    stream.name
                ))
            })?;

        if request.sequence > highest_delivered {
            return Err(Status::out_of_range(format!(
                "cannot acknowledge sequence {} because the highest sequence delivered to \
                 consumer {consumer:?} on stream {:?} is {highest_delivered}",
                request.sequence, stream.name
            )));
        }

        let offset = self
            .storage_writer
            .acknowledge_consumer_offset(stream.name.clone(), consumer.to_owned(), request.sequence)
            .await
            .map_err(internal)?;

        Ok(Response::new(AckResponse { offset }))
    }

    /// Moves a consumer offset to earliest, latest, or an absolute sequence.
    async fn seek(&self, request: Request<SeekRequest>) -> Result<Response<SeekResponse>, Status> {
        let request = request.into_inner();
        let consumer = validate_consumer(
            &request.consumer,
            self.config.runtime.max_consumer_name_bytes,
        )?;
        let stream = self.stream(&request.stream)?;
        let position =
            SeekPosition::try_from(request.position).unwrap_or(SeekPosition::Unspecified);

        let offset = match position {
            SeekPosition::Earliest => earliest_retained_offset(&self.store).map_err(internal)?,
            SeekPosition::Latest => self.store.last_sequence().map_err(internal)?.unwrap_or(0),
            SeekPosition::Absolute => request.sequence,
            SeekPosition::Unspecified => {
                return Err(Status::invalid_argument(
                    "seek position must be earliest, latest, or absolute",
                ));
            }
        };

        self.storage_writer
            .set_consumer_offset(stream.name.clone(), consumer.to_owned(), offset)
            .await
            .map_err(internal)?;
        delivery_high_watermarks(&self.delivery_high_watermarks)
            .remove(&(stream.name.clone(), consumer.to_owned()));

        Ok(Response::new(SeekResponse { offset }))
    }
}

impl LightCdcService {
    /// Claims the one active subscription allowed for a stream and consumer.
    fn claim_subscription(
        &self,
        stream: &str,
        consumer: &str,
    ) -> Result<ActiveSubscription, Status> {
        let key = (stream.to_owned(), consumer.to_owned());
        let mut active = active_subscriptions(&self.active_subscriptions);

        if !active.insert(key.clone()) {
            return Err(Status::already_exists(format!(
                "consumer {consumer:?} already has an active subscription to stream {stream:?}"
            )));
        }
        if active.len() > self.config.runtime.max_active_subscriptions {
            active.remove(&key);
            return Err(Status::resource_exhausted(
                "active subscription limit reached",
            ));
        }
        drop(active);

        Ok(ActiveSubscription {
            key,
            active: Arc::clone(&self.active_subscriptions),
        })
    }

    /// Looks up a configured stream or returns a gRPC status error.
    fn stream(&self, name: &str) -> Result<StreamConfig, Status> {
        if name.is_empty() {
            return Err(Status::invalid_argument("stream name is required"));
        }
        if name.len() > 128 {
            return Err(Status::invalid_argument("stream name exceeds 128 bytes"));
        }

        self.config
            .stream(name)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("stream {name:?} is not defined")))
    }
}

impl From<CoreChangeEvent> for ChangeEvent {
    /// Converts the internal event model into the protobuf event model.
    fn from(event: CoreChangeEvent) -> Self {
        Self {
            sequence: event.sequence,
            event_id: event.event_id,
            source: Some(proto::SourceMetadata {
                database: event.source.database,
                slot: event.source.slot,
                lsn: event.source.lsn,
            }),
            transaction: event
                .transaction
                .map(|transaction| proto::TransactionMetadata {
                    transaction_id: transaction.transaction_id,
                    begin_lsn: transaction.begin_lsn,
                    commit_lsn: transaction.commit_lsn,
                }),
            schema: event.schema,
            table: event.table,
            operation: operation_to_proto(event.operation),
            key: event.key,
            before: event.before,
            after: event.after,
            commit_timestamp_ms: event.commit_timestamp_ms,
        }
    }
}

/// Converts a core operation into its protobuf enum value.
fn operation_to_proto(operation: Operation) -> i32 {
    match operation {
        Operation::Insert => proto::Operation::Insert as i32,
        Operation::Update => proto::Operation::Update as i32,
        Operation::Delete => proto::Operation::Delete as i32,
        Operation::Truncate => proto::Operation::Truncate as i32,
    }
}

/// Validates that a consumer name was provided.
fn validate_consumer(consumer: &str, max_bytes: usize) -> Result<&str, Status> {
    if consumer.is_empty() {
        Err(Status::invalid_argument("consumer name is required"))
    } else if consumer.len() > max_bytes {
        Err(Status::invalid_argument(format!(
            "consumer name exceeds configured {max_bytes}-byte limit"
        )))
    } else {
        Ok(consumer)
    }
}

/// Estimates encoded event memory before cloning it into the protobuf response.
fn event_encoded_size(event: &CoreChangeEvent) -> usize {
    event
        .event_id
        .len()
        .saturating_add(event.schema.len())
        .saturating_add(event.table.len())
        .saturating_add(event.source.database.len())
        .saturating_add(event.source.slot.len())
        .saturating_add(event.source.lsn.len())
        .saturating_add(event.key.as_ref().map_or(0, Vec::len))
        .saturating_add(event.before.as_ref().map_or(0, Vec::len))
        .saturating_add(event.after.as_ref().map_or(0, Vec::len))
}

/// Recovers the active-subscription set if another task panicked while holding it.
fn active_subscriptions(
    active: &Mutex<HashSet<SubscriptionKey>>,
) -> std::sync::MutexGuard<'_, HashSet<SubscriptionKey>> {
    active
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Recovers the delivery map if another task panicked while holding it.
fn delivery_high_watermarks(
    high_watermarks: &Mutex<HashMap<SubscriptionKey, u64>>,
) -> std::sync::MutexGuard<'_, HashMap<SubscriptionKey, u64>> {
    high_watermarks
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Records the highest sequence made available to one stream consumer.
fn record_delivery(
    high_watermarks: &Mutex<HashMap<SubscriptionKey, u64>>,
    key: &SubscriptionKey,
    sequence: u64,
) {
    delivery_high_watermarks(high_watermarks)
        .entry(key.clone())
        .and_modify(|highest| *highest = (*highest).max(sequence))
        .or_insert(sequence);
}

/// Returns the offset immediately before the oldest retained event.
fn earliest_retained_offset(store: &RedbEventStore) -> Result<u64, StorageError> {
    match store.first_sequence()? {
        Some(first_sequence) => Ok(first_sequence.saturating_sub(1)),
        None => Ok(store.last_sequence()?.unwrap_or(0)),
    }
}

/// Converts an expired replay position into an actionable consumer error.
fn replay_status(error: StorageError) -> Status {
    match error {
        StorageError::SequenceExpired {
            requested,
            first_available,
        } => Status::failed_precondition(format!(
            "consumer offset expired at sequence {requested}; first retained sequence is \
             {first_available}; seek to earliest or latest before subscribing again"
        )),
        error => internal(format!("failed to read events from redb: {error}")),
    }
}

/// Converts internal errors into gRPC internal status errors.
fn internal(error: impl ToString) -> Status {
    let message = error.to_string();
    warn!(error = %message, "gRPC request failed");
    Status::internal("internal service error")
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;

    use lightcdc_core::{
        ChangeEvent as CoreChangeEvent, LoggingConfig, Operation as CoreOperation, RuntimeConfig,
        SourceConfig, SourceMetadata, StreamConfig,
    };
    use lightcdc_storage::{LogOpenOptions, RetentionPolicy, SegmentOptions};
    use tempfile::TempDir;
    use tokio::time::{Duration, timeout};
    use tokio_stream::StreamExt;
    use tonic_health::pb::{HealthCheckRequest, health_server::Health};

    use super::*;

    #[tokio::test]
    async fn subscribe_filters_events_by_stream() {
        let service = service_with_events(&[
            event(1, "public", "customers"),
            event(2, "public", "orders"),
            event(3, "public", "orders"),
        ]);

        let response = service
            .subscribe(Request::new(SubscribeRequest {
                stream: "orders".to_owned(),
                consumer: "search-indexer".to_owned(),
                limit: 1,
            }))
            .await
            .expect("subscribe response");
        let mut stream = response.into_inner();
        let event = stream
            .next()
            .await
            .expect("stream item")
            .expect("change event");

        assert_eq!(event.sequence, 2);
        assert_eq!(event.table, "orders");
        assert_eq!(event.operation, proto::Operation::Insert as i32);
    }

    #[tokio::test]
    async fn commit_notification_wakes_a_waiting_subscriber() {
        let service = service_with_events(&[]);
        let mut subscription = service
            .subscribe(Request::new(subscription("search-indexer", 1)))
            .await
            .expect("subscribe response")
            .into_inner();

        assert!(
            timeout(Duration::from_millis(20), subscription.next())
                .await
                .is_err(),
            "empty subscription should wait for a durable event"
        );

        service
            .store
            .append_event(&event(1, "public", "orders"))
            .expect("append event");
        service.event_notifier.notify();

        let delivered = timeout(Duration::from_secs(1), subscription.next())
            .await
            .expect("subscriber should wake immediately")
            .expect("stream item")
            .expect("change event");
        assert_eq!(delivered.sequence, 1);
    }

    #[tokio::test]
    async fn shutdown_closes_a_waiting_subscription() {
        let service = service_with_events(&[]);
        let mut subscription = service
            .subscribe(Request::new(subscription("search-indexer", 0)))
            .await
            .expect("subscribe response")
            .into_inner();

        service
            ._shutdown_owner
            .as_ref()
            .expect("standalone shutdown owner")
            .trigger();

        assert!(
            timeout(Duration::from_secs(1), subscription.next())
                .await
                .expect("subscription shutdown")
                .is_none()
        );
    }

    #[tokio::test]
    async fn subscription_names_counts_and_event_sizes_are_bounded() {
        let mut service = service_with_events(&[event(1, "public", "orders")]);
        let config = Arc::get_mut(&mut service.service.config).expect("unique test config");
        config.runtime.max_consumer_name_bytes = 3;
        config.runtime.max_active_subscriptions = 1;
        config.runtime.max_outbound_event_bytes = 1;

        let name_error = service
            .subscribe(Request::new(subscription("long", 0)))
            .await
            .expect_err("long consumer name");
        assert_eq!(name_error.code(), tonic::Code::InvalidArgument);

        let mut first = service
            .subscribe(Request::new(subscription("one", 0)))
            .await
            .expect("first subscription")
            .into_inner();
        let oversized = first
            .next()
            .await
            .expect("oversized event status")
            .expect_err("event should exceed outbound limit");
        assert_eq!(oversized.code(), tonic::Code::ResourceExhausted);
        wait_for_subscription_release(&service, "one").await;

        config_for_service(&mut service)
            .runtime
            .max_outbound_event_bytes = 1024;
        let _active = service
            .subscribe(Request::new(subscription("one", 0)))
            .await
            .expect("active subscription");
        let limit_error = service
            .subscribe(Request::new(subscription("two", 0)))
            .await
            .expect_err("subscription count limit");
        assert_eq!(limit_error.code(), tonic::Code::ResourceExhausted);
    }

    #[tokio::test]
    async fn health_separates_liveness_from_capture_readiness() {
        let (state, state_rx) = runtime_state_channel();
        let (shutdown, shutdown_rx) = shutdown_channel();
        let reporter = tonic_health::server::HealthReporter::new();
        let health = tonic_health::server::HealthService::from_health_reporter(reporter.clone());
        let task = tokio::spawn(report_health(reporter, state_rx, shutdown_rx));
        tokio::task::yield_now().await;

        assert_eq!(
            health_status(&health, LIVENESS_SERVICE).await,
            tonic_health::pb::health_check_response::ServingStatus::Serving as i32
        );
        assert_eq!(
            health_status(&health, READINESS_SERVICE).await,
            tonic_health::pb::health_check_response::ServingStatus::NotServing as i32
        );

        state.transition(RuntimeState::Capturing, None);
        tokio::task::yield_now().await;
        assert_eq!(
            health_status(&health, READINESS_SERVICE).await,
            tonic_health::pb::health_check_response::ServingStatus::Serving as i32
        );

        shutdown.trigger();
        task.await.expect("health task");
        assert_eq!(
            health_status(&health, READINESS_SERVICE).await,
            tonic_health::pb::health_check_response::ServingStatus::NotServing as i32
        );
    }

    #[test]
    fn internal_status_does_not_expose_private_error_details() {
        let status = internal("/private/path/events.redb failed");
        assert_eq!(status.message(), "internal service error");
    }

    #[tokio::test]
    async fn ack_persists_stream_scoped_consumer_offset() {
        let service = service_with_events(&[event(42, "public", "orders")]);
        let mut subscription = service
            .subscribe(Request::new(subscription("search-indexer", 1)))
            .await
            .expect("subscribe response")
            .into_inner();
        let delivered = subscription
            .next()
            .await
            .expect("stream item")
            .expect("change event");

        let response = service
            .ack(Request::new(AckRequest {
                stream: "orders".to_owned(),
                consumer: "search-indexer".to_owned(),
                sequence: delivered.sequence,
            }))
            .await
            .expect("ack response")
            .into_inner();

        assert_eq!(response.offset, 42);
        assert_eq!(
            service
                .store
                .consumer_offset("orders", "search-indexer")
                .expect("consumer offset"),
            Some(42)
        );
    }

    #[tokio::test]
    async fn ack_does_not_move_consumer_offset_backward() {
        let service = service_with_events(&[event(41, "public", "orders")]);
        let mut subscription = service
            .subscribe(Request::new(subscription("search-indexer", 1)))
            .await
            .expect("subscribe response")
            .into_inner();
        let delivered = subscription
            .next()
            .await
            .expect("stream item")
            .expect("change event");
        service
            .store
            .set_consumer_offset("orders", "search-indexer", 42)
            .expect("set initial consumer offset");

        let response = service
            .ack(Request::new(AckRequest {
                stream: "orders".to_owned(),
                consumer: "search-indexer".to_owned(),
                sequence: delivered.sequence,
            }))
            .await
            .expect("ack response")
            .into_inner();

        assert_eq!(response.offset, 42);
        assert_eq!(
            service
                .store
                .consumer_offset("orders", "search-indexer")
                .expect("consumer offset"),
            Some(42)
        );
    }

    #[tokio::test]
    async fn ack_rejects_a_sequence_above_the_highest_delivered() {
        let service = service_with_events(&[event(1, "public", "orders")]);
        let mut subscription = service
            .subscribe(Request::new(subscription("search-indexer", 1)))
            .await
            .expect("subscribe response")
            .into_inner();
        subscription
            .next()
            .await
            .expect("stream item")
            .expect("change event");

        let error = service
            .ack(Request::new(AckRequest {
                stream: "orders".to_owned(),
                consumer: "search-indexer".to_owned(),
                sequence: 2,
            }))
            .await
            .expect_err("ack above delivered high-water mark should fail");

        assert_eq!(error.code(), tonic::Code::OutOfRange);
        assert_eq!(
            service
                .store
                .consumer_offset("orders", "search-indexer")
                .expect("consumer offset"),
            None
        );
    }

    #[tokio::test]
    async fn ack_rejects_a_consumer_without_a_delivery() {
        let service = service_with_events(&[]);

        let error = service
            .ack(Request::new(AckRequest {
                stream: "orders".to_owned(),
                consumer: "search-indexer".to_owned(),
                sequence: 1,
            }))
            .await
            .expect_err("ack without a delivery should fail");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn rejects_a_second_active_subscription_for_the_same_consumer() {
        let service = service_with_events(&[event(1, "public", "orders")]);
        let mut first = service
            .subscribe(Request::new(subscription("search-indexer", 0)))
            .await
            .expect("first subscribe response")
            .into_inner();
        first
            .next()
            .await
            .expect("first event")
            .expect("change event");

        let error = service
            .subscribe(Request::new(subscription("search-indexer", 0)))
            .await
            .expect_err("duplicate active subscription should fail");

        assert_eq!(error.code(), tonic::Code::AlreadyExists);
    }

    #[tokio::test]
    async fn disconnect_without_ack_redelivers_the_event() {
        let service = service_with_events(&[event(1, "public", "orders")]);
        let mut first = service
            .subscribe(Request::new(subscription("search-indexer", 0)))
            .await
            .expect("first subscribe response")
            .into_inner();
        let first_event = first
            .next()
            .await
            .expect("first stream item")
            .expect("first change event");
        drop(first);
        wait_for_subscription_release(&service, "search-indexer").await;

        let mut resumed = service
            .subscribe(Request::new(subscription("search-indexer", 1)))
            .await
            .expect("resumed subscribe response")
            .into_inner();
        let redelivered = resumed
            .next()
            .await
            .expect("resumed stream item")
            .expect("redelivered change event");

        assert_eq!(redelivered.sequence, first_event.sequence);
        assert_eq!(
            service
                .store
                .consumer_offset("orders", "search-indexer")
                .expect("consumer offset"),
            None
        );
    }

    #[tokio::test]
    async fn delayed_ack_after_disconnect_remains_valid() {
        let service = service_with_events(&[event(1, "public", "orders")]);
        let mut subscription = service
            .subscribe(Request::new(subscription("search-indexer", 0)))
            .await
            .expect("subscribe response")
            .into_inner();
        let delivered = subscription
            .next()
            .await
            .expect("stream item")
            .expect("change event");
        drop(subscription);
        wait_for_subscription_release(&service, "search-indexer").await;

        let response = service
            .ack(Request::new(AckRequest {
                stream: "orders".to_owned(),
                consumer: "search-indexer".to_owned(),
                sequence: delivered.sequence,
            }))
            .await
            .expect("delayed ack response")
            .into_inner();

        assert_eq!(response.offset, delivered.sequence);
    }

    #[tokio::test]
    async fn reconnect_after_ack_resumes_after_the_acknowledged_event() {
        let service =
            service_with_events(&[event(1, "public", "orders"), event(2, "public", "orders")]);
        let mut first = service
            .subscribe(Request::new(subscription("search-indexer", 0)))
            .await
            .expect("first subscribe response")
            .into_inner();
        let first_event = first
            .next()
            .await
            .expect("first stream item")
            .expect("first change event");

        service
            .ack(Request::new(AckRequest {
                stream: "orders".to_owned(),
                consumer: "search-indexer".to_owned(),
                sequence: first_event.sequence,
            }))
            .await
            .expect("ack response");
        drop(first);
        wait_for_subscription_release(&service, "search-indexer").await;

        let mut resumed = service
            .subscribe(Request::new(subscription("search-indexer", 1)))
            .await
            .expect("resumed subscribe response")
            .into_inner();
        let next_event = resumed
            .next()
            .await
            .expect("resumed stream item")
            .expect("next change event");

        assert_eq!(next_event.sequence, 2);
    }

    #[tokio::test]
    async fn expired_consumer_offset_requires_an_explicit_seek() {
        let service = service_with_events(&[
            event(1, "public", "orders"),
            event(2, "public", "orders"),
            event(3, "public", "orders"),
        ]);
        service
            .store
            .set_consumer_offset("orders", "search-indexer", 1)
            .expect("set stale offset");
        service
            .store
            .prune_events(
                RetentionPolicy {
                    max_events: Some(1),
                    max_age: None,
                    delete_batch_size: 10,
                },
                i64::MAX,
            )
            .expect("prune events");

        let mut expired = service
            .subscribe(Request::new(subscription("search-indexer", 1)))
            .await
            .expect("subscribe response")
            .into_inner();
        let error = expired
            .next()
            .await
            .expect("stream status")
            .expect_err("expired offset should fail");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.message().contains("first retained sequence is 3"));
    }

    #[tokio::test]
    async fn seek_earliest_resumes_at_the_retained_prefix() {
        let service = service_with_events(&[
            event(1, "public", "orders"),
            event(2, "public", "orders"),
            event(3, "public", "orders"),
        ]);
        service
            .store
            .prune_events(
                RetentionPolicy {
                    max_events: Some(1),
                    max_age: None,
                    delete_batch_size: 10,
                },
                i64::MAX,
            )
            .expect("prune events");
        let seek = service
            .seek(Request::new(SeekRequest {
                stream: "orders".to_owned(),
                consumer: "search-indexer".to_owned(),
                position: SeekPosition::Earliest as i32,
                sequence: 0,
            }))
            .await
            .expect("seek response")
            .into_inner();

        assert_eq!(seek.offset, 2);

        let mut subscription = service
            .subscribe(Request::new(subscription("search-indexer", 1)))
            .await
            .expect("subscribe response")
            .into_inner();
        let event = subscription
            .next()
            .await
            .expect("stream item")
            .expect("retained event");
        assert_eq!(event.sequence, 3);
    }

    #[tokio::test]
    async fn seek_latest_sets_consumer_offset_to_last_sequence() {
        let service = service_with_events(&[
            event(1, "public", "orders"),
            event(2, "public", "customers"),
        ]);

        let response = service
            .seek(Request::new(SeekRequest {
                stream: "orders".to_owned(),
                consumer: "search-indexer".to_owned(),
                position: SeekPosition::Latest as i32,
                sequence: 0,
            }))
            .await
            .expect("seek response")
            .into_inner();

        assert_eq!(response.offset, 2);
        assert_eq!(
            service
                .store
                .consumer_offset("orders", "search-indexer")
                .expect("consumer offset"),
            Some(2)
        );
    }

    #[tokio::test]
    async fn seek_clears_the_previous_delivery_high_water_mark() {
        let service = service_with_events(&[event(1, "public", "orders")]);
        let mut subscription = service
            .subscribe(Request::new(subscription("search-indexer", 1)))
            .await
            .expect("subscribe response")
            .into_inner();
        subscription
            .next()
            .await
            .expect("stream item")
            .expect("change event");

        service
            .seek(Request::new(SeekRequest {
                stream: "orders".to_owned(),
                consumer: "search-indexer".to_owned(),
                position: SeekPosition::Earliest as i32,
                sequence: 0,
            }))
            .await
            .expect("seek response");

        let error = service
            .ack(Request::new(AckRequest {
                stream: "orders".to_owned(),
                consumer: "search-indexer".to_owned(),
                sequence: 1,
            }))
            .await
            .expect_err("seek should require a new delivery before ack");

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }

    struct TestService {
        service: LightCdcService,
        _temp: TempDir,
    }

    impl Deref for TestService {
        type Target = LightCdcService;

        fn deref(&self) -> &Self::Target {
            &self.service
        }
    }

    fn service_with_events(events: &[CoreChangeEvent]) -> TestService {
        let temp = TempDir::new().expect("temp dir");
        let data_dir = temp.path().to_path_buf();
        let store = RedbEventStore::open_with_segment_options(
            &LogOpenOptions {
                data_dir: data_dir.clone(),
                database_file: "test.redb".to_owned(),
            },
            SegmentOptions {
                max_events: 2,
                ..SegmentOptions::default()
            },
        )
        .expect("open store");

        for event in events {
            store.append_event(event).expect("append event");
        }

        let mut config = config();
        config.runtime.data_dir = data_dir.display().to_string();
        TestService {
            service: LightCdcService::new(config, store),
            _temp: temp,
        }
    }

    fn subscription(consumer: &str, limit: u32) -> SubscribeRequest {
        SubscribeRequest {
            stream: "orders".to_owned(),
            consumer: consumer.to_owned(),
            limit,
        }
    }

    async fn wait_for_subscription_release(service: &LightCdcService, consumer: &str) {
        timeout(Duration::from_secs(1), async {
            loop {
                let key = ("orders".to_owned(), consumer.to_owned());
                if !active_subscriptions(&service.active_subscriptions).contains(&key) {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("subscription task should stop after client disconnects");
    }

    async fn health_status<H: Health>(health: &H, service: &str) -> i32 {
        health
            .check(Request::new(HealthCheckRequest {
                service: service.to_owned(),
            }))
            .await
            .expect("health response")
            .into_inner()
            .status
    }

    fn config_for_service(service: &mut TestService) -> &mut Config {
        Arc::get_mut(&mut service.service.config).expect("unique test config")
    }

    fn config() -> Config {
        Config {
            source: SourceConfig {
                name: "default".to_owned(),
                host: "localhost".to_owned(),
                port: 5432,
                database: "lightcdc".to_owned(),
                user: "lightcdc".to_owned(),
                password: "lightcdc".to_owned(),
                publication: "publication".to_owned(),
                slot: "slot".to_owned(),
            },
            runtime: RuntimeConfig {
                data_dir: "data".to_owned(),
                storage_file: "lightcdc.redb".to_owned(),
                channel_capacity: 1024,
                shutdown_timeout_ms: 10000,
                max_active_subscriptions: 1_024,
                max_consumer_name_bytes: 128,
                max_outbound_event_bytes: 16 * 1024 * 1024,
                heartbeat_interval_ms: 10_000,
                transaction_memory_threshold_bytes: 16 * 1024 * 1024,
                max_transaction_bytes: 1024 * 1024 * 1024,
                max_transaction_events: 1_000_000,
                capture_batch_max_transactions: 100,
                capture_batch_max_events: 500,
                capture_batch_max_bytes: 4 * 1024 * 1024,
                capture_batch_max_delay_ms: 20,
                segment_max_events: 1_000_000,
                segment_max_bytes: 256 * 1024 * 1024,
                segment_max_age_seconds: 15 * 60,
                retention_max_events: None,
                retention_max_age_seconds: None,
                retention_check_interval_ms: 1_000,
                retention_delete_batch_size: 100_000,
            },
            logging: LoggingConfig {
                level: "info".to_owned(),
            },
            streams: vec![StreamConfig {
                name: "orders".to_owned(),
                source: "default".to_owned(),
                tables: vec!["public.orders".to_owned()],
            }],
        }
    }

    fn event(sequence: u64, schema: &str, table: &str) -> CoreChangeEvent {
        CoreChangeEvent {
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
            operation: CoreOperation::Insert,
            key: None,
            before: None,
            after: Some(br#"{"id":"1"}"#.to_vec()),
            commit_timestamp_ms: None,
        }
    }
}
