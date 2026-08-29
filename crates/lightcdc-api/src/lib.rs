//! Serves durable redb events through the generated LightCDC gRPC contract.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt, fs,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context as TaskContext, Poll},
};

use anyhow::Context;
use lightcdc_core::{ApiConfig, ChangeEvent as CoreChangeEvent, Config, Operation, StreamConfig};
use lightcdc_runtime::{
    CaptureStorageHandle, CaptureStorageWriter, ProductionMetrics, RuntimeState,
    RuntimeStateReceiver, ShutdownHandle, ShutdownReceiver, StorageReaderHandle, StorageReaderPool,
    runtime_state_channel, shutdown_channel,
};
use lightcdc_storage::{RedbEventStore, StorageError};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
};
use tokio_stream::{
    StreamExt,
    wrappers::{ReceiverStream, TcpListenerStream},
};
use tonic::{
    Request, Response, Status,
    metadata::MetadataMap,
    transport::{Identity, Server, ServerTlsConfig, server::Connected},
};
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
    /// Direct test access; production reads go through the bounded reader pool.
    #[cfg(test)]
    store: Arc<RedbEventStore>,
    /// Queues consumer offset mutations behind capture and retention writes.
    storage_writer: CaptureStorageHandle,
    /// Keeps the writer thread alive when the service was constructed standalone.
    _storage_writer_owner: Option<Arc<CaptureStorageWriter>>,
    /// Runs synchronous redb reads on a fixed set of OS threads.
    storage_reader: StorageReaderHandle,
    /// Keeps the bounded reader pool alive for every service clone.
    _storage_reader_owner: Arc<StorageReaderPool>,
    /// Stops long-lived subscription workers during graceful server shutdown.
    shutdown: ShutdownReceiver,
    /// Keeps standalone shutdown channels open when no external owner exists.
    _shutdown_owner: Option<ShutdownHandle>,
    event_notifier: EventNotifier,
    /// Prevents two workers from advancing the same consumer concurrently.
    active_subscriptions: Arc<Mutex<HashSet<SubscriptionKey>>>,
    /// Bounds acknowledgements to sequences this process actually delivered.
    delivery_high_watermarks: Arc<Mutex<DeliveryWatermarks>>,
    /// Resolved bearer principals and per-stream permissions.
    authorizer: Arc<ApiAuthorizer>,
    /// Fixed-cardinality counters and health signals in production runtimes.
    metrics: Option<ProductionMetrics>,
}

/// Groups externally owned runtime handles shared with the gRPC server.
pub struct ApiRuntime {
    pub storage_writer: CaptureStorageHandle,
    pub state: RuntimeStateReceiver,
    pub shutdown: ShutdownReceiver,
    pub metrics: Option<ProductionMetrics>,
}

/// Marks API startup failures that require a configuration change.
#[derive(Debug)]
pub struct ApiConfigurationFailure;

impl fmt::Display for ApiConfigurationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("gRPC API configuration requires operator action")
    }
}

#[derive(Clone, Debug, Default)]
struct ApiAuthorizer {
    principals: Vec<ApiPrincipal>,
}

#[derive(Clone, Debug)]
struct ApiPrincipal {
    name: String,
    token: Vec<u8>,
    streams: HashSet<String>,
    allow_seek: bool,
}

/// Holds a connection permit until tonic drops the accepted socket.
struct LimitedConnection {
    stream: TcpStream,
    _permit: OwnedSemaphorePermit,
    metrics: Option<ProductionMetrics>,
}

impl Drop for LimitedConnection {
    fn drop(&mut self) {
        if let Some(metrics) = &self.metrics {
            metrics.record_api_connection_finished();
        }
    }
}

impl AsyncRead for LimitedConnection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(context, buffer)
    }
}

impl AsyncWrite for LimitedConnection {
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

impl Connected for LimitedConnection {
    type ConnectInfo = SocketAddr;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.stream
            .peer_addr()
            .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)))
    }
}

#[derive(Clone, Copy)]
enum ApiPermission {
    Consume,
    Seek,
}

impl ApiAuthorizer {
    fn from_config(config: &ApiConfig) -> anyhow::Result<Self> {
        let mut principals = Vec::with_capacity(config.tokens.len());
        for principal in &config.tokens {
            let token = match (&principal.token_env, &principal.token_file) {
                (Some(variable), None) => std::env::var(variable).with_context(|| {
                    format!(
                        "API token environment variable {variable:?} for principal {:?} is unset",
                        principal.name
                    )
                })?,
                (None, Some(path)) => fs::read_to_string(path)
                    .with_context(|| {
                        format!(
                            "read API token file {path:?} for principal {:?}",
                            principal.name
                        )
                    })?
                    .trim_end_matches(['\r', '\n'])
                    .to_owned(),
                _ => anyhow::bail!(
                    "API principal {:?} must configure exactly one token source",
                    principal.name
                ),
            };
            if token.is_empty() {
                anyhow::bail!("API token for principal {:?} is empty", principal.name);
            }
            if principals
                .iter()
                .any(|existing: &ApiPrincipal| constant_time_eq(&existing.token, token.as_bytes()))
            {
                anyhow::bail!("API principals must not resolve to duplicate bearer tokens");
            }
            principals.push(ApiPrincipal {
                name: principal.name.clone(),
                token: token.into_bytes(),
                streams: principal.streams.iter().cloned().collect(),
                allow_seek: principal.allow_seek,
            });
        }
        Ok(Self { principals })
    }

    fn authorize(
        &self,
        metadata: &MetadataMap,
        stream: &str,
        permission: ApiPermission,
    ) -> Result<(), Status> {
        if self.principals.is_empty() {
            return Ok(());
        }
        let bearer = metadata
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .ok_or_else(|| Status::unauthenticated("valid bearer token required"))?;
        let principal = self
            .principals
            .iter()
            .find(|principal| constant_time_eq(&principal.token, bearer.as_bytes()))
            .ok_or_else(|| Status::unauthenticated("valid bearer token required"))?;
        if !principal.streams.contains("*") && !principal.streams.contains(stream) {
            warn!(principal = %principal.name, %stream, "gRPC stream access denied");
            return Err(Status::permission_denied(
                "principal is not authorized for this stream",
            ));
        }
        if matches!(permission, ApiPermission::Seek) && !principal.allow_seek {
            warn!(principal = %principal.name, %stream, "gRPC seek access denied");
            return Err(Status::permission_denied(
                "principal is not authorized to seek consumers",
            ));
        }
        Ok(())
    }
}

fn constant_time_eq(expected: &[u8], actual: &[u8]) -> bool {
    let mut difference = expected.len() ^ actual.len();
    for index in 0..expected.len().max(actual.len()) {
        difference |= usize::from(
            expected.get(index).copied().unwrap_or_default()
                ^ actual.get(index).copied().unwrap_or_default(),
        );
    }
    difference == 0
}

/// Identifies one consumer independently within one configured stream.
type SubscriptionKey = (String, String);

pub use lightcdc_runtime::EventNotifier;

/// Bounds per-consumer delivery watermarks to the durable consumer limit so a
/// long-lived server never accumulates an entry per distinct consumer name.
#[derive(Debug)]
struct DeliveryWatermarks {
    capacity: usize,
    /// Most-recently-used consumer keys, oldest first.
    order: VecDeque<SubscriptionKey>,
    highest: HashMap<SubscriptionKey, u64>,
}

impl DeliveryWatermarks {
    fn new(capacity: usize) -> Self {
        Self {
            // The runtime validates the configured limit only against its upper
            // bound, so treat an absent zero as the default.
            capacity: if capacity == 0 { 10_000 } else { capacity },
            order: VecDeque::new(),
            highest: HashMap::new(),
        }
    }

    /// Returns the watermark and marks the consumer most-recently-used.
    fn get(&mut self, key: &SubscriptionKey) -> Option<u64> {
        let highest = self.highest.get(key).copied()?;
        self.order.retain(|candidate| candidate != key);
        self.order.push_back(key.clone());
        Some(highest)
    }

    /// Records a delivered sequence, evicting the least-recently-used consumer
    /// when the map is at capacity.
    fn record(&mut self, key: SubscriptionKey, sequence: u64) {
        self.order.retain(|candidate| candidate != &key);
        self.highest
            .entry(key.clone())
            .and_modify(|highest| *highest = (*highest).max(sequence))
            .or_insert(sequence);
        if self.highest.len() > self.capacity {
            if let Some(evicted) = self.order.pop_front() {
                self.highest.remove(&evicted);
            }
        }
        self.order.push_back(key);
    }

    fn remove(&mut self, key: &SubscriptionKey) {
        self.order.retain(|candidate| candidate != key);
        self.highest.remove(key);
    }
}

/// Removes a subscription identity from the active set when its task exits.
struct ActiveSubscription {
    key: SubscriptionKey,
    active: Arc<Mutex<HashSet<SubscriptionKey>>>,
    metrics: Option<ProductionMetrics>,
}

impl Drop for ActiveSubscription {
    fn drop(&mut self) {
        active_subscriptions(&self.active).remove(&self.key);
        if let Some(metrics) = &self.metrics {
            metrics.record_subscription_finished();
        }
    }
}

/// Groups constructor-only dependencies so service ownership remains explicit.
struct ServiceParts {
    config: Config,
    store: RedbEventStore,
    event_notifier: EventNotifier,
    storage_writer: CaptureStorageHandle,
    storage_writer_owner: Option<Arc<CaptureStorageWriter>>,
    storage_reader: StorageReaderHandle,
    storage_reader_owner: Arc<StorageReaderPool>,
    shutdown: ShutdownReceiver,
    shutdown_owner: Option<ShutdownHandle>,
    metrics: Option<ProductionMetrics>,
}

impl LightCdcService {
    /// Creates a gRPC service from configuration and a shared event store.
    pub fn new(config: Config, store: RedbEventStore) -> anyhow::Result<Self> {
        Self::new_with_notifier(config, store, EventNotifier::new())
    }

    /// Creates a standalone service that owns its storage writer thread.
    pub fn new_with_notifier(
        config: Config,
        store: RedbEventStore,
        event_notifier: EventNotifier,
    ) -> anyhow::Result<Self> {
        let authorizer = ApiAuthorizer::from_config(&config.api)
            .map_err(|error| error.context(ApiConfigurationFailure))?;
        let (shutdown_owner, shutdown) = shutdown_channel();
        let storage_writer_owner = Arc::new(CaptureStorageWriter::start(
            store.clone(),
            config.source.name.clone(),
        )?);
        let storage_writer = storage_writer_owner.handle();
        let (storage_reader_owner, storage_reader) = start_storage_readers(&config, &store)?;
        Ok(Self::from_parts(
            ServiceParts {
                config,
                store,
                event_notifier,
                storage_writer,
                storage_writer_owner: Some(storage_writer_owner),
                storage_reader,
                storage_reader_owner,
                shutdown,
                shutdown_owner: Some(shutdown_owner),
                metrics: None,
            },
            authorizer,
        ))
    }

    /// Creates a service sharing an existing capture storage writer.
    pub fn new_with_storage_writer(
        config: Config,
        store: RedbEventStore,
        event_notifier: EventNotifier,
        storage_writer: CaptureStorageHandle,
    ) -> anyhow::Result<Self> {
        let authorizer = ApiAuthorizer::from_config(&config.api)
            .map_err(|error| error.context(ApiConfigurationFailure))?;
        let (shutdown_owner, shutdown) = shutdown_channel();
        let (storage_reader_owner, storage_reader) = start_storage_readers(&config, &store)?;
        Ok(Self::from_parts(
            ServiceParts {
                config,
                store,
                event_notifier,
                storage_writer,
                storage_writer_owner: None,
                storage_reader,
                storage_reader_owner,
                shutdown,
                shutdown_owner: Some(shutdown_owner),
                metrics: None,
            },
            authorizer,
        ))
    }

    /// Creates a service sharing capture storage and coordinated shutdown.
    pub fn new_with_runtime(
        config: Config,
        store: RedbEventStore,
        event_notifier: EventNotifier,
        storage_writer: CaptureStorageHandle,
        shutdown: ShutdownReceiver,
        metrics: Option<ProductionMetrics>,
    ) -> anyhow::Result<Self> {
        let authorizer = ApiAuthorizer::from_config(&config.api)
            .map_err(|error| error.context(ApiConfigurationFailure))?;
        let (storage_reader_owner, storage_reader) = start_storage_readers(&config, &store)?;
        Ok(Self::from_parts(
            ServiceParts {
                config,
                store,
                event_notifier,
                storage_writer,
                storage_writer_owner: None,
                storage_reader,
                storage_reader_owner,
                shutdown,
                shutdown_owner: None,
                metrics,
            },
            authorizer,
        ))
    }

    fn from_parts(parts: ServiceParts, authorizer: ApiAuthorizer) -> Self {
        #[cfg(not(test))]
        drop(parts.store);
        let delivery_capacity = parts.config.runtime.max_durable_consumers;
        Self {
            config: Arc::new(parts.config),
            #[cfg(test)]
            store: Arc::new(parts.store),
            storage_writer: parts.storage_writer,
            _storage_writer_owner: parts.storage_writer_owner,
            storage_reader: parts.storage_reader,
            _storage_reader_owner: parts.storage_reader_owner,
            shutdown: parts.shutdown,
            _shutdown_owner: parts.shutdown_owner,
            event_notifier: parts.event_notifier,
            active_subscriptions: Arc::new(Mutex::new(HashSet::new())),
            delivery_high_watermarks: Arc::new(Mutex::new(DeliveryWatermarks::new(
                delivery_capacity,
            ))),
            authorizer: Arc::new(authorizer),
            metrics: parts.metrics,
        }
    }
}

fn start_storage_readers(
    config: &Config,
    store: &RedbEventStore,
) -> anyhow::Result<(Arc<StorageReaderPool>, StorageReaderHandle)> {
    let owner = Arc::new(StorageReaderPool::start(
        store.clone(),
        config.runtime.replay_reader_threads,
        config.runtime.replay_reader_queue_capacity,
    )?);
    let handle = owner.handle();
    Ok((owner, handle))
}

/// Prevents an unauthenticated plaintext API from binding beyond loopback.
fn validate_api_binding(addr: SocketAddr, config: &ApiConfig) -> anyhow::Result<()> {
    let tls_enabled = config.tls_cert_file.is_some() && config.tls_key_file.is_some();
    if addr.ip().is_loopback() {
        if !tls_enabled && !config.allow_insecure_localhost {
            anyhow::bail!("plaintext loopback gRPC requires api.allow_insecure_localhost = true");
        }
        return Ok(());
    }
    if !tls_enabled {
        anyhow::bail!("non-loopback gRPC binding requires api TLS certificate and key files");
    }
    if config.tokens.is_empty() {
        anyhow::bail!("non-loopback gRPC binding requires at least one API bearer principal");
    }
    Ok(())
}

/// Loads the PEM identity only after binding policy has accepted the config.
fn server_tls_identity(config: &ApiConfig) -> anyhow::Result<Option<Identity>> {
    match (&config.tls_cert_file, &config.tls_key_file) {
        (Some(cert_path), Some(key_path)) => {
            let cert = fs::read(cert_path)
                .with_context(|| format!("read gRPC TLS certificate {cert_path:?}"))?;
            let key = fs::read(key_path)
                .with_context(|| format!("read gRPC TLS private key {key_path:?}"))?;
            Ok(Some(Identity::from_pem(cert, key)))
        }
        (None, None) => Ok(None),
        _ => anyhow::bail!("gRPC TLS certificate and key must be configured together"),
    }
}

/// Runs the lightcdc gRPC server until it is stopped.
pub async fn serve(addr: SocketAddr, config: Config, store: RedbEventStore) -> anyhow::Result<()> {
    validate_api_binding(addr, &config.api)
        .map_err(|error| error.context(ApiConfigurationFailure))?;
    let tls =
        server_tls_identity(&config.api).map_err(|error| error.context(ApiConfigurationFailure))?;
    let (state, state_rx) = runtime_state_channel();
    state.transition(RuntimeState::Capturing, None);
    let (_shutdown, shutdown_rx) = shutdown_channel();
    let service = LightCdcService::new(config, store)?;
    serve_service(addr, service, state_rx, shutdown_rx, tls).await
}

/// Runs the gRPC server with notifications from an in-process capture loop.
pub async fn serve_with_notifier(
    addr: SocketAddr,
    config: Config,
    store: RedbEventStore,
    event_notifier: EventNotifier,
    storage_writer: CaptureStorageHandle,
) -> anyhow::Result<()> {
    let (state, state_rx) = runtime_state_channel();
    state.transition(RuntimeState::Capturing, None);
    let (_shutdown, shutdown_rx) = shutdown_channel();
    serve_with_runtime(
        addr,
        config,
        store,
        event_notifier,
        ApiRuntime {
            storage_writer,
            state: state_rx,
            shutdown: shutdown_rx,
            metrics: None,
        },
    )
    .await
}

/// Runs gRPC with externally coordinated runtime state and graceful shutdown.
pub async fn serve_with_runtime(
    addr: SocketAddr,
    config: Config,
    store: RedbEventStore,
    event_notifier: EventNotifier,
    runtime: ApiRuntime,
) -> anyhow::Result<()> {
    validate_api_binding(addr, &config.api)
        .map_err(|error| error.context(ApiConfigurationFailure))?;
    let tls =
        server_tls_identity(&config.api).map_err(|error| error.context(ApiConfigurationFailure))?;
    let ApiRuntime {
        storage_writer,
        state,
        shutdown,
        metrics,
    } = runtime;
    let service = LightCdcService::new_with_runtime(
        config,
        store,
        event_notifier,
        storage_writer,
        shutdown.clone(),
        metrics,
    )?;
    serve_service(addr, service, state, shutdown, tls).await
}

/// Registers an already-built service with tonic and listens on the address.
async fn serve_service(
    addr: SocketAddr,
    service: LightCdcService,
    state: RuntimeStateReceiver,
    mut shutdown: ShutdownReceiver,
    tls: Option<Identity>,
) -> anyhow::Result<()> {
    info!(%addr, "starting lightcdc gRPC server");
    let max_inbound_request_bytes = service.config.runtime.max_inbound_request_bytes;
    let max_outbound_event_bytes = service.config.runtime.max_outbound_event_bytes;
    let max_requests_per_connection = service.config.runtime.max_requests_per_connection;
    let max_api_connections = service.config.runtime.max_api_connections;
    let max_header_list_bytes = service.config.runtime.max_header_list_bytes;
    let metrics = service.metrics.clone();
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    let health_task = tokio::spawn(report_health(
        health_reporter,
        state,
        shutdown.clone(),
        metrics.clone(),
    ));
    let mut server = Server::builder()
        .concurrency_limit_per_connection(max_requests_per_connection)
        .http2_max_header_list_size(max_header_list_bytes);
    if let Some(identity) = tls {
        server = server
            .tls_config(ServerTlsConfig::new().identity(identity))
            .context("configure gRPC TLS identity")?;
    }
    let result = server
        .add_service(health_service)
        .add_service(
            LightCdcServer::new(service)
                .max_decoding_message_size(max_inbound_request_bytes)
                .max_encoding_message_size(max_outbound_event_bytes.saturating_add(64 * 1024)),
        )
        .serve_with_incoming_shutdown(
            limited_incoming(addr, max_api_connections, metrics).await?,
            async move { shutdown.cancelled().await },
        )
        .await;
    health_task.abort();
    let _ = health_task.await;
    result.context("serve gRPC API")
}

async fn limited_incoming(
    addr: SocketAddr,
    maximum: usize,
    metrics: Option<ProductionMetrics>,
) -> anyhow::Result<impl tokio_stream::Stream<Item = Result<LimitedConnection, std::io::Error>>> {
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind gRPC listener {addr}"))?;
    Ok(limited_incoming_from(listener, maximum, metrics))
}

fn limited_incoming_from(
    listener: TcpListener,
    maximum: usize,
    metrics: Option<ProductionMetrics>,
) -> impl tokio_stream::Stream<Item = Result<LimitedConnection, std::io::Error>> {
    let permits = Arc::new(Semaphore::new(maximum));
    TcpListenerStream::new(listener).filter_map(move |incoming| match incoming {
        Ok(stream) => match Arc::clone(&permits).try_acquire_owned() {
            Ok(permit) => {
                if let Some(metrics) = &metrics {
                    metrics.record_api_connection_started();
                }
                Some(Ok(LimitedConnection {
                    stream,
                    _permit: permit,
                    metrics: metrics.clone(),
                }))
            }
            Err(_) => {
                if let Some(metrics) = &metrics {
                    metrics.record_api_connection_limit_rejection();
                }
                None
            }
        },
        Err(error) => Some(Err(error)),
    })
}

const LIVENESS_SERVICE: &str = "lightcdc.liveness";
const READINESS_SERVICE: &str = "lightcdc.readiness";

/// Maps runtime lifecycle transitions onto the standard gRPC health protocol.
async fn report_health(
    reporter: tonic_health::server::HealthReporter,
    mut state: RuntimeStateReceiver,
    mut shutdown: ShutdownReceiver,
    metrics: Option<ProductionMetrics>,
) {
    reporter
        .set_service_status(LIVENESS_SERVICE, ServingStatus::Serving)
        .await;
    loop {
        let status = state.current();
        let storage_ready = metrics
            .as_ref()
            .is_none_or(ProductionMetrics::storage_ready);
        let serving = if status.state == RuntimeState::Capturing && storage_ready {
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
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
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
        self.authorizer.authorize(
            request.metadata(),
            &request.get_ref().stream,
            ApiPermission::Consume,
        )?;
        let request = request.into_inner();
        let consumer = validate_consumer(
            &request.consumer,
            self.config.runtime.max_consumer_name_bytes,
        )?;
        let stream = self.stream(&request.stream)?;
        let subscription = self.claim_subscription(&stream.name, consumer)?;
        let stored_offset = self
            .storage_reader
            .consumer_offset(stream.name.clone(), consumer.to_owned())
            .await
            .map_err(|error| internal_with_metrics(&self.metrics, error))?;
        let start_offset = match stored_offset {
            Some(offset) => offset,
            None => earliest_retained_offset(&self.storage_reader)
                .await
                .map_err(|error| internal_with_metrics(&self.metrics, error))?,
        };
        let limit = request.limit as usize;
        let (tx, rx) = mpsc::channel(self.config.runtime.channel_capacity);
        let storage_reader = self.storage_reader.clone();
        let mut event_notifications = self.event_notifier.subscribe();
        let delivery_key = (stream.name.clone(), consumer.to_owned());
        let delivery_high_watermarks = Arc::clone(&self.delivery_high_watermarks);
        let mut shutdown = self.shutdown.clone();
        let max_outbound_event_bytes = self.config.runtime.max_outbound_event_bytes;
        let replay_batch_events = self.config.runtime.replay_batch_events;
        let replay_batch_max_bytes = self.config.runtime.replay_batch_max_bytes;
        let metrics = self.metrics.clone();

        // This worker owns cloned state because it can outlive the subscribe RPC.
        tokio::spawn(async move {
            let _subscription = subscription;
            let mut next_sequence = start_offset + 1;
            let mut emitted = 0usize;

            loop {
                if shutdown.is_triggered() {
                    return;
                }
                let batch = match storage_reader
                    .replay_from(next_sequence, replay_batch_events, replay_batch_max_bytes)
                    .await
                {
                    Ok(batch) => batch,
                    Err(error) => {
                        record_replay_failure(&metrics, &error);
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

                let mut events = batch.into_iter();
                while let Some(event) = events.next() {
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

                    if tx.capacity() == 0 {
                        // A stalled client must not pin the reader-pool permit or
                        // the remaining batch memory while it waits for channel
                        // space; drop the batch before blocking on reserve.
                        drop(events);
                        if let Some(metrics) = &metrics {
                            metrics.record_subscription_backpressure();
                        }
                        let permit = tokio::select! {
                            result = tx.reserve() => match result {
                                Ok(permit) => permit,
                                Err(_) => return,
                            },
                            _ = shutdown.cancelled() => return,
                        };
                        record_delivery(&delivery_high_watermarks, &delivery_key, event.sequence);
                        if let Some(metrics) = &metrics {
                            metrics.record_delivery();
                        }
                        permit.send(Ok(event.into()));
                        emitted += 1;

                        if limit > 0 && emitted >= limit {
                            return;
                        }

                        // Resume replay with a fresh reader-pool permit.
                        let batch = match storage_reader
                            .replay_from(next_sequence, replay_batch_events, replay_batch_max_bytes)
                            .await
                        {
                            Ok(batch) => batch,
                            Err(error) => {
                                record_replay_failure(&metrics, &error);
                                let _ = tx.send(Err(replay_status(error))).await;
                                return;
                            }
                        };
                        events = batch.into_iter();
                        continue;
                    }

                    let permit = tokio::select! {
                        result = tx.reserve() => match result {
                            Ok(permit) => permit,
                            Err(_) => return,
                        },
                        _ = shutdown.cancelled() => return,
                    };
                    record_delivery(&delivery_high_watermarks, &delivery_key, event.sequence);
                    if let Some(metrics) = &metrics {
                        metrics.record_delivery();
                    }
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
        self.authorizer.authorize(
            request.metadata(),
            &request.get_ref().stream,
            ApiPermission::Consume,
        )?;
        let request = request.into_inner();
        let consumer = validate_consumer(
            &request.consumer,
            self.config.runtime.max_consumer_name_bytes,
        )?;
        let stream = self.stream(&request.stream)?;
        let key = (stream.name.clone(), consumer.to_owned());
        if request.sequence == 0 {
            return Err(Status::invalid_argument(
                "sequence must be greater than zero",
            ));
        }
        let highest_delivered = delivery_high_watermarks(&self.delivery_high_watermarks)
            .get(&key)
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
            .acknowledge_consumer_offset(
                stream.name.clone(),
                consumer.to_owned(),
                request.sequence,
                self.config.runtime.max_durable_consumers,
            )
            .await
            .map_err(|error| consumer_write_status(&self.metrics, error))?;
        if let Some(metrics) = &self.metrics {
            metrics.record_acknowledgement();
        }

        Ok(Response::new(AckResponse { offset }))
    }

    /// Moves a consumer offset to earliest, latest, or an absolute sequence.
    async fn seek(&self, request: Request<SeekRequest>) -> Result<Response<SeekResponse>, Status> {
        self.authorizer.authorize(
            request.metadata(),
            &request.get_ref().stream,
            ApiPermission::Seek,
        )?;
        let request = request.into_inner();
        let consumer = validate_consumer(
            &request.consumer,
            self.config.runtime.max_consumer_name_bytes,
        )?;
        let stream = self.stream(&request.stream)?;
        let key = (stream.name.clone(), consumer.to_owned());
        if active_subscriptions(&self.active_subscriptions).contains(&key) {
            return Err(Status::failed_precondition(format!(
                "consumer {consumer:?} has an active subscription on stream {:?}; end the \
                 subscription before seeking",
                stream.name
            )));
        }
        let position =
            SeekPosition::try_from(request.position).unwrap_or(SeekPosition::Unspecified);

        let offset = match position {
            SeekPosition::Earliest => earliest_retained_offset(&self.storage_reader)
                .await
                .map_err(|error| internal_with_metrics(&self.metrics, error))?,
            SeekPosition::Latest => self
                .storage_reader
                .last_sequence()
                .await
                .map_err(|error| internal_with_metrics(&self.metrics, error))?
                .unwrap_or(0),
            SeekPosition::Absolute => request.sequence,
            SeekPosition::Unspecified => {
                return Err(Status::invalid_argument(
                    "seek position must be earliest, latest, or absolute",
                ));
            }
        };

        self.storage_writer
            .set_consumer_offset(
                stream.name.clone(),
                consumer.to_owned(),
                offset,
                self.config.runtime.max_durable_consumers,
            )
            .await
            .map_err(|error| consumer_write_status(&self.metrics, error))?;
        delivery_high_watermarks(&self.delivery_high_watermarks).remove(&key);
        if let Some(metrics) = &self.metrics {
            metrics.record_seek();
        }

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
            if let Some(metrics) = &self.metrics {
                metrics.record_subscription_limit_rejection();
            }
            return Err(Status::resource_exhausted(
                "active subscription limit reached",
            ));
        }
        drop(active);
        if let Some(metrics) = &self.metrics {
            metrics.record_subscription_started();
        }

        Ok(ActiveSubscription {
            key,
            active: Arc::clone(&self.active_subscriptions),
            metrics: self.metrics.clone(),
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
    } else if consumer.starts_with("sink:") {
        Err(Status::invalid_argument(
            "consumer names beginning with sink: are reserved for in-process sinks",
        ))
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
    high_watermarks: &Mutex<DeliveryWatermarks>,
) -> std::sync::MutexGuard<'_, DeliveryWatermarks> {
    high_watermarks
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Records the highest sequence made available to one stream consumer.
fn record_delivery(
    high_watermarks: &Mutex<DeliveryWatermarks>,
    key: &SubscriptionKey,
    sequence: u64,
) {
    delivery_high_watermarks(high_watermarks).record(key.clone(), sequence);
}

/// Returns the offset immediately before the oldest retained event.
async fn earliest_retained_offset(reader: &StorageReaderHandle) -> anyhow::Result<u64> {
    match reader.first_sequence().await? {
        Some(first_sequence) => Ok(first_sequence.saturating_sub(1)),
        None => Ok(reader.last_sequence().await?.unwrap_or(0)),
    }
}

/// Records replay failure metrics, treating an expired offset as benign rather
/// than a storage error.
fn record_replay_failure(metrics: &Option<ProductionMetrics>, error: &anyhow::Error) {
    if let Some(metrics) = metrics {
        metrics.record_replay_error();
        if !matches!(
            error.downcast_ref::<StorageError>(),
            Some(StorageError::SequenceExpired { .. })
        ) {
            metrics.record_storage_error();
        }
    }
}

/// Converts an expired replay position into an actionable consumer error.
fn replay_status(error: anyhow::Error) -> Status {
    match error.downcast_ref::<StorageError>() {
        Some(StorageError::SequenceExpired {
            requested,
            first_available,
        }) => Status::failed_precondition(format!(
            "consumer offset expired at sequence {requested}; first retained sequence is \
             {first_available}; seek to earliest or latest before subscribing again"
        )),
        _ => internal(format!("failed to read events from redb: {error}")),
    }
}

/// Converts internal errors into gRPC internal status errors.
fn internal(error: impl ToString) -> Status {
    let message = error.to_string();
    warn!(error = %message, "gRPC request failed");
    Status::internal("internal service error")
}

fn internal_with_metrics(metrics: &Option<ProductionMetrics>, error: impl ToString) -> Status {
    if let Some(metrics) = metrics {
        metrics.record_storage_error();
    }
    internal(error)
}

fn consumer_write_status(metrics: &Option<ProductionMetrics>, error: anyhow::Error) -> Status {
    if let Some(StorageError::ConsumerLimitReached { maximum }) =
        error.downcast_ref::<StorageError>()
    {
        if let Some(metrics) = metrics {
            metrics.record_durable_consumer_limit_rejection();
        }
        warn!(%error, "durable consumer limit reached");
        return Status::resource_exhausted(format!(
            "maximum durable consumer count of {maximum} reached"
        ));
    }
    internal_with_metrics(metrics, error)
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
    async fn connection_limit_rejects_excess_and_recovers_after_disconnect() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let addr = listener.local_addr().expect("listener address");
        let mut incoming = Box::pin(limited_incoming_from(listener, 1, None));

        let _first_client = TcpStream::connect(addr).await.expect("first client");
        let first_server = incoming
            .next()
            .await
            .expect("first incoming connection")
            .expect("first accepted connection");

        let _excess_client = TcpStream::connect(addr).await.expect("excess client");
        assert!(
            timeout(Duration::from_millis(25), incoming.next())
                .await
                .is_err(),
            "an excess connection must not reach tonic"
        );

        drop(first_server);
        let _next_client = TcpStream::connect(addr).await.expect("next client");
        incoming
            .next()
            .await
            .expect("next incoming connection")
            .expect("connection accepted after permit release");
    }

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

    #[test]
    fn external_consumers_cannot_use_the_internal_sink_namespace() {
        let error = validate_consumer("sink:orders-cache", 128)
            .expect_err("sink consumer namespace must be reserved");

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(error.message().contains("reserved"));
    }

    #[tokio::test]
    async fn bearer_principals_enforce_stream_and_seek_permissions() {
        let secrets = TempDir::new().expect("secrets temp dir");
        let token_file = secrets.path().join("api-token");
        fs::write(&token_file, "secret-token\n").expect("write API token");
        let service = service_with_events_config(&[event(1, "public", "orders")], |config| {
            config.api.tokens.push(lightcdc_core::ApiTokenConfig {
                name: "orders-reader".to_owned(),
                token_env: None,
                token_file: Some(token_file.display().to_string()),
                streams: vec!["orders".to_owned()],
                allow_seek: false,
            });
        });

        let missing = service
            .subscribe(Request::new(subscription("reader", 1)))
            .await
            .expect_err("missing bearer token");
        assert_eq!(missing.code(), tonic::Code::Unauthenticated);

        let mut seek_request = Request::new(SeekRequest {
            stream: "orders".to_owned(),
            consumer: "reader".to_owned(),
            position: SeekPosition::Latest as i32,
            sequence: 0,
        });
        seek_request.metadata_mut().insert(
            "authorization",
            "Bearer secret-token"
                .parse()
                .expect("authorization metadata"),
        );
        let denied = service
            .seek(seek_request)
            .await
            .expect_err("seek needs separate permission");
        assert_eq!(denied.code(), tonic::Code::PermissionDenied);

        let mut subscribe_request = Request::new(subscription("reader", 1));
        subscribe_request.metadata_mut().insert(
            "authorization",
            "Bearer secret-token"
                .parse()
                .expect("authorization metadata"),
        );
        service
            .subscribe(subscribe_request)
            .await
            .expect("authorized stream subscription");
    }

    #[test]
    fn external_binding_requires_tls_and_authentication() {
        let external: SocketAddr = "0.0.0.0:50051".parse().expect("external address");
        let loopback: SocketAddr = "127.0.0.1:50051".parse().expect("loopback address");
        let mut api = ApiConfig::default();
        assert!(validate_api_binding(external, &api).is_err());
        assert!(validate_api_binding(loopback, &api).is_err());

        api.allow_insecure_localhost = true;
        assert!(validate_api_binding(loopback, &api).is_ok());

        api.tls_cert_file = Some("server.pem".to_owned());
        api.tls_key_file = Some("server.key".to_owned());
        api.tokens.push(lightcdc_core::ApiTokenConfig {
            name: "reader".to_owned(),
            token_env: Some("TOKEN".to_owned()),
            token_file: None,
            streams: vec!["orders".to_owned()],
            allow_seek: false,
        });
        assert!(validate_api_binding(external, &api).is_ok());
    }

    #[tokio::test]
    async fn health_separates_liveness_from_capture_readiness() {
        let (state, state_rx) = runtime_state_channel();
        let (shutdown, shutdown_rx) = shutdown_channel();
        let reporter = tonic_health::server::HealthReporter::new();
        let health = tonic_health::server::HealthService::from_health_reporter(reporter.clone());
        let task = tokio::spawn(report_health(reporter, state_rx, shutdown_rx, None));
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

    #[tokio::test]
    async fn health_requires_a_successful_production_storage_sample() {
        let (state, state_rx) = runtime_state_channel();
        state.transition(RuntimeState::Capturing, None);
        let (shutdown, shutdown_rx) = shutdown_channel();
        let reporter = tonic_health::server::HealthReporter::new();
        let health = tonic_health::server::HealthService::from_health_reporter(reporter.clone());
        let metrics = ProductionMetrics::new(1_000_000, 1_000, 32, 10_000, 16, 8);
        let task = tokio::spawn(report_health(
            reporter,
            state_rx,
            shutdown_rx,
            Some(metrics),
        ));
        tokio::task::yield_now().await;

        assert_eq!(
            health_status(&health, READINESS_SERVICE).await,
            tonic_health::pb::health_check_response::ServingStatus::NotServing as i32
        );

        shutdown.trigger();
        task.await.expect("health task");
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
                    max_bytes: None,
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
                    max_bytes: None,
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
    async fn seek_bounds_durable_consumer_identities_without_blocking_existing_ones() {
        let mut service = service_with_events(&[event(1, "public", "orders")]);
        config_for_service(&mut service)
            .runtime
            .max_durable_consumers = 1;

        let seek = |consumer: &str| {
            Request::new(SeekRequest {
                stream: "orders".to_owned(),
                consumer: consumer.to_owned(),
                position: SeekPosition::Latest as i32,
                sequence: 0,
            })
        };
        service
            .seek(seek("first"))
            .await
            .expect("create first durable consumer");

        let error = service
            .seek(seek("second"))
            .await
            .expect_err("second durable consumer must be rejected");
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        assert_eq!(
            service
                .store
                .stats()
                .expect("store stats")
                .consumer_offset_count,
            1
        );

        service
            .seek(seek("first"))
            .await
            .expect("existing durable consumer remains writable");
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
        drop(subscription);
        wait_for_subscription_release(&service, "search-indexer").await;

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

    #[tokio::test]
    async fn seek_rejects_an_active_subscription_for_the_same_consumer() {
        let service = service_with_events(&[event(1, "public", "orders")]);
        let mut subscription = service
            .subscribe(Request::new(subscription("search-indexer", 0)))
            .await
            .expect("subscribe response")
            .into_inner();
        subscription
            .next()
            .await
            .expect("stream item")
            .expect("change event");

        let error = service
            .seek(Request::new(SeekRequest {
                stream: "orders".to_owned(),
                consumer: "search-indexer".to_owned(),
                position: SeekPosition::Earliest as i32,
                sequence: 0,
            }))
            .await
            .expect_err("seek while subscribed must be rejected");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.message().contains("active subscription"));

        // An unrelated consumer may still seek while this subscription is active.
        service
            .seek(Request::new(SeekRequest {
                stream: "orders".to_owned(),
                consumer: "other-indexer".to_owned(),
                position: SeekPosition::Latest as i32,
                sequence: 0,
            }))
            .await
            .expect("seek for an unrelated consumer should succeed");
    }

    #[tokio::test]
    async fn ack_rejects_a_zero_sequence() {
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
                sequence: 0,
            }))
            .await
            .expect_err("ack of sequence zero must be rejected");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(error.message().contains("greater than zero"));
    }

    #[test]
    fn delivery_watermarks_bounds_the_map_to_capacity() {
        let mut watermarks = DeliveryWatermarks::new(2);
        watermarks.record(("orders".to_owned(), "one".to_owned()), 1);
        watermarks.record(("orders".to_owned(), "two".to_owned()), 2);
        watermarks.record(("orders".to_owned(), "three".to_owned()), 3);
        assert_eq!(watermarks.highest.len(), 2);
        assert!(
            watermarks
                .get(&("orders".to_owned(), "one".to_owned()))
                .is_none()
        );

        // Touching a key keeps it in the map past capacity.
        watermarks.record(("orders".to_owned(), "two".to_owned()), 4);
        watermarks.record(("orders".to_owned(), "four".to_owned()), 5);
        assert_eq!(watermarks.highest.len(), 2);
        assert!(
            watermarks
                .get(&("orders".to_owned(), "three".to_owned()))
                .is_none()
        );
        assert_eq!(
            watermarks.get(&("orders".to_owned(), "two".to_owned())),
            Some(4)
        );
        assert_eq!(
            watermarks.get(&("orders".to_owned(), "four".to_owned())),
            Some(5)
        );

        watermarks.remove(&("orders".to_owned(), "two".to_owned()));
        assert!(
            watermarks
                .get(&("orders".to_owned(), "two".to_owned()))
                .is_none()
        );
    }

    #[tokio::test]
    async fn stalled_subscriber_releases_the_reader_pool_for_other_consumers() {
        let mut service = service_with_events(&[
            event(1, "public", "orders"),
            event(2, "public", "orders"),
        ]);
        let config = config_for_service(&mut service);
        config.runtime.channel_capacity = 1;
        config.runtime.replay_reader_threads = 1;
        config.runtime.replay_reader_queue_capacity = 1;
        config.runtime.replay_batch_events = 1;

        let stalled = service
            .subscribe(Request::new(subscription("stalled", 0)))
            .await
            .expect("stalled subscribe response")
            .into_inner();
        // Let the stalled worker take the reader-pool permit and block on a full
        // channel; it must release the permit before waiting on the client.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut reader = service
            .subscribe(Request::new(subscription("reader", 1)))
            .await
            .expect("reader subscribe response")
            .into_inner();
        let delivered = timeout(Duration::from_secs(2), reader.next())
            .await
            .expect("reader should progress while the first consumer is stalled")
            .expect("reader stream item")
            .expect("reader change event");
        assert_eq!(delivered.sequence, 1);

        drop(stalled);
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
        service_with_events_config(events, |_| {})
    }

    fn service_with_events_config(
        events: &[CoreChangeEvent],
        configure: impl FnOnce(&mut Config),
    ) -> TestService {
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
        configure(&mut config);
        TestService {
            service: LightCdcService::new(config, store).expect("start test service"),
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
                password_env: None,
                password_file: None,
                tls_mode: lightcdc_core::PostgresTlsMode::Disable,
                tls_ca_file: None,
                publication: "publication".to_owned(),
                slot: "slot".to_owned(),
            },
            runtime: RuntimeConfig {
                data_dir: "data".to_owned(),
                storage_file: "lightcdc.redb".to_owned(),
                channel_capacity: 1024,
                shutdown_timeout_ms: 10000,
                max_active_subscriptions: 1_024,
                max_durable_consumers: 10_000,
                replay_reader_threads: 2,
                replay_reader_queue_capacity: 1_024,
                replay_batch_events: 256,
                replay_batch_max_bytes: 64 * 1024 * 1024,
                max_consumer_name_bytes: 128,
                max_outbound_event_bytes: 16 * 1024 * 1024,
                max_inbound_request_bytes: 64 * 1024,
                max_requests_per_connection: 128,
                max_api_connections: 1_024,
                max_header_list_bytes: 32 * 1024,
                max_storage_bytes: 100 * 1024 * 1024 * 1024,
                min_free_disk_bytes: 1024 * 1024 * 1024,
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
                retention_max_bytes: None,
                retention_max_age_seconds: None,
                retention_check_interval_ms: 1_000,
                retention_delete_batch_size: 100_000,
            },
            logging: LoggingConfig {
                level: "info".to_owned(),
            },
            observability: lightcdc_core::ObservabilityConfig::default(),
            api: lightcdc_core::ApiConfig {
                allow_insecure_localhost: true,
                ..lightcdc_core::ApiConfig::default()
            },
            streams: vec![StreamConfig {
                name: "orders".to_owned(),
                source: "default".to_owned(),
                tables: vec!["public.orders".to_owned()],
            }],
            sinks: Vec::new(),
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
