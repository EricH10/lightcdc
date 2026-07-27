use std::{net::SocketAddr, sync::Arc, time::Duration};

use lightcdc_core::{ChangeEvent as CoreChangeEvent, Config, Operation, StreamConfig};
use lightcdc_storage::RedbEventStore;
use tokio::{sync::mpsc, time::sleep};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, transport::Server};
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
    config: Arc<Config>,
    store: Arc<RedbEventStore>,
    poll_interval: Duration,
}

impl LightCdcService {
    /// Creates a gRPC service from configuration and a shared event store.
    pub fn new(config: Config, store: RedbEventStore) -> Self {
        Self {
            config: Arc::new(config),
            store: Arc::new(store),
            poll_interval: Duration::from_millis(250),
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

    info!(%addr, "starting lightcdc gRPC server");
    Server::builder()
        .add_service(LightCdcServer::new(service))
        .serve(addr)
        .await
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
        let consumer = validate_consumer(&request.consumer)?;
        let stream = self.stream(&request.stream)?;
        let start_offset = self
            .store
            .consumer_offset(&stream.name, consumer)
            .map_err(internal)?
            .unwrap_or(0);
        let limit = request.limit as usize;
        let (tx, rx) = mpsc::channel(32);
        let store = Arc::clone(&self.store);
        let poll_interval = self.poll_interval;

        tokio::spawn(async move {
            let mut next_sequence = start_offset + 1;
            let mut emitted = 0usize;

            loop {
                let batch = match store.replay_from(next_sequence, 256) {
                    Ok(batch) => batch,
                    Err(error) => {
                        let _ = tx
                            .send(Err(Status::internal(format!(
                                "failed to read events from redb: {error}"
                            ))))
                            .await;
                        return;
                    }
                };

                if batch.is_empty() {
                    sleep(poll_interval).await;
                    continue;
                }

                for event in batch {
                    next_sequence = event.sequence + 1;
                    if !stream.matches_event(&event) {
                        continue;
                    }

                    if tx.send(Ok(event.into())).await.is_err() {
                        return;
                    }
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
        let consumer = validate_consumer(&request.consumer)?;
        let stream = self.stream(&request.stream)?;

        let offset = self
            .store
            .acknowledge_consumer_offset(&stream.name, consumer, request.sequence)
            .map_err(internal)?;

        Ok(Response::new(AckResponse { offset }))
    }

    /// Moves a consumer offset to earliest, latest, or an absolute sequence.
    async fn seek(&self, request: Request<SeekRequest>) -> Result<Response<SeekResponse>, Status> {
        let request = request.into_inner();
        let consumer = validate_consumer(&request.consumer)?;
        let stream = self.stream(&request.stream)?;
        let position =
            SeekPosition::try_from(request.position).unwrap_or(SeekPosition::Unspecified);

        let offset = match position {
            SeekPosition::Earliest => 0,
            SeekPosition::Latest => self.store.last_sequence().map_err(internal)?.unwrap_or(0),
            SeekPosition::Absolute => request.sequence,
            SeekPosition::Unspecified => {
                return Err(Status::invalid_argument(
                    "seek position must be earliest, latest, or absolute",
                ));
            }
        };

        self.store
            .set_consumer_offset(&stream.name, consumer, offset)
            .map_err(internal)?;

        Ok(Response::new(SeekResponse { offset }))
    }
}

impl LightCdcService {
    /// Looks up a configured stream or returns a gRPC status error.
    fn stream(&self, name: &str) -> Result<StreamConfig, Status> {
        if name.is_empty() {
            return Err(Status::invalid_argument("stream name is required"));
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
fn validate_consumer(consumer: &str) -> Result<&str, Status> {
    if consumer.is_empty() {
        Err(Status::invalid_argument("consumer name is required"))
    } else {
        Ok(consumer)
    }
}

/// Converts internal errors into gRPC internal status errors.
fn internal(error: impl ToString) -> Status {
    let message = error.to_string();
    warn!(error = %message, "gRPC request failed");
    Status::internal(message)
}

#[cfg(test)]
mod tests {
    use lightcdc_core::{
        ChangeEvent as CoreChangeEvent, LoggingConfig, Operation as CoreOperation, RuntimeConfig,
        SourceConfig, SourceMetadata, StreamConfig,
    };
    use lightcdc_storage::LogOpenOptions;
    use tempfile::TempDir;
    use tokio_stream::StreamExt;

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
    async fn ack_persists_stream_scoped_consumer_offset() {
        let service = service_with_events(&[]);

        let response = service
            .ack(Request::new(AckRequest {
                stream: "orders".to_owned(),
                consumer: "search-indexer".to_owned(),
                sequence: 42,
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
        let service = service_with_events(&[]);
        service
            .store
            .set_consumer_offset("orders", "search-indexer", 42)
            .expect("set initial consumer offset");

        let response = service
            .ack(Request::new(AckRequest {
                stream: "orders".to_owned(),
                consumer: "search-indexer".to_owned(),
                sequence: 41,
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

    fn service_with_events(events: &[CoreChangeEvent]) -> LightCdcService {
        let temp = TempDir::new().expect("temp dir");
        let store = RedbEventStore::open(&LogOpenOptions {
            data_dir: temp.path().to_path_buf(),
            database_file: "test.redb".to_owned(),
        })
        .expect("open store");

        for event in events {
            store.append_event(event).expect("append event");
        }

        let mut config = config();
        config.runtime.data_dir = temp.path().display().to_string();
        LightCdcService::new(config, store)
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
