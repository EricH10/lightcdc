//! Constructs built-in sink adapters from the main LightCDC configuration.

use std::time::Duration;

use anyhow::Context;
use lightcdc_core::{Config, SinkDestinationConfig};
use lightcdc_redis::RedisSink;
use lightcdc_runtime::{
    CaptureStorageHandle, EventNotifier, ShutdownReceiver, SinkRegistration, SinkRuntime,
    SinkWorkerConfig,
};
use lightcdc_storage::RedbEventStore;

pub(crate) fn start(
    config: &Config,
    store: RedbEventStore,
    storage_writer: CaptureStorageHandle,
    notifier: EventNotifier,
    shutdown: ShutdownReceiver,
) -> anyhow::Result<SinkRuntime> {
    let registrations = config
        .sinks
        .iter()
        .map(|sink| {
            let stream = config
                .stream(&sink.stream)
                .expect("sink streams were validated")
                .clone();
            let adapter: Box<dyn lightcdc_runtime::Sink> = match &sink.destination {
                SinkDestinationConfig::Redis(redis) => Box::new(
                    RedisSink::new(redis.clone())
                        .with_context(|| format!("initialize Redis sink {:?}", sink.name))?,
                ),
            };
            Ok(SinkRegistration {
                config: SinkWorkerConfig {
                    name: sink.name.clone(),
                    stream,
                    batch_max_events: sink.batch_max_events,
                    batch_max_bytes: sink.batch_max_bytes,
                    retry_initial: Duration::from_millis(sink.retry_initial_ms),
                    retry_max: Duration::from_millis(sink.retry_max_ms),
                    maximum_consumers: config.runtime.max_durable_consumers,
                },
                sink: adapter,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    SinkRuntime::start(
        store,
        config.runtime.replay_reader_threads,
        config.runtime.replay_reader_queue_capacity,
        registrations,
        storage_writer,
        notifier,
        shutdown,
    )
}
