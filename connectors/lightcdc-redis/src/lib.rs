//! Redis cache adapter for the in-process LightCDC sink runtime.

mod mapping;

use std::{env, fs, future::Future, pin::Pin, time::Duration};

use anyhow::{Context, anyhow};
use lightcdc_core::{ChangeEvent, RedisSinkConfig};
use lightcdc_runtime::{Sink, SinkDeliveryError};
use mapping::{CacheMutation, map_event};
use redis::{RetryMethod, aio::{ConnectionManager, ConnectionManagerConfig}};

/// Lazily connected Redis destination that bulk-pipelines complete event batches.
///
/// Timeouts bound every Redis interaction so a half-open connection cannot
/// stall the sink worker indefinitely; failures surface as retryable errors.
/// Connection timeout: maximum time to establish the TCP connection and handshake.
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);
/// Response timeout: maximum time for Redis to answer one pipelined batch.
/// The crate default of 500ms is too tight for large batches, and no deadline
/// would let a hung server block the sink forever.
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);

pub struct RedisSink {
    url: String,
    rules: Vec<lightcdc_core::RedisCacheRule>,
    max_commands_per_batch: usize,
    connection_config: ConnectionManagerConfig,
    connection: Option<ConnectionManager>,
}

impl RedisSink {
    pub fn new(config: RedisSinkConfig) -> anyhow::Result<Self> {
        let url = resolve_url(&config)?;
        redis::Client::open(url.as_str()).context("validate Redis sink URL")?;
        let connection_config = ConnectionManagerConfig::new()
            .set_connection_timeout(Some(CONNECTION_TIMEOUT))
            .set_response_timeout(Some(RESPONSE_TIMEOUT));
        Ok(Self {
            url,
            rules: config.rules,
            max_commands_per_batch: config.max_commands_per_batch,
            connection_config,
            connection: None,
        })
    }

    async fn deliver_batch(&mut self, events: &[ChangeEvent]) -> Result<(), SinkDeliveryError> {
        let mut mutations = Vec::new();
        for event in events {
            let mapped = map_event(&self.rules, event).map_err(|error| {
                SinkDeliveryError::Terminal(error.context("map Redis cache mutation"))
            })?;
            if mutations.len().saturating_add(mapped.len()) > self.max_commands_per_batch {
                return Err(SinkDeliveryError::Terminal(anyhow!(
                    "Redis batch expands beyond max_commands_per_batch {}",
                    self.max_commands_per_batch
                )));
            }
            mutations.extend(mapped);
        }
        if mutations.is_empty() {
            return Ok(());
        }

        if self.connection.is_none() {
            let client = redis::Client::open(self.url.as_str()).map_err(|error| {
                SinkDeliveryError::Terminal(anyhow!(error).context("invalid Redis sink URL"))
            })?;
            self.connection = Some(
                client
                    .get_connection_manager_with_config(self.connection_config.clone())
                    .await
                    .map_err(|error| classify_redis(error, "connect to Redis sink"))?,
            );
        }
        let result = apply_mutations(
            self.connection
                .as_mut()
                .expect("Redis connection was initialized"),
            &mutations,
        )
        .await;
        if matches!(result, Err(SinkDeliveryError::Retryable(_))) {
            self.connection = None;
        }
        result
    }
}

impl Sink for RedisSink {
    fn deliver<'a>(
        &'a mut self,
        events: &'a [ChangeEvent],
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkDeliveryError>> + Send + 'a>> {
        Box::pin(self.deliver_batch(events))
    }
}

fn resolve_url(config: &RedisSinkConfig) -> anyhow::Result<String> {
    match (&config.url, &config.url_env, &config.url_file) {
        (Some(url), None, None) => Ok(url.clone()),
        (None, Some(variable), None) => env::var(variable)
            .with_context(|| format!("Redis URL environment variable {variable:?} is unset")),
        (None, None, Some(path)) => fs::read_to_string(path)
            .with_context(|| format!("read Redis URL file {path:?}"))
            .map(|value| value.trim_end_matches(['\r', '\n']).to_owned()),
        _ => Err(anyhow!(
            "configure exactly one Redis sink URL, URL environment variable, or URL file"
        )),
    }
}

async fn apply_mutations(
    redis: &mut ConnectionManager,
    mutations: &[CacheMutation],
) -> Result<(), SinkDeliveryError> {
    let mut pipeline = redis::pipe();
    for mutation in mutations {
        match mutation {
            CacheMutation::Delete { key } => {
                pipeline.cmd("DEL").arg(key).ignore();
            }
            CacheMutation::Set {
                key,
                value,
                ttl_seconds,
            } => {
                let command = pipeline.cmd("SET").arg(key).arg(value);
                if let Some(ttl_seconds) = ttl_seconds {
                    command.arg("EX").arg(*ttl_seconds);
                }
                command.ignore();
            }
        }
    }
    pipeline
        .exec_async(redis)
        .await
        .map_err(|error| classify_redis(error, "apply Redis sink batch"))
}

fn classify_redis(error: redis::RedisError, action: &str) -> SinkDeliveryError {
    use redis::ErrorKind;

    let kind = error.kind();
    let retry_method = error.retry_method();
    let error = anyhow!(error).context(action.to_owned());
    if kind == ErrorKind::AuthenticationFailed || matches!(retry_method, RetryMethod::NoRetry) {
        SinkDeliveryError::Terminal(error)
    } else {
        SinkDeliveryError::Retryable(error)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use lightcdc_core::{Operation, RedisCacheAction, RedisCacheRule, SourceMetadata};
    use redis::AsyncCommands;

    use super::*;

    fn event(sequence: u64, id: &str, status: &str) -> ChangeEvent {
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
            operation: Operation::Update,
            key: Some(format!(r#"{{"id":"{id}"}}"#).into_bytes()),
            before: None,
            after: Some(format!(r#"{{"id":"{id}","status":"{status}"}}"#).into_bytes()),
            commit_timestamp_ms: None,
        }
    }

    #[tokio::test]
    #[ignore = "requires Redis on LIGHTCDC_TEST_REDIS_URL or redis://127.0.0.1:6379"]
    async fn bulk_delivery_converges_when_replayed() {
        let url = env::var("LIGHTCDC_TEST_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let prefix = format!("lightcdc:test:{suffix}:order");
        let mut sink = RedisSink::new(RedisSinkConfig {
            url: Some(url.clone()),
            url_env: None,
            url_file: None,
            max_commands_per_batch: 100,
            rules: vec![RedisCacheRule {
                table: "public.orders".to_owned(),
                key: format!("{prefix}:{{id}}"),
                action: RedisCacheAction::Upsert,
                ttl_seconds: None,
            }],
        })
        .expect("Redis sink");
        let events = [
            event(1, "1", "pending"),
            event(2, "1", "paid"),
            event(3, "2", "new"),
        ];
        sink.deliver_batch(&events).await.expect("deliver batch");
        sink.deliver_batch(&events).await.expect("replay batch");

        let client = redis::Client::open(url).expect("Redis client");
        let mut redis = client
            .get_connection_manager()
            .await
            .expect("Redis connection");
        assert_eq!(
            redis
                .get::<_, String>(format!("{prefix}:1"))
                .await
                .expect("order 1"),
            r#"{"id":"1","status":"paid"}"#
        );
        assert_eq!(
            redis
                .get::<_, String>(format!("{prefix}:2"))
                .await
                .expect("order 2"),
            r#"{"id":"2","status":"new"}"#
        );
        let _: usize = redis
            .del(&[format!("{prefix}:1"), format!("{prefix}:2")])
            .await
            .expect("cleanup");
    }
}
