//! Runs the ordered LightCDC subscription and atomic Redis cache writer.

use std::time::Duration;

use anyhow::{Context, anyhow};
use lightcdc_api::proto::{
    AckRequest, SeekPosition, SeekRequest, SubscribeRequest, light_cdc_client::LightCdcClient,
};
use redis::{AsyncCommands, Script, aio::ConnectionManager};
use thiserror::Error;
use tonic::{Code, Status};
use tracing::{info, warn};

use crate::{
    config::ConnectorConfig,
    mapping::{CacheMutation, map_event},
};

const APPLY_SCRIPT: &str = r#"
local current = redis.call('GET', KEYS[1]) or '00000000000000000000'
local incoming = ARGV[1]
if current >= incoming then
  return 0
end

local operation_count = tonumber(ARGV[2])
for index = 1, operation_count do
  local argument = 3 + ((index - 1) * 3)
  local action = ARGV[argument]
  local value = ARGV[argument + 1]
  local ttl = tonumber(ARGV[argument + 2])
  local key = KEYS[index + 1]
  if action == 'delete' then
    redis.call('DEL', key)
  elseif ttl > 0 then
    redis.call('SET', key, value, 'EX', ttl)
  else
    redis.call('SET', key, value)
  end
end

redis.call('SET', KEYS[1], incoming)
return 1
"#;

#[derive(Debug, Error)]
pub(crate) enum SessionError {
    #[error("transient connector failure: {0:#}")]
    Transient(anyhow::Error),
    #[error("terminal connector failure: {0:#}")]
    Terminal(anyhow::Error),
}

/// Reconnects transient Redis and gRPC failures until shutdown is requested.
pub(crate) async fn run(config: ConnectorConfig) -> anyhow::Result<()> {
    let redis_url = config.redis_url()?;
    redis::Client::open(redis_url.as_str()).context("validate Redis URL")?;
    let mut delay = config.initial_reconnect_delay();

    loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal.context("listen for shutdown signal")?;
                info!("Redis connector received shutdown signal");
                return Ok(());
            }
            result = run_session(&config, &redis_url) => {
                match result {
                    Ok(()) => return Err(anyhow!("LightCDC subscription ended unexpectedly")),
                    Err(SessionError::Terminal(error)) => return Err(error),
                    Err(SessionError::Transient(error)) => {
                        warn!(%error, retry_in_ms = delay.as_millis(), "Redis connector session failed; reconnecting");
                    }
                }
            }
        }

        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal.context("listen for shutdown signal")?;
                return Ok(());
            }
            () = tokio::time::sleep(delay) => {}
        }
        delay = doubled_delay(delay, config.max_reconnect_delay());
    }
}

async fn run_session(config: &ConnectorConfig, redis_url: &str) -> Result<(), SessionError> {
    let redis_client = redis::Client::open(redis_url)
        .map_err(|error| SessionError::Terminal(anyhow!(error).context("invalid Redis URL")))?;
    let mut redis = redis_client
        .get_connection_manager()
        .await
        .map_err(|error| classify_redis(error, "connect to Redis"))?;
    let progress_key = config.progress_key();
    let redis_offset = read_progress(&mut redis, &progress_key).await?;

    let mut client = LightCdcClient::connect(config.lightcdc.endpoint.clone())
        .await
        .map_err(|error| SessionError::Transient(anyhow!(error).context("connect to LightCDC")))?;
    let mut ack_client = client.clone();
    client
        .seek(SeekRequest {
            stream: config.lightcdc.stream.clone(),
            consumer: config.lightcdc.consumer.clone(),
            position: SeekPosition::Absolute as i32,
            sequence: redis_offset,
        })
        .await
        .map_err(|status| classify_status(status, "reconcile LightCDC with Redis progress"))?;
    let mut events = client
        .subscribe(SubscribeRequest {
            stream: config.lightcdc.stream.clone(),
            consumer: config.lightcdc.consumer.clone(),
            limit: 0,
        })
        .await
        .map_err(|status| classify_status(status, "subscribe to LightCDC"))?
        .into_inner();

    info!(
        endpoint = %config.lightcdc.endpoint,
        stream = %config.lightcdc.stream,
        consumer = %config.lightcdc.consumer,
        redis_offset,
        "Redis connector session is ready"
    );

    while let Some(event) = events
        .message()
        .await
        .map_err(|status| classify_status(status, "receive LightCDC event"))?
    {
        let mutations = map_event(&config.rules, &event)
            .map_err(|error| SessionError::Terminal(error.context("map Redis cache mutation")))?;
        apply_event(&mut redis, &progress_key, event.sequence, &mutations).await?;
        ack_client
            .ack(AckRequest {
                stream: config.lightcdc.stream.clone(),
                consumer: config.lightcdc.consumer.clone(),
                sequence: event.sequence,
            })
            .await
            .map_err(|status| classify_status(status, "acknowledge LightCDC event"))?;
    }

    Err(SessionError::Transient(anyhow!(
        "LightCDC closed the subscription stream"
    )))
}

async fn read_progress(
    redis: &mut ConnectionManager,
    progress_key: &str,
) -> Result<u64, SessionError> {
    let stored: Option<String> = redis
        .get(progress_key)
        .await
        .map_err(|error| classify_redis(error, "read Redis progress"))?;
    let Some(stored) = stored else {
        return Ok(0);
    };
    stored.parse::<u64>().map_err(|error| {
        SessionError::Terminal(anyhow!(error).context(format!(
            "Redis progress key {progress_key:?} contains invalid sequence {stored:?}"
        )))
    })
}

async fn apply_event(
    redis: &mut ConnectionManager,
    progress_key: &str,
    sequence: u64,
    mutations: &[CacheMutation],
) -> Result<(), SessionError> {
    let script = Script::new(APPLY_SCRIPT);
    let mut invocation = script.prepare_invoke();
    invocation
        .key(progress_key)
        .arg(format!("{sequence:020}"))
        .arg(mutations.len());
    for mutation in mutations {
        match mutation {
            CacheMutation::Delete { key } => {
                invocation.key(key).arg("delete").arg(&[] as &[u8]).arg(0);
            }
            CacheMutation::Set {
                key,
                value,
                ttl_seconds,
            } => {
                invocation
                    .key(key)
                    .arg("set")
                    .arg(value)
                    .arg(ttl_seconds.unwrap_or(0));
            }
        }
    }
    let _: i32 = invocation
        .invoke_async(redis)
        .await
        .map_err(|error| classify_redis(error, "atomically apply Redis event"))?;
    Ok(())
}

fn classify_status(status: Status, action: &str) -> SessionError {
    let error = anyhow!(status).context(action.to_owned());
    match error
        .downcast_ref::<Status>()
        .map(Status::code)
        .unwrap_or(Code::Unknown)
    {
        Code::Cancelled
        | Code::Unknown
        | Code::DeadlineExceeded
        | Code::ResourceExhausted
        | Code::Aborted
        | Code::Internal
        | Code::Unavailable => SessionError::Transient(error),
        _ => SessionError::Terminal(error),
    }
}

fn classify_redis(error: redis::RedisError, action: &str) -> SessionError {
    use redis::ErrorKind;

    let kind = error.kind();
    let error = anyhow!(error).context(action.to_owned());
    match kind {
        ErrorKind::AuthenticationFailed
        | ErrorKind::InvalidClientConfig
        | ErrorKind::UnexpectedReturnType => SessionError::Terminal(error),
        _ => SessionError::Transient(error),
    }
}

fn doubled_delay(delay: Duration, maximum: Duration) -> Duration {
    delay.saturating_mul(2).min(maximum)
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[tokio::test]
    #[ignore = "requires Redis on LIGHTCDC_TEST_REDIS_URL or redis://127.0.0.1:6379"]
    async fn redis_progress_and_cache_mutation_are_atomic_and_idempotent() {
        let url = std::env::var("LIGHTCDC_TEST_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());
        let client = redis::Client::open(url).expect("Redis URL");
        let mut redis = client
            .get_connection_manager()
            .await
            .expect("connect Redis");
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let progress = format!("lightcdc:test:{suffix}:offset");
        let cache_key = format!("lightcdc:test:{suffix}:cache");

        apply_event(
            &mut redis,
            &progress,
            1,
            &[CacheMutation::Set {
                key: cache_key.clone(),
                value: br#"{"id":"1"}"#.to_vec(),
                ttl_seconds: None,
            }],
        )
        .await
        .expect("apply first event");
        assert_eq!(
            redis
                .get::<_, Option<String>>(&progress)
                .await
                .expect("progress"),
            Some("00000000000000000001".to_owned())
        );

        apply_event(
            &mut redis,
            &progress,
            1,
            &[CacheMutation::Delete {
                key: cache_key.clone(),
            }],
        )
        .await
        .expect("skip duplicate event");
        assert!(
            redis
                .exists::<_, bool>(&cache_key)
                .await
                .expect("cache key survives duplicate")
        );

        apply_event(
            &mut redis,
            &progress,
            2,
            &[CacheMutation::Delete {
                key: cache_key.clone(),
            }],
        )
        .await
        .expect("apply next event");
        assert!(
            !redis
                .exists::<_, bool>(&cache_key)
                .await
                .expect("cache key deleted")
        );

        let _: usize = redis
            .del(&[progress.as_str(), cache_key.as_str()])
            .await
            .expect("cleanup keys");
    }
}
