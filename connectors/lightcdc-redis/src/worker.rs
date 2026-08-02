//! Runs the ordered LightCDC subscription and retry-safe Redis cache writer.

use std::time::Duration;

use anyhow::{Context, anyhow};
use lightcdc_api::proto::{AckRequest, SubscribeRequest, light_cdc_client::LightCdcClient};
use redis::{RetryMethod, aio::ConnectionManager};
use thiserror::Error;
use tonic::{
    Code, Request, Status,
    metadata::MetadataValue,
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
};
use tracing::{info, warn};

use crate::{
    config::ConnectorConfig,
    mapping::{CacheMutation, map_event},
};

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
    let bearer_token = config.bearer_token()?;
    redis::Client::open(redis_url.as_str()).context("validate Redis URL")?;
    let mut delay = config.initial_reconnect_delay();
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            signal = &mut shutdown => {
                signal?;
                info!("Redis connector received shutdown signal");
                return Ok(());
            }
            result = run_session(&config, &redis_url, bearer_token.as_deref()) => {
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
            signal = &mut shutdown => {
                signal?;
                return Ok(());
            }
            () = tokio::time::sleep(delay) => {}
        }
        delay = doubled_delay(delay, config.max_reconnect_delay());
    }
}

/// Resolves the process signals used by terminals and production supervisors.
async fn shutdown_signal() -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate = signal(SignalKind::terminate()).context("install SIGTERM handler")?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.context("listen for SIGINT")?,
            _ = terminate.recv() => {}
        }
    }

    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await.context("listen for Ctrl-C")?;

    Ok(())
}

async fn run_session(
    config: &ConnectorConfig,
    redis_url: &str,
    bearer_token: Option<&str>,
) -> Result<(), SessionError> {
    let redis_client = redis::Client::open(redis_url)
        .map_err(|error| SessionError::Terminal(anyhow!(error).context("invalid Redis URL")))?;
    let mut redis = redis_client
        .get_connection_manager()
        .await
        .map_err(|error| classify_redis(error, "connect to Redis"))?;
    let channel = connect_lightcdc(config).await?;
    let mut client = LightCdcClient::new(channel);
    let mut ack_client = client.clone();
    let mut events = client
        .subscribe(authenticated_request(
            SubscribeRequest {
                stream: config.lightcdc.stream.clone(),
                consumer: config.lightcdc.consumer.clone(),
                limit: 0,
            },
            bearer_token,
        )?)
        .await
        .map_err(|status| classify_status(status, "subscribe to LightCDC"))?
        .into_inner();
    info!(
        endpoint = %config.lightcdc.endpoint,
        stream = %config.lightcdc.stream,
        consumer = %config.lightcdc.consumer,
        "Redis connector session is ready"
    );

    let mut unacknowledged = 0_u64;
    let mut last_sequence = None;
    while let Some(event) = events
        .message()
        .await
        .map_err(|status| classify_status(status, "receive LightCDC event"))?
    {
        let mutations = map_event(&config.rules, &event)
            .map_err(|error| SessionError::Terminal(error.context("map Redis cache mutation")))?;
        apply_mutations(&mut redis, &mutations).await?;
        last_sequence = Some(event.sequence);
        unacknowledged += 1;
        if unacknowledged >= config.lightcdc.ack_every {
            acknowledge(
                &mut ack_client,
                config,
                bearer_token,
                last_sequence.expect("an event is pending acknowledgement"),
            )
            .await?;
            unacknowledged = 0;
            last_sequence = None;
        }
    }
    if let Some(sequence) = last_sequence {
        acknowledge(&mut ack_client, config, bearer_token, sequence).await?;
    }

    Err(SessionError::Transient(anyhow!(
        "LightCDC closed the subscription stream"
    )))
}

async fn acknowledge(
    client: &mut LightCdcClient<Channel>,
    config: &ConnectorConfig,
    bearer_token: Option<&str>,
    sequence: u64,
) -> Result<(), SessionError> {
    client
        .ack(authenticated_request(
            AckRequest {
                stream: config.lightcdc.stream.clone(),
                consumer: config.lightcdc.consumer.clone(),
                sequence,
            },
            bearer_token,
        )?)
        .await
        .map_err(|status| classify_status(status, "acknowledge LightCDC events"))?;
    Ok(())
}

async fn connect_lightcdc(config: &ConnectorConfig) -> Result<Channel, SessionError> {
    let mut endpoint =
        Endpoint::from_shared(config.lightcdc.endpoint.clone()).map_err(|error| {
            SessionError::Terminal(anyhow!(error).context("invalid LightCDC endpoint"))
        })?;
    if config.lightcdc.endpoint.starts_with("https://") {
        let mut tls = ClientTlsConfig::new().with_enabled_roots();
        if let Some(path) = &config.lightcdc.tls_ca_file {
            let certificate = std::fs::read(path).map_err(|error| {
                SessionError::Terminal(
                    anyhow!(error).context(format!("read LightCDC CA file {path:?}")),
                )
            })?;
            tls = tls.ca_certificate(Certificate::from_pem(certificate));
        }
        endpoint = endpoint.tls_config(tls).map_err(|error| {
            SessionError::Terminal(anyhow!(error).context("configure LightCDC TLS"))
        })?;
    } else if config.lightcdc.tls_ca_file.is_some() {
        return Err(SessionError::Terminal(anyhow!(
            "lightcdc.tls_ca_file requires an https:// endpoint"
        )));
    }
    endpoint
        .connect()
        .await
        .map_err(|error| SessionError::Transient(anyhow!(error).context("connect to LightCDC")))
}

fn authenticated_request<T>(
    message: T,
    bearer_token: Option<&str>,
) -> Result<Request<T>, SessionError> {
    let mut request = Request::new(message);
    if let Some(token) = bearer_token {
        let value = MetadataValue::try_from(format!("Bearer {token}")).map_err(|error| {
            SessionError::Terminal(anyhow!(error).context("encode bearer token"))
        })?;
        request.metadata_mut().insert("authorization", value);
    }
    Ok(request)
}

async fn apply_mutations(
    redis: &mut ConnectionManager,
    mutations: &[CacheMutation],
) -> Result<(), SessionError> {
    if mutations.is_empty() {
        return Ok(());
    }

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
        .map_err(|error| classify_redis(error, "apply Redis cache mutations"))?;
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
    let retry_method = error.retry_method();
    let error = anyhow!(error).context(action.to_owned());
    if kind == ErrorKind::AuthenticationFailed || matches!(retry_method, RetryMethod::NoRetry) {
        SessionError::Terminal(error)
    } else {
        SessionError::Transient(error)
    }
}

fn doubled_delay(delay: Duration, maximum: Duration) -> Duration {
    delay.saturating_mul(2).min(maximum)
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use redis::AsyncCommands;

    use super::*;

    #[test]
    fn adds_bearer_authorization_metadata() {
        let request = authenticated_request((), Some("connector-secret")).expect("request");

        assert_eq!(
            request
                .metadata()
                .get("authorization")
                .expect("authorization")
                .to_str()
                .expect("ASCII metadata"),
            "Bearer connector-secret"
        );
    }

    #[test]
    fn rejects_tokens_that_cannot_be_encoded_as_metadata() {
        let error = authenticated_request((), Some("line-one\nline-two"))
            .expect_err("newline is invalid metadata");

        assert!(matches!(error, SessionError::Terminal(_)));
    }

    #[tokio::test]
    #[ignore = "requires Redis on LIGHTCDC_TEST_REDIS_URL or redis://127.0.0.1:6379"]
    async fn redis_cache_mutations_converge_when_replayed() {
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
        let old_key = format!("lightcdc:test:{suffix}:old");
        let new_key = format!("lightcdc:test:{suffix}:new");
        redis
            .set::<_, _, ()>(&old_key, "stale")
            .await
            .expect("seed old cache key");
        let update = [
            CacheMutation::Delete {
                key: old_key.clone(),
            },
            CacheMutation::Set {
                key: new_key.clone(),
                value: br#"{"id":"1"}"#.to_vec(),
                ttl_seconds: None,
            },
        ];

        apply_mutations(&mut redis, &update)
            .await
            .expect("apply cache update");
        apply_mutations(&mut redis, &update)
            .await
            .expect("replay cache update");
        assert!(
            !redis
                .exists::<_, bool>(&old_key)
                .await
                .expect("old cache key deleted")
        );
        assert_eq!(
            redis
                .get::<_, Vec<u8>>(&new_key)
                .await
                .expect("new cache value"),
            br#"{"id":"1"}"#
        );

        let _: usize = redis
            .del(&[old_key.as_str(), new_key.as_str()])
            .await
            .expect("cleanup keys");
    }
}
