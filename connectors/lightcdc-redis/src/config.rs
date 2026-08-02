//! Defines and validates standalone Redis connector configuration.

use std::{env, fs, path::Path, time::Duration};

use anyhow::{Context, anyhow};
use serde::Deserialize;

/// Top-level Redis connector configuration.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ConnectorConfig {
    /// LightCDC gRPC subscription settings.
    pub(crate) lightcdc: LightCdcConfig,
    /// Redis connection and progress-key settings.
    pub(crate) redis: RedisConfig,
    /// Ordered cache mapping rules.
    #[serde(default)]
    pub(crate) rules: Vec<CacheRule>,
}

/// Identifies the durable LightCDC consumer used by this connector.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct LightCdcConfig {
    pub(crate) endpoint: String,
    pub(crate) stream: String,
    pub(crate) consumer: String,
    /// Environment variable containing the LightCDC bearer token.
    pub(crate) token_env: Option<String>,
    /// File containing the LightCDC bearer token.
    pub(crate) token_file: Option<String>,
    /// Optional PEM CA bundle for a private LightCDC gRPC certificate.
    pub(crate) tls_ca_file: Option<String>,
    /// Applied events per cumulative LightCDC acknowledgement.
    #[serde(default = "default_ack_every")]
    pub(crate) ack_every: u64,
    #[serde(default = "default_reconnect_initial_ms")]
    pub(crate) reconnect_initial_ms: u64,
    #[serde(default = "default_reconnect_max_ms")]
    pub(crate) reconnect_max_ms: u64,
}

/// Locates Redis without requiring a plaintext password in the TOML file.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct RedisConfig {
    /// Direct Redis URL, suitable for local development.
    pub(crate) url: Option<String>,
    /// Environment variable containing the Redis URL in production.
    pub(crate) url_env: Option<String>,
    /// File containing the Redis URL, including credentials when required.
    pub(crate) url_file: Option<String>,
    /// Redis key storing the last atomically applied LightCDC sequence.
    pub(crate) progress_key: Option<String>,
}

/// Maps one PostgreSQL table to a Redis cache behavior.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct CacheRule {
    /// Exact table name in `schema.table` form.
    pub(crate) table: String,
    /// Cache key with `{column}` placeholders.
    pub(crate) key: String,
    /// Whether matching rows invalidate or update the cache.
    pub(crate) action: CacheAction,
    /// Optional Redis expiration for upserted values.
    pub(crate) ttl_seconds: Option<u64>,
}

/// Supported idempotent Redis cache mutations.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CacheAction {
    Invalidate,
    Upsert,
}

impl ConnectorConfig {
    /// Loads TOML and rejects settings that would create ambiguous behavior.
    pub(crate) fn from_path(path: &Path) -> anyhow::Result<Self> {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("read Redis connector config {}", path.display()))?;
        let config: Self = toml::from_str(&raw)
            .with_context(|| format!("parse Redis connector config {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    pub(crate) fn redis_url(&self) -> anyhow::Result<String> {
        match (&self.redis.url, &self.redis.url_env, &self.redis.url_file) {
            (Some(url), None, None) => Ok(url.clone()),
            (None, Some(variable), None) => env::var(variable)
                .with_context(|| format!("Redis URL environment variable {variable:?} is unset")),
            (None, None, Some(path)) => fs::read_to_string(path)
                .with_context(|| format!("read Redis URL file {path:?}"))
                .map(|value| value.trim_end_matches(['\r', '\n']).to_owned()),
            _ => Err(anyhow!(
                "configure exactly one of redis.url, redis.url_env, or redis.url_file"
            )),
        }
    }

    pub(crate) fn bearer_token(&self) -> anyhow::Result<Option<String>> {
        match (&self.lightcdc.token_env, &self.lightcdc.token_file) {
            (None, None) => Ok(None),
            (Some(variable), None) => env::var(variable)
                .with_context(|| {
                    format!("LightCDC token environment variable {variable:?} is unset")
                })
                .map(Some),
            (None, Some(path)) => fs::read_to_string(path)
                .with_context(|| format!("read LightCDC token file {path:?}"))
                .map(|value| Some(value.trim_end_matches(['\r', '\n']).to_owned())),
            (Some(_), Some(_)) => Err(anyhow!(
                "configure at most one of lightcdc.token_env or lightcdc.token_file"
            )),
        }
    }

    pub(crate) fn progress_key(&self) -> String {
        self.redis.progress_key.clone().unwrap_or_else(|| {
            format!(
                "lightcdc:redis:{}:{}:offset",
                self.lightcdc.stream, self.lightcdc.consumer
            )
        })
    }

    pub(crate) fn initial_reconnect_delay(&self) -> Duration {
        Duration::from_millis(self.lightcdc.reconnect_initial_ms)
    }

    pub(crate) fn max_reconnect_delay(&self) -> Duration {
        Duration::from_millis(self.lightcdc.reconnect_max_ms)
    }

    fn validate(&self) -> anyhow::Result<()> {
        if self.lightcdc.endpoint.trim().is_empty()
            || self.lightcdc.stream.trim().is_empty()
            || self.lightcdc.consumer.trim().is_empty()
        {
            return Err(anyhow!(
                "lightcdc endpoint, stream, and consumer must not be empty"
            ));
        }
        if self.lightcdc.reconnect_initial_ms == 0
            || self.lightcdc.reconnect_max_ms < self.lightcdc.reconnect_initial_ms
        {
            return Err(anyhow!(
                "reconnect delays must be nonzero and reconnect_max_ms must be at least reconnect_initial_ms"
            ));
        }
        if self.lightcdc.ack_every == 0 {
            return Err(anyhow!("lightcdc.ack_every must be greater than zero"));
        }
        if self.rules.is_empty() {
            return Err(anyhow!("at least one Redis cache rule is required"));
        }
        let _ = self.redis_url()?;
        if self.bearer_token()?.as_deref() == Some("") {
            return Err(anyhow!("LightCDC bearer token must not be empty"));
        }
        let progress_key = self.progress_key();
        if progress_key.is_empty() {
            return Err(anyhow!("redis.progress_key must not be empty"));
        }
        for rule in &self.rules {
            let Some((schema, table)) = rule.table.split_once('.') else {
                return Err(anyhow!(
                    "Redis rule table {:?} must use schema.table form",
                    rule.table
                ));
            };
            if schema.is_empty() || table.is_empty() || table.contains('.') {
                return Err(anyhow!(
                    "Redis rule table {:?} must use schema.table form",
                    rule.table
                ));
            }
            if rule.key.is_empty() || !rule.key.contains('{') {
                return Err(anyhow!(
                    "Redis rule for {:?} needs a key template with at least one {{column}} placeholder",
                    rule.table
                ));
            }
            if rule.ttl_seconds == Some(0) {
                return Err(anyhow!("Redis rule TTL must be greater than zero"));
            }
            if rule.action == CacheAction::Invalidate && rule.ttl_seconds.is_some() {
                return Err(anyhow!(
                    "Redis invalidate rule for {:?} cannot configure a TTL",
                    rule.table
                ));
            }
        }
        Ok(())
    }
}

fn default_ack_every() -> u64 {
    100
}

fn default_reconnect_initial_ms() -> u64 {
    250
}

fn default_reconnect_max_ms() -> u64 {
    15_000
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn derives_a_namespaced_progress_key() {
        let config: ConnectorConfig = toml::from_str(
            r#"
            [lightcdc]
            endpoint = "http://127.0.0.1:50051"
            stream = "orders"
            consumer = "redis-cache"

            [redis]
            url = "redis://127.0.0.1:6379"

            [[rules]]
            table = "public.orders"
            key = "order:{id}"
            action = "invalidate"
            "#,
        )
        .expect("config");

        config.validate().expect("valid config");
        assert_eq!(
            config.progress_key(),
            "lightcdc:redis:orders:redis-cache:offset"
        );
        assert_eq!(config.lightcdc.ack_every, 100);
    }

    #[test]
    fn loads_connector_secrets_from_files() {
        let temp = TempDir::new().expect("temp dir");
        let token_path = temp.path().join("lightcdc-token");
        let redis_path = temp.path().join("redis-url");
        fs::write(&token_path, "secret-token\n").expect("write token");
        fs::write(&redis_path, "rediss://redis.internal:6379\n").expect("write Redis URL");
        let config: ConnectorConfig = toml::from_str(&format!(
            r#"
            [lightcdc]
            endpoint = "https://lightcdc.internal:50051"
            stream = "orders"
            consumer = "redis-cache"
            token_file = {token_path:?}

            [redis]
            url_file = {redis_path:?}

            [[rules]]
            table = "public.orders"
            key = "order:{{id}}"
            action = "invalidate"
            "#,
            token_path = token_path.display().to_string(),
            redis_path = redis_path.display().to_string(),
        ))
        .expect("config");

        config.validate().expect("valid config");
        assert_eq!(
            config.bearer_token().expect("token").as_deref(),
            Some("secret-token")
        );
        assert_eq!(
            config.redis_url().expect("Redis URL"),
            "rediss://redis.internal:6379"
        );
    }
}
