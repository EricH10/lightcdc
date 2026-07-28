use std::{fs, path::Path};

use serde::{Deserialize, Serialize};

use crate::{ChangeEvent, Error, Result};

/// Holds all user-configurable settings loaded from TOML.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub source: SourceConfig,
    pub runtime: RuntimeConfig,
    pub logging: LoggingConfig,
    #[serde(default = "default_streams")]
    pub streams: Vec<StreamConfig>,
}

/// Describes the PostgreSQL source used for logical replication.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SourceConfig {
    #[serde(default = "default_source_name")]
    pub name: String,
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    pub publication: String,
    pub slot: String,
}

/// Describes local runtime settings such as storage paths and buffer sizes.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RuntimeConfig {
    pub data_dir: String,
    pub storage_file: String,
    pub channel_capacity: usize,
    pub shutdown_timeout_ms: u64,
    #[serde(default = "default_transaction_memory_threshold_bytes")]
    pub transaction_memory_threshold_bytes: u64,
    #[serde(default = "default_max_transaction_bytes")]
    pub max_transaction_bytes: u64,
    #[serde(default = "default_max_transaction_events")]
    pub max_transaction_events: usize,
}

/// Describes process logging settings.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LoggingConfig {
    pub level: String,
}

/// Names a consumable stream and the tables it includes.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StreamConfig {
    pub name: String,
    #[serde(default = "default_stream_source")]
    pub source: String,
    #[serde(default = "default_stream_tables")]
    pub tables: Vec<String>,
}

impl Config {
    /// Loads and parses a TOML configuration file.
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path).map_err(|source| Error::ReadConfig {
            path: path.display().to_string(),
            source,
        })?;

        toml::from_str(&raw).map_err(|source| Error::ParseConfig {
            path: path.display().to_string(),
            source,
        })
    }

    /// Finds a configured stream by name.
    pub fn stream(&self, name: &str) -> Option<&StreamConfig> {
        self.streams.iter().find(|stream| stream.name == name)
    }
}

impl SourceConfig {
    /// Builds a PostgreSQL connection string for client libraries.
    pub fn connection_string(&self) -> String {
        format!(
            "host={} port={} dbname={} user={} password={}",
            self.host, self.port, self.database, self.user, self.password
        )
    }

    /// Builds a PostgreSQL connection string that hides the password.
    pub fn redacted_connection_string(&self) -> String {
        format!(
            "host={} port={} dbname={} user={} password=<redacted>",
            self.host, self.port, self.database, self.user
        )
    }
}

impl StreamConfig {
    /// Returns true when a change event belongs to this stream.
    pub fn matches_event(&self, event: &ChangeEvent) -> bool {
        self.matches_table(&event.schema, &event.table)
    }

    /// Returns true when a schema and table match this stream's table patterns.
    pub fn matches_table(&self, schema: &str, table: &str) -> bool {
        let qualified = format!("{schema}.{table}");

        self.tables.iter().any(|pattern| {
            pattern == "*"
                || pattern == &qualified
                || pattern
                    .strip_suffix(".*")
                    .is_some_and(|prefix| prefix == schema)
        })
    }
}

fn default_streams() -> Vec<StreamConfig> {
    vec![StreamConfig {
        name: "all".to_owned(),
        source: default_stream_source(),
        tables: default_stream_tables(),
    }]
}

fn default_stream_source() -> String {
    "default".to_owned()
}

fn default_source_name() -> String {
    "default".to_owned()
}

fn default_stream_tables() -> Vec<String> {
    vec!["*".to_owned()]
}

fn default_transaction_memory_threshold_bytes() -> u64 {
    16 * 1024 * 1024
}

fn default_max_transaction_bytes() -> u64 {
    1024 * 1024 * 1024
}

fn default_max_transaction_events() -> usize {
    1_000_000
}

#[cfg(test)]
mod tests {
    use crate::{Operation, SourceMetadata};

    use super::StreamConfig;

    #[test]
    fn transaction_limits_have_bounded_defaults() {
        let config: super::Config = toml::from_str(
            r#"
            [source]
            host = "localhost"
            port = 5432
            database = "lightcdc"
            user = "lightcdc"
            password = "secret"
            publication = "publication"
            slot = "slot"

            [runtime]
            data_dir = "data"
            storage_file = "events.redb"
            channel_capacity = 32
            shutdown_timeout_ms = 1000

            [logging]
            level = "info"
            "#,
        )
        .expect("config");

        assert_eq!(
            config.runtime.transaction_memory_threshold_bytes,
            16 * 1024 * 1024
        );
        assert_eq!(config.runtime.max_transaction_bytes, 1024 * 1024 * 1024);
        assert_eq!(config.runtime.max_transaction_events, 1_000_000);
    }

    #[test]
    fn stream_matches_exact_qualified_table() {
        let stream = StreamConfig {
            name: "orders".to_owned(),
            source: "default".to_owned(),
            tables: vec!["public.orders".to_owned()],
        };

        assert!(stream.matches_event(&event("public", "orders")));
        assert!(!stream.matches_event(&event("public", "customers")));
    }

    #[test]
    fn stream_matches_schema_wildcard() {
        let stream = StreamConfig {
            name: "public".to_owned(),
            source: "default".to_owned(),
            tables: vec!["public.*".to_owned()],
        };

        assert!(stream.matches_event(&event("public", "orders")));
        assert!(!stream.matches_event(&event("internal", "orders")));
    }

    fn event(schema: &str, table: &str) -> crate::ChangeEvent {
        crate::ChangeEvent {
            sequence: 1,
            event_id: "event".to_owned(),
            source: SourceMetadata {
                database: "lightcdc".to_owned(),
                slot: "slot".to_owned(),
                lsn: "0/1".to_owned(),
            },
            transaction: None,
            schema: schema.to_owned(),
            table: table.to_owned(),
            operation: Operation::Insert,
            key: None,
            before: None,
            after: None,
            commit_timestamp_ms: None,
        }
    }
}
