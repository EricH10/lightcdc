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

/// Describes the configured database and connector-specific replication settings.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceConfig {
    Postgres(PostgresSourceConfig),
    Mysql(MySqlSourceConfig),
}

/// Describes a PostgreSQL source used for logical replication.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PostgresSourceConfig {
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

/// Describes a MySQL source used for row-based binlog replication.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MySqlSourceConfig {
    #[serde(default = "default_source_name")]
    pub name: String,
    pub host: String,
    #[serde(default = "default_mysql_port")]
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    pub server_id: u32,
}

/// Describes local runtime settings such as storage paths and buffer sizes.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RuntimeConfig {
    pub data_dir: String,
    pub storage_file: String,
    pub channel_capacity: usize,
    pub shutdown_timeout_ms: u64,
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
    /// Returns the stable name used to scope this source's durable checkpoint.
    pub fn name(&self) -> &str {
        match self {
            Self::Postgres(source) => &source.name,
            Self::Mysql(source) => &source.name,
        }
    }

    /// Builds a connection description that hides the password.
    pub fn redacted_connection_string(&self) -> String {
        match self {
            Self::Postgres(source) => source.redacted_connection_string(),
            Self::Mysql(source) => source.redacted_connection_string(),
        }
    }
}

impl PostgresSourceConfig {
    /// Builds a PostgreSQL connection string for client libraries.
    pub fn connection_string(&self) -> String {
        format!(
            "host={} port={} dbname={} user={} password={}",
            self.host, self.port, self.database, self.user, self.password
        )
    }
}

impl PostgresSourceConfig {
    /// Builds a PostgreSQL connection description that hides the password.
    pub fn redacted_connection_string(&self) -> String {
        format!(
            "host={} port={} dbname={} user={} password=<redacted>",
            self.host, self.port, self.database, self.user
        )
    }
}

impl MySqlSourceConfig {
    /// Builds a MySQL connection description that hides the password.
    pub fn redacted_connection_string(&self) -> String {
        format!(
            "mysql://{}:<redacted>@{}:{}/{}",
            self.user, self.host, self.port, self.database
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

fn default_mysql_port() -> u16 {
    3306
}

fn default_stream_tables() -> Vec<String> {
    vec!["*".to_owned()]
}

#[cfg(test)]
mod tests {
    use crate::{Operation, SourceMetadata};

    use super::{Config, SourceConfig, StreamConfig};

    #[test]
    fn parses_postgres_source_config() {
        let config: Config = toml::from_str(
            r#"
            [source]
            type = "postgres"
            name = "default"
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
        .expect("postgres config");

        assert!(matches!(config.source, SourceConfig::Postgres(_)));
        assert_eq!(config.source.name(), "default");
    }

    #[test]
    fn parses_mysql_source_config_with_default_port() {
        let config: Config = toml::from_str(
            r#"
            [source]
            type = "mysql"
            name = "mysql"
            host = "localhost"
            database = "lightcdc"
            user = "lightcdc"
            password = "secret"
            server_id = 5401

            [runtime]
            data_dir = "data"
            storage_file = "events.redb"
            channel_capacity = 32
            shutdown_timeout_ms = 1000

            [logging]
            level = "info"
            "#,
        )
        .expect("mysql config");

        let SourceConfig::Mysql(source) = config.source else {
            panic!("expected mysql source");
        };
        assert_eq!(source.port, 3306);
        assert_eq!(source.server_id, 5401);
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
