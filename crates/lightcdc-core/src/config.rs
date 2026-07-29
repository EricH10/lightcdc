//! Defines the user configuration schema and compiles stream table selections.

use std::{
    collections::{BTreeSet, HashSet},
    fs,
    path::Path,
};

use serde::{Deserialize, Serialize};
use thiserror::Error as ThisError;

use crate::{ChangeEvent, Error, Result};

/// Holds all user-configurable settings loaded from TOML.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    /// The PostgreSQL source captured by this process.
    pub source: SourceConfig,
    /// Local buffering, batching, heartbeat, and retention settings.
    pub runtime: RuntimeConfig,
    /// Process logging configuration.
    pub logging: LoggingConfig,
    /// Durable consumer-facing stream definitions.
    #[serde(default = "default_streams")]
    pub streams: Vec<StreamConfig>,
}

/// Describes the PostgreSQL source used for logical replication.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SourceConfig {
    /// Stable local identity used to scope checkpoints and staging files.
    #[serde(default = "default_source_name")]
    pub name: String,
    /// PostgreSQL server hostname.
    pub host: String,
    /// PostgreSQL server port.
    pub port: u16,
    /// Database that owns the publication and replication slot.
    pub database: String,
    /// Login role with replication privileges.
    pub user: String,
    /// Login password used by PostgreSQL clients.
    pub password: String,
    /// Operator-managed publication read by pgoutput.
    pub publication: String,
    /// Durable logical replication slot assigned to this LightCDC source.
    pub slot: String,
}

/// Describes local runtime settings such as storage paths and buffer sizes.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RuntimeConfig {
    /// Directory containing redb and transaction staging data.
    pub data_dir: String,
    /// redb filename inside `data_dir`.
    pub storage_file: String,
    /// Reserved for future pipeline channel sizing; currently unused.
    pub channel_capacity: usize,
    /// Reserved for graceful shutdown coordination; currently unused.
    pub shutdown_timeout_ms: u64,
    /// Delay between transactional logical heartbeat messages.
    #[serde(default = "default_heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u64,
    /// Decoded transaction bytes retained before spilling to disk.
    #[serde(default = "default_transaction_memory_threshold_bytes")]
    pub transaction_memory_threshold_bytes: u64,
    /// Hard decoded or staged byte limit for one source transaction.
    #[serde(default = "default_max_transaction_bytes")]
    pub max_transaction_bytes: u64,
    /// Hard event-count limit for one source transaction.
    #[serde(default = "default_max_transaction_events")]
    pub max_transaction_events: usize,
    /// Maximum complete source transactions grouped into one redb commit.
    #[serde(default = "default_capture_batch_max_transactions")]
    pub capture_batch_max_transactions: usize,
    /// Soft event boundary for one grouped redb commit.
    #[serde(default = "default_capture_batch_max_events")]
    pub capture_batch_max_events: usize,
    /// Soft decoded-byte boundary for one grouped redb commit.
    #[serde(default = "default_capture_batch_max_bytes")]
    pub capture_batch_max_bytes: u64,
    /// Maximum time the oldest transaction waits for a grouped redb commit.
    #[serde(default = "default_capture_batch_max_delay_ms")]
    pub capture_batch_max_delay_ms: u64,
    /// Optional maximum number of event payloads retained locally.
    #[serde(default)]
    pub retention_max_events: Option<u64>,
    /// Optional maximum age of event payloads retained locally.
    #[serde(default)]
    pub retention_max_age_seconds: Option<u64>,
    /// Delay between background retention sweeps.
    #[serde(default = "default_retention_check_interval_ms")]
    pub retention_check_interval_ms: u64,
    /// Maximum event prefix removed by one retention sweep.
    #[serde(default = "default_retention_delete_batch_size")]
    pub retention_delete_batch_size: usize,
}

/// Describes process logging settings.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LoggingConfig {
    /// Default tracing filter when `RUST_LOG` is absent.
    pub level: String,
}

/// Names a consumable stream and the tables it includes.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StreamConfig {
    /// Stable name clients pass to subscribe, acknowledge, and seek.
    pub name: String,
    /// Source identity whose events feed this stream.
    #[serde(default = "default_stream_source")]
    pub source: String,
    /// Included table patterns using `*`, `schema.*`, or `schema.table`.
    #[serde(default = "default_stream_tables")]
    pub tables: Vec<String>,
}

/// Compiles configured durable streams into the tables capture must preserve.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturePlan {
    source_name: String,
    patterns: BTreeSet<TablePattern>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum TablePattern {
    All,
    Schema(String),
    Exact { schema: String, table: String },
}

/// Reports invalid stream configuration while building a capture plan.
#[derive(Debug, Clone, PartialEq, Eq, ThisError)]
pub enum CapturePlanError {
    #[error("at least one stream must be configured")]
    NoStreams,

    #[error("stream name must not be empty")]
    EmptyStreamName,

    #[error("stream name {0:?} is configured more than once")]
    DuplicateStream(String),

    #[error("stream {stream:?} references source {actual:?}; expected {expected:?}")]
    UnknownSource {
        stream: String,
        actual: String,
        expected: String,
    },

    #[error("stream {0:?} must select at least one table")]
    NoTables(String),

    #[error(
        "stream {stream:?} has invalid table pattern {pattern:?}; use \"*\", \"schema.*\", or \"schema.table\""
    )]
    InvalidTablePattern { stream: String, pattern: String },
}

impl Config {
    /// Loads and parses a TOML configuration file.
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path).map_err(|source| Error::ReadConfig {
            path: path.display().to_string(),
            source,
        })?;

        let config: Self = toml::from_str(&raw).map_err(|source| Error::ParseConfig {
            path: path.display().to_string(),
            source,
        })?;
        config
            .capture_plan()
            .map_err(|error| Error::InvalidConfig {
                path: path.display().to_string(),
                reason: error.to_string(),
            })?;
        Ok(config)
    }

    /// Finds a configured stream by name.
    pub fn stream(&self, name: &str) -> Option<&StreamConfig> {
        self.streams.iter().find(|stream| stream.name == name)
    }

    /// Compiles the union of tables required by every configured durable stream.
    pub fn capture_plan(&self) -> std::result::Result<CapturePlan, CapturePlanError> {
        CapturePlan::from_streams(&self.source.name, &self.streams)
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
        self.tables
            .iter()
            .filter_map(|pattern| parse_table_pattern(pattern))
            .any(|pattern| pattern.matches(schema, table))
    }
}

impl CapturePlan {
    /// Validates streams and combines their table patterns into one source plan.
    fn from_streams(
        source_name: &str,
        streams: &[StreamConfig],
    ) -> std::result::Result<Self, CapturePlanError> {
        if streams.is_empty() {
            return Err(CapturePlanError::NoStreams);
        }

        let mut stream_names = HashSet::new();
        let mut patterns = BTreeSet::new();
        for stream in streams {
            if stream.name.trim().is_empty() {
                return Err(CapturePlanError::EmptyStreamName);
            }
            if !stream_names.insert(stream.name.as_str()) {
                return Err(CapturePlanError::DuplicateStream(stream.name.clone()));
            }
            if stream.source != source_name {
                return Err(CapturePlanError::UnknownSource {
                    stream: stream.name.clone(),
                    actual: stream.source.clone(),
                    expected: source_name.to_owned(),
                });
            }
            if stream.tables.is_empty() {
                return Err(CapturePlanError::NoTables(stream.name.clone()));
            }

            for pattern in &stream.tables {
                let pattern = parse_table_pattern(pattern).ok_or_else(|| {
                    CapturePlanError::InvalidTablePattern {
                        stream: stream.name.clone(),
                        pattern: pattern.clone(),
                    }
                })?;
                patterns.insert(pattern);
            }
        }

        Ok(Self {
            source_name: source_name.to_owned(),
            patterns,
        })
    }

    /// Creates an unrestricted plan for direct connector users.
    pub fn all(source_name: impl Into<String>) -> Self {
        Self {
            source_name: source_name.into(),
            patterns: BTreeSet::from([TablePattern::All]),
        }
    }

    /// Returns the configured source this plan belongs to.
    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    /// Returns true when this table must be retained for at least one stream.
    pub fn matches_table(&self, schema: &str, table: &str) -> bool {
        self.patterns
            .iter()
            .any(|pattern| pattern.matches(schema, table))
    }

    /// Expands wildcard patterns against current database tables.
    pub fn required_tables<'a>(
        &self,
        database_tables: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> BTreeSet<String> {
        let mut required = self
            .patterns
            .iter()
            .filter_map(|pattern| match pattern {
                TablePattern::Exact { schema, table } => Some(format!("{schema}.{table}")),
                TablePattern::All | TablePattern::Schema(_) => None,
            })
            .collect::<BTreeSet<_>>();

        required.extend(
            database_tables
                .into_iter()
                .filter(|(schema, table)| self.matches_table(schema, table))
                .map(|(schema, table)| format!("{schema}.{table}")),
        );
        required
    }
}

impl TablePattern {
    fn matches(&self, schema: &str, table: &str) -> bool {
        match self {
            Self::All => true,
            Self::Schema(expected) => expected == schema,
            Self::Exact {
                schema: expected_schema,
                table: expected_table,
            } => expected_schema == schema && expected_table == table,
        }
    }
}

/// Parses `*`, `schema.*`, or `schema.table` into a reusable matcher.
fn parse_table_pattern(pattern: &str) -> Option<TablePattern> {
    if pattern == "*" {
        return Some(TablePattern::All);
    }

    let (schema, table) = pattern.split_once('.')?;
    if schema.is_empty()
        || table.is_empty()
        || schema.contains('.')
        || table.contains('.')
        || schema.trim() != schema
        || table.trim() != table
    {
        return None;
    }

    if table == "*" {
        Some(TablePattern::Schema(schema.to_owned()))
    } else {
        Some(TablePattern::Exact {
            schema: schema.to_owned(),
            table: table.to_owned(),
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

fn default_heartbeat_interval_ms() -> u64 {
    10_000
}

fn default_max_transaction_bytes() -> u64 {
    1024 * 1024 * 1024
}

fn default_max_transaction_events() -> usize {
    1_000_000
}

fn default_capture_batch_max_transactions() -> usize {
    100
}

fn default_capture_batch_max_events() -> usize {
    500
}

fn default_capture_batch_max_bytes() -> u64 {
    4 * 1024 * 1024
}

fn default_capture_batch_max_delay_ms() -> u64 {
    20
}

fn default_retention_check_interval_ms() -> u64 {
    1_000
}

fn default_retention_delete_batch_size() -> usize {
    100_000
}

#[cfg(test)]
mod tests {
    use crate::{Operation, SourceMetadata};

    use super::{CapturePlanError, Config, StreamConfig};

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
        assert_eq!(config.runtime.heartbeat_interval_ms, 10_000);
        assert_eq!(config.runtime.max_transaction_bytes, 1024 * 1024 * 1024);
        assert_eq!(config.runtime.max_transaction_events, 1_000_000);
        assert_eq!(config.runtime.capture_batch_max_transactions, 100);
        assert_eq!(config.runtime.capture_batch_max_events, 500);
        assert_eq!(config.runtime.capture_batch_max_bytes, 4 * 1024 * 1024);
        assert_eq!(config.runtime.capture_batch_max_delay_ms, 20);
        assert_eq!(config.runtime.retention_max_events, None);
        assert_eq!(config.runtime.retention_max_age_seconds, None);
        assert_eq!(config.runtime.retention_check_interval_ms, 1_000);
        assert_eq!(config.runtime.retention_delete_batch_size, 100_000);
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

    #[test]
    fn capture_plan_unions_tables_across_streams() {
        let config = config_with_streams(vec![
            StreamConfig {
                name: "orders".to_owned(),
                source: "default".to_owned(),
                tables: vec!["public.orders".to_owned()],
            },
            StreamConfig {
                name: "search".to_owned(),
                source: "default".to_owned(),
                tables: vec!["catalog.*".to_owned(), "public.orders".to_owned()],
            },
        ]);

        let plan = config.capture_plan().expect("capture plan");

        assert!(plan.matches_table("public", "orders"));
        assert!(plan.matches_table("catalog", "products"));
        assert!(!plan.matches_table("public", "customers"));
        assert_eq!(
            plan.required_tables([
                ("public", "orders"),
                ("public", "customers"),
                ("catalog", "products"),
            ]),
            ["catalog.products", "public.orders"]
                .into_iter()
                .map(str::to_owned)
                .collect()
        );
    }

    #[test]
    fn capture_plan_rejects_duplicate_stream_names() {
        let config = config_with_streams(vec![
            StreamConfig {
                name: "orders".to_owned(),
                source: "default".to_owned(),
                tables: vec!["public.orders".to_owned()],
            },
            StreamConfig {
                name: "orders".to_owned(),
                source: "default".to_owned(),
                tables: vec!["public.customers".to_owned()],
            },
        ]);

        assert_eq!(
            config.capture_plan(),
            Err(CapturePlanError::DuplicateStream("orders".to_owned()))
        );
    }

    #[test]
    fn capture_plan_rejects_unknown_sources_and_malformed_patterns() {
        let unknown_source = config_with_streams(vec![StreamConfig {
            name: "orders".to_owned(),
            source: "other".to_owned(),
            tables: vec!["public.orders".to_owned()],
        }]);
        assert!(matches!(
            unknown_source.capture_plan(),
            Err(CapturePlanError::UnknownSource { .. })
        ));

        let malformed = config_with_streams(vec![StreamConfig {
            name: "orders".to_owned(),
            source: "default".to_owned(),
            tables: vec!["orders".to_owned()],
        }]);
        assert!(matches!(
            malformed.capture_plan(),
            Err(CapturePlanError::InvalidTablePattern { .. })
        ));
    }

    fn config_with_streams(streams: Vec<StreamConfig>) -> Config {
        Config {
            source: super::SourceConfig {
                name: "default".to_owned(),
                host: "localhost".to_owned(),
                port: 5432,
                database: "lightcdc".to_owned(),
                user: "lightcdc".to_owned(),
                password: "secret".to_owned(),
                publication: "publication".to_owned(),
                slot: "slot".to_owned(),
            },
            runtime: super::RuntimeConfig {
                data_dir: "data".to_owned(),
                storage_file: "events.redb".to_owned(),
                channel_capacity: 32,
                shutdown_timeout_ms: 1_000,
                heartbeat_interval_ms: 10_000,
                transaction_memory_threshold_bytes: 16 * 1024 * 1024,
                max_transaction_bytes: 1024 * 1024 * 1024,
                max_transaction_events: 1_000_000,
                capture_batch_max_transactions: 100,
                capture_batch_max_events: 500,
                capture_batch_max_bytes: 4 * 1024 * 1024,
                capture_batch_max_delay_ms: 20,
                retention_max_events: None,
                retention_max_age_seconds: None,
                retention_check_interval_ms: 1_000,
                retention_delete_batch_size: 100_000,
            },
            logging: super::LoggingConfig {
                level: "info".to_owned(),
            },
            streams,
        }
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
