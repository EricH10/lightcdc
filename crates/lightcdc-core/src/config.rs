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
    /// gRPC transport and authorization policy.
    #[serde(default)]
    pub api: ApiConfig,
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
    /// Login password used by PostgreSQL clients; prefer an env var or file.
    #[serde(default)]
    pub password: String,
    /// Environment variable containing the PostgreSQL password.
    #[serde(default)]
    pub password_env: Option<String>,
    /// File containing only the PostgreSQL password.
    #[serde(default)]
    pub password_file: Option<String>,
    /// PostgreSQL transport security; defaults to certificate and hostname verification.
    #[serde(default)]
    pub tls_mode: PostgresTlsMode,
    /// Optional PEM CA bundle used instead of platform roots.
    #[serde(default)]
    pub tls_ca_file: Option<String>,
    /// Operator-managed publication read by pgoutput.
    pub publication: String,
    /// Durable logical replication slot assigned to this LightCDC source.
    pub slot: String,
}

/// Supported PostgreSQL TLS modes avoid encryption without authentication.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PostgresTlsMode {
    /// Local-development opt-out for a server without TLS.
    Disable,
    /// Verify the certificate chain and configured PostgreSQL hostname.
    #[default]
    VerifyFull,
}

/// Describes local runtime settings such as storage paths and buffer sizes.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RuntimeConfig {
    /// Directory containing redb and transaction staging data.
    pub data_dir: String,
    /// redb filename inside `data_dir`.
    pub storage_file: String,
    /// Per-subscription bounded outbound event channel capacity.
    pub channel_capacity: usize,
    /// Reserved for graceful shutdown coordination; currently unused.
    pub shutdown_timeout_ms: u64,
    /// Maximum simultaneous gRPC subscriptions across all consumers.
    #[serde(default = "default_max_active_subscriptions")]
    pub max_active_subscriptions: usize,
    /// Fixed OS threads available for synchronous redb replay reads.
    #[serde(default = "default_replay_reader_threads")]
    pub replay_reader_threads: usize,
    /// Bounded queued redb reads shared by all subscriptions.
    #[serde(default = "default_replay_reader_queue_capacity")]
    pub replay_reader_queue_capacity: usize,
    /// Maximum events loaded by one reader command.
    #[serde(default = "default_replay_batch_events")]
    pub replay_batch_events: usize,
    /// Soft serialized-byte boundary for one reader command.
    #[serde(default = "default_replay_batch_max_bytes")]
    pub replay_batch_max_bytes: u64,
    /// Maximum UTF-8 bytes in a consumer identity supplied over gRPC.
    #[serde(default = "default_max_consumer_name_bytes")]
    pub max_consumer_name_bytes: usize,
    /// Maximum encoded bytes delivered in one consumer event.
    #[serde(default = "default_max_outbound_event_bytes")]
    pub max_outbound_event_bytes: usize,
    /// Maximum decoded protobuf bytes accepted for one RPC request.
    #[serde(default = "default_max_inbound_request_bytes")]
    pub max_inbound_request_bytes: usize,
    /// Maximum concurrent HTTP/2 requests accepted on one connection.
    #[serde(default = "default_max_requests_per_connection")]
    pub max_requests_per_connection: usize,
    /// Maximum simultaneous TCP connections accepted by the gRPC listener.
    #[serde(default = "default_max_api_connections")]
    pub max_api_connections: usize,
    /// Maximum HTTP/2 header-list bytes accepted per request.
    #[serde(default = "default_max_header_list_bytes")]
    pub max_header_list_bytes: u32,
    /// Hard bound across the control database, segments, and staging files.
    #[serde(default = "default_max_storage_bytes")]
    pub max_storage_bytes: u64,
    /// Free filesystem bytes reserved for recovery and PostgreSQL WAL safety.
    #[serde(default = "default_min_free_disk_bytes")]
    pub min_free_disk_bytes: u64,
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
    /// Preferred maximum event count in one sequence segment.
    #[serde(default = "default_segment_max_events")]
    pub segment_max_events: u64,
    /// Preferred maximum serialized event bytes in one sequence segment.
    #[serde(default = "default_segment_max_bytes")]
    pub segment_max_bytes: u64,
    /// Maximum time a non-empty sequence segment remains writable.
    #[serde(default = "default_segment_max_age_seconds")]
    pub segment_max_age_seconds: u64,
    /// Optional maximum number of event payloads retained locally.
    #[serde(default)]
    pub retention_max_events: Option<u64>,
    /// Optional maximum logical bytes across event segment files.
    #[serde(default)]
    pub retention_max_bytes: Option<u64>,
    /// Optional maximum age of event payloads retained locally.
    #[serde(default)]
    pub retention_max_age_seconds: Option<u64>,
    /// Delay between background retention sweeps.
    #[serde(default = "default_retention_check_interval_ms")]
    pub retention_check_interval_ms: u64,
    /// Target maximum events retired per sweep; one whole segment may exceed it.
    #[serde(default = "default_retention_delete_batch_size")]
    pub retention_delete_batch_size: usize,
}

/// Describes process logging settings.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LoggingConfig {
    /// Default tracing filter when `RUST_LOG` is absent.
    pub level: String,
}

/// Controls secure gRPC binding, TLS identity, and bearer principals.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ApiConfig {
    /// Explicitly permits plaintext unauthenticated serving on loopback only.
    #[serde(default)]
    pub allow_insecure_localhost: bool,
    /// PEM certificate chain presented by the gRPC server.
    #[serde(default)]
    pub tls_cert_file: Option<String>,
    /// PEM private key paired with `tls_cert_file`.
    #[serde(default)]
    pub tls_key_file: Option<String>,
    /// Bearer principals authorized to consume configured streams.
    #[serde(default)]
    pub tokens: Vec<ApiTokenConfig>,
}

/// Resolves one bearer token and its least-privilege stream permissions.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiTokenConfig {
    /// Stable operator-facing principal name; never treated as a credential.
    pub name: String,
    /// Environment variable containing the bearer token.
    pub token_env: Option<String>,
    /// File containing only the bearer token.
    pub token_file: Option<String>,
    /// Allowed stream names, or `*` for every configured stream.
    pub streams: Vec<String>,
    /// Separately authorizes the administrative seek operation.
    #[serde(default)]
    pub allow_seek: bool,
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
            .validate_source_password_config()
            .map_err(|reason| Error::InvalidConfig {
                path: path.display().to_string(),
                reason,
            })?;
        config
            .capture_plan()
            .map_err(|error| Error::InvalidConfig {
                path: path.display().to_string(),
                reason: error.to_string(),
            })?;
        config
            .validate_runtime()
            .map_err(|reason| Error::InvalidConfig {
                path: path.display().to_string(),
                reason,
            })?;
        config
            .validate_api()
            .map_err(|reason| Error::InvalidConfig {
                path: path.display().to_string(),
                reason,
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

    /// Rejects zero, contradictory, and ineffectively large runtime settings.
    pub fn validate_runtime(&self) -> std::result::Result<(), String> {
        let runtime = &self.runtime;
        let storage_file = Path::new(&runtime.storage_file);
        if runtime.storage_file.is_empty()
            || storage_file.is_absolute()
            || storage_file.components().count() != 1
        {
            return Err(
                "runtime.storage_file must be one relative filename without path components"
                    .to_owned(),
            );
        }
        let positive = [
            ("runtime.channel_capacity", runtime.channel_capacity as u128),
            (
                "runtime.shutdown_timeout_ms",
                runtime.shutdown_timeout_ms as u128,
            ),
            (
                "runtime.max_active_subscriptions",
                runtime.max_active_subscriptions as u128,
            ),
            (
                "runtime.replay_reader_threads",
                runtime.replay_reader_threads as u128,
            ),
            (
                "runtime.replay_reader_queue_capacity",
                runtime.replay_reader_queue_capacity as u128,
            ),
            (
                "runtime.replay_batch_events",
                runtime.replay_batch_events as u128,
            ),
            (
                "runtime.replay_batch_max_bytes",
                runtime.replay_batch_max_bytes as u128,
            ),
            (
                "runtime.max_consumer_name_bytes",
                runtime.max_consumer_name_bytes as u128,
            ),
            (
                "runtime.max_outbound_event_bytes",
                runtime.max_outbound_event_bytes as u128,
            ),
            (
                "runtime.max_inbound_request_bytes",
                runtime.max_inbound_request_bytes as u128,
            ),
            (
                "runtime.max_requests_per_connection",
                runtime.max_requests_per_connection as u128,
            ),
            (
                "runtime.max_api_connections",
                runtime.max_api_connections as u128,
            ),
            (
                "runtime.max_header_list_bytes",
                runtime.max_header_list_bytes as u128,
            ),
            (
                "runtime.max_storage_bytes",
                runtime.max_storage_bytes as u128,
            ),
            (
                "runtime.min_free_disk_bytes",
                runtime.min_free_disk_bytes as u128,
            ),
            (
                "runtime.heartbeat_interval_ms",
                runtime.heartbeat_interval_ms as u128,
            ),
            (
                "runtime.transaction_memory_threshold_bytes",
                runtime.transaction_memory_threshold_bytes as u128,
            ),
            (
                "runtime.max_transaction_bytes",
                runtime.max_transaction_bytes as u128,
            ),
            (
                "runtime.max_transaction_events",
                runtime.max_transaction_events as u128,
            ),
            (
                "runtime.capture_batch_max_transactions",
                runtime.capture_batch_max_transactions as u128,
            ),
            (
                "runtime.capture_batch_max_events",
                runtime.capture_batch_max_events as u128,
            ),
            (
                "runtime.capture_batch_max_bytes",
                runtime.capture_batch_max_bytes as u128,
            ),
            (
                "runtime.capture_batch_max_delay_ms",
                runtime.capture_batch_max_delay_ms as u128,
            ),
            (
                "runtime.segment_max_events",
                runtime.segment_max_events as u128,
            ),
            (
                "runtime.segment_max_bytes",
                runtime.segment_max_bytes as u128,
            ),
            (
                "runtime.segment_max_age_seconds",
                runtime.segment_max_age_seconds as u128,
            ),
            (
                "runtime.retention_check_interval_ms",
                runtime.retention_check_interval_ms as u128,
            ),
            (
                "runtime.retention_delete_batch_size",
                runtime.retention_delete_batch_size as u128,
            ),
        ];
        if let Some((name, _)) = positive.into_iter().find(|(_, value)| *value == 0) {
            return Err(format!("{name} must be greater than zero"));
        }
        if runtime.channel_capacity > 65_536 {
            return Err("runtime.channel_capacity must not exceed 65536".to_owned());
        }
        if runtime.max_active_subscriptions > 100_000 {
            return Err("runtime.max_active_subscriptions must not exceed 100000".to_owned());
        }
        if runtime.replay_reader_threads > 64 {
            return Err("runtime.replay_reader_threads must not exceed 64".to_owned());
        }
        if runtime.replay_reader_queue_capacity > 65_536 {
            return Err("runtime.replay_reader_queue_capacity must not exceed 65536".to_owned());
        }
        if runtime.replay_batch_events > 4_096 {
            return Err("runtime.replay_batch_events must not exceed 4096".to_owned());
        }
        if runtime.replay_batch_max_bytes > 1024 * 1024 * 1024 {
            return Err("runtime.replay_batch_max_bytes must not exceed 1 GiB".to_owned());
        }
        if runtime.max_inbound_request_bytes > 16 * 1024 * 1024 {
            return Err("runtime.max_inbound_request_bytes must not exceed 16 MiB".to_owned());
        }
        if runtime.max_requests_per_connection > 100_000 {
            return Err("runtime.max_requests_per_connection must not exceed 100000".to_owned());
        }
        if runtime.max_api_connections > 100_000 {
            return Err("runtime.max_api_connections must not exceed 100000".to_owned());
        }
        if runtime.max_header_list_bytes > 1024 * 1024 {
            return Err("runtime.max_header_list_bytes must not exceed 1 MiB".to_owned());
        }
        if runtime.max_consumer_name_bytes > 1_024 {
            return Err("runtime.max_consumer_name_bytes must not exceed 1024".to_owned());
        }
        if runtime.transaction_memory_threshold_bytes > runtime.max_transaction_bytes {
            return Err(
                "runtime.transaction_memory_threshold_bytes must not exceed runtime.max_transaction_bytes"
                    .to_owned(),
            );
        }
        if runtime.max_storage_bytes <= runtime.segment_max_bytes {
            return Err(
                "runtime.max_storage_bytes must be greater than runtime.segment_max_bytes"
                    .to_owned(),
            );
        }
        if runtime.max_storage_bytes <= runtime.max_transaction_bytes {
            return Err(
                "runtime.max_storage_bytes must be greater than runtime.max_transaction_bytes"
                    .to_owned(),
            );
        }
        if runtime
            .retention_max_bytes
            .is_some_and(|maximum| maximum > runtime.max_storage_bytes)
        {
            return Err(
                "runtime.retention_max_bytes must not exceed runtime.max_storage_bytes".to_owned(),
            );
        }
        for (name, limit) in [
            ("runtime.retention_max_events", runtime.retention_max_events),
            ("runtime.retention_max_bytes", runtime.retention_max_bytes),
            (
                "runtime.retention_max_age_seconds",
                runtime.retention_max_age_seconds,
            ),
        ] {
            if limit == Some(0) {
                return Err(format!("{name} must be greater than zero when configured"));
            }
        }
        Ok(())
    }

    fn validate_source_password_config(&self) -> std::result::Result<(), String> {
        let direct = !self.source.password.is_empty();
        let selected = usize::from(direct)
            + usize::from(self.source.password_env.is_some())
            + usize::from(self.source.password_file.is_some());
        if selected != 1 {
            return Err(
                "configure exactly one of source.password, source.password_env, or source.password_file"
                    .to_owned(),
            );
        }
        Ok(())
    }

    /// Resolves the PostgreSQL password only for commands that connect upstream.
    pub fn resolve_source_password(&mut self) -> std::result::Result<(), String> {
        self.validate_source_password_config()?;
        let direct = (!self.source.password.is_empty()).then_some(self.source.password.clone());
        self.source.password = match (
            direct,
            &self.source.password_env,
            &self.source.password_file,
        ) {
            (Some(password), None, None) => password,
            (None, Some(variable), None) => std::env::var(variable).map_err(|_| {
                format!("PostgreSQL password environment variable {variable:?} is unset")
            })?,
            (None, None, Some(path)) => fs::read_to_string(path)
                .map_err(|error| {
                    format!("failed to read PostgreSQL password file {path:?}: {error}")
                })?
                .trim_end_matches(['\r', '\n'])
                .to_owned(),
            _ => unreachable!("exactly one password source was selected"),
        };
        if self.source.password.is_empty() {
            return Err("PostgreSQL password must not be empty".to_owned());
        }
        Ok(())
    }

    fn validate_api(&self) -> std::result::Result<(), String> {
        if self.api.tls_cert_file.is_some() != self.api.tls_key_file.is_some() {
            return Err(
                "api.tls_cert_file and api.tls_key_file must be configured together".to_owned(),
            );
        }
        let mut names = HashSet::new();
        for token in &self.api.tokens {
            if token.name.trim().is_empty() || !names.insert(token.name.as_str()) {
                return Err("API token principal names must be nonempty and unique".to_owned());
            }
            if token.token_env.is_some() == token.token_file.is_some() {
                return Err(format!(
                    "API principal {:?} must configure exactly one of token_env or token_file",
                    token.name
                ));
            }
            if token.streams.is_empty() {
                return Err(format!(
                    "API principal {:?} must allow at least one stream",
                    token.name
                ));
            }
            for stream in &token.streams {
                if stream != "*" && self.stream(stream).is_none() {
                    return Err(format!(
                        "API principal {:?} references unknown stream {stream:?}",
                        token.name
                    ));
                }
            }
        }
        Ok(())
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

fn default_max_active_subscriptions() -> usize {
    1_024
}

fn default_replay_reader_threads() -> usize {
    2
}

fn default_replay_reader_queue_capacity() -> usize {
    1_024
}

fn default_replay_batch_events() -> usize {
    256
}

fn default_replay_batch_max_bytes() -> u64 {
    64 * 1024 * 1024
}

fn default_max_consumer_name_bytes() -> usize {
    128
}

fn default_max_outbound_event_bytes() -> usize {
    16 * 1024 * 1024
}

fn default_max_inbound_request_bytes() -> usize {
    64 * 1024
}

fn default_max_requests_per_connection() -> usize {
    128
}

fn default_max_api_connections() -> usize {
    1_024
}

fn default_max_header_list_bytes() -> u32 {
    32 * 1024
}

fn default_max_storage_bytes() -> u64 {
    100 * 1024 * 1024 * 1024
}

fn default_min_free_disk_bytes() -> u64 {
    1024 * 1024 * 1024
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
    1_000
}

fn default_capture_batch_max_bytes() -> u64 {
    4 * 1024 * 1024
}

fn default_capture_batch_max_delay_ms() -> u64 {
    20
}

fn default_segment_max_events() -> u64 {
    1_000_000
}

fn default_segment_max_bytes() -> u64 {
    256 * 1024 * 1024
}

fn default_segment_max_age_seconds() -> u64 {
    15 * 60
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
    use tempfile::TempDir;

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
        assert_eq!(config.runtime.max_active_subscriptions, 1_024);
        assert_eq!(config.runtime.max_consumer_name_bytes, 128);
        assert_eq!(config.runtime.max_outbound_event_bytes, 16 * 1024 * 1024);
        assert_eq!(config.runtime.heartbeat_interval_ms, 10_000);
        assert_eq!(config.runtime.max_transaction_bytes, 1024 * 1024 * 1024);
        assert_eq!(config.runtime.max_transaction_events, 1_000_000);
        assert_eq!(config.runtime.capture_batch_max_transactions, 100);
        assert_eq!(config.runtime.capture_batch_max_events, 1_000);
        assert_eq!(config.runtime.capture_batch_max_bytes, 4 * 1024 * 1024);
        assert_eq!(config.runtime.capture_batch_max_delay_ms, 20);
        assert_eq!(config.runtime.segment_max_events, 1_000_000);
        assert_eq!(config.runtime.segment_max_bytes, 256 * 1024 * 1024);
        assert_eq!(config.runtime.segment_max_age_seconds, 15 * 60);
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

    #[test]
    fn runtime_validation_rejects_zero_and_contradictory_limits() {
        let mut config = config_with_streams(vec![StreamConfig {
            name: "orders".to_owned(),
            source: "default".to_owned(),
            tables: vec!["public.orders".to_owned()],
        }]);
        config.runtime.channel_capacity = 0;
        assert_eq!(
            config.validate_runtime(),
            Err("runtime.channel_capacity must be greater than zero".to_owned())
        );

        config.runtime.channel_capacity = 32;
        config.runtime.transaction_memory_threshold_bytes = 2_000;
        config.runtime.max_transaction_bytes = 1_000;
        assert!(
            config
                .validate_runtime()
                .expect_err("contradictory transaction limits")
                .contains("must not exceed")
        );
    }

    #[test]
    fn source_password_can_be_loaded_from_a_secret_file() {
        let temp = TempDir::new().expect("temp dir");
        let secret = temp.path().join("postgres-password");
        std::fs::write(&secret, "file-secret\n").expect("write secret");
        let mut config = config_with_streams(vec![StreamConfig {
            name: "orders".to_owned(),
            source: "default".to_owned(),
            tables: vec!["public.orders".to_owned()],
        }]);
        config.source.password.clear();
        config.source.password_file = Some(secret.display().to_string());

        config.resolve_source_password().expect("resolve password");

        assert_eq!(config.source.password, "file-secret");
    }

    #[test]
    fn offline_config_loading_does_not_require_the_postgres_secret() {
        let temp = TempDir::new().expect("temp dir");
        let path = temp.path().join("lightcdc.toml");
        let mut config = config_with_streams(vec![StreamConfig {
            name: "orders".to_owned(),
            source: "default".to_owned(),
            tables: vec!["public.orders".to_owned()],
        }]);
        config.source.password.clear();
        config.source.password_env = Some("LIGHTCDC_TEST_MISSING_PASSWORD".to_owned());
        std::fs::write(&path, toml::to_string(&config).expect("serialize config"))
            .expect("write config");

        let loaded = Config::from_path(&path).expect("load without resolving secret");

        assert!(loaded.source.password.is_empty());
        assert_eq!(
            loaded.source.password_env.as_deref(),
            Some("LIGHTCDC_TEST_MISSING_PASSWORD")
        );
    }

    #[test]
    fn api_policy_rejects_unknown_streams_and_ambiguous_token_sources() {
        let mut config = config_with_streams(vec![StreamConfig {
            name: "orders".to_owned(),
            source: "default".to_owned(),
            tables: vec!["public.orders".to_owned()],
        }]);
        config.api.tokens.push(super::ApiTokenConfig {
            name: "reader".to_owned(),
            token_env: Some("TOKEN".to_owned()),
            token_file: Some("token.file".to_owned()),
            streams: vec!["missing".to_owned()],
            allow_seek: false,
        });
        assert!(
            config
                .validate_api()
                .expect_err("ambiguous token source")
                .contains("exactly one")
        );

        config.api.tokens[0].token_file = None;
        assert!(
            config
                .validate_api()
                .expect_err("unknown stream")
                .contains("unknown stream")
        );
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
                password_env: None,
                password_file: None,
                tls_mode: super::PostgresTlsMode::Disable,
                tls_ca_file: None,
                publication: "publication".to_owned(),
                slot: "slot".to_owned(),
            },
            runtime: super::RuntimeConfig {
                data_dir: "data".to_owned(),
                storage_file: "events.redb".to_owned(),
                channel_capacity: 32,
                shutdown_timeout_ms: 1_000,
                max_active_subscriptions: 1_024,
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
            logging: super::LoggingConfig {
                level: "info".to_owned(),
            },
            api: super::ApiConfig {
                allow_insecure_localhost: true,
                ..super::ApiConfig::default()
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
