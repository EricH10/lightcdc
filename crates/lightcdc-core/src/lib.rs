//! Shares configuration and canonical event types across LightCDC crates.

/// Configuration loading and stream matching.
pub mod config;
/// Shared error types for the core crate.
pub mod error;
/// Shared CDC event types.
pub mod event;

pub use config::{
    ApiConfig, ApiTokenConfig, CapturePlan, CapturePlanError, Config, LoggingConfig,
    ObservabilityConfig, PostgresTlsMode, RedisCacheAction, RedisCacheRule, RedisSinkConfig,
    RuntimeConfig, SinkConfig, SinkDestinationConfig, SourceConfig, StreamConfig,
};
pub use error::{Error, Result};
pub use event::{ChangeEvent, Operation, SourceMetadata, TransactionMetadata};
