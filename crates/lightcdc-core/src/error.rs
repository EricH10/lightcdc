use thiserror::Error as ThisError;

/// Core crate result type.
pub type Result<T> = std::result::Result<T, Error>;

/// Represents failures while loading core configuration.
#[derive(Debug, ThisError)]
pub enum Error {
    #[error("failed to read config at {path}: {source}")]
    ReadConfig {
        path: String,
        source: std::io::Error,
    },

    #[error("failed to parse config at {path}: {source}")]
    ParseConfig {
        path: String,
        source: toml::de::Error,
    },
}
