//! Initializes process-wide tracing for CLI commands.

use anyhow::anyhow;
use tracing_subscriber::EnvFilter;

/// Initializes tracing from the environment or configured log level.
pub(crate) fn init_logging(level: &str) -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new(level))?;

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .try_init()
        .map_err(|error| anyhow!("failed to initialize tracing subscriber: {error}"))
}
