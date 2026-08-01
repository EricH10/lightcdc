//! Starts the optional LightCDC-to-Redis cache connector.

use std::path::PathBuf;

use clap::Parser;
use tracing_subscriber::EnvFilter;

use crate::config::ConnectorConfig;

mod config;
mod mapping;
mod worker;

#[derive(Debug, Parser)]
#[command(name = "lightcdc-redis")]
#[command(about = "Apply ordered LightCDC events to Redis cache keys")]
struct Args {
    #[arg(short, long, default_value = "redis-connector.example.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init()
        .map_err(|error| anyhow::anyhow!("initialize Redis connector logging: {error}"))?;
    let config = ConnectorConfig::from_path(&args.config)?;
    worker::run(config).await
}
