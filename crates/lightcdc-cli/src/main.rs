//! Parses the command line and dispatches into focused runtime modules.

use std::time::Duration;

use clap::Parser;

use crate::{
    cli::{CaptureOptions, Cli, Command},
    commands::{inspect, replay, serve},
};

mod capture;
mod cli;
mod commands;
mod display;
mod logging;
mod store;

/// Parses CLI arguments and dispatches to the requested command.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Capture {
            config,
            stop_after_events,
            output,
            metrics_file,
            metrics_interval_seconds,
        } => {
            capture::capture(
                config,
                CaptureOptions {
                    stop_after_events,
                    output,
                    metrics_file,
                    metrics_interval: Duration::from_secs(metrics_interval_seconds),
                },
            )
            .await
        }
        Command::Run {
            config,
            addr,
            stop_after_events,
            output,
            metrics_file,
            metrics_interval_seconds,
        } => {
            capture::run(
                config,
                addr,
                CaptureOptions {
                    stop_after_events,
                    output,
                    metrics_file,
                    metrics_interval: Duration::from_secs(metrics_interval_seconds),
                },
            )
            .await
        }
        Command::Replay {
            config,
            stream,
            from,
            limit,
            pretty,
        } => replay(config, stream, from, limit, pretty).await,
        Command::Inspect {
            config,
            limit,
            sequence,
        } => inspect(config, limit, sequence),
        Command::Serve { config, addr } => serve(config, addr).await,
    }
}
