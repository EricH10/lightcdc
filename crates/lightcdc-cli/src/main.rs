//! Parses the command line and dispatches into focused runtime modules.

use std::{process::ExitCode, time::Duration};

use clap::Parser;

use crate::{
    cli::{CaptureOptions, Cli, Command},
    commands::{inspect, replay, serve},
    maintenance::{backup, check, restore},
};

mod capture;
mod cli;
mod commands;
mod display;
mod failure;
mod logging;
mod maintenance;
mod observability;
mod store;

/// Parses CLI arguments and dispatches to the requested command.
#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match dispatch(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let class = failure::classify(&error);
            eprintln!("lightcdc: {} failure: {error:#}", class.label());
            class.code()
        }
    }
}

async fn dispatch(cli: Cli) -> anyhow::Result<()> {
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
        Command::Check { config } => check(config),
        Command::Backup { config, output } => backup(config, output),
        Command::Restore { config, input } => restore(config, input),
        Command::Serve { config, addr } => serve(config, addr).await,
    }
}
