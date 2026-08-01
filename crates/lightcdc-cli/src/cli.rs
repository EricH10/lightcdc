//! Defines command-line arguments without owning command execution.

use std::{net::SocketAddr, path::PathBuf, time::Duration};

use clap::{Parser, Subcommand, ValueEnum};

/// Parses the top-level lightcdc command line.
#[derive(Debug, Parser)]
#[command(name = "lightcdc")]
#[command(about = "Lightweight PostgreSQL CDC runtime")]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) command: Command,
}

/// Lists the CLI commands supported by lightcdc.
#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Captures PostgreSQL changes without starting the consumer API.
    Capture {
        #[arg(short, long, default_value = "lightcdc.example.toml")]
        config: PathBuf,

        /// Stop after at least this many durable events in a bounded run.
        #[arg(long, alias = "max-events")]
        stop_after_events: Option<usize>,

        #[arg(long, value_enum, default_value_t = CaptureOutput::None)]
        output: CaptureOutput,

        #[arg(long)]
        metrics_file: Option<PathBuf>,

        #[arg(long, default_value_t = 1)]
        metrics_interval_seconds: u64,
    },

    /// Runs capture and the consumer API against one shared segmented store.
    Run {
        #[arg(short, long, default_value = "lightcdc.example.toml")]
        config: PathBuf,

        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: SocketAddr,

        /// Stop after at least this many durable events in a bounded run.
        #[arg(long, alias = "max-events")]
        stop_after_events: Option<usize>,

        #[arg(long, value_enum, default_value_t = CaptureOutput::None)]
        output: CaptureOutput,

        #[arg(long)]
        metrics_file: Option<PathBuf>,

        #[arg(long, default_value_t = 1)]
        metrics_interval_seconds: u64,
    },

    /// Prints retained events from a local sequence, optionally by stream.
    Replay {
        #[arg(short, long, default_value = "lightcdc.example.toml")]
        config: PathBuf,

        #[arg(long)]
        stream: Option<String>,

        #[arg(long, default_value_t = 1)]
        from: u64,

        #[arg(long, default_value_t = 100)]
        limit: usize,

        #[arg(long)]
        pretty: bool,
    },

    /// Displays redb table counts, offsets, and recent events.
    Inspect {
        #[arg(short, long, default_value = "lightcdc.example.toml")]
        config: PathBuf,

        #[arg(long, default_value_t = 20)]
        limit: usize,

        #[arg(long)]
        sequence: Option<u64>,
    },

    /// Serves retained events without running PostgreSQL capture.
    Serve {
        #[arg(short, long, default_value = "lightcdc.example.toml")]
        config: PathBuf,

        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: SocketAddr,
    },
}

/// Controls whether capture writes each durable event to standard output.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub(crate) enum CaptureOutput {
    Json,
    None,
}

/// Holds optional capture behavior used by bounded runs and benchmarks.
pub(crate) struct CaptureOptions {
    /// Stops after at least this many events are durable; the final source
    /// transaction remains whole, so the count can exceed the requested target.
    pub(crate) stop_after_events: Option<usize>,
    pub(crate) output: CaptureOutput,
    pub(crate) metrics_file: Option<PathBuf>,
    pub(crate) metrics_interval: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capture_and_run_default_to_no_payload_output() {
        let capture = Cli::try_parse_from(["lightcdc", "capture"]).expect("capture CLI");
        let run = Cli::try_parse_from(["lightcdc", "run"]).expect("run CLI");

        assert!(matches!(
            capture.command,
            Command::Capture {
                output: CaptureOutput::None,
                ..
            }
        ));
        assert!(matches!(
            run.command,
            Command::Run {
                output: CaptureOutput::None,
                ..
            }
        ));
    }
}
