use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use clap::Parser;
use serde_json::Value;

const CSV_HEADER: &str = "run_id,target_tps,rows_per_transaction,target_events_per_second,ack_every,actual_tps,actual_events_per_second,generated_events,captured_events,consumed_events,source_transactions,storage_commits,events_per_storage_commit,transactions_per_storage_commit,acknowledgements,max_capture_events_per_second,max_consumer_events_per_second,max_persist_p95_ms,max_end_to_end_p95_ms,max_ack_p95_ms,max_retained_wal_bytes,dropped_samples";

/// Summarizes one benchmark result directory as a CSV row.
#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    result_dir: PathBuf,

    #[arg(long)]
    header: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let summary = BenchmarkSummary::read(&args.result_dir)?;
    if args.header {
        println!("{CSV_HEADER}");
    }
    println!("{}", summary.csv_row());
    Ok(())
}

#[derive(Debug)]
struct BenchmarkSummary {
    run_id: String,
    target_tps: u64,
    rows_per_transaction: u64,
    ack_every: u64,
    actual_tps: f64,
    generated_events: u64,
    captured_events: u64,
    consumed_events: u64,
    source_transactions: u64,
    storage_commits: u64,
    acknowledgements: u64,
    max_capture_events_per_second: f64,
    max_consumer_events_per_second: f64,
    max_persist_p95_ms: f64,
    max_end_to_end_p95_ms: f64,
    max_ack_p95_ms: f64,
    max_retained_wal_bytes: u64,
    dropped_samples: u64,
}

impl BenchmarkSummary {
    fn read(result_dir: &Path) -> Result<Self> {
        let environment = read_environment(&result_dir.join("environment.txt"))?;
        let capture = read_json_lines(&result_dir.join("capture.jsonl"))?;
        let consumer = read_json_lines(&result_dir.join("consumer.jsonl"))?;
        let pgbench = fs::read_to_string(result_dir.join("pgbench.log"))
            .context("read pgbench benchmark log")?;

        let run_id = required_environment(&environment, "run_id")?.to_owned();
        let target_tps = parse_environment(&environment, "rate")?;
        let rows_per_transaction = parse_environment(&environment, "rows_per_transaction")?;
        let ack_every = parse_environment(&environment, "ack_every")?;
        let processed_transactions = parse_processed_transactions(&pgbench).unwrap_or_default();
        let consumed_events = max_u64(&consumer, "events_total");
        // Capture JSONL is periodic, so delivered events are its durable lower bound.
        let captured_events = max_u64(&capture, "events_total").max(consumed_events);

        Ok(Self {
            run_id,
            target_tps,
            rows_per_transaction,
            ack_every,
            actual_tps: parse_actual_tps(&pgbench).unwrap_or_default(),
            generated_events: processed_transactions.saturating_mul(rows_per_transaction),
            captured_events,
            consumed_events,
            source_transactions: max_u64(&capture, "transactions_total"),
            storage_commits: max_u64(&capture, "storage_commits_total"),
            acknowledgements: max_u64(&consumer, "acknowledgements_total"),
            max_capture_events_per_second: max_f64(&capture, &["events_per_second"]),
            max_consumer_events_per_second: max_f64(&consumer, &["events_per_second"]),
            max_persist_p95_ms: max_f64(&capture, &["persist_latency_ms", "p95"]),
            max_end_to_end_p95_ms: max_f64(&consumer, &["end_to_end_latency_ms", "p95"]),
            max_ack_p95_ms: max_f64(&consumer, &["acknowledgement_latency_ms", "p95"]),
            max_retained_wal_bytes: max_retained_wal(&result_dir.join("postgres.csv"))?,
            dropped_samples: max_u64(&capture, "dropped_samples_total"),
        })
    }

    fn csv_row(&self) -> String {
        let target_events_per_second = self.target_tps.saturating_mul(self.rows_per_transaction);
        let actual_events_per_second = self.actual_tps * self.rows_per_transaction as f64;
        let events_per_storage_commit = divide(self.captured_events, self.storage_commits);
        let transactions_per_storage_commit =
            divide(self.source_transactions, self.storage_commits);

        format!(
            "{},{},{},{},{},{:.3},{:.3},{},{},{},{},{},{:.3},{:.3},{},{:.3},{:.3},{:.3},{:.3},{:.3},{},{}",
            self.run_id,
            self.target_tps,
            self.rows_per_transaction,
            target_events_per_second,
            self.ack_every,
            self.actual_tps,
            actual_events_per_second,
            self.generated_events,
            self.captured_events,
            self.consumed_events,
            self.source_transactions,
            self.storage_commits,
            events_per_storage_commit,
            transactions_per_storage_commit,
            self.acknowledgements,
            self.max_capture_events_per_second,
            self.max_consumer_events_per_second,
            self.max_persist_p95_ms,
            self.max_end_to_end_p95_ms,
            self.max_ack_p95_ms,
            self.max_retained_wal_bytes,
            self.dropped_samples,
        )
    }
}

fn divide(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn read_environment(path: &Path) -> Result<HashMap<String, String>> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("read benchmark environment {}", path.display()))?;
    Ok(contents
        .lines()
        .filter_map(|line| line.split_once('='))
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect())
}

fn required_environment<'a>(
    environment: &'a HashMap<String, String>,
    key: &str,
) -> Result<&'a str> {
    environment
        .get(key)
        .map(String::as_str)
        .with_context(|| format!("benchmark environment is missing {key}"))
}

fn parse_environment<T>(environment: &HashMap<String, String>, key: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    required_environment(environment, key)?
        .parse()
        .with_context(|| format!("parse benchmark environment value {key}"))
}

fn read_json_lines(path: &Path) -> Result<Vec<Value>> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("read benchmark metrics {}", path.display()))?;
    contents
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).context("parse benchmark JSON line"))
        .collect()
}

fn max_u64(reports: &[Value], key: &str) -> u64 {
    reports
        .iter()
        .filter_map(|report| report.get(key)?.as_u64())
        .max()
        .unwrap_or_default()
}

fn max_f64(reports: &[Value], path: &[&str]) -> f64 {
    reports
        .iter()
        .filter_map(|report| {
            path.iter()
                .try_fold(report, |value, key| value.get(key))
                .and_then(Value::as_f64)
        })
        .fold(0.0, f64::max)
}

fn parse_processed_transactions(log: &str) -> Option<u64> {
    log.lines().find_map(|line| {
        let value = line
            .trim()
            .strip_prefix("number of transactions actually processed:")?
            .trim()
            .split('/')
            .next()?
            .trim()
            .replace(',', "");
        value.parse().ok()
    })
}

fn parse_actual_tps(log: &str) -> Option<f64> {
    log.lines().find_map(|line| {
        line.trim()
            .strip_prefix("tps =")?
            .split_whitespace()
            .next()?
            .parse()
            .ok()
    })
}

fn max_retained_wal(path: &Path) -> Result<u64> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("read PostgreSQL metrics {}", path.display()))?;
    Ok(contents
        .lines()
        .skip(1)
        .filter_map(|line| line.split(',').nth(3)?.trim().parse().ok())
        .max()
        .unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_pgbench_totals_and_rate() {
        let log = "
number of transactions actually processed: 1,234/1,234
tps = 345.230010 (without initial connection time)
";

        assert_eq!(parse_processed_transactions(log), Some(1_234));
        assert_eq!(parse_actual_tps(log), Some(345.230010));
    }

    #[test]
    fn reads_nested_metric_maximum() {
        let reports = vec![
            serde_json::json!({"latency": {"p95": 1.5}}),
            serde_json::json!({"latency": {"p95": 3.25}}),
        ];

        assert_eq!(max_f64(&reports, &["latency", "p95"]), 3.25);
    }
}
