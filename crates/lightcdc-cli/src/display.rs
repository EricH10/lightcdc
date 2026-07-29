//! Formats captured events and local store inspection output.

use anyhow::Context;
use lightcdc_core::{ChangeEvent, Operation};

/// Converts a change event into CLI JSON output.
pub(crate) fn event_to_json(event: &ChangeEvent, pretty: bool) -> anyhow::Result<String> {
    let value = serde_json::json!({
        "sequence": event.sequence,
        "event_id": event.event_id,
        "source": event.source,
        "transaction": event.transaction,
        "schema": event.schema,
        "table": event.table,
        "operation": event.operation,
        "key": optional_json_bytes(event.key.as_deref())?,
        "before": optional_json_bytes(event.before.as_deref())?,
        "after": optional_json_bytes(event.after.as_deref())?,
        "commit_timestamp_ms": event.commit_timestamp_ms,
    });

    if pretty {
        Ok(serde_json::to_string_pretty(&value)?)
    } else {
        Ok(serde_json::to_string(&value)?)
    }
}

/// Parses optional JSON row bytes into JSON values.
fn optional_json_bytes(bytes: Option<&[u8]>) -> anyhow::Result<Option<serde_json::Value>> {
    bytes
        .map(|bytes| serde_json::from_slice(bytes).context("event row payload was not JSON"))
        .transpose()
}

/// Prints a compact ASCII table with bounded column widths.
pub(crate) fn print_table(
    title: &str,
    headers: &[&str],
    rows: &[Vec<String>],
    max_widths: &[usize],
) {
    let widths = headers
        .iter()
        .enumerate()
        .map(|(column, header)| {
            rows.iter()
                .filter_map(|row| row.get(column))
                .map(|value| value.chars().count())
                .chain(std::iter::once(header.len()))
                .max()
                .unwrap_or(header.len())
                .min(max_widths[column])
        })
        .collect::<Vec<_>>();

    println!("\n{title}");
    print_table_row(
        &headers.iter().map(ToString::to_string).collect::<Vec<_>>(),
        &widths,
    );
    println!(
        "{}",
        widths
            .iter()
            .map(|width| "-".repeat(*width))
            .collect::<Vec<_>>()
            .join("  ")
    );

    if rows.is_empty() {
        println!("(empty)");
        return;
    }

    for row in rows {
        print_table_row(row, &widths);
    }
}

/// Prints one row from a compact ASCII table.
fn print_table_row(row: &[String], widths: &[usize]) {
    let cells = row
        .iter()
        .zip(widths)
        .map(|(value, width)| {
            let value = truncate(value, *width);
            format!("{value:width$}")
        })
        .collect::<Vec<_>>();
    println!("{}", cells.join("  "));
}

/// Truncates a display value without splitting a Unicode character.
fn truncate(value: &str, width: usize) -> String {
    if value.chars().count() <= width {
        return value.to_owned();
    }

    if width <= 3 {
        return ".".repeat(width);
    }

    let mut output = value.chars().take(width - 3).collect::<String>();
    output.push_str("...");
    output
}

/// Formats a byte count for interactive output.
pub(crate) fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;

    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }

    if unit == 0 {
        format!("{bytes} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

/// Returns the stable display name for an event operation.
pub(crate) fn operation_name(operation: Operation) -> &'static str {
    match operation {
        Operation::Insert => "insert",
        Operation::Update => "update",
        Operation::Delete => "delete",
        Operation::Truncate => "truncate",
    }
}
