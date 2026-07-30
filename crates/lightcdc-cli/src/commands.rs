//! Implements local replay, inspection, and serve-only CLI commands.

use std::{net::SocketAddr, path::PathBuf};

use anyhow::Context;
use lightcdc_core::{ChangeEvent, Config, StreamConfig};
use lightcdc_storage::RedbEventStore;

use crate::{
    display::{event_to_json, human_bytes, operation_name, print_table},
    logging::init_logging,
    store::{open_event_store, storage_options},
};

/// Replays stored events to stdout, optionally filtering by stream.
pub(crate) async fn replay(
    config_path: PathBuf,
    stream: Option<String>,
    from: u64,
    limit: usize,
    pretty: bool,
) -> anyhow::Result<()> {
    let config = Config::from_path(&config_path)
        .with_context(|| format!("could not load config from {}", config_path.display()))?;

    init_logging(&config.logging.level)?;

    let stream = stream
        .as_deref()
        .map(|name| {
            config
                .stream(name)
                .with_context(|| format!("stream {name:?} is not defined in config"))
        })
        .transpose()?;
    let store = open_event_store(&config)?;
    let events = replay_from_store(&store, from, limit, stream)
        .with_context(|| format!("failed to replay events from sequence {from}"))?;

    for event in events {
        println!("{}", event_to_json(&event, pretty)?);
    }

    Ok(())
}

/// Reads events from storage while applying optional stream filtering.
fn replay_from_store(
    store: &RedbEventStore,
    from: u64,
    limit: usize,
    stream: Option<&StreamConfig>,
) -> anyhow::Result<Vec<ChangeEvent>> {
    if limit == 0 {
        return Ok(Vec::new());
    }

    let mut next_sequence = from;
    let mut output = Vec::with_capacity(limit);
    const BATCH_SIZE: usize = 256;

    while output.len() < limit {
        let batch = store.replay_from(next_sequence, BATCH_SIZE)?;
        if batch.is_empty() {
            break;
        }

        for event in batch {
            next_sequence = event.sequence + 1;
            if stream.is_none_or(|stream| stream.matches_event(&event)) {
                output.push(event);
                if output.len() >= limit {
                    break;
                }
            }
        }
    }

    Ok(output)
}

/// Prints a human-readable snapshot of the configured redb event store.
pub(crate) fn inspect(
    config_path: PathBuf,
    limit: usize,
    selected_sequence: Option<u64>,
) -> anyhow::Result<()> {
    let config = Config::from_path(&config_path)
        .with_context(|| format!("could not load config from {}", config_path.display()))?;
    let storage = storage_options(&config);
    let database_path = storage.data_dir.join(&storage.database_file);
    let store = open_event_store(&config).with_context(|| {
        format!(
            "failed to open {}; stop any other lightcdc process using this redb file",
            database_path.display()
        )
    })?;
    let stats = store.stats().context("failed to read redb table counts")?;
    let source_offsets = store
        .source_offsets()
        .context("failed to read source offsets")?;
    let consumer_offsets = store
        .consumer_offsets()
        .context("failed to read consumer offsets")?;
    let last_sequence = store
        .last_sequence()
        .context("failed to read last event sequence")?;
    let first_sequence = store
        .first_sequence()
        .context("failed to read first event sequence")?;
    let segments_path = database_path.with_file_name(format!("{}.segments", storage.database_file));
    let file_size = store_disk_usage(&database_path, &segments_path);

    println!("LightCDC redb inspector");
    println!("Control        {}", database_path.display());
    println!("Segments       {}", segments_path.display());
    println!("File size      {}", human_bytes(file_size));
    println!(
        "Event range    {}",
        match (first_sequence, last_sequence) {
            (Some(first), Some(last)) => format!("{first}..={last}"),
            _ => "empty".to_owned(),
        }
    );

    print_table(
        "Tables",
        &["NAME", "ROWS"],
        &[
            vec!["events".to_owned(), stats.event_count.to_string()],
            vec!["event_ids".to_owned(), stats.event_id_count.to_string()],
            vec![
                "source_offsets".to_owned(),
                stats.source_offset_count.to_string(),
            ],
            vec![
                "consumer_offsets".to_owned(),
                stats.consumer_offset_count.to_string(),
            ],
            vec!["segments".to_owned(), stats.segment_count.to_string()],
            vec![
                "sealed_segments".to_owned(),
                stats.sealed_segment_count.to_string(),
            ],
        ],
        &[24, 12],
    );

    let source_rows = source_offsets
        .into_iter()
        .map(|offset| vec![offset.source_name, offset.lsn])
        .collect::<Vec<_>>();
    print_table(
        "Source offsets",
        &["SOURCE", "LSN"],
        &source_rows,
        &[24, 24],
    );

    let consumer_rows = consumer_offsets
        .into_iter()
        .map(|offset| {
            let lag = last_sequence.unwrap_or(0).saturating_sub(offset.sequence);
            vec![
                offset.stream_name,
                offset.consumer_name,
                offset.sequence.to_string(),
                lag.to_string(),
            ]
        })
        .collect::<Vec<_>>();
    print_table(
        "Consumer offsets",
        &["STREAM", "CONSUMER", "OFFSET", "LAG"],
        &consumer_rows,
        &[24, 28, 12, 12],
    );

    let recent_events = match (last_sequence, limit) {
        (Some(last), limit) if limit > 0 => {
            let from = last.saturating_sub(limit.saturating_sub(1) as u64);
            store
                .replay_from(from, limit)
                .context("failed to read recent events")?
        }
        _ => Vec::new(),
    };
    let event_rows = recent_events
        .iter()
        .map(|event| {
            vec![
                event.sequence.to_string(),
                operation_name(event.operation).to_owned(),
                format!("{}.{}", event.schema, event.table),
                event.source.lsn.clone(),
                event.event_id.clone(),
            ]
        })
        .collect::<Vec<_>>();
    print_table(
        "Recent events",
        &["SEQ", "OPERATION", "RELATION", "LSN", "EVENT ID"],
        &event_rows,
        &[12, 12, 32, 20, 44],
    );

    if let Some(sequence) = selected_sequence {
        let event = store
            .replay_from(sequence, 1)
            .with_context(|| format!("failed to read event sequence {sequence}"))?
            .into_iter()
            .find(|event| event.sequence == sequence)
            .with_context(|| format!("event sequence {sequence} is not stored"))?;

        println!("\nEvent {sequence}");
        println!("{}", event_to_json(&event, true)?);
    }

    Ok(())
}

/// Sums logical bytes across the control file and all sequence segments.
fn store_disk_usage(control_path: &std::path::Path, segments_path: &std::path::Path) -> u64 {
    let control_bytes = std::fs::metadata(control_path)
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    let segment_bytes = std::fs::read_dir(segments_path)
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .filter_map(|entry| entry.metadata().ok())
        .map(|metadata| metadata.len())
        .sum::<u64>();
    control_bytes.saturating_add(segment_bytes)
}

/// Serves the gRPC API against an existing event store when capture is not running.
pub(crate) async fn serve(config_path: PathBuf, addr: SocketAddr) -> anyhow::Result<()> {
    let config = Config::from_path(&config_path)
        .with_context(|| format!("could not load config from {}", config_path.display()))?;

    init_logging(&config.logging.level)?;

    let store = open_event_store(&config)?;

    lightcdc_api::serve(addr, config, store).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use lightcdc_core::{Operation, SourceMetadata};
    use lightcdc_storage::LogOpenOptions;
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn stream_replay_filters_by_configured_table() {
        let temp = TempDir::new().expect("temp dir");
        let store = RedbEventStore::open(&LogOpenOptions {
            data_dir: temp.path().to_path_buf(),
            database_file: "test.redb".to_owned(),
        })
        .expect("open store");
        store
            .append_event(&event(1, "public", "orders"))
            .expect("append orders event");
        store
            .append_event(&event(2, "public", "customers"))
            .expect("append customers event");
        store
            .append_event(&event(3, "public", "orders"))
            .expect("append second orders event");

        let stream = StreamConfig {
            name: "orders".to_owned(),
            source: "default".to_owned(),
            tables: vec!["public.orders".to_owned()],
        };

        let events = replay_from_store(&store, 1, 10, Some(&stream)).expect("replay events");

        assert_eq!(
            events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
    }

    #[test]
    fn stream_replay_limit_counts_emitted_events() {
        let temp = TempDir::new().expect("temp dir");
        let store = RedbEventStore::open(&LogOpenOptions {
            data_dir: temp.path().to_path_buf(),
            database_file: "test.redb".to_owned(),
        })
        .expect("open store");
        store
            .append_event(&event(1, "public", "customers"))
            .expect("append customers event");
        store
            .append_event(&event(2, "public", "orders"))
            .expect("append orders event");
        store
            .append_event(&event(3, "public", "orders"))
            .expect("append second orders event");

        let stream = StreamConfig {
            name: "orders".to_owned(),
            source: "default".to_owned(),
            tables: vec!["public.orders".to_owned()],
        };

        let events = replay_from_store(&store, 1, 1, Some(&stream)).expect("replay events");

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].sequence, 2);
    }

    fn event(sequence: u64, schema: &str, table: &str) -> ChangeEvent {
        ChangeEvent {
            sequence,
            event_id: format!("event-{sequence}"),
            source: SourceMetadata {
                database: "lightcdc".to_owned(),
                slot: "slot".to_owned(),
                lsn: format!("0/{sequence:X}"),
            },
            transaction: None,
            schema: schema.to_owned(),
            table: table.to_owned(),
            operation: Operation::Insert,
            key: None,
            before: None,
            after: Some(br#"{"id":"1"}"#.to_vec()),
            commit_timestamp_ms: None,
        }
    }
}
