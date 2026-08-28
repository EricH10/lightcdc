# Code Flow

## CLI Module Map

- `main.rs` parses arguments and dispatches one command.
- `cli.rs` defines command-line arguments and bounded capture options.
- `capture/mod.rs` wires configuration, storage, metrics, retention, and serving.
- `capture/supervisor.rs` owns validation, reconnects, heartbeats, and session lifetime.
- `capture/pipeline.rs` overlaps PostgreSQL reads with one in-flight redb write.
- `capture/metrics.rs` owns optional non-blocking benchmark metrics.
- `commands.rs` implements replay, inspect, and serve-only commands.
- `display.rs` formats events and inspector tables.
- `store.rs` translates runtime configuration into redb open options.
- `lightcdc-runtime` owns the dedicated synchronous storage writer, bounded
  reader pool, sink workers, runtime state, and production metrics.

## Runtime Flow

1. `lightcdc run` loads `lightcdc.example.toml`, opens the segmented store once,
   starts gRPC, and starts capture.
2. `capture_with_store` compiles the configured streams into one `CapturePlan`
   and prepares the optional bounded-run target, metrics, transaction staging,
   heartbeats, and the dedicated storage writer.
3. `supervise_capture` validates the source and publication table membership,
   starts logical heartbeats, reconnects when needed, and asks
   `connect_capture_reader` to reload the durable source LSN and next local
   sequence.
4. `run_capture_session` waits for either PostgreSQL input or storage completion
   and delegates each transition to a named handler.
5. `ReplicationReader` connects to PostgreSQL logical replication and waits for pgoutput messages.
6. `PgOutputDecoder` turns pgoutput bytes into relation metadata and row changes.
7. `CapturePlan::matches_table` rejects changes no configured stream needs
   before `ReplicationReader::row_change` assigns a local sequence.
8. PostgreSQL `Commit` makes the reader emit a `CapturedTransaction` with all
   selected events, including zero events for a checkpoint-only transaction.
9. Capture groups complete source transactions until a count, size, or 20 ms
   deadline is reached.
10. `CaptureStorageWriter::submit` moves the group through a bounded channel to
   the long-lived `lightcdc-redb-writer` OS thread.
11. While that thread commits the group and source checkpoint to the active
   segment through `RedbEventStore::persist_transaction_batch`, the replication
   task may assemble the next group.
12. Capture receives the write result, then `ReplicationReader::ack` tells
    PostgreSQL the final group WAL position is durable.
13. A PostgreSQL disconnect drops the current reader, waits with capped
   exponential backoff and jitter, and reconnects from the redb source LSN.
14. Transactional logical heartbeats provide safe commit boundaries while only
    unrelated tables are changing; their eventless redb commits advance only
    the source checkpoint.
15. The retention timer serializes whole-file deletion of expired sealed
    segments through the same redb writer thread used by capture.
16. `LightCdcService::subscribe` claims one active `(stream, consumer)`, replays
    stored events, and waits for capture's post-commit notification when caught up.
17. `StreamConfig::matches_event` selects each stream's view of the shared log.
18. `LightCdcService::ack` records a consumer's last handled event sequence.
19. `LightCdcService::seek` moves a consumer to earliest, latest, or a specific sequence.
20. The example consumer processes, prints, and acknowledges each event in
    sequence.

## Main Objects

- `Config` holds source, runtime, logging, and stream settings loaded from TOML.
- `SourceConfig` describes the PostgreSQL database, publication, and replication slot.
- `StreamConfig` names a consumer-facing stream and the tables it includes.
- `CapturePlan` is the validated union of tables required by every stream.
- `ChangeEvent` is the normalized event stored locally and delivered to consumers.
- `SourceMetadata` identifies the database, slot, and WAL location for an event.
- `TransactionMetadata` carries transaction id and commit LSN details when available.
- `Operation` describes whether a row was inserted, updated, deleted, or truncated.
- `ReplicationReader` reads PostgreSQL logical replication and emits committed transactions.
- `LogicalHeartbeatEmitter` creates safe idle transaction checkpoints.
- `PgOutputDecoder` parses pgoutput protocol bytes into table and row changes.
- `CapturedTransaction` pairs committed events with the WAL LSN to acknowledge.
- `CaptureBatch` owns one bounded group of complete source transactions.
- `CapturePipeline` keeps the current batch, one pending write, and replay state together.
- `CaptureStorageWriter` owns the dedicated redb writer thread and command channel.
- `PendingCaptureWrite` represents the one group that may be committing while
  capture assembles its successor.
- `RedbEventStore` presents one ordered log across a control database, one
  active event segment, and immutable sealed segments.
- `SegmentOptions` controls preferred event-count, byte, and age rotation
  boundaries plus the sealed-segment handle cache.
- `EventNotifier` wakes live subscribers after capture durably commits events.
- `LightCdcService` exposes the stored event log through gRPC Subscribe, Ack, and Seek.
- `LogOpenOptions` describes where the control database and segment directory
  live.

## Main Functions

- `Config::from_path` loads and parses a TOML config file.
- `Config::stream` finds a configured stream by name.
- `Config::capture_plan` validates streams and compiles their table union.
- `StreamConfig::matches_event` checks whether an event belongs to a stream.
- `validate_source_config_with_plan` checks source, slot, publication, and table alignment.
- `ReplicationReader::connect_from_with_buffer_and_plan` starts filtered logical
  replication from a saved LSN.
- `ReplicationReader::next_transaction` waits until PostgreSQL commits the next transaction.
- `ReplicationReader::next_transaction_until` waits only through a group-commit deadline.
- `PgOutputDecoder::decode` decodes one pgoutput protocol message.
- `supervise_capture` owns validation, connection, shutdown, and reconnect policy.
- `run_capture_session` dispatches connected-session progress to named transition functions.
- `buffer_captured_transaction` groups one committed PostgreSQL transaction for storage.
- `finish_capture_read_error` drains complete work before handling a disconnect.
- `CaptureStorageWriter::submit` transfers one owned group to the redb thread.
- `complete_capture_write` applies metrics and output, then acknowledges the
  group's final source LSN.
- `RedbEventStore::next_sequence` returns the next local sequence number to use.
- `RedbEventStore::persist_transaction` atomically stores committed events and their source LSN.
- `RedbEventStore::persist_transaction_batch` atomically stores several source
  transactions and their final LSN.
- `RedbEventStore::prune_events` retires expired sealed segments and advances
  the durable retention floor.
- `RedbEventStore::replay_from` reads stored events from a sequence number.
- `RedbEventStore::consumer_offset` reads where a consumer last acked.
- `LightCdcService::subscribe` streams matching events to a consumer.
- `LightCdcService::ack` persists a consumer's last processed sequence.
- `LightCdcService::seek` changes where a consumer will resume.
- `replay_from_store` powers the CLI replay command with optional stream filtering.
