# Code Flow

## Runtime Flow

1. `lightcdc run` loads `lightcdc.example.toml`, opens redb once, starts gRPC, and starts capture.
2. `capture_with_store` selects the configured PostgreSQL or MySQL connector.
3. The connector supervisor reloads the durable source checkpoint and next
   local sequence before every connection.
4. The source `ReplicationReader` waits for replication protocol messages and
   converts row changes into buffered `ChangeEvent` values.
5. A source commit makes the reader emit a `CapturedTransaction`.
6. `RedbEventStore::persist_transaction` stores all events and the source
   checkpoint in one redb transaction.
7. PostgreSQL acknowledges the durable commit WAL position; MySQL relies on the
   saved binlog position for its next connection.
8. A disconnect waits with capped exponential backoff and jitter, then resumes
   from the redb checkpoint.
9. `LightCdcService::subscribe` claims one active `(stream, consumer)`, replays
   stored events, and keeps polling for new ones.
10. `StreamConfig::matches_event` filters events by configured table patterns.
11. `LightCdcService::ack` records a consumer's last handled event sequence.
12. `LightCdcService::seek` moves a consumer to earliest, latest, or a specific sequence.
13. The example consumer processes, prints, and acknowledges each event in
    sequence.

## Main Objects

- `Config` holds source, runtime, logging, and stream settings loaded from TOML.
- `SourceConfig` selects `PostgresSourceConfig` or `MySqlSourceConfig`.
- `StreamConfig` names a consumer-facing stream and the tables it includes.
- `ChangeEvent` is the normalized event stored locally and delivered to consumers.
- `SourceMetadata` identifies the database and source position for an event.
- `TransactionMetadata` carries transaction id and commit position details when available.
- `Operation` describes whether a row was inserted, updated, deleted, or truncated.
- Each connector's `ReplicationReader` emits committed source transactions.
- `PgOutputDecoder` parses pgoutput protocol bytes into table and row changes.
- `CapturedTransaction` pairs committed events with a source resume checkpoint.
- `RedbEventStore` stores events, source offsets, and stream consumer offsets.
- `LightCdcService` exposes the stored event log through gRPC Subscribe, Ack, and Seek.
- `LogOpenOptions` describes where the local redb database file lives.

## Main Functions

- `Config::from_path` loads and parses a TOML config file.
- `Config::stream` finds a configured stream by name.
- `StreamConfig::matches_event` checks whether an event belongs to a stream.
- `ReplicationReader::connect_from` starts replication from a saved source checkpoint.
- `ReplicationReader::next_transaction` waits until the source commits a transaction.
- `PgOutputDecoder::decode` decodes one pgoutput protocol message.
- `RedbEventStore::next_sequence` returns the next local sequence number to use.
- `RedbEventStore::persist_transaction` atomically stores committed events and
  their source checkpoint.
- `RedbEventStore::replay_from` reads stored events from a sequence number.
- `RedbEventStore::consumer_offset` reads where a consumer last acked.
- `LightCdcService::subscribe` streams matching events to a consumer.
- `LightCdcService::ack` persists a consumer's last processed sequence.
- `LightCdcService::seek` changes where a consumer will resume.
- `replay_from_store` powers the CLI replay command with optional stream filtering.
