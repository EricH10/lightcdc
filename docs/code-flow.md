# Code Flow

## Runtime Flow

1. `lightcdc run` loads `lightcdc.example.toml`, opens redb once, starts gRPC, and starts capture.
2. `capture_with_store` supervises PostgreSQL capture and reloads the durable
   source LSN and next local sequence before every connection.
3. `ReplicationReader` connects to PostgreSQL logical replication and waits for pgoutput messages.
4. `PgOutputDecoder` turns pgoutput bytes into relation metadata and row changes.
5. `ReplicationReader::row_change` converts each row change into a buffered `ChangeEvent`.
6. PostgreSQL `Commit` makes the reader emit a `CapturedTransaction` with all buffered events.
7. `RedbEventStore::persist_transaction` stores all events and the commit LSN in one redb transaction.
8. `ReplicationReader::ack` tells PostgreSQL the commit WAL position is durable.
9. A PostgreSQL disconnect drops the current reader, waits with capped
   exponential backoff and jitter, and reconnects from the redb source LSN.
10. `LightCdcService::subscribe` claims one active `(stream, consumer)`, replays
   stored events, and keeps polling for new ones.
11. `StreamConfig::matches_event` filters events by configured table patterns.
12. `LightCdcService::ack` records a consumer's last handled event sequence.
13. `LightCdcService::seek` moves a consumer to earliest, latest, or a specific sequence.
14. The example consumer processes, prints, and acknowledges each event in
    sequence.

## Main Objects

- `Config` holds source, runtime, logging, and stream settings loaded from TOML.
- `SourceConfig` describes the PostgreSQL database, publication, and replication slot.
- `StreamConfig` names a consumer-facing stream and the tables it includes.
- `ChangeEvent` is the normalized event stored locally and delivered to consumers.
- `SourceMetadata` identifies the database, slot, and WAL location for an event.
- `TransactionMetadata` carries transaction id and commit LSN details when available.
- `Operation` describes whether a row was inserted, updated, deleted, or truncated.
- `ReplicationReader` reads PostgreSQL logical replication and emits committed transactions.
- `PgOutputDecoder` parses pgoutput protocol bytes into table and row changes.
- `CapturedTransaction` pairs committed events with the WAL LSN to acknowledge.
- `RedbEventStore` stores events, source offsets, and stream consumer offsets.
- `LightCdcService` exposes the stored event log through gRPC Subscribe, Ack, and Seek.
- `LogOpenOptions` describes where the local redb database file lives.

## Main Functions

- `Config::from_path` loads and parses a TOML config file.
- `Config::stream` finds a configured stream by name.
- `StreamConfig::matches_event` checks whether an event belongs to a stream.
- `ReplicationReader::connect_from` starts logical replication from a saved LSN.
- `ReplicationReader::next_transaction` waits until PostgreSQL commits the next transaction.
- `PgOutputDecoder::decode` decodes one pgoutput protocol message.
- `RedbEventStore::next_sequence` returns the next local sequence number to use.
- `RedbEventStore::persist_transaction` atomically stores committed events and their source LSN.
- `RedbEventStore::replay_from` reads stored events from a sequence number.
- `RedbEventStore::consumer_offset` reads where a consumer last acked.
- `LightCdcService::subscribe` streams matching events to a consumer.
- `LightCdcService::ack` persists a consumer's last processed sequence.
- `LightCdcService::seek` changes where a consumer will resume.
- `replay_from_store` powers the CLI replay command with optional stream filtering.
