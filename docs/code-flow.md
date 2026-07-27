# Code Flow

## Runtime Flow

1. `lightcdc run` loads `lightcdc.example.toml`, opens redb once, starts gRPC, and starts capture.
2. `capture_with_store` resumes from the stored source LSN and chooses the next local event sequence.
3. `ReplicationReader` connects to PostgreSQL logical replication and waits for pgoutput messages.
4. `PgOutputDecoder` turns pgoutput bytes into relation metadata and row changes.
5. `ReplicationReader::row_change` converts each row change into a buffered `ChangeEvent`.
6. PostgreSQL `Commit` makes the reader emit a `CapturedTransaction` with all buffered events.
7. `RedbEventStore::persist_transaction` stores all events and the commit LSN in one redb transaction.
8. `ReplicationReader::ack` tells PostgreSQL the commit WAL position is durable.
9. `LightCdcService::subscribe` replays stored events for a named stream and keeps polling for new ones.
10. `StreamConfig::matches_event` filters events by configured table patterns.
11. `LightCdcService::ack` records a consumer's last handled event sequence.
12. `LightCdcService::seek` moves a consumer to earliest, latest, or a specific sequence.
13. The example consumer subscribes, prints each event, and acks it after successful printing.

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
