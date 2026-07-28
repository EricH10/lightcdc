# Architecture

`lightcdc` is designed as a single-process CDC runtime with a narrow first target: PostgreSQL logical replication.

The intended data path is:

```text
PostgreSQL logical replication
    -> replication reader
    -> redb-backed local event store
    -> configured stream
    -> replay CLI or gRPC consumer
```

Milestone 0 established the workspace, local database, configuration model, and
CLI entrypoint. The current decoder handles relation, insert, update, delete,
and truncate messages.

## Crate Boundaries

- `lightcdc-core`: shared config, event, and error types.
- `lightcdc-postgres`: PostgreSQL connectivity and replication support.
- `lightcdc-storage`: transaction staging plus redb-backed event, source offset,
  and consumer offset storage.
- `lightcdc-api`: gRPC Subscribe, Ack, and Seek service.
- `lightcdc-cli`: user-facing binary.

## Streams and Consumers

Streams are configured views over the captured event log:

```toml
[[streams]]
name = "orders"
source = "default"
tables = ["public.orders"]
```

External consumers connect to a stream by name and provide their own consumer identity. Offsets are scoped by both stream and consumer, so `search-indexer` on `orders` does not collide with `search-indexer` on another stream.

Each `(stream, consumer)` currently has one ordered cursor and allows one active
subscription. Its acknowledgement is cumulative, so consumers process and
acknowledge events sequentially. See
[`consumer-delivery.md`](consumer-delivery.md) for the full contract and the
planned leased-message model for parallel worker pools.

The gRPC API exposes:

- `Subscribe`: stream events for a configured stream and consumer.
- `Ack`: persist a stream-scoped consumer offset.
- `Seek`: move a consumer offset to earliest, latest, or an explicit sequence.

## PostgreSQL Client Choice

Initial dependencies: `tokio-postgres` and `pgwire-replication`.

Reasoning:

- `tokio-postgres` is used for normal SQL connectivity checks.
- `pgwire-replication` is used for logical replication because `tokio-postgres` does not expose a high-level CopyBoth logical replication stream.
- The project still owns `pgoutput` row decoding so the CDC event model stays explicit and easy to test.

Open concern:

- Truncates produce one normalized event per affected relation.
- Relation messages replace cached metadata, allowing row decoding to continue
  after tested add-column schema changes.
- Unchanged external TOAST values are represented explicitly as
  `{"__unchanged_toast": true}`. With `REPLICA IDENTITY FULL`, the tested update
  also carries the complete value in its old row image.
- More `pgoutput` message variants and schema-change forms still need explicit
  support and tests before broader claims.

## Local Storage Choice

Initial dependency: `redb`.

Reasoning:

- It is a Rust-native embedded key-value database.
- Its table model maps well to ordered CDC event storage: `sequence -> event payload`.
- It supports separate tables for event ID deduplication, source offsets, and consumer offsets without inventing a storage engine too early.
- It keeps the project closer to systems engineering than SQL schema design while still avoiding a premature custom append-only log.

Tradeoff:

- SQLite would be easier to inspect manually and more widely deployed. redb is a better fit for the shape of this runtime, but the project should keep the storage boundary narrow enough to swap later if benchmarks or operational needs point elsewhere.

## Transaction Buffering

PostgreSQL emits row changes before the transaction's commit message, so capture
must retain a whole source transaction before making it visible. Each active
transaction tracks its event count, estimated decoded bytes, and serialized
staging bytes.

- Events remain in memory through `transaction_memory_threshold_bytes`.
- Crossing that threshold writes length-prefixed JSON records below the
  source-scoped `data_dir/staging/` directory.
- `max_transaction_bytes` and `max_transaction_events` are hard bounds. Crossing
  either stops capture without persisting or acknowledging the transaction.
- redb reads a staged file one event at a time while writing the events,
  deduplication IDs, and source commit LSN in one atomic write transaction.
- Completed files are deleted after processing. Files left by an abrupt exit are
  treated as disposable scratch data and removed on the next successful
  replication connection for that source.

The staged file is deliberately not a recovery log. PostgreSQL WAL is replayed
from the source LSN already committed in redb, which keeps one authoritative
durability boundary and avoids trying to resume a partially decoded transaction.

## Failure Classification

The capture supervisor retries failures that may recover without an operator
change and stops on failures that would repeat indefinitely:

- Network I/O, closed connections, worker termination, PostgreSQL startup or
  shutdown, resource pressure, and a temporarily active slot are retryable.
- Authentication, invalid source configuration, protocol or decoder failures,
  transaction-buffer failures, and local persistence failures stop capture.

Before opening logical replication, startup validates `wal_level`, replication
privilege, publication existence, and the slot's type, output plugin, and
database. Every retry reconnects from the source LSN atomically committed in
redb, so retrying never depends on an in-memory position.

## Benchmark Instrumentation

Production instrumentation remains disabled unless `--metrics-file` is passed.
The disabled path contains no clocks, counters, histograms, channels, worker
threads, or per-event work. Enabled capture records one fixed-size sample per
durable source transaction into a bounded nonblocking channel.

A dedicated standard thread owns aggregation, persistence-latency histograms,
JSON encoding, and metrics file I/O. A full channel drops instrumentation
samples instead of slowing capture, and every report exposes the cumulative
drop count so invalid benchmark runs are visible.

Consumer delivery and end-to-end latency are measured by the separate benchmark
consumer rather than adding measurement work to the gRPC service.
