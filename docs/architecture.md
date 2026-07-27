# Architecture

`lightcdc` is designed as a single-process CDC runtime with PostgreSQL logical
replication and MySQL row-binlog source connectors.

The intended data path is:

```text
PostgreSQL logical replication or MySQL binary log
    -> source-specific replication reader
    -> redb-backed local event store
    -> configured stream
    -> replay CLI or gRPC consumer
```

Both source readers buffer row changes until the source transaction commits.
The CLI then atomically stores the transaction's normalized events and its
source-specific checkpoint in redb.

## Crate Boundaries

- `lightcdc-core`: shared config, event, and error types.
- `lightcdc-postgres`: PostgreSQL connectivity and replication support.
- `lightcdc-mysql`: MySQL connectivity, validation, and row-binlog support.
- `lightcdc-storage`: redb-backed event, source offset, and consumer offset storage.
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

- The current decoder supports the first useful subset: relation metadata, insert, update, delete, and truncate metadata. More `pgoutput` message types need explicit tests before broader claims.

## MySQL Client Choice

Initial dependency: `mysql_async` with its binlog feature.

Reasoning:

- It uses Tokio, matching the runtime already used by the CLI and API.
- It handles the MySQL replication protocol, table-map events, row events, and
  compressed transaction payloads.
- LightCDC still owns normalization and transaction/checkpoint persistence.

The initial resume cursor is binary-log filename plus position. A first launch
starts at the current binlog end; GTID-based failover is deferred. See
[`mysql.md`](mysql.md) for server requirements and current limitations.

## Local Storage Choice

Initial dependency: `redb`.

Reasoning:

- It is a Rust-native embedded key-value database.
- Its table model maps well to ordered CDC event storage: `sequence -> event payload`.
- It supports separate tables for event ID deduplication, source offsets, and consumer offsets without inventing a storage engine too early.
- It keeps the project closer to systems engineering than SQL schema design while still avoiding a premature custom append-only log.

Tradeoff:

- SQLite would be easier to inspect manually and more widely deployed. redb is a better fit for the shape of this runtime, but the project should keep the storage boundary narrow enough to swap later if benchmarks or operational needs point elsewhere.
