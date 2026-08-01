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
- `lightcdc-runtime`: shared storage writer, capture batching, and write-command
  coordination used by capture, retention, and gRPC.
- `lightcdc-redis`: optional external gRPC consumer that atomically applies
  ordered cache mutations and Redis-side progress before acknowledging events.
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

## Capture Selection and Idle Progress

`Config::capture_plan` compiles the union of table patterns required by every
configured durable stream. Consumer connection state is deliberately absent
from this plan: an offline consumer still expects its configured history to be
captured for later replay.

Startup expands `*` and `schema.*` patterns against current user tables and
compares the required set with `pg_publication_tables`. Missing required tables
are a fatal configuration error. Extra published tables produce a warning, and
the replication reader filters their row and truncate messages before assigning
local sequences or staging events. The operator owns publication DDL; LightCDC
does not silently add or remove tables.

A source transaction may therefore commit with zero selected events. It still
travels through `CapturedTransaction` and the dedicated writer so redb advances
the source LSN atomically without changing the event high-water mark. Multiple
configured streams selecting one table still share one stored event.

PostgreSQL does not emit transaction boundaries for ordinary changes excluded
by a narrow publication. A long-lived normal SQL connection therefore calls
`pg_logical_emit_message` at `heartbeat_interval_ms`. The replication
connection requests logical messages, receives each transactional heartbeat
with a safe commit boundary, and persists its eventless checkpoint before
acknowledging PostgreSQL. Keepalive `wal_end` values are never treated as
durable checkpoints because they may be ahead of uncommitted relevant work.

Changing configured table selection changes future capture only. Adding a table
to a stream requires adding it to the publication before startup and using a
snapshot or backfill when earlier state is required.

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

Initial dependency: `redb`, arranged as one control database and a sequence of
event-segment databases.

Reasoning:

- It is a Rust-native embedded key-value database.
- Its table model maps well to ordered CDC event storage: `sequence -> event payload`.
- Its transactions keep event payloads, deduplication IDs, and source
  checkpoints atomic inside each segment.
- It keeps the project closer to systems engineering than SQL schema design while still avoiding a premature custom append-only log.

Tradeoff:

- SQLite would be easier to inspect manually and more widely deployed. redb is
  a better fit for the shape of this runtime, but the project keeps the storage
  boundary narrow enough to swap later if benchmarks or operational needs point
  elsewhere.

The configured `database_file` is the control database. It stores consumer
offsets, the retention floor, and format metadata. Event files live beside it
under `<database_file>.segments/`. Exactly one segment is active and writable;
sealed segments are immutable. Replay scans segment metadata in sequence order,
opens sealed files through a bounded handle cache, and reads the active segment
through its shared handle. The active segment is visible immediately.

Rotation happens only between complete capture groups. Event-count, serialized
byte, and age thresholds are preferred limits because the group that crosses a
limit remains whole. An unusually large PostgreSQL transaction therefore gets
one oversized segment rather than being split.

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
- redb reads staged files one event at a time while writing events,
  deduplication IDs, and the final source commit LSN in one atomic write
  transaction.
- Completed files are deleted after processing. Files left by an abrupt exit are
  treated as disposable scratch data and removed on the next successful
  replication connection for that source.

The staged file is deliberately not a recovery log. PostgreSQL WAL is replayed
from the source LSN already committed in redb, which keeps one authoritative
durability boundary and avoids trying to resume a partially decoded transaction.

## Capture Group Commits

After PostgreSQL commits a source transaction, capture may hold that complete
transaction briefly while collecting more committed transactions. The group
flushes when it reaches the configured transaction, event, decoded-byte, or
time limit. Defaults are 100 transactions, 500 events, 4 MiB, and 20 ms.

One write transaction in the active segment streams every group member, updates
the event-ID index, and advances the source offset to the final transaction's
LSN. Only after that commit succeeds does capture acknowledge the final LSN to
PostgreSQL. A crash before the segment commit replays the whole group; a crash
afterward resumes from its final durable LSN. Grouping therefore changes
visibility latency but not the no-loss boundary or individual transaction
metadata.

After reconnecting from an existing source offset, capture reconciles replayed
transactions individually before group commits resume. This lets it reset
temporary local sequence numbers if PostgreSQL repeats the last durable
transaction.

## Dedicated Capture Writer

Capture starts one long-lived `lightcdc-redb-writer` standard thread. A bounded
Tokio channel transfers ownership of a completed `CaptureBatch` to that thread,
where `blocking_recv` waits without occupying a Tokio worker. The writer returns
the owned batch and persistence result through a one-shot channel.

Capture keeps at most one redb write in flight. While that group commits, the
replication task may decode and assemble the next group. Before submitting the
next write, capture waits for the previous result, acknowledges its final
PostgreSQL LSN, and handles any replay reconciliation. This overlaps WAL
decoding with redb durability without allowing source offsets, local sequences,
or storage writes to reorder.

An idle replication stream also waits on the in-flight write completion. A
completed commit is therefore acknowledged promptly even when PostgreSQL sends
no subsequent transaction.

## Event Retention

Optional hard retention removes whole sealed segment files when either the
configured maximum event count or maximum commit age is exceeded. Each sweep
runs on the same writer thread as capture, so retention and source commits
cannot race or reorder. The active segment is never deleted.

The control database keeps the retention floor, consumer offsets, and durable
format version. New active segments inherit source checkpoints and the
event-sequence high-water mark, preventing sequence reuse after older files are
deleted and preserving duplicate detection at the latest durable checkpoint.

Consumer offsets do not pin retention indefinitely. A subscription whose next
sequence is older than the retention floor fails explicitly and must seek to
the oldest retained event or to the current high-water mark. This bounds the
log despite abandoned consumers and avoids silently skipping missing history.
Count and age limits are segment-granular. A count sweep may retain up to one
segment less than the requested maximum after the active segment and sweep
interval temporarily exceed it. Age retention cannot delete a segment until
its newest event has expired. Segment boundaries should therefore be
comfortably smaller than their corresponding retention windows. Deleting the
whole file releases disk space without rewriting retained events.

Startup reconstructs the catalog from versioned segment headers. Interrupted
temporary files are discarded, while a segment renamed for deletion is either
restored or finished according to the durable control retention floor. A
legacy single-file store is migrated into the first segment. A sidecar format
marker is checked before redb opens any file, allowing a newer unsupported
format to be rejected without mutating it.

## Failure Classification

The capture supervisor retries failures that may recover without an operator
change and stops on failures that would repeat indefinitely:

- Network I/O, closed connections, worker termination, PostgreSQL startup or
  shutdown, resource pressure, and a temporarily active slot are retryable.
- Authentication, invalid source configuration, protocol or decoder failures,
  transaction-buffer failures, and local persistence failures stop capture.

Before opening logical replication, startup validates `wal_level`, replication
privilege, publication existence, configured table membership, and the slot's
type, output plugin, and database. Every retry reconnects from the source LSN
atomically committed in redb, so retrying never depends on an in-memory
position.

## Live Delivery Notifications

The combined `lightcdc run` process shares an `EventNotifier` between capture
and the gRPC service. Capture signals it only after a source transaction is
durable in redb. A caught-up subscription sleeps on that signal instead of
polling redb on a timer, then resumes ordered replay from its next sequence.

The notification intentionally contains no event data. Tokio watch
notifications may coalesce, but redb remains the source of truth and one wakeup
causes the subscriber to drain every available event. This removes idle polling
and its former latency floor without making correctness depend on an in-memory
message.

## Benchmark Instrumentation

Production instrumentation remains disabled unless `--metrics-file` is passed.
The disabled path contains no clocks, counters, histograms, channels, worker
threads, or per-event work. Enabled capture records one fixed-size sample per
durable redb group commit into a bounded nonblocking channel.

A dedicated standard thread owns aggregation, persistence-latency histograms,
JSON encoding, and metrics file I/O. A full channel drops instrumentation
samples instead of slowing capture, and every report exposes the cumulative
drop count so invalid benchmark runs are visible. Reports distinguish source
transactions from storage commits and expose events and transactions per
storage commit.

Consumer delivery and end-to-end latency are measured by the separate benchmark
consumer rather than adding measurement work to the gRPC service.
