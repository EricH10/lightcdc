# lightcdc

`lightcdc` is a lightweight Rust CDC runtime for PostgreSQL. The long-term goal is to capture logical replication changes, persist them to a local embedded event store, replay them independently per consumer, and eventually run sandboxed WASM transforms before delivery.

This repository currently has a local end-to-end MVP with PostgreSQL capture,
durable redb replay, config-defined streams, and named gRPC consumers.

See [`docs/roadmap.md`](docs/roadmap.md) for the ordered path from the current
local MVP to a production-ready runtime.

See [`docs/debugging.md`](docs/debugging.md) for the project VS Code debugger
setup and a Rust debugging walkthrough.

See [`docs/consumer-delivery.md`](docs/consumer-delivery.md) for the current
ordered consumer and acknowledgement contract.

See [`docs/redis-connector.md`](docs/redis-connector.md) for the optional,
crash-safe Redis cache invalidation and update connector.

See [`docs/postgres-support.md`](docs/postgres-support.md) for the PostgreSQL 17
feature matrix, change-only bootstrap boundary, and least-privilege role.

See [`docs/operations.md`](docs/operations.md) for production deployment,
health probes, backup, recovery, and upgrade procedures.

## Prerequisites

- Rust 1.89 or newer
- Docker and Docker Compose

## Local Setup

Start PostgreSQL with logical replication enabled:

```bash
docker compose up -d postgres
```

Run the CLI:

```bash
cargo run -p lightcdc-cli -- capture --config lightcdc.example.toml
```

## Production Image

Build the pinned Debian Bookworm image and verify its CLI:

```bash
docker build -t lightcdc:0.1.0 .
docker run --rm lightcdc:0.1.0 --version
```

The image runs as UID/GID 10001 and contains both `lightcdc` and the optional
`lightcdc-redis` connector. Production deployment requires a read-only config
mount, mounted secrets, and a persistent `/var/lib/lightcdc` volume. Set
`storage.data_dir = "/var/lib/lightcdc"` in the mounted configuration. The
complete command, signal, health, and supported-platform contract is in the
operations runbook.

Capture does not print row payloads by default. Add `--output json` only for
local debugging when stdout is approved to contain source data.

For a bounded local smoke test:

```bash
cargo run -p lightcdc-cli -- capture --config lightcdc.example.toml --stop-after-events 3
```

Capture keeps the union of tables selected by configured durable streams, even
when no consumer is currently connected:

```toml
[[streams]]
name = "orders"
source = "default"
tables = ["public.orders"]
```

The PostgreSQL publication must contain every table matched by the stream
configuration. Startup fails when a required table is missing and warns when
the publication contains unnecessary tables. The publication remains
operator-managed; LightCDC also filters unmatched published tables before they
receive local event sequences or enter redb.

Transactional logical heartbeats advance the replication slot safely while
only unpublished or filtered tables are changing:

```toml
heartbeat_interval_ms = 10000
```

Heartbeat transactions persist only the source checkpoint and do not create
consumer events. Changing a stream's tables affects future capture only;
historical rows and changes require a snapshot or backfill.

Consumer resource limits are validated at startup and applied by the gRPC
service:

```toml
channel_capacity = 1024
max_active_subscriptions = 1024
max_consumer_name_bytes = 128
max_outbound_event_bytes = 16777216
max_inbound_request_bytes = 65536
max_requests_per_connection = 128
max_api_connections = 1024
max_header_list_bytes = 32768
replay_reader_threads = 2
replay_reader_queue_capacity = 1024
replay_batch_events = 256
replay_batch_max_bytes = 67108864
max_storage_bytes = 107374182400
min_free_disk_bytes = 1073741824
shutdown_timeout_ms = 10000
```

`channel_capacity` is the bounded per-subscription event queue. An oversized
event or exhausted subscription limit returns `RESOURCE_EXHAUSTED` without
advancing the consumer offset. Synchronous redb replay runs on a fixed reader
pool, not Tokio workers. Its command queue, event count, byte size, and
simultaneous completed batches are bounded. Tonic also enforces request,
header-list, TCP connection, and per-connection concurrent-request limits.

Transaction buffering is bounded by three `[runtime]` settings:

```toml
transaction_memory_threshold_bytes = 16777216
max_transaction_bytes = 1073741824
max_transaction_events = 1000000
```

Transactions stay in memory through the threshold, then spill to
`data_dir/staging/<source>/`. The staged file is scratch space: redb stores the
committed transaction and source LSN atomically, PostgreSQL is acknowledged only
after that commit, and source-scoped leftovers from an abrupt exit are removed
after the replacement capture acquires the replication slot.

Complete source transactions are grouped into one durable redb commit using:

```toml
capture_batch_max_transactions = 100
capture_batch_max_events = 1000
capture_batch_max_bytes = 4194304
capture_batch_max_delay_ms = 20
```

The first reached limit flushes the group. A source transaction is never split,
and PostgreSQL is acknowledged only through the final LSN committed with the
whole group. A dedicated `lightcdc-redb-writer` OS thread owns capture writes.
While it commits one group, the Tokio replication task can decode the next
group. Capture permits only one in-flight write and waits for its successful
completion before submitting another group or acknowledging PostgreSQL.
For sustained high-throughput workloads, `capture_batch_max_events = 2000`
with `capture_batch_max_delay_ms = 40` further reduces durable commit
frequency at the cost of additional batching latency.

The event log uses one writable segment plus immutable sealed segments. Rotation
is checked between capture groups, so a PostgreSQL transaction is never split
between files:

```toml
segment_max_events = 1000000
segment_max_bytes = 268435456
segment_max_age_seconds = 900
```

The limits are preferred boundaries: a capture group that crosses one stays
whole, then seals the segment. The active segment is immediately readable by
replay and gRPC consumers; consumers do not wait for it to seal.

Event-log retention is optional and targets a maximum retained event count,
logical segment bytes, event age, or whichever boundary is reached first:

```toml
retention_max_events = 10000000
retention_max_bytes = 10737418240
retention_max_age_seconds = 604800
retention_check_interval_ms = 1000
retention_delete_batch_size = 100000
```

Omit both maximum settings to disable retention. Sweeps run on the dedicated
redb writer thread so they cannot overlap capture commits. They delete whole
sealed segment files, returning their disk space without rewriting retained
events. Limits are segment-granular: the active segment and sweep interval can
temporarily exceed a boundary, count retention may then keep up to one segment
less than the configured maximum, and age retention waits until every event in
a sealed segment has expired. Keep segment limits comfortably below the
retention window. `retention_delete_batch_size` is a target work budget measured
in events; one indivisible segment may exceed it. A retention I/O failure stops
capture without acknowledging the current PostgreSQL transaction, rather than
allowing storage to grow while sweeps silently fail.

`max_storage_bytes` is a hard pre-commit ceiling across the data directory.
`min_free_disk_bytes` reserves filesystem space both while spilling a large
source transaction and before committing its redb batch. The writer reserves
additional copy-on-write headroom. Reaching either boundary stops capture
without advancing PostgreSQL acknowledgement, so an operator can free space or
raise a deliberately sized limit and resume without losing the transaction.

Retention is a hard log boundary: a durable consumer that falls behind receives
an explicit expired offset error and must seek to `earliest` or `latest`.
`earliest` means the oldest payload still retained.

Run capture and the gRPC API together:

```bash
cargo run -p lightcdc-cli -- run --config lightcdc.example.toml --addr 127.0.0.1:50051
```

Production PostgreSQL connections default to certificate and hostname
verification. Load the password from a mounted secret and configure a private
CA only when platform trust roots do not contain it:

```toml
[source]
password_file = "/run/secrets/postgres-password"
tls_mode = "verify_full"
# tls_ca_file = "/run/secrets/postgres-ca.pem"
```

A non-loopback gRPC bind requires TLS and at least one bearer principal:

```toml
[api]
tls_cert_file = "/run/secrets/lightcdc-server.pem"
tls_key_file = "/run/secrets/lightcdc-server-key.pem"

[[api.tokens]]
name = "orders-reader"
token_file = "/run/secrets/orders-reader-token"
streams = ["orders"]
allow_seek = false
```

Bearer tokens authorize `Subscribe` and `Ack` for their configured streams;
`Seek` additionally requires `allow_seek = true`. Plaintext unauthenticated
serving is restricted to loopback and must be explicitly enabled with
`api.allow_insecure_localhost = true` for local development.

Replay captured events from the local redb store:

```bash
cargo run -p lightcdc-cli -- replay --config lightcdc.example.toml --from 1 --limit 10
cargo run -p lightcdc-cli -- replay --config lightcdc.example.toml --stream orders --from 1 --limit 10
```

Use pretty JSON for interactive inspection:

```bash
cargo run -p lightcdc-cli -- replay --config lightcdc.example.toml --from 1 --limit 1 --pretty
```

Inspect the redb tables, source checkpoints, consumer offsets, lag, and recent
events:

```bash
cargo run -p lightcdc-cli -- inspect --config lightcdc.example.toml
cargo run -p lightcdc-cli -- inspect --config lightcdc.example.toml --sequence 42
```

The second command also prints the full decoded event at sequence 42. Stop
`capture`, `run`, or `serve` before inspecting because redb allows only one
process to open the control and active segment files.

Run a deep offline integrity traversal, or create and restore a checksummed
backup directory:

```bash
cargo run -p lightcdc-cli -- check --config lightcdc.example.toml
cargo run -p lightcdc-cli -- backup --config lightcdc.example.toml \
  --output /backups/lightcdc-2026-08-01
cargo run -p lightcdc-cli -- restore --config lightcdc.example.toml \
  --input /backups/lightcdc-2026-08-01
```

Stop every LightCDC command using the store first. Backup holds redb's process
lock, verifies every retained event and metadata table, copies only durable
control/segment files, records SHA-256 checksums and source recovery metadata,
then atomically publishes the destination. Restore requires an empty configured
data directory and verifies checksums plus the complete temporary store before
publishing it. See [`docs/operations.md`](docs/operations.md) for the required
PostgreSQL slot/WAL coordination and recovery boundaries.

Serve the gRPC API:

```bash
cargo run -p lightcdc-cli -- serve --config lightcdc.example.toml --addr 127.0.0.1:50051
```

Use `serve` when capture is not running. For live capture plus streaming
consumers, use `run` so both paths share one segmented store inside the same
process.

The server implements the standard gRPC health protocol. Check
`lightcdc.liveness` for process liveness and `lightcdc.readiness` for capture
readiness. Readiness is serving only in the `capturing` state; startup,
PostgreSQL reconnect, draining, and terminal states report not serving while
retained-event APIs remain available until shutdown begins.

Run an example gRPC consumer that prints and acks events:

```bash
cargo run -p lightcdc-api --example consumer -- \
  --endpoint http://127.0.0.1:50051 \
  --stream orders \
  --consumer example-printer \
  --seek latest \
  --seed-sql sql/demo_orders.sql \
  --limit 32
```

Run the Redis cache connector:

```bash
docker compose up -d redis
cargo run -p lightcdc-redis -- --config redis-connector.example.toml
```

The connector uses a named LightCDC consumer and atomically stores each applied
sequence with its Redis cache mutation before acknowledging LightCDC. See the
connector guide for invalidation, upsert, replay, and retention behavior.

Run checks:

```bash
cargo fmt --all
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Run Docker-backed integration tests:

```bash
docker compose up -d postgres
cargo test -p lightcdc-cli --test capture_integration -- --ignored --test-threads=1
```

Run a short end-to-end load test:

```bash
docker compose up -d postgres
DURATION_SECONDS=10 CLIENTS=4 THREADS=2 bench/run-local.sh
```

The harness uses release binaries, disables per-event output, drives PostgreSQL
with a parameterized `pgbench` workload, and records capture, redb, gRPC,
acknowledgement, WAL, CPU, memory, and disk measurements. See
[`bench/README.md`](bench/README.md) for fixed-rate limit discovery, environment
guidance, and the instrumentation overhead policy.

## Current Scope

Implemented basics:

- Cargo workspace
- Core event and config types
- CLI command shape
- PostgreSQL connectivity check
- Segmented redb event store with atomic source checkpoints
- Logical replication stream connection
- Combined capture plus gRPC serving command
- `pgoutput` relation, insert, update, delete, and truncate decoding
- Source offset persistence and idempotent duplicate replay handling
- Configured-stream capture planning, publication-table validation,
  capture-side filtering, and logical heartbeat checkpoints
- Whole-segment count and age retention with explicit stale-consumer behavior
- Bounded transaction accounting with disk-backed spill staging and crash cleanup
- Opt-in capture group-commit metrics and an end-to-end load-test harness
- Docker-backed integration tests for capture, abrupt process recovery,
  PostgreSQL reconnect, large transactions, delete identities, TOAST values,
  truncates, and relation refresh after schema changes
- Config-defined streams
- Stream-filtered replay
- gRPC `Subscribe`, `Ack`, and `Seek`
- Local Docker Compose PostgreSQL
- Optional Redis cache invalidation and JSON-row upsert connector
- Init SQL for a demo table and publication
- Architecture and local development notes

Not implemented yet:

- Complete `pgoutput` coverage
- WASM transform runtime
- Webhook destinations
