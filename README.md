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
capture_batch_max_events = 500
capture_batch_max_bytes = 4194304
capture_batch_max_delay_ms = 20
```

The first reached limit flushes the group. A source transaction is never split,
and PostgreSQL is acknowledged only through the final LSN committed with the
whole group. A dedicated `lightcdc-redb-writer` OS thread owns capture writes.
While it commits one group, the Tokio replication task can decode the next
group. Capture permits only one in-flight write and waits for its successful
completion before submitting another group or acknowledging PostgreSQL.

Event-log retention is optional and enforced by maximum retained events, event
age, or whichever boundary is reached first:

```toml
retention_max_events = 10000000
retention_max_age_seconds = 604800
retention_check_interval_ms = 1000
retention_delete_batch_size = 100000
```

Omit both maximum settings to disable retention. Sweeps run on the dedicated
redb writer thread so they cannot overlap capture commits. Retention is a hard
log boundary: a durable consumer that falls behind receives an explicit expired
offset error and must seek to `earliest` or `latest`. `earliest` means the oldest
payload still retained. redb reuses deleted pages for later writes, although
the database file is not guaranteed to shrink immediately.

Run capture and the gRPC API together:

```bash
cargo run -p lightcdc-cli -- run --config lightcdc.example.toml --addr 127.0.0.1:50051
```

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
process to open this database file.

Serve the gRPC API:

```bash
cargo run -p lightcdc-cli -- serve --config lightcdc.example.toml --addr 127.0.0.1:50051
```

Use `serve` when capture is not running. For live capture plus streaming consumers, use `run` so both paths share one redb handle inside the same process.

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
- redb-backed local event store scaffold
- Logical replication stream connection
- Combined capture plus gRPC serving command
- `pgoutput` relation, insert, update, delete, and truncate decoding
- Source offset persistence and idempotent duplicate replay handling
- Configured-stream capture planning, publication-table validation,
  capture-side filtering, and logical heartbeat checkpoints
- Count- and age-based event retention with explicit stale-consumer behavior
- Bounded transaction accounting with disk-backed spill staging and crash cleanup
- Opt-in capture group-commit metrics and an end-to-end load-test harness
- Docker-backed integration tests for capture, abrupt process recovery,
  PostgreSQL reconnect, large transactions, delete identities, TOAST values,
  truncates, and relation refresh after schema changes
- Config-defined streams
- Stream-filtered replay
- gRPC `Subscribe`, `Ack`, and `Seek`
- Local Docker Compose PostgreSQL
- Init SQL for a demo table and publication
- Architecture and local development notes

Not implemented yet:

- Complete `pgoutput` coverage
- Durable event-format versioning and migrations
- WASM transform runtime
- Webhook destinations
