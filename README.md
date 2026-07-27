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
cargo run -p lightcdc-cli -- capture --config lightcdc.example.toml --max-events 3
```

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

## Current Scope

Implemented basics:

- Cargo workspace
- Core event and config types
- CLI command shape
- PostgreSQL connectivity check
- redb-backed local event store scaffold
- Logical replication stream connection
- Combined capture plus gRPC serving command
- Basic `pgoutput` relation, insert, update, and delete decoding
- Source offset persistence and idempotent duplicate replay handling
- Docker-backed integration tests for capture, abrupt process recovery, and
  PostgreSQL reconnect
- Config-defined streams
- Stream-filtered replay
- gRPC `Subscribe`, `Ack`, and `Seek`
- Local Docker Compose PostgreSQL
- Init SQL for a demo table and publication
- Architecture and local development notes

Not implemented yet:

- Complete `pgoutput` coverage
- Durable recovery policy tests
- WASM transform runtime
- Webhook destinations
