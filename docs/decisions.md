# Decisions

## Accepted

- Use stable Rust with edition 2024.
- Start with PostgreSQL only.
- Use TOML for the first configuration format.
- Use `tracing` for structured logging.
- Use redb for the first local event store and offset store.
- Use `pgwire-replication` for the logical replication stream and keep `tokio-postgres` for ordinary SQL validation.
- Decode the first `pgoutput` row changes locally in `lightcdc-postgres`.
- Keep Docker-backed integration tests ignored by default so `cargo test --workspace` remains fast and does not require external services.
- Start consumer-facing access with a local `replay` CLI before adding a network API.
- Use config-defined streams as the named feeds external consumers connect to.
- Use gRPC as the first service-to-service streaming API.
- Scope consumer offsets by stream name and consumer name.

## Pending

- Choose the durable event payload serialization format.
- Decide whether to keep Docker Compose integration tests or move to Testcontainers.
- Broaden automated integration coverage beyond the happy-path capture and resume cases.
