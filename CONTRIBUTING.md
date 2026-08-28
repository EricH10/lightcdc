# Contributing to LightCDC

Thanks for helping improve LightCDC. Correctness at the PostgreSQL WAL,
durability, replay, and acknowledgement boundaries matters more than adding a
feature quickly.

## Before You Start

- Open an issue before a large behavioral change, new durable format, connector,
  or public API change.
- Keep changes scoped to one concern and preserve existing module boundaries.
- Never include production row payloads, credentials, private keys, or store
  files in an issue, test fixture, or commit.
- Report suspected vulnerabilities through GitHub private security advisories as
  described in [SECURITY.md](SECURITY.md).

## Development Setup

Install Rust 1.89 or newer and Docker with Docker Compose, then start the test
services:

```bash
docker compose up -d --wait postgres redis
```

The example configuration and SQL initialize a local-only PostgreSQL source and
Redis instance. Do not reuse those credentials outside local development.

## Required Checks

Run the same checks used by CI:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --workspace --release --locked
cargo deny check
```

Changes to capture, storage, replay, configuration, or Redis delivery should
also run the relevant Docker-backed tests:

```bash
cargo test -p lightcdc-cli --test capture_integration --locked -- --ignored --test-threads=1
cargo test -p lightcdc-redis --locked -- --ignored --test-threads=1
```

## Change Guidelines

- Add focused tests for behavioral changes and failure boundaries.
- Preserve backward compatibility for durable formats or add an explicit,
  tested migration.
- Treat PostgreSQL acknowledgement advancement as a data-safety boundary.
- Keep public errors actionable without logging row payloads or secrets.
- Update configuration examples, operator documentation, and metrics contracts
  when their behavior changes.
- Record user-visible changes in [CHANGELOG.md](CHANGELOG.md).

Benchmark changes with the harness in [bench/README.md](bench/README.md), and
include the workload configuration with any performance claim.
