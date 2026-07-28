# Roadmap

`lightcdc` currently has an end-to-end local MVP:

```text
PostgreSQL -> logical replication -> pgoutput decoding -> redb
    -> configured stream -> gRPC consumer -> acknowledgement
```

The MVP proves the product path, but it is not production-ready yet. Work is
ordered so correctness and recovery come before throughput and product
features.

## Milestone 2: Correctness and Crash Recovery - COMPLETE

- Persist a transaction's events and source LSN in one atomic redb transaction. DONE
- Acknowledge PostgreSQL at transaction commit boundaries. DONE
- Add crash tests around receive, persist, checkpoint, and acknowledgement.
  DONE: coverage includes pre-commit rollback, store reopen, uncommitted source
  transactions, restart after persistence without PostgreSQL acknowledgement,
  and abrupt process kills immediately before and after the atomic redb commit.
- Guarantee no lost captured row events while the replication slot, required WAL,
  and redb remain available. DONE for the currently supported row events and
  tested failure boundaries.
- Make consumer acknowledgements monotonic. DONE
- Define the consumer contract: acknowledging sequence N means every previously
  delivered matching event through N has completed successfully. DONE
- Reject acknowledgements beyond the highest sequence delivered to that consumer,
  including reconnect and stale-session behavior. DONE: delivery high-water
  marks are tracked per `(stream, consumer)`, retained across reconnects, and
  cleared by an explicit seek. Delayed acknowledgements remain valid under the
  documented simple cursor model.
- Add consumer crash tests proving unacknowledged events are redelivered. DONE
- Define contiguous acknowledgement behavior before supporting parallel consumer
  processing. DONE: the current cursor requires sequential processing and one
  active subscription per `(stream, consumer)`.
- Test large transactions, deletes, TOAST values, truncates, and schema changes.
  DONE: Docker-backed coverage includes a 1,000-row atomic transaction,
  full-row and primary-key-only deletes, unchanged TOAST markers, normalized
  truncate events, and relation refresh after adding a column.
- Track each transaction's event count and decoded byte size. DONE
- Add configurable transaction limits that fail without acknowledging PostgreSQL.
  DONE: byte and event-count bounds stop capture before redb persistence or
  PostgreSQL acknowledgement.
- Spill transactions that exceed the in-memory threshold to disk-backed staging.
  DONE: redb consumes length-prefixed staged events incrementally in one atomic
  commit, normal completion removes the file, and startup removes source-scoped
  leftovers after acquiring the replication slot.

Completion criteria met: tested abrupt restarts at the important durability
boundaries lose no supported events and recovery produces only documented
duplicates.

## Deferred Correctness and Recovery Edges

These do not block the local MVP or Milestone 3, but should be covered before a
production-stability claim:

- Add a process-kill integration test while a spill file is actively being
  written before the source transaction commits. Source-scoped orphan cleanup is
  already unit tested and process tested after source commit.
- Test staging disk exhaustion, permission failures, truncated staging records,
  and cleanup failures.
- Extend `pgoutput` coverage and schema-evolution tests beyond the currently
  supported messages and add-column case.
- Version the durable event and staging formats and define their migration policy
  before promising compatibility across releases.
- Test the configured byte and event limits at their exact boundary values.

## Planned Migrations and Backfills

- Document batched and throttled updates as the default migration strategy.
- Define how operators temporarily exclude a table from capture without treating
  the resulting downstream divergence as an accident.
- Add an explicit, audited operation for intentionally skipping to a commit LSN;
  never silently discard a transaction.
- Define a consistent snapshot and resume-LSN workflow for rebuilding downstream
  state after a skipped migration.
- Explore transaction markers so consumers can distinguish planned backfills
  from ordinary application changes.
- Ensure paused or failed capture reports replication-slot WAL retention risk.

Complete when operators can safely process, throttle, skip, or resnapshot around
a bulk migration without accidental data loss or unbounded memory and disk use.

## Milestone 3: Availability and Supervision

- Reconnect to PostgreSQL with exponential backoff and jitter. DONE
- Resume capture from the durable source LSN after reconnecting. DONE
- Separate transient failures from permanent configuration failures. DONE:
  typed PostgreSQL and replication errors distinguish retryable connection,
  resource, shutdown, and active-slot failures from configuration, auth,
  protocol, decoding, buffering, and storage failures.
- Continue serving stored events while capture is temporarily unavailable.
  IMPLEMENTED by the retrying capture supervisor and independent gRPC task;
  direct combined-runtime integration coverage remains.
- Track runtime states such as starting, capturing, retrying, and failed.
- Add graceful shutdown at a known durability boundary.

Complete when temporary PostgreSQL and network outages recover without operator
intervention.

## Milestone 4: Health and Observability

- Add gRPC health and readiness reporting.
- Report PostgreSQL connection state and reconnect count.
- Report received, persisted, and acknowledged positions.
- Report capture throughput, consumer lag, redb latency, and disk usage.
  PARTIAL: the opt-in benchmark harness records capture and consumer throughput,
  redb persistence latency, WAL lag, CPU, memory, and disk usage as JSONL/CSV;
  production health endpoints and alerts remain.
- Warn and report metrics when backfills or migrations create unusually large
  transactions or sustained staging-disk growth.
- Report active consumers and bounded-channel pressure.

Complete when an operator can distinguish a healthy, lagging, retrying, and
disk-constrained runtime without reading debug logs.

## Milestone 5: Efficient Consumer Fanout

Each subscription already runs in its own Tokio task, and duplicate active
subscriptions for one `(stream, consumer)` are rejected. Improve this fanout by
removing constant per-consumer polling:

- Notify subscribers when the durable high-water mark advances.
- Keep redb as the source of truth when notifications are missed.
- Give each subscription a bounded delivery channel.
- Batch redb reads, writes, and acknowledgements where safe.
- Move synchronous storage work onto a dedicated blocking boundary.
- Limit active subscriptions to protect memory and file descriptors.
- Define duplicate consumer-name and consumer-group behavior. DONE for the
  ordered-cursor mode; shared groups are deferred to leased-message delivery.
- Add a leased-message shared-worker mode with opaque acknowledgement IDs,
  visibility timeouts, negative acknowledgements, and bounded in-flight work,
  following Sequin's delivery approach.
- Benchmark 1, 10, 100, and 1,000 consumers. IN PROGRESS: the repeatable
  single-consumer rate, payload, transaction-size, and acknowledgement harness
  exists; concurrent-consumer matrix coverage remains.

Complete when additional consumers have measured, bounded resource costs and a
slow consumer cannot stall capture or unrelated consumers.

## Milestone 6: Retention and Slow Consumers

- Add maximum event age and storage size policies.
- Track the lowest sequence still required by active consumers.
- Define expiration behavior for abandoned consumers.
- Compact events that are no longer needed.
- Warn and shed work safely before disk exhaustion.

Complete when storage growth is bounded and stale consumers have explicit,
observable behavior.

## Milestone 7: Security and Production Configuration

- Add PostgreSQL and gRPC TLS.
- Add consumer authentication and per-stream authorization.
- Load secrets from environment variables or secret files.
- Enforce event, request, subscription, and connection limits.
- Validate publications, slots, and configured tables at startup. PARTIAL:
  `wal_level`, replication privilege, publication, slot type, `pgoutput` plugin,
  and slot database are validated; configured table validation remains.

## Milestone 8: Product Features

- Add a consistent initial table snapshot before live streaming.
- Add webhook delivery with retries and a dead-letter queue.
- Add sandboxed WASM transforms with transform versioning and replay.
- Add multiple PostgreSQL sources and additional output adapters.
- Keep the storage boundary replaceable if one-node redb storage is outgrown.

## Current Next Step

Test that the combined runtime keeps serving stored events while PostgreSQL
capture is unavailable, then add explicit runtime states and graceful shutdown
at a known durability boundary.
