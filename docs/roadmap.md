# Roadmap

`lightcdc` currently has an end-to-end local MVP:

```text
PostgreSQL -> logical replication -> pgoutput decoding -> redb
    -> configured stream -> gRPC consumer -> acknowledgement
```

The MVP proves the product path, but it is not production-ready yet. Work is
ordered so correctness and recovery come before throughput and product
features.

## Milestone 2: Correctness and Crash Recovery

- Persist a transaction's events and source LSN in one atomic redb transaction. DONE
- Acknowledge PostgreSQL at transaction commit boundaries. DONE
- Add crash tests around receive, persist, checkpoint, and acknowledgement.
  PARTIAL: pre-commit rollback, store reopen, uncommitted source transactions,
  and restart after persistence without PostgreSQL acknowledgement are covered;
  abrupt process-kill tests remain.
- Guarantee no lost captured row events while the replication slot, required WAL,
  and redb remain available. CAPTURE PATH COVERED; process-kill testing remains.
- Make consumer acknowledgements monotonic. DONE
- Define the consumer contract: acknowledging sequence N means every previously
  delivered event through N has completed successfully.
- Reject acknowledgements beyond the highest sequence delivered to that consumer,
  including reconnect and stale-session behavior.
- Add consumer crash tests proving unacknowledged events are redelivered.
- Define contiguous acknowledgement behavior before supporting parallel consumer
  processing.
- Test large transactions, deletes, TOAST values, truncates, and schema changes.
- Track each transaction's event count and decoded byte size.
- Add configurable transaction limits that fail without acknowledging PostgreSQL.
- Spill transactions that exceed the in-memory threshold to disk-backed staging.
- Warn and report metrics when backfills or migrations create unusually large transactions.
- Version the durable event format and define its migration policy.

Complete when abrupt restarts at each durability boundary lose no events and
recovery produces only documented duplicates.

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

- Reconnect to PostgreSQL with exponential backoff and jitter.
- Resume capture from the durable source LSN after reconnecting.
- Separate transient failures from permanent configuration failures.
- Continue serving stored events while capture is temporarily unavailable.
- Track runtime states such as starting, capturing, retrying, and failed.
- Add graceful shutdown at a known durability boundary.

Complete when temporary PostgreSQL and network outages recover without operator
intervention.

## Milestone 4: Health and Observability

- Add gRPC health and readiness reporting.
- Report PostgreSQL connection state and reconnect count.
- Report received, persisted, and acknowledged positions.
- Report capture throughput, consumer lag, redb latency, and disk usage.
- Report active consumers and bounded-channel pressure.

Complete when an operator can distinguish a healthy, lagging, retrying, and
disk-constrained runtime without reading debug logs.

## Milestone 5: Efficient Consumer Fanout

Each subscription already runs in its own Tokio task. Improve this fanout by
removing constant per-consumer polling:

- Notify subscribers when the durable high-water mark advances.
- Keep redb as the source of truth when notifications are missed.
- Give each subscription a bounded delivery channel.
- Batch redb reads, writes, and acknowledgements where safe.
- Move synchronous storage work onto a dedicated blocking boundary.
- Limit active subscriptions to protect memory and file descriptors.
- Define duplicate consumer-name and consumer-group behavior.
- Benchmark 1, 10, 100, and 1,000 consumers.

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
- Validate publications, slots, and configured tables at startup.

## Milestone 8: Product Features

- Add a consistent initial table snapshot before live streaming.
- Add webhook delivery with retries and a dead-letter queue.
- Add sandboxed WASM transforms with transform versioning and replay.
- Add multiple PostgreSQL sources and additional output adapters.
- Keep the storage boundary replaceable if one-node redb storage is outgrown.

## Current Next Step

Add fault-injection restart tests around transaction persistence and PostgreSQL
acknowledgement, followed by the PostgreSQL reconnect supervisor. Improve
consumer fanout after the durability contract is proven.
