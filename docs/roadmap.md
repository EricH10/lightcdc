# Roadmap

`lightcdc` currently has an end-to-end local MVP:

```text
PostgreSQL -> logical replication -> pgoutput decoding -> redb
    -> configured stream -> gRPC consumer -> acknowledgement
```

The MVP proves the product path, but it is not production-ready yet. Work is
ordered so correctness and recovery come before throughput and product
features.

## Production Readiness Gate

The first production claim should stay intentionally narrow: one LightCDC
process, one PostgreSQL source and replication slot, one local durable redb
store, config-defined streams, and ordered gRPC consumers. High availability,
shared worker groups, transforms, webhooks, and additional source databases are
valuable later features, not requirements for an honest single-node release.

The following are release gates, even when the broader milestone containing
them has optional work remaining.

### Data Correctness and Compatibility

- Preserve source transactions, local events, and the source checkpoint
  atomically. DONE for currently supported `pgoutput` row events.
- Persist and validate PostgreSQL source identity, not only database, slot, and
  publication names, so a replaced or restored source cannot silently continue
  against old local state. DONE with the cluster system identifier and database
  OID durably bound to the configured source name.
- Reconcile the local durable source LSN with the slot's
  `confirmed_flush_lsn` before capture. Refuse startup with a clear data-gap
  error when PostgreSQL can no longer replay the local missing range. DONE.
- Publish and enforce a supported PostgreSQL and `pgoutput` feature matrix,
  including partitioned tables, replica identity modes, schema changes,
  publication changes, messages, streaming transactions, and prepared
  transactions. Unsupported modes must fail before acknowledging affected WAL.
- Derive the union of tables required by configured durable streams and push
  that selection into the PostgreSQL publication. Capture must not depend on
  which consumers happen to be connected, and startup must reject missing
  required tables while reporting unnecessary published tables. DONE for the
  compiled plan and operator-managed publication validation.
- Add a capture-side table filter as defense in depth so an intentionally broad
  publication does not fill redb with events no configured stream can consume.
  Persist checkpoint-only source transactions when every event is filtered.
  DONE.
- Advance safely across long periods containing only unrelated source changes.
  Use a transaction-boundary checkpoint or logical heartbeat rather than
  acknowledging a keepalive WAL end that may include uncommitted relevant work.
  DONE with configurable transactional logical heartbeats.
- Provide a consistent bootstrap procedure: either a built-in initial snapshot
  or a documented external snapshot plus resume-LSN workflow. A change-only
  release must say so prominently.
- Version the durable event, metadata, consumer-offset, and staging formats.
  Test forward migrations and reject unsupported downgrade or newer formats
  without mutating the store. PARTIAL: the control database and segment headers
  are versioned; legacy single-file migration and mutation-free rejection of a
  newer control format are tested. Control format v2 adds source identity with
  a tested v1 forward migration. Event payload and staging migrations remain.
- Test the no-loss boundary across process kill, host restart, PostgreSQL
  restart, network interruption, storage failure, retention, and supported
  schema changes. PARTIAL: process, reconnect, transaction, retention, and
  selected schema-change boundaries are covered.

### Recovery and Resource Safety

- Shut down on SIGINT and SIGTERM at a known durability boundary: stop accepting
  new work, finish or replay the current source transaction, resolve the
  in-flight redb write, acknowledge only durable LSNs, drain or close gRPC
  streams, and honor a tested timeout. DONE: capture cancellation leaves the
  partially decoded transaction unacknowledged for replay, the redb writer
  resolves accepted work, gRPC streams drain, and the combined runtime has a
  SIGTERM process test. `shutdown_timeout_ms` bounds server and writer drain.
- Define and test backup and restore for the redb store together with PostgreSQL
  slot state. Document the no-loss boundary, recovery point objective, recovery
  time objective, and what happens when local storage is permanently lost.
  DONE for offline checksummed backup/restore with a tested round trip and
  documented PostgreSQL WAL/slot coordination boundary.
- Detect redb corruption and incompatible files at startup, fail without
  destructive repair, and provide a tested integrity-check and recovery
  procedure. DONE with catalog/format checks at open and an offline deep check
  that traverses event payloads, metadata, identities, and offsets; corruption
  and tampered-backup fixtures fail before restore publication.
- Enforce byte-size retention, staging-space limits, minimum free-space
  thresholds, and a reserved emergency margin. Stop or shed work before disk
  exhaustion and surface the resulting PostgreSQL WAL-retention risk. DONE for
  logical segment-byte retention, a hard total data-directory ceiling, and a
  free-space reserve checked during staging and immediately before commit.
- Classify storage and retention failures as healthy, degraded, retryable, or
  terminal instead of logging every retention failure and retrying forever.
  DONE for capture writes and retention: resource/storage failures stop capture
  without acknowledging PostgreSQL; source reconnect classification remains
  independent.
- Move consumer replay, ACK, and seek storage operations off Tokio worker
  threads. PARTIAL: capture, retention, ACK, and seek mutations share the
  dedicated storage thread; replay reads still need a bounded reader pool.
- Bound active subscriptions, per-connection buffers, request and consumer-name
  sizes, outbound event sizes, and total memory. Validate every configured
  numeric limit and reject zero, contradictory, or ineffective settings.
  PARTIAL: active subscriptions, per-subscription channels, consumer names, and
  outbound events are bounded; transport request limits and aggregate replay
  memory remain.
- Remove or implement inert configuration fields such as `channel_capacity`;
  production configuration must not appear to control behavior that ignores it.
  DONE: `channel_capacity` controls each subscription's outbound queue.

### Security and API Safety

- Add PostgreSQL TLS with certificate verification and gRPC TLS, with secure
  production defaults and an explicit local-development opt-out. DONE:
  PostgreSQL defaults to verify-full using platform or configured CA roots;
  plaintext PostgreSQL and gRPC require explicit local configuration.
- Add gRPC authentication and per-stream authorization before binding beyond
  localhost. Protect administrative operations such as seek separately from
  ordinary subscribe and ACK access. DONE with bearer principals, stream
  allowlists, constant-time token checks, and separate seek permission.
- Load passwords, keys, and tokens from environment variables or secret files;
  avoid requiring plaintext secrets in the main TOML file. DONE for PostgreSQL,
  the gRPC API, and the Redis connector.
- Sanitize external gRPC errors so storage paths, database details, and internal
  failures are logged server-side without being returned to untrusted clients.
  DONE for internal storage failures.
- Default production capture output to no event payloads so row data is not
  accidentally written to logs and capture throughput is not silently reduced.
  DONE; JSON output requires explicit CLI opt-in.
- Document a least-privilege PostgreSQL role and test it in integration tests.

### Operations, Releases, and Support

- Expose the standard gRPC health service and separate liveness from readiness.
  Readiness must reflect capture state, storage writability, source continuity,
  and whether serving retained events is still safe. PARTIAL: standard named
  liveness/readiness services reflect lifecycle and source reconnect state;
  explicit disk-pressure and integrity signals remain.
- Export low-overhead production metrics for source LSN and WAL lag, capture
  rate, redb latency, retention lag, consumer lag, reconnects, subscription
  pressure, disk and staging usage, and terminal state. Define actionable alert
  thresholds and do not rely on benchmark-only JSONL metrics.
- Add structured runtime states and stable exit behavior so supervisors can
  distinguish starting, capturing, retrying, degraded, draining, and terminal
  configuration or data-loss failures. PARTIAL: shared states now drive health
  and clean/terminal exits; production metrics still need state transitions.
- Provide a production container or release binaries that run as a non-root
  user, use a persistent volume, handle signals, expose health checks, and pin
  supported Rust, OS, architecture, PostgreSQL, and redb versions.
- Add required CI for formatting, clippy, unit tests, Docker-backed PostgreSQL
  tests, release builds, durable-format migration fixtures, and the minimum
  supported Rust version.
- Add dependency vulnerability and license checks, automated dependency updates,
  release versioning, changelog and upgrade notes, and checksums or provenance
  for distributed artifacts.
- Write an operator runbook for installation, upgrades, rollback, backup,
  restore, slot loss, source failover, WAL growth, disk pressure, stale
  consumers, corruption, and collecting diagnostics without exposing row data.
- Declare the single-node availability boundary. A first release may require
  restart or restore after host loss; active-passive failover and shared storage
  do not block release if that limitation and recovery procedure are explicit.

### Capacity Sign-Off

- Establish a supported capacity envelope by payload size, source transaction
  size, retention window, consumer count, acknowledgement frequency, and disk.
- Run multi-hour soak tests with retention active and verify bounded memory,
  bounded allocated disk, stable latency, no sequence gaps, and no growing
  PostgreSQL WAL lag.
- Exercise slow and disconnected consumers, PostgreSQL outages, process restarts,
  disk pressure, and reconnect storms during load.
- Drive sustained unrelated-table writes and verify they do not consume redb
  retention capacity, materially reduce useful capture throughput, or cause
  unbounded replication-slot WAL retention while relevant tables are idle.
- Restore append-like sustained throughput while retention is active, or publish
  a lower measured limit. A production release needs predictable capacity, not
  maximum possible benchmark throughput.

Production-ready means every item above is complete or has an explicit,
documented limitation that does not permit silent data loss, unauthorized
access, or unbounded resource growth. It does not require completing every
feature milestone below.

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
- Persist PostgreSQL source identity and reject source replacement, stale local
  restore, or a slot checkpoint ahead of the durable local source LSN.
- Add restart and restore fixtures that exercise compatible migrations,
  incompatible formats, corruption, and local/source checkpoint divergence.

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
  DONE, including draining and degraded states.
- Add graceful shutdown at a known durability boundary. DONE.

Complete when temporary PostgreSQL and network outages recover without operator
intervention.

## Milestone 4: Health and Observability

- Add gRPC health and readiness reporting. DONE for lifecycle/source state;
  disk and detailed lag readiness remain.
- Report PostgreSQL connection state and reconnect count.
- Report received, persisted, and acknowledged positions.
- Report slot `confirmed_flush_lsn`, retained WAL bytes, `wal_status`,
  `safe_wal_size`, and source/local checkpoint divergence.
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
subscriptions for one `(stream, consumer)` are rejected. Constant
per-consumer polling has been removed:

- Notify subscribers when the durable high-water mark advances. DONE: `run`
  shares a Tokio watch notifier between capture and gRPC.
- Keep redb as the source of truth when notifications are missed. DONE:
  notifications carry no payload and only trigger another ordered redb replay.
- Give each subscription a bounded delivery channel. DONE.
- Batch redb reads, writes, and acknowledgements where safe. PARTIAL: capture
  now group-commits complete source transactions using count, event, byte, and
  time limits; consumer acknowledgement batching remains client-controlled.
- Move synchronous storage work onto a dedicated blocking boundary. PARTIAL:
  capture pipelines one in-flight group through a long-lived redb writer thread
  while the Tokio replication task assembles the next group; gRPC replay,
  acknowledgement, and seek still call redb from Tokio tasks.
- Limit active subscriptions to protect memory and file descriptors.
- Define duplicate consumer-name and consumer-group behavior. DONE for the
  ordered-cursor mode; shared groups are deferred to leased-message delivery.
- Add a leased-message shared-worker mode with opaque acknowledgement IDs,
  visibility timeouts, negative acknowledgements, and bounded in-flight work,
  following Sequin's delivery approach.
- Benchmark 1, 10, 100, and 1,000 consumers. IN PROGRESS: the repeatable
  single-consumer rate, payload, transaction-size, and acknowledgement harness
  includes a normalized capture-commit/acknowledgement cost matrix and a short
  48.7k events/second dedicated-writer probe; concurrent-consumer matrix and
  sustained incompressible-payload coverage remain.

Complete when additional consumers have measured, bounded resource costs and a
slow consumer cannot stall capture or unrelated consumers.

## Milestone 6: Retention and Slow Consumers

- Add maximum event age and storage size policies. DONE for maximum age,
  retained event count, and whole-segment logical byte limits.
- Track the lowest sequence still required by active consumers. SUPERSEDED for
  the initial hard-retention model: abandoned consumers do not pin disk.
- Define expiration behavior for abandoned consumers. DONE: an expired offset
  fails explicitly and requires a seek to the retained prefix or latest.
- Compact events that are no longer needed. DONE with immutable event segments
  and whole-file deletion; retained events are not rewritten.
- Restore append-like sustained throughput while retention is active. PARTIAL:
  count-, byte-, and age-bounded redb segments now make retention a whole-file
  operation. Transaction-level replay markers now replace the per-event capture
  index and moved the short local benchmark boundary from about 80k to around
  100k events/second. Longer sustained retention benchmarks remain. Consider
  encoded event blocks or multiple writable shards only when measured demand
  justifies their added ordering and replay complexity.
- Warn and shed work safely before disk exhaustion. DONE by refusing the next
  source commit or staging record while preserving the configured free-space
  reserve; PostgreSQL WAL acknowledgement does not advance.

Complete when storage growth is bounded and stale consumers have explicit,
observable behavior.

## Milestone 7: Security and Production Configuration

- Add PostgreSQL and gRPC TLS. DONE.
- Add consumer authentication and per-stream authorization. DONE.
- Load secrets from environment variables or secret files. DONE.
- Enforce event, request, subscription, and connection limits.
- Sanitize public API errors and default to not logging captured row payloads.
- Validate publications, slots, and configured tables at startup. PARTIAL:
  `wal_level`, replication privilege, publication, slot type, `pgoutput` plugin,
  slot database, configured stream/publication table alignment, and
  unnecessary-table reporting are validated; broader supported-feature
  validation remains.

## Milestone 8: Product Features

- Add a consistent initial table snapshot before live streaming.
- Add webhook delivery with retries and a dead-letter queue.
- Add sandboxed WASM transforms with transform versioning and replay.
- Add multiple PostgreSQL sources and additional output adapters.
- Maintain the optional Redis cache connector and extend its explicit truncate,
  TOAST-upsert, and Redis Cluster boundaries only with safe semantics. INITIAL
  standalone connector DONE for atomic invalidation/upsert plus replay-safe
  Redis progress.
- Keep the storage boundary replaceable if one-node redb storage is outgrown.

## Current Next Step

Finish the combined-runtime outage test, explicit runtime states, graceful
shutdown, and standard health/readiness service. Then enforce subscription and
disk bounds before returning to retention throughput work.
