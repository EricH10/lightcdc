# lightcdc Load Tests

This harness measures the complete local path:

```text
pgbench -> PostgreSQL WAL -> lightcdc capture -> redb -> gRPC -> Ack
```

It is intended for repeatable comparisons and limit discovery. A result is not
portable across different CPUs, disks, PostgreSQL settings, build profiles, or
virtualization environments, so every run records its configuration and tool
versions.

## Instrumentation Overhead Policy

Normal `lightcdc` production runs maintain fixed-cardinality counters with
relaxed atomics. Capture records one clock sample and fixed histogram bucket per
durable group commit, not per event. One OS thread samples redb metadata and
filesystem state at the configured interval; Prometheus rendering allocates
only when scraped. There are no dynamic labels or per-event metric maps.

Passing `--metrics-file` opts into capture instrumentation:

- One additional nonblocking fixed-size channel send occurs per durable storage
  group commit, not per event.
- A dedicated standard thread owns all counters, rate calculations, histogram
  updates, JSON serialization, and file I/O.
- A bounded channel prevents instrumentation from applying backpressure.
  `dropped_samples_total` must be zero for a valid run.

The benchmark consumer performs its own latency histograms outside the lightcdc
runtime. `--output none` prevents event JSON serialization and terminal output
from dominating capture measurements.

## Prerequisites

- A release Rust toolchain
- The repository's PostgreSQL 17 container
- Local `psql` and `pgbench` commands, or the running PostgreSQL container
- No other process using `lightcdc_benchmark_slot` or `bench/.data`

Start PostgreSQL:

```bash
docker compose up -d postgres
```

## Quick Run

```bash
DURATION_SECONDS=10 CLIENTS=4 THREADS=2 bench/run-local.sh
```

The default workload inserts one 256-byte row per transaction at the maximum
rate `pgbench` can produce.

The benchmark config retains at most 1,000,000 event payloads. Retention work is
part of the measured storage cost, and `process.csv` should show aggregate
control-plus-segment file growth approaching a plateau after the retained
window fills. Retention unlinks whole sealed segment files, so their disk space
is returned without rewriting retained events.

Useful controls:

| Variable | Default | Meaning |
| --- | ---: | --- |
| `DURATION_SECONDS` | `60` | Workload duration |
| `CLIENTS` | `8` | Concurrent PostgreSQL sessions |
| `THREADS` | `4` | pgbench worker threads |
| `RATE` | `0` | Target transactions/second; `0` means unthrottled |
| `ROWS_PER_TRANSACTION` | `1` | Inserted CDC events per source transaction |
| `PAYLOAD_BYTES` | `256` | Text payload size |
| `ACK_EVERY` | `5000` | Events processed per cumulative acknowledgement |
| `CONSUMER_COUNT` | `1` | Independent named consumers replaying every event |
| `SLOW_CONSUMER_DELAY_MICROS` | `0` | Per-event processing delay for the final consumer |
| `EXPECTED_SLOW_CONSUMER_EXPIRATION` | `false` | Require only the delayed consumer to cross retention and fail explicitly |
| `CAPTURE_BATCH_MAX_EVENTS` | `1000` | Soft event limit for one capture storage commit |
| `CAPTURE_BATCH_MAX_DELAY_MS` | `20` | Maximum capture group-commit delay |
| `WORKLOAD` | `insert` | `insert` or `update` |
| `PRELOAD_ROWS` | `100000` | Rows prepared for the update workload |

`ACK_EVERY=5000` is at-least-once safe because each acknowledgement is
cumulative and durable. A consumer crash can redeliver up to roughly 5,000
events, so handlers must remain idempotent. Use a smaller value when the
redelivery window matters more than acknowledgement throughput.

The generated payload is intentionally compressible. Add an incompressible
payload scenario before using these results to size TOAST-heavy production
traffic.

To prove that a healthy consumer remains current while a slow consumer crosses
the retained replay window, run a multi-consumer test with a processing delay:

```bash
CONSUMER_COUNT=2 \
SLOW_CONSUMER_DELAY_MICROS=1000 \
EXPECTED_SLOW_CONSUMER_EXPIRATION=true \
ROWS_PER_TRANSACTION=100 \
RATE=200 \
bench/run-local.sh
```

The run succeeds only when the fast consumer finishes normally and the delayed
consumer receives the API's actionable `consumer offset expired` error. This
models an application processing delay before acknowledgement; it does not
artificially delay network reads after processing.

For the high-throughput capture profile, use:

```bash
CAPTURE_BATCH_MAX_EVENTS=2000 \
CAPTURE_BATCH_MAX_DELAY_MS=40 \
ACK_EVERY=5000 \
bench/run-local.sh
```

The extra delay is a throughput/latency tradeoff and is not the default.

## Measure Commit And ACK Cost

Run the normalized four-way matrix:

```bash
DURATION_SECONDS=10 \
EVENT_RATES="300" \
bench/run-cost-matrix.sh
```

For each requested event rate it crosses:

| Rows/transaction | `ACK_EVERY` | Capture commits | Consumer offset commits |
| ---: | ---: | --- | --- |
| 1 | 1 | One per event | One per event |
| 1 | 100 | One per event | One per 100 events |
| 100 | 1 | One per 100 events | One per event |
| 100 | 100 | One per 100 events | One per 100 events |

The runner converts the requested event rate into a transaction rate for each
row count. Integer rounding is reflected in `target_events_per_second` in the
summary. It writes and prints
`bench/results/<matrix-id>-summary.csv`, including generated, captured, and
consumed totals; source transaction and storage commit counts; events per
storage commit; actual event rate; persistence, end-to-end, and ACK p95
latencies; retained WAL; and dropped metric samples.

Useful cost-matrix controls:

| Variable | Default | Meaning |
| --- | --- | --- |
| `EVENT_RATES` | `300` | Space-separated target CDC event rates |
| `ROWS_PER_TRANSACTION_VALUES` | `1 100` | Source transaction sizes |
| `ACK_EVERY_VALUES` | `1 100` | Consumer acknowledgement batch sizes |
| `REPETITIONS` | `1` | Runs per combination |

Use `EVENT_RATES="300 600 1200"` to compare how each write pattern approaches
its sustainable limit.

## Find The Sustainable Limit

Run fixed-rate steps before an unthrottled run:

```bash
RATES="500 1000 2500 5000 10000 0" \
REPETITIONS=3 \
DURATION_SECONDS=120 \
bench/run-matrix.sh
```

The sustainable limit is the highest fixed rate where:

- PostgreSQL retained WAL does not grow continuously.
- Capture and consumer event rates converge on the generated event rate.
- `dropped_samples_total` and process errors remain zero.
- p95 and p99 latency settle rather than increasing throughout the run.
- CPU, memory, and disk usage remain acceptable for the target environment.

Use at least a 30-minute soak after locating the approximate limit.

## Results

A dated baseline report from before commit notifications replaced polling is
available at
[Windows baseline benchmark — 2026-07-28](../docs/benchmarks/2026-07-28-windows-baseline.md).
The first normalized write-cost comparison is recorded in
[macOS commit cost benchmark — 2026-07-27](../docs/benchmarks/2026-07-27-macos-commit-cost.md).
The capture group-commit follow-up is recorded in
[macOS capture group commit benchmark — 2026-07-27](../docs/benchmarks/2026-07-27-capture-group-commits.md).
The pipelined storage follow-up is recorded in
[macOS dedicated redb writer benchmark — 2026-07-28](../docs/benchmarks/2026-07-28-dedicated-redb-writer.md).
The first bounded-log run is recorded in
[macOS retention smoke benchmark — 2026-07-28](../docs/benchmarks/2026-07-28-retention-smoke.md).

Each run creates `bench/results/<run-id>/` containing:

- `environment.txt`: Git state, tool versions, and scenario parameters.
- `pgbench.log`: generated transaction rate, schedule lag, and SQL latency.
- `capture.jsonl`: redb persistence throughput and latency percentiles.
- `consumer.jsonl`: delivery, end-to-end, and acknowledgement percentiles.
- `postgres.csv`: WAL retention, slot activity, and PostgreSQL decoding spills.
- `process.csv`: lightcdc CPU, resident memory, and local data size.
- `lightcdc.log`, `consumer.log`, and sampler logs for diagnosis.

The runner recreates the dedicated `lightcdc_benchmark_slot` after setup so
capture, consumer, and generated totals begin at the same WAL boundary. It does
not modify the development `lightcdc_slot`.

End-to-end latency compares the PostgreSQL commit timestamp with the consumer's
wall clock. Keep machines synchronized with NTP when components run on separate
hosts; negative samples are counted as `clock_skew_samples_total`.

## Environment Progression

1. Use a quiet desktop with a stable SSD for development baselines.
2. Repeat important tests on a fixed, non-burstable Linux server with local
   NVMe.
3. For production-style numbers, place PostgreSQL and lightcdc on separate
   hosts and run pgbench close to PostgreSQL.

Do not compare Docker Desktop results directly with bare-metal Linux results.
Use flamegraphs or Tokio Console only after a normal run identifies a
bottleneck, since profilers change the measured workload.

Primary tool references:

- [PostgreSQL pgbench](https://www.postgresql.org/docs/17/pgbench.html)
- [PostgreSQL monitoring statistics](https://www.postgresql.org/docs/17/monitoring-stats.html)
- [cargo-flamegraph](https://github.com/flamegraph-rs/flamegraph)
- [Tokio Console](https://github.com/tokio-rs/console)
