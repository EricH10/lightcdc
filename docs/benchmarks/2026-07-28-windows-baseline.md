# Windows Baseline Benchmark — 2026-07-28

This report records a short local load-test baseline for LightCDC at commit
`b97f4467be27adc2f80eab6bb6b0845a97e34224`.

## Summary

- The highest measured stable step was a target of 300 transactions per second,
  which produced 287.95 TPS.
- At targets of 350 TPS and above, end-to-end latency and backlog grew throughout
  the active load period. Those rates only caught up during the post-load drain
  window and are not sustainable continuous rates in this environment.
- Every fixed-rate run completed without pgbench failures, dropped samples,
  reconnects, clock-skew samples, or missing timestamps.
- Every generated event was eventually captured, consumed, and acknowledged in
  the fixed-rate runs.
- This is a short development-machine baseline, not a production capacity claim.

The short-run crossover was between approximately 288 actual TPS and 345 actual
TPS.

## Environment

| Component | Configuration |
| --- | --- |
| Operating system | Windows 10 Pro 22H2, build 19045 |
| Processor | AMD Ryzen 5 9600X, 6 cores / 12 logical processors |
| Memory | 15.1 GB |
| Rust | 1.97.1 stable, `x86_64-pc-windows-gnu` |
| PostgreSQL | 17.10, native portable Windows installation |
| PostgreSQL endpoint | `localhost:5432` |
| PostgreSQL logical replication | `wal_level=logical`, `max_replication_slots=10`, `max_wal_senders=10` |
| Shell/toolchain | MSYS2 at `D:\Tools\msys64` |
| Build profile | Release |

PostgreSQL was stopped after the benchmark runs.

## Workload

The repository's `bench/run-local.sh` and `bench/run-matrix.sh` harnesses were
used with the insert workload:

- Duration: 10 seconds
- Clients: 4
- Threads: 2
- Rows per transaction: 1
- Payload: 256 compressible bytes
- Consumer acknowledgement interval: `ACK_EVERY=1`
- Post-load consumer drain allowance: 12 seconds
- Repetitions: 1 per fixed-rate step

## Fixed-rate results

| Target TPS | Actual TPS | Generated | Captured / consumed / acked | Max persist p95 | Max end-to-end p95 | Max end-to-end p99 | Max ACK p95 | Max retained WAL | Final retained WAL | Verdict |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 100 | 102.68 | 1,023 | 1,023 | 13.926 ms | 278.135 ms | 300.155 ms | 15.245 ms | 44.2 KB | 0 | Stable |
| 150 | 146.07 | 1,456 | 1,456 | 12.706 ms | 279.183 ms | 287.048 ms | 12.886 ms | 71.7 KB | 17.8 KB | Stable |
| 200 | 203.25 | 2,005 | 2,005 | 16.523 ms | 697.303 ms | 712.507 ms | 15.409 ms | 105.5 KB | 0 | Stable |
| 250 | 246.28 | 2,455 | 2,455 | 10.994 ms | 382.206 ms | 398.197 ms | 10.256 ms | 113.4 KB | 0 | Stable |
| 300 | 287.95 | 2,844 | 2,844 | 14.877 ms | 787.481 ms | 793.248 ms | 15.581 ms | 109.2 KB | 41.9 KB | Highest measured stable step |
| 350 | 345.23 | 3,441 | 3,441 | — | 2.38 s | — | — | — | 0 | Unsustainable during continuous load |
| 400 | 401.61 | 3,966 | 3,966 | 10.215 ms | 3.089 s | 3.095 s | 11.190 ms | 536.6 KB | 0.2 KB | Unsustainable during continuous load |

At the 300 TPS target, end-to-end p95 latency peaked at 787.5 ms around four
seconds into the sample, then recovered to approximately 276 ms by elapsed
second 12 while processing rates remained roughly 248–329 events per second.

At the 350 TPS target, p95 latency increased from approximately 292 ms at
elapsed second 3 to 1.43 seconds at second 10 and 2.38 seconds at second 15.
The consumer caught up only after load generation stopped.

At the 400 TPS target, p95 latency grew continuously from approximately 302 ms
at elapsed second 3 to 3.09 seconds at second 15. Retained WAL reached 536.6 KB,
and the consumer again caught up only during the drain period.

## Unthrottled overload probe

An unthrottled 10-second run generated 147,534 transactions at 14,935.00 TPS
with 0.267 ms average pgbench latency and no transaction failures. During the
approximately 22-second capture and drain window:

- 3,290 events were captured.
- 3,255 events were consumed.
- Final retained WAL was approximately 65.89 MB, with a maximum of 66.47 MB.
- End-to-end p95 and p99 latency grew to approximately 19.8 seconds.

This probe was intentionally far beyond the consumer's capacity and demonstrates
the expected backlog and WAL-retention behavior under overload. Its raw
environment metadata omitted the Git SHA because the isolated shell used for
that run did not have Git on `PATH`; the repository was clean and at
`b97f4467be27adc2f80eab6bb6b0845a97e34224`.

## Validity and limitations

The result should be treated as a directional baseline because it uses:

- One 10-second repetition per rate
- A short post-load drain window
- A compressible 256-byte payload
- One row per transaction
- Acknowledgement after every event
- A Windows desktop and native local PostgreSQL instance

CPU and RSS measurements are not valid for these runs. The MSYS2 `ps` command
could not observe the native Windows processes, so the generated `process.csv`
files contain zero values. pgbench, capture, consumer, latency, delivery, and
WAL measurements remain valid.

For a production-oriented capacity result, run at least three repetitions per
rate and a 30-minute soak test on production-like Linux and NVMe infrastructure.
Additional useful coverage includes incompressible payloads and a matrix over
`ACK_EVERY` and rows per transaction.

## Reproduction

After starting a PostgreSQL instance configured for logical replication, run:

```bash
DURATION_SECONDS=10 \
CLIENTS=4 \
THREADS=2 \
RATES="100 150 200 250 300 350 400" \
REPETITIONS=1 \
bench/run-matrix.sh
```

The overload probe can be reproduced with:

```bash
DURATION_SECONDS=10 \
CLIENTS=4 \
THREADS=2 \
RATE=0 \
bench/run-local.sh
```

The raw artifacts are intentionally excluded from Git under `bench/results/`.
The local run directories used for this report were:

- `20260728T031937Z` — unthrottled probe
- `20260728T032300Z-matrix-rate-100-run-1`
- `20260728T032300Z-matrix-rate-150-run-1`
- `20260728T032300Z-matrix-rate-200-run-1`
- `20260728T032700Z-matrix-rate-250-run-1`
- `20260728T032700Z-matrix-rate-300-run-1`
- `20260728T032700Z-matrix-rate-400-run-1`
- `20260728T033100Z-matrix-rate-350-run-1`
