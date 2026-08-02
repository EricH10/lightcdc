# Production Capacity Baseline - 2026-08-02

This report defines the first measured single-node capacity boundary for
LightCDC. It is a conservative operating envelope, not a claim that the tested
laptop is representative of a production server or that every workload with the
same event count has the same cost.

## Supported Baseline

The initial supported continuous profile is:

| Dimension | Supported value |
| --- | --- |
| Source | One PostgreSQL 17 database and one logical replication slot |
| LightCDC topology | One process, one local redb data directory, global event order |
| Event rate | 10,000 events/second |
| Source transaction size | 100 captured rows per transaction |
| Captured payload | 256-byte compressible text field plus benchmark row metadata |
| Consumers | One ordered gRPC consumer |
| Acknowledgement cadence | One cumulative ACK per 5,000 processed events |
| Capture grouping | 1,000 events or 20 ms per durable group commit |
| Retention | 1,000,000 events and 10 GiB logical segment bytes |
| Segment bounds | 1,000,000 events, 256 MiB, or 15 minutes |
| Storage safety | 100 GiB data-directory ceiling and 1 GiB free-space reserve |

The rate is supported only when the configured transaction, event, replay,
consumer, retention, and disk limits also fit the deployment. Workloads with
larger or incompressible values, frequent TOAST values, one-row source
transactions, more consumers, smaller ACK batches, or slower storage require a
benchmark on the target system. Limits stop capture before acknowledging
PostgreSQL; operators must monitor retained WAL while correcting the condition.

## Test Environment

- MacBook Pro `Mac17,9`, Apple M5 Pro, 15 cores, 24 GB RAM
- macOS 26.4 arm64
- Docker Desktop 29.6.2 using an 8.3 GB Linux VM, 15 visible CPUs, and overlayfs
- PostgreSQL 17.10 and `pgbench` 17.10 in Docker
- LightCDC release binary running on the macOS host
- Rust 1.97.1 for benchmark builds; workspace MSRV 1.89
- Sustained-run commit `1ba7c2614a48635bf3c12d1e8e4563dc274bc9ac`
- Failure and filtering runs used that base plus the production hardening
  recorded with this report

Docker Desktop, PostgreSQL, the source workload, LightCDC, and redb all shared
one laptop. These results must not be compared directly with bare-metal Linux
or a deployment where PostgreSQL and LightCDC have separate local NVMe disks.

## Short Capacity Runs

Every run used 256-byte compressible payloads, one consumer, ACKs every 5,000
events, one-million-event retention, and a 60-second source workload unless the
row says otherwise. Generated, captured, and consumed totals matched exactly;
the benchmark metrics channel dropped no samples.

| Requested | Source transaction | Actual | Events | Persist p99 | Consumer p99 | Max WAL lag | CPU avg/max | RSS max |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 40k/s | 1 row | 9,660/s | 579,548 | 103.9 ms | 109.1 ms | 4.7 MB | 22.0% / 33.2% | 565 MB |
| 40k/s | 100 rows | 39,985/s | 2,399,000 | 100.9 ms | 226.1 ms | 17.5 MB | 48.7% / 76.9% | 517 MB |
| 50k/s | 100 rows | 49,347/s | 2,960,700 | 215.4 ms | 462.2 ms | 25.3 MB | 51.9% / 129.7% | 439 MB |
| 60k/s | 100 rows | 59,519/s | 3,571,300 | 123.6 ms | 385.1 ms | 26.1 MB | 58.5% / 112.8% | 442 MB |

The one-row case was limited by PostgreSQL in Docker producing source
transactions, not by LightCDC capture. The 50k and 60k runs demonstrate useful
headroom, but their tail latency is too variable to publish as the first
continuous operating envelope.

With retention active, physical data-directory usage approached roughly 1.7 to
1.9 GiB rather than growing with total events produced. Whole sealed segments
are unlinked; redb's logical allocated file length can be larger than physical
blocks reported by `du`.

## Fanout And Slow Consumers

A 60-second run at 20,187 events/second delivered all 1,211,100 events to each
of ten independent named consumers. Every consumer ended at sequence 1,211,100
with zero lag. Consumer p99 latency ranged from 135.0 to 156.1 ms. LightCDC CPU
averaged 84.9% and peaked at 160.3%, showing that fanout has a measurable but
bounded per-consumer cost and does not serialize capture behind the slowest
subscriber.

A separate 20k/s run used one normal consumer and a second consumer with 1 ms of
simulated processing per event. The normal consumer received all 1,195,900
events. The delayed consumer processed 26,771 events before its replay position
fell outside retention; the API rejected sequence 27,073 when the first retained
sequence was 138,701 and required an explicit seek. The abandoned consumer did
not pin storage or block the healthy consumer.

## Sustained Run

The supported 10k/s profile ran for 2,929 seconds (48 minutes 49 seconds) with
retention active before an operator-requested shutdown. It captured and
delivered exactly 29,208,400 events, averaging 9,971 events/second, with no
capture reconnects, dropped metric samples, missing commit timestamps, or
consumer sequence gap.

| Measurement | Result |
| --- | ---: |
| Capture and consumer total | 29,208,400 events each |
| CPU average / maximum | 27.4% / 46.1% |
| Maximum RSS | 673 MiB |
| Data directory maximum / final | 1.99 GiB / 1.73 GiB |
| PostgreSQL WAL lag maximum / final | 7.57 MiB / 2.14 MiB |
| Final interval persist / consumer p99 | 9.4 ms / 33.0 ms |
| Worst one-second persist / consumer p99 | 1.59 s / 1.69 s |

The worst latency intervals coincided with Rust builds competing for the same
laptop early in the run. After the first 20 minutes, the worst one-second
persist and consumer p99 values were 156.6 ms and 326.1 ms. Physical disk usage
repeatedly fell after retention removed sealed segments instead of growing with
the cumulative event total.

This was not a multi-hour soak and is not a universal production capacity
guarantee. The old runner also left its container-side `pgbench` process alive
after the manual stop; that process ran only after LightCDC and its recorded
measurement ended. The runner now records and terminates its exact container
PID on normal completion or interruption. Deployments must repeat the supported
profile on their target Linux, storage, payload, and consumer topology.

## Resilience Runs

- Five forced logical-replication backend terminations during a 10k/s workload
  produced, durably stored, and delivered exactly 293,600 events. Capture
  recorded five reconnects and zero dropped metric samples.
- An abrupt LightCDC SIGKILL eight seconds into a 10k/s workload followed by a
  restart on the same redb directory produced, stored, and delivered exactly
  303,300 unique events.
- An impossible configured free-space reserve stopped the first capture write
  with zero persisted events and left 22,556,608 bytes of PostgreSQL WAL
  retained, proving the rejected write was not acknowledged upstream.
- Existing Docker integration tests cover process kills immediately before and
  after atomic redb commit, retention expiry, PostgreSQL connection loss,
  supported schema changes, large transactions, TOAST, truncate, and source
  identity/checkpoint mismatch.

## Publication Filtering

A mixed ten-second run wrote 100,100 relevant rows and 100,100 unrelated rows
in the same source transactions. Exactly the 100,100 relevant events were
stored and delivered.

An unrelated-only run wrote 105,900 rows through a deliberately broad
publication. It stored and delivered zero events while advancing the durable
source LSN. The resulting store had zero replay markers, one segment, and 624
KiB of physical data; maximum WAL lag was 3.53 MiB. Eventless source
transactions overwrite only the checkpoint, so unrelated traffic does not
consume retention capacity.

## Reproduction

Start the benchmark PostgreSQL service, then run the supported profile:

```bash
docker compose up -d postgres

RUN_ID=production-10k-49m \
DURATION_SECONDS=3000 \
ROWS_PER_TRANSACTION=100 \
RATE=100 \
ACK_EVERY=5000 \
bench/run-local.sh
```

Run the short 40k probe with `RATE=400` and `DURATION_SECONDS=60`. The harness
writes version, configuration, PostgreSQL, process, capture, consumer, and WAL
measurements below `bench/results/<run-id>/`; those raw files are intentionally
gitignored because they can be large and machine-specific.

The focused failure and filtering commands are documented in
`bench/README.md`; none requires a soak run.

## Boundaries

- LightCDC is change-only. Initial downstream state requires an externally
  coordinated snapshot and resume LSN.
- The first production release is single-node and single-writer. Host loss
  requires restart or offline restore; it does not provide automatic failover.
- The supported profile does not include Redis Cluster, shared consumer groups,
  built-in snapshots, or transaction-parallel delivery.
- The Redis sink is an independent ordered worker. Its own Redis latency,
  operation mix, key size, and retry behavior must be capacity-tested against
  the target Redis deployment.
- Larger values and incompressible payloads consume the byte limits earlier.
  Event count alone is not a safe sizing method.
