# macOS Segmented Retention Benchmark - 2026-07-29

This report measures capture, whole-segment retention, and one gRPC consumer
after introducing segmented redb event storage. It also verifies the fix for a
replay scan that previously stalled capture after about nine segment rotations.

## Environment

| Component | Configuration |
| --- | --- |
| Host | Apple silicon MacBook Pro, macOS 26.4 |
| PostgreSQL | PostgreSQL 17.10 in the repository Docker container |
| Rust | 1.97.1 stable, release profile |
| Workload | 100 inserted rows per PostgreSQL transaction |
| Payload | One compressible 256-byte text value per event |
| Consumer | One consumer, cumulative ACK every 1,000 events |
| Segment limit | 100,000 events or 256 MiB |
| Retention | Maximum 1,000,000 live events |
| Capture group | At most 500 events with a 20 ms delay |
| Source revision | `3ca03e3` plus the replay and segment-close changes described below |

These are short development probes on a shared laptop. They are useful for
finding the current boundary, but they are not a production capacity guarantee.

## Results

| Target events/s | Actual input | Generated / captured / consumed | Active capture average | Worst interval e2e p99 | Max retained WAL |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 30,000 | 30,135 | 1,356,000 / 1,356,000 / 1,356,000 | 29,445/s | 237 ms | 12.7 MiB |
| 40,000 | 39,860 | 1,793,600 / 1,793,600 / 1,793,600 | 38,956/s | 443 ms | 17.0 MiB |
| 50,000 | 49,845 | 1,495,200 / 1,495,200 / 1,495,200 | 43,930/s | 3,123 ms | 72.2 MiB |

The 30,000 and 40,000 event-per-second runs ended with no PostgreSQL WAL
backlog and no consumer lag. The 50,000 event-per-second run delivered every
event during its drain window, but capture accumulated several seconds of lag.
On this machine, approximately 40,000 events per second is the clean sustained
rate for this workload and approximately 50,000 events per second is burst
capacity.

The 40,000 event-per-second run retained 995,000 events in ten segment files.
Its maximum persistence p95 was 18.8 ms, maximum acknowledgement p99 was
10.2 ms, LightCDC CPU peaked at 111.5%, RSS peaked at 996 MiB, and allocated
data size peaked at 2.06 GiB.

## Replay Scan Finding

The first segmented runs stopped making practical progress near 900,000
events. PostgreSQL transaction commit LSNs can overlap row WAL LSNs during
normal concurrent traffic, which caused the capture writer to run a global
event-ID replay scan on every batch. The scan opened every segment separately
for every event and thrashed the eight-entry segment cache.

Capture now performs the global replay check only on the first batch after a
connection or replay. That batch checks all candidate event IDs while each
segment is open once. After successful reconciliation, steady-state batches
use the durable source checkpoint and active-segment uniqueness checks.
Crash-before-commit and crash-after-commit integration tests both pass.

redb also performs allocator repair and file synchronization when a database
handle closes. Sealed handles are now retired on one background OS thread so
cache eviction does not block the capture writer. A path cannot be reopened
until its prior handle has finished closing.

## Conclusion

Whole-segment deletion avoids the severe ongoing write amplification of
row-by-row retention and keeps capture near 40,000 events per second while
retention is active. The next performance work should focus on storage density,
segment cache memory, and longer soak tests. A 50,000 event-per-second sustained
claim would still require reducing rotation stalls or increasing storage
parallelism.

## Larger Segment Follow-up

A follow-up repeated the 40,000 event-per-second workload with larger segments.
The normal limit is one million events **or** 256 MiB of staged event data,
whichever comes first. This workload reaches 256 MiB around 140,000 events, so
a separate 4 GiB byte-limit run was required to isolate true million-event
segments.

| Event / byte limits | Active capture | Average / peak CPU | Peak RSS | Worst interval e2e p99 |
| --- | ---: | ---: | ---: | ---: |
| 100,000 / 256 MiB | 38,956/s | 76.0% / 111.5% | 996 MiB | 443 ms |
| 1,000,000 / 256 MiB | 39,477/s | 74.2% / 98.0% | 513 MiB | 296 ms |
| 1,000,000 / 4 GiB | 38,959/s | 73.2% / 92.6% | 365 MiB | 289 ms |

Every run captured and consumed every generated event and ended with zero WAL
and consumer lag. Larger segments materially reduced peak CPU, memory, and tail
latency, but did not improve fixed-rate throughput. This suggests that frequent
redb handle retirement and per-segment caches explain much of the resource
spikes, while steady per-event encoding and redb writes remain the throughput
limit.
