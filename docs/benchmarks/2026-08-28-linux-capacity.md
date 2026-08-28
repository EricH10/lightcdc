# Bare-Metal Linux Capacity Probe - 2026-08-28

This report tests LightCDC's single-consumer throughput boundary on a bare-metal
Linux host. It includes short capacity probes and one 10-minute run, not a soak
test.

## Environment

- AMD Ryzen 5 9600X, 6 cores and 12 threads
- 14 GiB usable RAM
- Samsung SSD 860 SATA storage
- PostgreSQL 17.6 on the same host
- Rust 1.98.0 release build
- LightCDC commit `8690fee`

Each run used one ordered gRPC consumer, 100 captured rows per PostgreSQL
transaction, 256-byte compressible payloads, one cumulative ACK per 5,000
events, 2,000-event or 40 ms capture groups, one-million-event retention, and a
20-second source workload. Captured and consumed totals include the runner's
bounded drain period. Percentiles are the worst reported one-second interval.

## Results

| Target | Actual source rate | Generated | Captured | Consumed | Consumer p99 | Max WAL lag |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 40k/s | 40,217/s | 804,000 | 804,000 | 804,000 | 218 ms | 22.0 MB |
| 70k/s | 69,691/s | 1,393,400 | 1,393,400 | 1,393,400 | 378 ms | 41.8 MB |
| 100k/s | 100,048/s | 2,000,300 | 2,000,300 | 2,000,300 | 2.64 s | 120.8 MB |
| Unthrottled | 154,406/s | 3,088,200 | 2,954,000 | 2,950,000 | 17.0 s | 731.2 MB |

The 40k/s and 70k/s runs remained current and delivered every generated event.
At 100k/s the bounded drain delivered every event, but latency grew throughout
the source workload, so 100k/s is burst capacity rather than a healthy operating
point on this host. In the unthrottled run PostgreSQL generated 154.4k events/s,
while capture and consumer delivery peaked near 112k/s and 110k/s respectively;
the remaining events stayed upstream as retained WAL when the bounded run ended.

## Ten-Minute 60k Boundary Run

The same host and workload profile then ran with a 60k events/s target for ten
minutes:

| Measurement | Result |
| --- | ---: |
| Actual source rate | 59,521 events/s |
| Generated, captured, and consumed | 35,712,700 events each |
| Worst one-second consumer p99 | 16.2 s |
| Worst one-second persistence p95 | 793 ms |
| Maximum / final WAL lag | 432 MB / 0.4 MB |
| LightCDC CPU average / maximum | 76.4% / 81.4% |
| Maximum RSS | 500 MiB |
| Maximum physical data directory | 2.0 GiB |
| Reconnects, redeliveries, dropped samples | 0 |

The run was lossless and drained its WAL backlog, but periodic storage and
retention stalls made 60k/s latency-unstable. It is useful headroom, not a
comfortable continuous operating rate on this SATA host.

Together with the earlier complete 60-second runs, this boundary test supports
a published measured envelope of 50k events/s for the documented payload,
transaction, batching, retention, and single-consumer configuration. It leaves
margin below the unstable 60k/s boundary. It does not replace target-system
benchmarking or longer validation for a deployment's latency and durability
objectives.
