# macOS Capture And ACK Batching Benchmark - 2026-07-29

This report tests whether larger capture commits and less frequent cumulative
consumer acknowledgements can move segmented redb storage beyond 50,000 events
per second.

## Environment

| Component | Configuration |
| --- | --- |
| Host | Apple silicon MacBook Pro, macOS 26.4 |
| PostgreSQL | PostgreSQL 17.10 in the repository Docker container |
| Workload | 100 inserted rows per PostgreSQL transaction |
| Payload | One compressible 256-byte text value per event |
| Duration | 30 seconds plus a 20-35 second drain |
| Retention | 1,000,000 events, whole-segment deletion |
| Segments | 1,000,000 events or 256 MiB |
| Consumer | One gRPC consumer |
| Source revision | Branch `perf/capture-ack-batching` based on `5076f96` |

These are short local development probes, not production capacity guarantees.
The reported p99 is the worst one-second interval rather than one whole-run
histogram.

## Results

| Input | Capture group / delay | ACK every | Result | Events/commit | Worst e2e p99 | Max WAL |
| ---: | ---: | ---: | --- | ---: | ---: | ---: |
| 50.4k/s | 500 / 20 ms | 1,000 | Overloaded | 500 | 3,410 ms | 90 MB |
| 50.2k/s | 1,000 / 20 ms | 1,000 | Caught up | 953 | 413 ms | 24 MB |
| 50.6k/s | 1,000 / 20 ms | 5,000 | Caught up | 944 | 251 ms | 23 MB |
| 60.0k/s | 1,000 / 20 ms | 5,000 | Caught up | 980 | 218 ms | 26 MB |
| 79.3k/s | 1,000 / 20 ms | 5,000 | Overloaded | 998 | 4,396 ms | 158 MB |
| 80.2k/s | 2,000 / 40 ms | 5,000 | Caught up | 1,993 | 330 ms | 40 MB |
| 101.0k/s | 2,000 / 40 ms | 5,000 | Overloaded | 1,995 | 5,683 ms | 236 MB |

Every run eventually captured and consumed every generated event with no
failed PostgreSQL transactions. "Caught up" means capture processed the load
within its active 30-second window and both PostgreSQL WAL and consumer lag
returned to zero. "Overloaded" means it required a measurable drain after the
producer stopped and accumulated multi-second delivery latency.

## Findings

At 50,000 events per second, increasing the capture group from 500 to 1,000
nearly halved capture commits, reduced maximum WAL by about 73%, and changed
worst interval p99 from 3.4 seconds to 413 ms. Acknowledging every 5,000 events
reduced consumer offset commits by about 80% and lowered p99 to 251 ms.

The balanced 1,000-event/20 ms profile sustained 60,000 events per second. The
2,000-event/40 ms profile sustained 80,000 events per second, but 100,000
events per second exceeded the single writer's capacity. This places the short
run clean boundary near 80,000 events per second for this workload and host.

The application default is now 1,000 events with a 20 ms delay. The 2,000-event
profile remains opt-in because its longer batching window raises low-load
latency. Consumer ACK frequency remains a downstream durability/redelivery
choice; the benchmark defaults to 5,000 and documents that a crash may redeliver
approximately that many events.

## Next Work

The next structural optimization should reduce per-event redb work rather than
increasing groups indefinitely. Candidates are transaction-level replay markers
instead of one event-ID index entry per event, and block records containing many
encoded events with a sparse sequence index.
