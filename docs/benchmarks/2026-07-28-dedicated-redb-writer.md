# macOS Dedicated redb Writer Benchmark - 2026-07-28

This report measures capture after redb group commits moved onto one long-lived
OS thread. The PostgreSQL replication task can assemble the next group while
one storage write is in flight, but it acknowledges and submits writes strictly
in durability order.

## Environment

| Component | Configuration |
| --- | --- |
| Host | Apple silicon MacBook Pro, macOS 26.4 |
| PostgreSQL | PostgreSQL 17.10 in the repository Docker container |
| Rust | 1.97.1 stable, release profile |
| Payload | One compressible 256-byte text value per event |
| Duration | 10 seconds plus a 15-second consumer drain |
| Clients / threads | 8 / 4 |
| Source revision | `1b7fc4b` plus the uncommitted notification, group-commit, and dedicated-writer changes |

These are short development probes. They do not establish sustained production
capacity, and the 100-row workload is not directly comparable to one PostgreSQL
transaction per event.

## Final Results

Both runs used the final 20 ms group delay. The 5,000 event run used one row per
PostgreSQL transaction and acknowledged every 100 events. The 50,000 event run
used 100 rows per transaction and acknowledged every 1,000 events.

| Target events/s | Actual events/s | Generated / captured / consumed | Events/commit | Persist p95 | End-to-end p95 | Worst interval p99 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 5,000 | 4,986.8 | 49,856 / 49,856 / 49,856 | 97.4 | 8.0 ms | 29 ms | 33 ms |
| 50,000 | 48,715.4 | 487,100 / 487,100 / 487,100 | 499.6 | 19.8 ms | 391 ms | 400 ms |

Neither run reported failed PostgreSQL transactions, capture reconnects, process
errors, or dropped metric samples.

## Resource Observations

| Actual events/s | Max LightCDC CPU | Max RSS | Final redb data size | Max retained WAL |
| ---: | ---: | ---: | ---: | ---: |
| 4,986.8 | 19.4% | 66.7 MiB | 105.3 MiB | 2.3 MiB |
| 48,715.4 | 109.1% | 690.3 MiB | 1.03 GiB | 20.2 MiB |

The 50,000 target's PostgreSQL generator produced 487.2 transactions per second,
or 48,715 events per second. Capture and delivery kept up with every generated
event and PostgreSQL schedule lag averaged 0.99 ms. The source slot retained
only about 1.3 MiB in the final sample.

## Batching Timer Finding

The first dedicated-writer run retained the old 5 ms delay. With one row per
transaction it formed groups averaging only 14 events and issued about 140
durable commits per second. Those frequent fsyncs contended with Docker
PostgreSQL on the same host, and pgbench achieved only about 2,000 transactions
per second.

Increasing the delay to 20 ms produced groups averaging 97 events, reduced the
5,000-event run to about 51 storage commits per second, restored the generated
rate to 4,987 events per second, and kept worst interval p99 at 33 ms. The 20 ms
value is therefore the new default.

## Conclusion

The dedicated writer plus tuned group delay reaches the initial 50,000
events-per-second throughput target on this laptop when PostgreSQL transactions
contain 100 events. LightCDC used a little over one CPU core, so CPU is not the
immediate limit in this workload.

The next limits are end-to-end queueing latency, memory growth, and roughly
2 KiB of redb growth per small event. Before making competitive production
claims, repeat the 50,000-event workload with incompressible 200-byte payloads,
whole-run percentiles, retention enabled, and a 30-minute soak.
