# macOS Capture Group Commit Benchmark - 2026-07-27

This report measures LightCDC after complete PostgreSQL source transactions
were grouped into durable redb commits. It uses the default capture group
limits: 100 transactions, 500 events, 4 MiB, or 5 ms.

## Environment

| Component | Configuration |
| --- | --- |
| Host | Apple silicon MacBook Pro, macOS 26.4 |
| PostgreSQL | PostgreSQL 17.10 in the repository Docker container |
| Rust | 1.97.1 stable, release profile |
| Workload | One 256-byte row per PostgreSQL transaction |
| Duration | 10 seconds plus a 5-15 second consumer drain |
| Clients / threads | 8 / 4 |
| Source revision | `1b7fc4b` plus the uncommitted notification and group-commit changes |

These are short, single-repetition development results rather than production
capacity claims.

## Results

| Target TPS | Actual TPS | ACK every | Generated | Captured / consumed | Storage commits | Transactions/commit | Max persist p95 | Max end-to-end p95 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 300 | 290.4 | 1 | 2,905 | 2,905 / 2,788 | 756 | 3.8 | 6.2 ms | 5.57 s |
| 300 | 294.4 | 100 | 2,942 | 2,942 / 2,942 | 820 | 3.6 | 6.3 ms | 19 ms |
| 1,000 | 982.7 | 100 | 9,827 | 9,827 / 9,827 | 860 | 11.4 | 6.6 ms | 23 ms |
| 2,000 | 2,004.0 | 100 | 20,034 | 20,034 / 20,034 | 848 | 23.6 | 7.2 ms | 25 ms |
| 5,000 | 4,942.5 | 100 | 49,410 | 49,410 / 49,410 | 875 | 56.5 | 7.8 ms | 25 ms |

No run reported dropped capture samples. With cumulative ACKs every 100 events,
capture and consumer throughput tracked the generated rate through the 5,000
TPS probe without a growing end-to-end latency trend.

At 300 TPS with one durable ACK per event, capture persisted every generated
event but the consumer remained 117 events behind when the drain window ended.
This isolates consumer offset commits as the remaining bottleneck in that
configuration.

## Comparison

Before capture group commits, the 300 TPS, one-row, ACK-every-event run captured
only about 1,757 of 2,916 generated events before shutdown and reached 9.31
seconds max interval p95. After group commits, capture persisted all 2,905
generated events in the equivalent run, although the per-event ACK consumer
still lagged.

With `ACK_EVERY=100`, group commits changed the same 300 TPS workload from a
2.74-second max interval p95 to 19 ms. At 5,000 TPS, 49,410 source transactions
required only 875 redb durability boundaries.

## Conclusion

Redb's per-commit durability cost was the primary capture limitation, but redb
itself was not the immediate event-throughput ceiling. Grouping complete source
transactions raised the observed short-run single-consumer capacity beyond
5,000 events per second while retaining atomic event and source-LSN
persistence.

The next storage optimization should batch or coalesce consumer offset commits.
Longer repetitions and a 30-minute soak are still required before treating
5,000 events per second as sustainable production capacity.
