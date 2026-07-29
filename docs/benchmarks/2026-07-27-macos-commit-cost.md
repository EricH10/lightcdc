# macOS Commit Cost Benchmark - 2026-07-27

This report measures the cost of durable capture commits and consumer
acknowledgement commits after replacing the gRPC subscriber's 250 ms redb poll
with post-commit notifications.

This is the pre-group-commit baseline. The follow-up is recorded in
[Capture Group Commit Benchmark](2026-07-27-capture-group-commits.md).

## Environment

| Component | Configuration |
| --- | --- |
| Host | Apple silicon MacBook Pro, macOS 26.4 |
| PostgreSQL | PostgreSQL 17.10 in the repository Docker container |
| Rust | 1.97.1 stable, release profile |
| Duration | 10 seconds plus a 5-second consumer drain |
| Clients / threads | 8 / 4 |
| Payload | 256 compressible bytes |
| Requested rate | 300 CDC events per second |
| Source revision | `1b7fc4b` plus the uncommitted notification and cost-matrix changes |

The run is a short directional development benchmark, not a production capacity
claim. The 100-row scenarios execute only about 2-3 source transactions per
second, so longer repetitions are needed for precise throughput comparisons.

## Cost Matrix

| Rows/transaction | ACK every | Actual events/s | Generated | Captured / consumed | ACK commits | Max persist p95 | Max end-to-end p95 | Max retained WAL |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 291.4 | 2,916 | 1,757 | 1,757 | 13.4 ms | 9.31 s | 910 KB |
| 1 | 100 | 292.9 | 2,928 | 2,928 | 30 | 6.1 ms | 2.74 s | 441 KB |
| 100 | 1 | 272.6 | 2,600 | 2,600 | 2,600 | 13.6 ms | 2.05 s | 215 KB |
| 100 | 100 | 209.1 | 2,000 | 2,000 | 20 | 7.7 ms | 18.0 ms | 172 KB |

The one-row, ACK-every-event scenario did not catch up during the drain window.
Batching ACKs allowed the same one-row capture path to deliver every generated
event and roughly doubled its observed peak capture rate. Batching source rows
also removed most capture commits, but ACK-every-event still serialized 2,600
consumer offset commits.

With both sides batched, end-to-end p95 stayed near 18 ms. Its lower actual
event rate is caused by the small sample of low-rate, 100-row source
transactions and should not be read as its capacity ceiling.

## Polling Comparison

An additional apples-to-apples run used one row per transaction,
`ACK_EVERY=1`, and a 100 TPS target:

| Version | Actual TPS | Captured / consumed | Max end-to-end p95 |
| --- | ---: | ---: | ---: |
| Windows baseline with 250 ms polling | 102.7 | 1,023 | 278.1 ms |
| macOS Docker run with commit notifications | 103.3 | 1,032 | 82.1 ms |

The environments differ, so the size of the improvement is directional.
However, the new result has no 250 ms latency floor, and every event completed
with no dropped metric samples.

## Conclusion

Commit notifications materially improve live-delivery latency but do not remove
the throughput ceiling. Durable redb commits and its single-writer contention
are now the dominant costs. The next optimization experiments should batch
consumer acknowledgements by default and move synchronous redb work behind a
dedicated storage task before considering more invasive storage changes.
