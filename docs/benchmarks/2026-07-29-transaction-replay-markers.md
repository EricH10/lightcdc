# Transaction Replay Marker Benchmark - 2026-07-29

This report measures replacing one capture-time redb replay-index row per event
with one replay marker per committed PostgreSQL transaction.

## Design

Each source transaction is identified by its commit/end LSN, scoped by the
configured source name. The marker, all transaction events, and the final source
checkpoint are committed in one redb write transaction. PostgreSQL's numeric
transaction ID is not the durable identity because it eventually wraps.

`ChangeEvent.event_id` remains part of the event contract for downstream
idempotency. Existing stores remain readable: the physical `event_ids` redb
table and metadata key retain their names, and reconnect recovery recognizes
legacy per-event entries.

## Environment

The environment and workload match
`docs/benchmarks/2026-07-29-capture-ack-batching.md`: an Apple silicon MacBook
Pro, PostgreSQL 17.10 in Docker, 100 inserted rows per source transaction,
30-second production, one gRPC consumer, and 5,000-event cumulative ACKs.
The 60k profile uses 1,000-event/20 ms capture groups; the 80k and 100k profiles
use 2,000-event/40 ms groups.

These short local probes compare implementations; they are not production
capacity guarantees. Latency is the worst p99 from any one-second interval.

## Results

| Input | Replay index | Result | Worst e2e p99 | Avg CPU | Peak RSS | Max WAL |
| ---: | --- | --- | ---: | ---: | ---: | ---: |
| 60k/s | Event IDs | Caught up | 218 ms | 54.2% | 517 MiB | 26 MB |
| 60k/s | Transaction markers | Caught up | 186 ms | 50.9% | 468 MiB | 29 MB |
| 80k/s | Event IDs | Caught up | 330 ms | 71.8% | 536 MiB | 40 MB |
| 80k/s | Transaction markers | Caught up | 324 ms | 59.9% | 412 MiB | 37 MB |
| 100k/s | Event IDs | Overloaded | 5,683 ms | 87.3% | 545 MiB | 236 MB |
| 100k/s | Transaction markers | Brief drain | 891 ms | 79.3% | 419 MiB | 62 MB |

The 100k marker run produced 2,985,500 events at 99,523 events/second. About
63,500 events remained when the producer stopped and were consumed during the
next one-second sample. The old implementation accumulated multi-second latency
and almost four times as much retained WAL under its comparable run.

At both 60k and 80k, allocated data-file blocks fell by roughly 5%. At 80k,
average CPU fell by 16.5% and peak RSS by 23%. Some individual persistence p99
samples regressed at those lower rates, so the useful signal is reduced resource
cost with essentially unchanged end-to-end latency rather than uniformly lower
commit latency.

For this 100-row transaction workload, capture now creates approximately one
replay row per 100 events. A 60k inspection showed 966,700 retained events and
9,671 replay markers.

## Conclusion

Transaction markers materially reduce redb write amplification and move the
short-run boundary from about 80k events/second to around 100k events/second on
this host. A conservative production target should remain below that boundary
until longer soak tests establish thermal, retention, and tail-latency behavior.

The next storage optimization should target the remaining per-event payload
table writes, likely through encoded event blocks with a sparse sequence index.
