# macOS Retention Smoke Benchmark - 2026-07-28

This report verifies hard event-count retention under concurrent PostgreSQL
capture and one gRPC consumer. It is a short boundary test, not a sustained
capacity claim.

## Environment

| Component | Configuration |
| --- | --- |
| Host | Apple silicon MacBook Pro, macOS 26.4 |
| PostgreSQL | PostgreSQL 17.10 in the repository Docker container |
| Workload | 100 inserted rows per source transaction |
| Requested rate | 500 transactions/s, or 50,000 events/s |
| Payload | One compressible 256-byte text value per event |
| Consumer | One consumer, cumulative ACK every 1,000 events |
| Duration | 25 seconds plus a 30-second consumer drain |
| Retention | Maximum 1,000,000 live event payloads |
| Source revision | `1b7fc4b` plus uncommitted capture and retention changes |

## Results

The final configuration checked retention every 100 ms and removed at most
10,000 events per sweep.

| Metric | Result |
| --- | ---: |
| Actual generated rate | 50,090.7 events/s |
| Generated / captured / consumed | 1,252,100 / 1,252,100 / 1,252,100 |
| Final retained sequence range | 252,101 through 1,252,100 |
| Final live event rows | 1,000,000 |
| Consumer lag after drain | 0 |
| Maximum capture interval rate | 50,919.6 events/s |
| Post-retention capture intervals | approximately 30,000-39,000 events/s |
| Maximum persistence p95 | 20.8 ms |
| Maximum end-to-end p95 | 3,412 ms |
| Maximum retained PostgreSQL WAL | 53.9 MiB |
| Dropped metric samples | 0 |
| Final allocated redb blocks | approximately 2.07 GiB |
| Final logical redb file length | 4.0 GiB |

The live event and event-ID tables both ended at exactly 1,000,000 rows.
The durable event high-water mark remained 1,252,100, proving that pruning did
not reuse local sequences.

## Sweep Granularity

An initial run checked once per second and allowed 100,000 deletions per sweep.
Those larger writes reduced post-boundary capture intervals to roughly
31,000-37,000 events/s. Splitting the work into 10,000-event sweeps every
100 ms improved some later intervals to roughly 37,000-39,000 events/s and
slightly lowered allocated disk usage, but did not sustain the requested
50,000 events/s after retention began.

No-op sweeps now use only a redb read transaction, avoiding an unnecessary
durable commit when the log is below its retention boundary.

## Conclusion

Retention correctly bounds live rows, preserves sequence and PostgreSQL replay
safety, and gives consumers explicit expired-offset behavior. redb reuses
deleted pages, so allocated disk blocks plateaued near the retained-window
size even though the logical file did not shrink.

At this workload, continuous prefix deletion is the current throughput limit.
The next performance investigation should profile retention writes and compare
larger retention windows, range-oriented deletion strategies available in
redb, and offline compaction before making a sustained 50,000 events/s claim.
