# Production Metrics

`capture`, `run`, and `serve` maintain one fixed-cardinality metric set. When
`observability.metrics_enabled` is true, Prometheus text is available from
`GET /metrics` at `observability.metrics_addr`. The listener defaults to
`127.0.0.1:9187`; it is plaintext and must stay on a trusted monitoring
network.
`observability.metrics_max_connections` defaults to 16; excess sockets are
closed before Axum handles a request.

The capture path uses relaxed atomic increments. Commit latency records one
fixed histogram bucket per grouped redb commit, not per event. One long-lived
OS thread samples redb metadata, consumer offsets, and filesystem usage at
`metrics_sample_interval_seconds`; it remains active when HTTP scraping is
disabled because readiness depends on the sample. Rendering and allocation
happen only during a scrape. There are no dynamic labels.

## Metric Groups

- `lightcdc_runtime_state{state=...}` and `lightcdc_terminal_state` expose the
  process lifecycle.
- `lightcdc_capture_*_total` counters support capture transaction, event, and
  byte rates with Prometheus `rate()`.
- `lightcdc_storage_commit_duration_seconds` is the grouped redb commit
  histogram. `lightcdc_storage_commits_total` is its count.
- `lightcdc_durable_source_lsn_bytes`, `lightcdc_source_wal_end_bytes`, and
  `lightcdc_source_wal_lag_bytes` expose the latest observed source boundary.
- `lightcdc_source_reconnects_total` reports entered reconnect delays.
- `lightcdc_retention_*`, `lightcdc_retained_events`, and
  `lightcdc_retained_sequence_span` expose retention work and retained-log
  growth.
- `lightcdc_consumer_offsets` and `lightcdc_consumer_lag_max_sequences` expose
  durable consumer pressure. Lag is the maximum global event-sequence distance
  to the log high-water mark. Because streams filter the shared sequence, it is
  a conservative upper bound, not a count of events matching that stream.
- `lightcdc_max_durable_consumers` and the corresponding rejection counter
  expose pressure on the bounded durable consumer registry.
- `lightcdc_active_subscriptions`, `lightcdc_subscription_backpressure_total`,
  and the subscription rejection counter expose fan-out pressure.
- `lightcdc_active_api_connections` and its limit/rejection metrics expose TCP
  pressure.
- `lightcdc_active_metrics_connections` and its limit/rejection metrics expose
  Prometheus listener pressure.
- `lightcdc_storage_bytes`, `lightcdc_staging_bytes`, filesystem size/free
  gauges, and configured ceilings expose disk pressure.
- `lightcdc_storage_sample_ok` detects stale or failed resource inspection.
  `lightcdc_storage_ready` additionally requires no latched storage failure,
  data usage below the hard ceiling, and free space above the reserve.

## Starting Alerts

Tune these thresholds with the supported capacity envelope; they are safe
starting points, not universal SLOs.

- Page immediately when `lightcdc_terminal_state == 1`,
  `lightcdc_storage_sample_ok == 0` for two sample intervals, or gRPC liveness
  fails.
- Warn when readiness is false for 2 minutes; page after 10 minutes when the
  process is expected to capture continuously.
- Warn when storage exceeds 80 percent of `lightcdc_max_storage_bytes`; page at
  90 percent. Also page before filesystem available bytes reaches twice
  `lightcdc_min_free_disk_bytes` so recovery reserve remains untouched.
- Warn when source WAL lag exceeds 256 MiB for 5 minutes and page above 1 GiB or
  the source's measured WAL budget. During an outage, alert against the
  PostgreSQL slot's retained-WAL capacity rather than using only this fixed
  threshold.
- Warn when the 10-minute p99 grouped commit latency exceeds 100 ms:

  ```promql
  histogram_quantile(0.99,
    sum by (le) (rate(lightcdc_storage_commit_duration_seconds_bucket[10m])))
  > 0.1
  ```

- Alert on any increase in retention errors, replay errors, durable-consumer or
  subscription-limit rejections, or API connection-limit rejections.
- Alert when consumer lag exceeds that downstream system's replay-time budget.
  A disconnected consumer may intentionally retain its offset, so route this
  alert by the operator-owned consumer inventory rather than deleting or
  advancing offsets automatically.
- Investigate sustained staging bytes or a rising staged-transaction rate;
  these indicate source transactions larger than the in-memory threshold.
