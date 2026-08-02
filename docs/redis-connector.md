# Redis Sink

The Redis adapter is an optional built-in sink that turns an ordered LightCDC
stream into Redis cache invalidations or JSON row updates. `lightcdc run` starts
it in the same process as capture and gRPC; there is no connector executable or
internal gRPC hop.

The shared sink runtime owns replay, batching, durable offsets, retry backoff,
and shutdown. The Redis adapter only maps canonical events and applies the
resulting commands. A Redis outage makes this sink lag while PostgreSQL capture,
gRPC consumers, and other sink workers continue.

## Delivery Safety

The runtime reads a bounded source batch, keeps only events matching the sink's
configured stream, and sends all generated `SET` and `DEL` commands in one
Redis pipeline. It advances the durable `sink:<name>` offset only after Redis
reports success. Every generated command is safe to repeat:

- replaying `DEL` leaves the key deleted;
- replaying `SET` writes the same row value and restarts its configured TTL; and
- replaying a key-changing update repeats both the old-key deletion and the
  new-key write.

A connection failure can leave part of a pipeline applied, and a crash can
happen after Redis succeeds but before redb records the new offset. LightCDC
then retries the complete batch. Its idempotent commands converge to the latest
ordered value, although replay can temporarily expose an earlier value or a
partially applied key change. Redis and redb do not form a distributed
transaction.

An existing sink resumes after its last durable offset. A newly named sink
starts at the earliest retained event. Renaming a sink therefore creates a new
delivery identity rather than renaming its old offset.

## Configuration

Define the sink in the main LightCDC configuration under a stream that contains
every rule table:

```toml
[[streams]]
name = "orders"
source = "default"
tables = ["public.orders"]

[[sinks]]
name = "orders-cache"
stream = "orders"
batch_max_events = 500
batch_max_bytes = 16777216
retry_initial_ms = 250
retry_max_ms = 15000

[sinks.destination]
type = "redis"
url = "redis://127.0.0.1:6379"
max_commands_per_batch = 10000

[[sinks.destination.rules]]
table = "public.orders"
key = "order:{id}"
action = "invalidate"
```

Start all required services and run the combined process:

```bash
docker compose up -d postgres redis
cargo run -p lightcdc-cli -- run --config lightcdc.example.toml
```

An invalidation rule deletes every distinct key rendered from the event's key,
before, and after rows. This also removes both keys when an update changes a
primary key. An upsert rule stores the exact JSON `after` row and optionally
sets a TTL; deletes still remove the key:

```toml
[[sinks.destination.rules]]
table = "public.orders"
key = "tenant:{tenant_id}:order:{id}"
action = "upsert"
ttl_seconds = 3600
```

PostgreSQL text-format values are JSON strings, so `{id}` renders without JSON
quotes. Placeholders must resolve to non-null scalar fields. Configure exactly
one URL source. Use an environment variable or mounted secret file for
production credentials:

```toml
[sinks.destination]
type = "redis"
url_file = "/run/secrets/redis-url"
max_commands_per_batch = 10000
```

`batch_max_events` and `batch_max_bytes` bound source replay. One event can
expand through multiple rules, so `max_commands_per_batch` independently bounds
the final Redis pipeline. A batch that exceeds that hard command limit is a
terminal configuration error and remains unacknowledged.

`redis://` and certificate-verified `rediss://` URLs are supported by the Redis
client. The initial adapter supports one standalone Redis endpoint. Redis
Cluster and Sentinel topology discovery have not yet been implemented or
validated.

## Explicit Limits

- `TRUNCATE` cannot be represented as a bounded per-row cache mutation. A
  matching truncate stops `run` without advancing the sink offset.
- An upsert containing an `__unchanged_toast` marker is incomplete and stops
  without advancing the offset. Use invalidation for tables with large TOASTed
  values unless a later enrichment layer can fetch the complete row.
- Cache changes made outside this sink are outside its delivery behavior.
- Retention must exceed the longest expected Redis outage. Otherwise a stale
  sink requires a deliberate cache rebuild and offset repair.
- Restoring Redis does not rewind LightCDC. After Redis data loss or a
  point-in-time restore, flush or rebuild the affected cache and deliberately
  repair the stopped sink's offset before restarting it.
- The first runtime is serial per sink to preserve order. Separate configured
  sinks run concurrently, but one sink does not yet partition a stream across
  workers.

Run the live duplicate-replay test with:

```bash
docker compose up -d redis
cargo test -p lightcdc-redis -- --ignored --test-threads=1
```
