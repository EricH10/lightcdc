# Redis Cache Connector

`lightcdc-redis` is an optional downstream process that turns an ordered
LightCDC stream into Redis cache invalidations or JSON row updates. It uses the
public gRPC API and does not run inside PostgreSQL capture. A Redis outage can
therefore make this consumer lag without stopping WAL capture or other
consumers.

The connector handles SIGINT and SIGTERM, stops between atomic event
applications, and exits successfully. If it is killed after Redis commits but
before LightCDC records the ACK, the same sequence is safely replayed as
described below.

## Delivery Safety

For each event, the connector runs one Redis Lua script that atomically:

1. compares the event sequence with a connector-specific Redis progress key;
2. applies every cache mutation only when the sequence is newer; and
3. stores the newly applied sequence.

Only after that script succeeds does the connector acknowledge the event to
LightCDC. A crash after Redis commits but before LightCDC receives the ACK
causes redelivery. The Lua script observes the sequence already in Redis, skips
the duplicate mutation, and allows the connector to ACK it again. Sequence
values are stored as zero-padded decimal strings so comparison remains exact
across the full `u64` range instead of using Lua's floating-point numbers.
Noncanonical progress values and cache rules that resolve to the reserved
progress key stop before mutation rather than risking ambiguous ordering.

At session startup, the connector seeks its named LightCDC consumer to the
Redis-side sequence. This makes Redis the authority for whether a cache event
was actually applied. If Redis is restored to an older point, retained events
are replayed. Startup fails clearly if the required sequence has expired from
LightCDC retention.

## Configuration

Start LightCDC, Redis, and the connector:

```bash
docker compose up -d postgres redis
cargo run -p lightcdc-cli -- run --config lightcdc.example.toml
cargo run -p lightcdc-redis -- --config redis-connector.example.toml
```

An invalidation rule deletes every distinct key rendered from the event's key,
before, and after rows. This also removes both keys when an update changes a
primary key:

```toml
[[rules]]
table = "public.orders"
key = "order:{id}"
action = "invalidate"
```

An upsert rule stores the exact JSON `after` row and optionally sets a TTL.
Deletes still remove the cache key:

```toml
[[rules]]
table = "public.orders"
key = "tenant:{tenant_id}:order:{id}"
action = "upsert"
ttl_seconds = 3600
```

PostgreSQL text-format values are JSON strings, so `{id}` renders without JSON
quotes. Placeholders must resolve to non-null scalar fields. In production,
put credentials in environment variables or mounted secret files instead of
TOML:

```toml
[lightcdc]
endpoint = "https://lightcdc.internal:50051"
stream = "orders"
consumer = "redis-orders-cache"
token_file = "/run/secrets/lightcdc-api-token"
# tls_ca_file = "/run/secrets/lightcdc-ca.pem" # private CA only
ack_every = 100

[redis]
url_file = "/run/secrets/redis-url"
```

The matching LightCDC API principal must allow this stream and must grant seek.
Seek is required because the connector reconciles the LightCDC consumer offset
to its atomically stored Redis progress after every restart:

```toml
[api]
tls_cert_file = "/run/secrets/lightcdc-server.pem"
tls_key_file = "/run/secrets/lightcdc-server-key.pem"

[[api.tokens]]
name = "redis-orders-cache"
token_file = "/run/secrets/lightcdc-api-token"
streams = ["orders"]
allow_seek = true
```

`ack_every` controls cumulative LightCDC acknowledgements, not Redis mutation
durability. Every event is still applied together with its Redis progress before
the connector handles the next event. A crash can replay up to that many events,
which the progress comparison skips idempotently. Keep `ack_every` comfortably
below the LightCDC event-retention window so an outage during a partial batch
cannot expire the last acknowledged position.

`redis://` and certificate-verified `rediss://` URLs are supported by the Redis
client. The initial connector supports one standalone Redis deployment; Redis
Cluster is not supported because one atomic script may touch cache keys in
different hash slots.

## Explicit Limits

- `TRUNCATE` cannot be represented as a bounded per-row cache mutation. A
  matching truncate stops the connector without ACKing the event.
- An upsert containing an `__unchanged_toast` marker is incomplete and stops
  without ACK. Use invalidation for tables with large TOASTed values unless a
  later enrichment layer can fetch the complete row.
- Cache changes made outside this connector are outside its delivery guarantee.
- LightCDC retention must exceed the longest expected Redis outage. Otherwise a
  stale connector requires a deliberate cache rebuild and seek.
- Redis and LightCDC are not one distributed transaction. The Redis-side
  sequence makes crashes idempotent, but restoring only one system to a newer
  point than the other requires replay or a deliberate cache rebuild.

Run the live atomicity and duplicate-replay test with:

```bash
docker compose up -d redis
cargo test -p lightcdc-redis \
  redis_progress_and_cache_mutation_are_atomic_and_idempotent -- --ignored
```
