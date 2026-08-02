# Redis Cache Connector

`lightcdc-redis` is an optional downstream process that turns an ordered
LightCDC stream into Redis cache invalidations or JSON row updates. It uses the
public gRPC API and does not run inside PostgreSQL capture. A Redis outage can
therefore make this consumer lag without stopping WAL capture or other
consumers.

The connector handles SIGINT and SIGTERM and exits successfully. If it is
killed after Redis applies commands but before LightCDC records the ACK, the
same event is safely replayed as described below.

## Delivery Safety

For each event, the connector maps the change to ordinary Redis `SET` and `DEL`
commands and sends them in one ordered pipeline. It acknowledges LightCDC only
after Redis reports success. Events are processed serially, and every generated
command is safe to repeat:

- replaying `DEL` leaves the key deleted;
- replaying `SET` writes the same row value and restarts its configured TTL; and
- replaying a key-changing update repeats both the old-key deletion and the
  new-key write.

A connection failure can leave only part of an event applied, and a crash can
happen after all commands succeed but before acknowledgement. In either case,
LightCDC retains the previous consumer offset and redelivers the event. Repeating
the complete command set converges to the latest ordered value before the offset
advances. During replay the cache can temporarily show an earlier value or a
partially applied key change, and a replayed upsert restarts its TTL. The
connector does not claim a distributed transaction between Redis and LightCDC.

At session startup, the connector simply subscribes with its stable consumer
name. LightCDC's durable consumer offset is the only delivery authority. A new
consumer starts at the earliest retained event, while an existing consumer
resumes after its last cumulative acknowledgement.

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

The matching LightCDC API principal only needs permission to consume this
stream. Routine connector startup does not seek the consumer:

```toml
[api]
tls_cert_file = "/run/secrets/lightcdc-server.pem"
tls_key_file = "/run/secrets/lightcdc-server-key.pem"

[[api.tokens]]
name = "redis-orders-cache"
token_file = "/run/secrets/lightcdc-api-token"
streams = ["orders"]
```

`ack_every` controls cumulative LightCDC acknowledgements. Every event is still
applied before the connector handles the next event. A crash can replay up to
that many already applied events, and their repeated commands converge to the
same cache state. Keep `ack_every` comfortably below the LightCDC event-retention
window so an outage during a partial batch cannot expire the last acknowledged
position.

`redis://` and certificate-verified `rediss://` URLs are supported by the Redis
client. The initial connector supports one standalone Redis endpoint. Redis
Cluster and Sentinel topology discovery have not yet been implemented or
validated.

## Explicit Limits

- `TRUNCATE` cannot be represented as a bounded per-row cache mutation. A
  matching truncate stops the connector without ACKing the event.
- An upsert containing an `__unchanged_toast` marker is incomplete and stops
  without ACK. Use invalidation for tables with large TOASTed values unless a
  later enrichment layer can fetch the complete row.
- Cache changes made outside this connector are outside its delivery behavior.
- LightCDC retention must exceed the longest expected Redis outage. Otherwise a
  stale connector requires a deliberate cache rebuild and seek.
- Restoring Redis does not rewind LightCDC. After Redis data loss or a point-in-
  time restore, flush or rebuild the affected cache and deliberately seek the
  stopped connector's consumer before restarting it.
- Redis and LightCDC are not one distributed transaction. A partially applied
  event is temporarily visible until redelivery repeats all of its retry-safe
  commands.

Run the live duplicate-replay test with:

```bash
docker compose up -d redis
cargo test -p lightcdc-redis \
  redis_cache_mutations_converge_when_replayed -- --ignored
```
