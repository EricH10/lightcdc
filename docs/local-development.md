# Local Development

Start PostgreSQL:

```bash
docker compose up -d postgres
```

Check health:

```bash
docker compose ps
```

Run the CLI:

```bash
cargo run -p lightcdc-cli -- capture --config lightcdc.example.toml
```

Run capture and gRPC together for live consumers:

```bash
cargo run -p lightcdc-cli -- run --config lightcdc.example.toml --addr 127.0.0.1:50051
```

Replay captured events:

```bash
cargo run -p lightcdc-cli -- replay --config lightcdc.example.toml --from 1 --limit 10
cargo run -p lightcdc-cli -- replay --config lightcdc.example.toml --stream orders --from 1 --limit 10
```

Pretty-print a single event:

```bash
cargo run -p lightcdc-cli -- replay --config lightcdc.example.toml --from 1 --limit 1 --pretty
```

Inspect the local redb store after stopping other LightCDC processes:

```bash
cargo run -p lightcdc-cli -- inspect --config lightcdc.example.toml
cargo run -p lightcdc-cli -- inspect --config lightcdc.example.toml --sequence 1
```

The overview shows the four physical LightCDC tables, source and consumer
checkpoints, consumer lag, and recent event metadata. `--sequence` adds the full
decoded JSON for one event.

Serve gRPC:

```bash
cargo run -p lightcdc-cli -- serve --config lightcdc.example.toml --addr 127.0.0.1:50051
```

Use `serve` for an existing store when `capture` is not running. For live local development, prefer `run`; redb locks the store file, so separate `capture` and `serve` processes cannot open the same database concurrently.

Run the streaming consumer demo:

```bash
cargo run -p lightcdc-api --example consumer -- \
  --endpoint http://127.0.0.1:50051 \
  --stream orders \
  --consumer example-printer \
  --seek latest \
  --seed-sql sql/demo_orders.sql \
  --limit 32
```

That command opens the `orders` gRPC stream, moves the `example-printer`
consumer to the current end of the local event log, runs
`sql/demo_orders.sql` against Postgres, prints the new insert/update/delete
events, and acks each one after printing. Keep `lightcdc run` running in another
terminal while you run it.

Run integration tests:

```bash
cargo test -p lightcdc-cli --test capture_integration -- --ignored --test-threads=1
```

Stop services:

```bash
docker compose down
```

Remove local database state:

```bash
docker compose down -v
```
