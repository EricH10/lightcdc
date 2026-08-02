# Operations Runbook

This runbook covers the first supported deployment boundary: one LightCDC
process, one PostgreSQL source and logical slot, one local persistent volume,
and ordered named consumers. It is a single-node service; restart and restore,
not automatic failover, are the current availability mechanism.

## Supported Deployment

The first release supports PostgreSQL 17.x with the feature boundaries in
`docs/postgres-support.md`, redb 4.1, and one Linux x86_64 LightCDC process.
Release archives target `x86_64-unknown-linux-gnu`. The production image builds
with Rust 1.89.0 and runs on Debian 12 Bookworm as the non-root UID/GID 10001.
The lockfile is part of the release input; do not rebuild a release after
changing it.

Build and inspect the image:

```bash
docker build -t lightcdc:0.1.0 .
docker run --rm lightcdc:0.1.0 --version
docker run --rm --entrypoint id lightcdc:0.1.0
```

Set `storage.data_dir = "/var/lib/lightcdc"` in the production config, then
mount that path from persistent storage. Mount config and secrets read-only;
the example below assumes the configured TLS and password files live under
`/run/secrets`:

```bash
docker run --name lightcdc --init \
  --stop-timeout 30 \
  --publish 50051:50051 \
  --publish 127.0.0.1:9187:9187 \
  --mount type=volume,source=lightcdc-data,target=/var/lib/lightcdc \
  --mount type=bind,source=/etc/lightcdc,target=/etc/lightcdc,readonly \
  --mount type=bind,source=/run/secrets/lightcdc,target=/run/secrets,readonly \
  lightcdc:0.1.0 run \
  --config /etc/lightcdc/lightcdc.toml \
  --addr 0.0.0.0:50051
```

Use a Docker stop timeout longer than `runtime.shutdown_timeout_ms`. The binary
is PID 1 under the image's exec-form entrypoint and handles SIGINT/SIGTERM;
`--init` additionally reaps unexpected child processes.

The gRPC port implements the standard health protocol. Probe
`lightcdc.liveness` to decide whether to restart the process and
`lightcdc.readiness` before routing consumers. Readiness is intentionally false
while capture is starting, reconnecting, degraded, draining, or failed. The
probe client must use the same TLS trust policy as other clients; health is not
a separate plaintext endpoint.

Prometheus metrics are served separately at `GET /metrics`. In a container,
set `observability.metrics_addr = "0.0.0.0:9187"` only on a private monitoring
network and restrict the published port at the host, security group, or network
policy. The endpoint has no TLS or authentication and deliberately omits
source, table, stream, consumer, and payload labels. See `docs/metrics.md` for
the metric and alert contract. Keep `observability.metrics_max_connections`
small; scrapers should reuse at most a few connections.

Tagged releases build and test the workspace on Ubuntu 24.04, publish both
binaries with the README, changelog, and licenses, and attach a SHA-256 file.
Verify an archive before installation:

```bash
sha256sum --check lightcdc-0.1.0-x86_64-unknown-linux-gnu.tar.gz.sha256
```

The supported deployment is single-node. Two processes must never open the
same redb volume concurrently, and an orchestrator must not start a replacement
until the previous process has stopped or the volume is fenced.

## Normal Shutdown

Send `SIGTERM` and allow at least `runtime.shutdown_timeout_ms`. Capture stops at
a transaction boundary, resolves accepted redb work, acknowledges only the
durable source LSN, and drains gRPC. Do not use `SIGKILL` for routine operations.
An abrupt kill is recoverable, but PostgreSQL may replay the last unacknowledged
transaction.

## Exit Status

Service managers can use the stable process status to choose whether to restart
or wait for an operator:

| Code | Class | Operator action |
| ---: | --- | --- |
| `0` | Clean stop | No failure; a signal or bounded capture completed. |
| `1` | Runtime | Inspect logs and health; retry only under the deployment's restart policy. |
| `2` | CLI usage | Correct command-line arguments. This code is owned by Clap. |
| `10` | Configuration | Correct configuration, secrets, PostgreSQL publication, permissions, or unsupported source settings before restarting. |
| `20` | Data safety | Do not loop-restart. Investigate a source identity mismatch, acknowledged WAL gap, incompatible or corrupt durable format, or failed integrity boundary. |

The final stderr line starts with `lightcdc: configuration failure`,
`lightcdc: data_safety failure`, or `lightcdc: runtime failure`. Runtime-state
metrics and gRPC health describe a process while it is alive; the exit status
is the supervisor contract after it terminates.

## Integrity Check

Stop `capture`, `run`, `serve`, `inspect`, and every other command using the data
directory, then run:

```bash
lightcdc check --config /etc/lightcdc/lightcdc.toml
```

The command opens every redb file, validates the segment catalog and format,
decodes every retained event, checks sequence continuity and metadata counts,
and validates source identity and consumer-offset records. It never attempts a
destructive repair. Preserve the failed volume and restore a verified backup if
the check fails.

## Backup

1. Gracefully stop LightCDC.
2. Record `confirmed_flush_lsn`, `restart_lsn`, and `wal_status` for the slot from
   `pg_replication_slots`.
3. Run the offline backup while PostgreSQL retains WAL from the recorded point:

```bash
lightcdc backup --config /etc/lightcdc/lightcdc.toml \
  --output /backups/lightcdc-$(date +%Y%m%d-%H%M%S)
```

4. Include that backup in the same recovery point as the PostgreSQL cluster
   backup or WAL archive that preserves the logical slot's required range.
5. Copy the backup off-host and test a restore regularly.

The backup contains a JSON manifest, source identity/checkpoint, full-store
integrity report, and SHA-256 for every durable file. Staging files are scratch
space and are intentionally excluded.

## Restore

1. Stop LightCDC and the Redis connector.
2. Keep the failed data directory for diagnosis; configure an empty replacement
   directory.
3. Restore the PostgreSQL recovery point and logical slot/WAL range coordinated
   with the LightCDC backup.
4. Run:

```bash
lightcdc restore --config /etc/lightcdc/lightcdc.toml \
  --input /backups/lightcdc-20260801-120000
lightcdc check --config /etc/lightcdc/lightcdc.toml
```

5. Start LightCDC and confirm readiness, source identity, local source LSN, slot
   `confirmed_flush_lsn`, retained WAL, and consumer lag before restoring normal
   traffic.

Restore verifies every checksum and opens the copied store in a temporary
directory before atomically publishing it. It refuses a nonempty target.

## Upgrade And Rollback

Take a verified offline backup before upgrading. On startup, LightCDC first
checks durable sidecar markers and rejects formats newer than the binary before
opening redb. It then applies supported control and event-payload migrations in
atomic redb transactions. Run `lightcdc check` after the upgrade and before
serving normal traffic.

A binary that does not support the resulting format refuses to start; it never
attempts a downgrade. Roll back by restoring the pre-upgrade backup together
with a PostgreSQL slot/WAL position that can replay from that backup's durable
source LSN. Staging files are versioned scratch data, excluded from backups, and
removed after an unclean restart rather than migrated.

## No-Loss Boundary

LightCDC never accepts a silent gap. On startup it compares the local durable
LSN with PostgreSQL's slot. If the slot has already acknowledged beyond an old
LightCDC backup, startup fails because PostgreSQL can no longer replay that
missing range. The remedies are a coordinated PostgreSQL/LightCDC recovery
point or a fresh downstream snapshot and slot, not forcing the checkpoint.

The event-log backup RPO is its creation time. A no-loss RPO additionally
requires PostgreSQL to retain WAL from that backup's source LSN. RTO is bounded
by backup verification/copy time, PostgreSQL recovery, retained WAL replay, and
consumer catch-up; measure it against production-sized stores.

If local storage is permanently lost and PostgreSQL no longer retains the
missing WAL, the current change-only release cannot reconstruct historical
state. Create a new slot and rebuild downstream state from an externally
consistent snapshot before resuming change delivery.

## Source Outage, Failover, And Slot Loss

During a network or PostgreSQL outage, leave the local store and slot intact.
LightCDC enters reconnecting state and resumes from its durable LSN after the
source returns. Alert on retained WAL growth while it is disconnected.

LightCDC binds each configured source name to PostgreSQL's cluster system
identifier and database OID. A promotion that preserves those identifiers and
the logical slot's required WAL can resume after the normal startup gap check.
A replacement cluster, logical restore, or database recreation is a different
source and startup is deliberately rejected. Do not edit the local identity.

If the slot is missing, invalidated, or has advanced past LightCDC's durable
LSN, stop consumers and determine whether a coordinated database/LightCDC
recovery point can restore the exact required WAL range. If it cannot, create a
new source name and slot, rebuild every downstream target from an externally
consistent snapshot, and begin a new change-only history. Recreating a slot and
reusing the old local log is not a no-loss recovery.

## Disk Pressure And WAL Growth

`runtime.max_storage_bytes` and `runtime.min_free_disk_bytes` stop capture before
the next durable commit or staging record would cross the configured boundary.
The transaction remains unacknowledged, so PostgreSQL retains WAL. Free space,
repair retention, or move the persistent volume promptly; do not delete redb
files manually. Monitor the slot because prolonged local disk pressure can move
the capacity problem upstream into PostgreSQL WAL storage.

Retention I/O failure also stops capture. A consumer older than the retained
prefix receives an explicit expired-offset error and must be rebuilt or seeked
deliberately.

For an abandoned durable consumer, either keep its offset and size retention
for the outage, or deliberately expire/rebuild it according to the downstream
system's recovery procedure. Never move an offset merely to silence a lag
alert. A seek to `latest` explicitly accepts a gap; a seek to `earliest`
replays only the retained prefix.

## Redis Recovery

The Redis connector resumes from its durable LightCDC consumer offset. A crash
or connection failure before acknowledgement can replay already applied cache
commands; repeating them repairs any partially applied event and converges to
the latest ordered value.

Restoring Redis to an older point does not move the LightCDC consumer offset
backward automatically. Flush or rebuild the affected cache, stop the connector,
and deliberately seek its consumer to the appropriate retained position before
restarting it. If that position has expired, rebuild from the source database.

## Diagnostics

Collect health status, sanitized logs, configuration with secret values removed,
`lightcdc inspect` output without `--sequence`, `lightcdc check` output, disk
usage, and PostgreSQL slot metrics. Do not enable JSON capture output or attach
event payloads unless the incident process explicitly permits source row data.

## Release Procedure

1. Update the workspace package version and every internal path dependency
   version together.
2. Move changelog entries from `Unreleased` into that version and document any
   durable-format or configuration change in the upgrade section.
3. Run formatting, clippy, workspace tests, Docker-backed integration tests,
   `cargo deny check`, the MSRV check, and the production image check.
4. Take and restore a production-sized backup with the candidate binary.
5. Create a signed `vMAJOR.MINOR.PATCH` tag whose version exactly matches every
   workspace package, then push it. The release workflow creates the archive
   and checksum; never replace an artifact for an existing tag.
