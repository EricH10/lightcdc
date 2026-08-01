# Operations Runbook

This runbook covers the first supported deployment boundary: one LightCDC
process, one PostgreSQL source and logical slot, one local persistent volume,
and ordered named consumers. It is a single-node service; restart and restore,
not automatic failover, are the current availability mechanism.

## Normal Shutdown

Send `SIGTERM` and allow at least `runtime.shutdown_timeout_ms`. Capture stops at
a transaction boundary, resolves accepted redb work, acknowledges only the
durable source LSN, and drains gRPC. Do not use `SIGKILL` for routine operations.
An abrupt kill is recoverable, but PostgreSQL may replay the last unacknowledged
transaction.

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

## Redis Recovery

The Redis connector stores its applied sequence atomically with each cache
mutation. Back up Redis according to its own durability policy. After restore,
the connector seeks LightCDC to Redis's stored sequence and replays retained
events. If that sequence has expired, flush/rebuild the affected cache and
perform a deliberate seek; never advance the Redis progress key by hand.

## Diagnostics

Collect health status, sanitized logs, configuration with secret values removed,
`lightcdc inspect` output without `--sequence`, `lightcdc check` output, disk
usage, and PostgreSQL slot metrics. Do not enable JSON capture output or attach
event payloads unless the incident process explicitly permits source row data.
