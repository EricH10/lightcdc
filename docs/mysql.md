# MySQL Capture

MySQL support reads row events from the binary log and normalizes them into the
same `ChangeEvent` format used by PostgreSQL capture.

## Required Server Settings

The source must use:

```text
log_bin = ON
binlog_format = ROW
binlog_row_image = FULL
binlog_row_metadata = FULL
binlog_row_value_options = ""
```

Each running LightCDC MySQL source also needs a nonzero `server_id` that is
unique among replication clients connected to that MySQL server. The configured
user needs `SELECT`, `REPLICATION SLAVE`, and `REPLICATION CLIENT`.

The CLI validates these requirements before opening the binlog stream. The
Docker Compose MySQL service and `sql/mysql-init/001_setup.sql` provide a
working local setup.

## Configuration

Use `lightcdc.mysql.example.toml` as a starting point:

```toml
[source]
type = "mysql"
name = "mysql-local"
host = "localhost"
port = 3306
database = "lightcdc"
user = "lightcdc"
password = "lightcdc"
server_id = 5401
```

Capture is limited to row events in the configured database. Configured streams
then filter the durable event log for consumers in the same way as PostgreSQL
streams.

## Checkpoints and Recovery

On the first launch, LightCDC starts at MySQL's current binary-log end. It does
not replay older retained binlogs automatically.

At every source commit, LightCDC writes the transaction's events and the next
binary-log file/position to redb in one atomic transaction. After a restart or
connection loss it resumes from that durable position. MySQL does not have a
PostgreSQL-style replication-slot acknowledgement; redb's checkpoint is the
resume authority.

Current checkpoints use binary-log file and byte position. GTID-based
checkpointing and failover to another MySQL server remain future work. TLS and
DDL/schema-change events are also not yet supported.
