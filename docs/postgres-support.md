# PostgreSQL Support Matrix

The first production release supports PostgreSQL 17.x with the built-in
`pgoutput` plugin, protocol version 1, and text-format tuple values. Other major
versions may work but are not part of the tested release contract.

## Supported Capture

| PostgreSQL behavior | Support |
| --- | --- |
| `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE` | Supported; the publication must enable all four operations. |
| Source transactions | Preserved atomically; events become visible only after commit. |
| Rolled-back transactions | Discarded and never acknowledged as local events. |
| Replica identity `DEFAULT` | Supported when the table has a valid primary key. |
| Replica identity `USING INDEX` | Supported when PostgreSQL accepts the identity index. |
| Replica identity `FULL` | Supported and required when consumers need complete old rows. |
| Unchanged TOAST columns | Emitted as `{"__unchanged_toast":true}` rather than guessed data. |
| `NULL` and text-format values | Supported; PostgreSQL values are represented as JSON null or strings. |
| Relation refresh after `ADD COLUMN` | Supported and Docker-tested. |
| Partitioned root table | Supported only with `publish_via_partition_root = true`. |
| Leaf partition by name | Supported only with `publish_via_partition_root = false`. |
| Origin and type metadata | Accepted but not exposed in `ChangeEvent`. |
| LightCDC logical heartbeat | The reserved `lightcdc.heartbeat` message is accepted as a checkpoint. |

## Rejected Or Unsupported

Startup rejects publications that omit an operation, apply row filters or
column lists to configured tables, route a configured partition through the
wrong relation name, or include a table without usable replica identity.
Configured generated columns are rejected because PostgreSQL 17 does not
publish their values through this protocol.

Replication slots with `two_phase = true` are rejected. Streaming in-progress
transactions and prepared-transaction messages are not requested because the
client deliberately uses pgoutput protocol version 1. Arbitrary logical
messages stop capture before the containing WAL is acknowledged; LightCDC does
not silently discard application messages.

DDL statements are not emitted as events. `ADD COLUMN` relation refresh is the
tested online schema change. For column removal/rename/type changes, replica
identity changes, partition topology changes, or publication membership
changes, stop LightCDC first, apply the DDL, rerun source validation, and restart.
Changing the publication while capture is live is outside the supported
contract because PostgreSQL can stop sending a table without an in-band event.

The release is change-only: it does not take an initial table snapshot. Build
downstream state from an externally coordinated snapshot before starting the
slot, or start from empty downstream state when historical rows are not needed.

## Least-Privilege Role

Create the publication and logical slot with an administrative migration role,
then run LightCDC with a separate login that cannot alter either object:

```sql
CREATE ROLE lightcdc_reader LOGIN REPLICATION PASSWORD 'use-a-secret-manager';
GRANT CONNECT ON DATABASE app TO lightcdc_reader;
GRANT USAGE ON SCHEMA public TO lightcdc_reader;
GRANT SELECT ON TABLE public.orders, public.customers TO lightcdc_reader;
```

Grant `SELECT` on every configured table and repeat the schema grant outside
`public`. Keep publication ownership and table DDL privileges away from this
role. LightCDC validates `wal_level`, role replication privilege, source
identity, slot/plugin/database/two-phase settings, publication completeness,
table filters, partition routing, and replica identity before capture.
