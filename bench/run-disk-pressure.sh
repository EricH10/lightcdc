#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-disk-pressure}"
RESULT_DIR="${RESULTS_ROOT:-bench/results}/$RUN_ID"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-lightcdc-postgres}"
PGDATABASE="${PGDATABASE:-lightcdc}"
PGUSER="${PGUSER:-lightcdc}"
PGPASSWORD="${PGPASSWORD:-lightcdc}"
SLOT_NAME="${SLOT_NAME:-lightcdc_benchmark_slot}"

if ! [[ "$SLOT_NAME" =~ ^[a-z0-9_]+$ ]]; then
    echo "SLOT_NAME must contain only lowercase letters, digits, and underscores" >&2
    exit 1
fi
for command in docker jq; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "required command not found: $command" >&2
        exit 1
    fi
done

set +e
RUN_ID="$RUN_ID" \
DURATION_SECONDS="${DURATION_SECONDS:-15}" \
CONSUMER_EXTRA_SECONDS=0 \
ROWS_PER_TRANSACTION="${ROWS_PER_TRANSACTION:-100}" \
RATE="${RATE:-100}" \
ACK_EVERY="${ACK_EVERY:-5000}" \
MAX_STORAGE_BYTES="${MAX_STORAGE_BYTES:-107374182400}" \
MIN_FREE_DISK_BYTES="${MIN_FREE_DISK_BYTES:-18446744073709551615}" \
bench/run-local.sh
status=$?
set -e

if [[ "$status" -eq 0 ]]; then
    echo "expected LightCDC to stop at the configured storage reserve" >&2
    exit 1
fi
if ! grep -q "storage resource limit reached" "$RESULT_DIR/lightcdc.log"; then
    echo "LightCDC did not report the expected storage resource limit" >&2
    exit 1
fi

captured_events="$(jq -s 'if length == 0 then 0 else .[-1].events_total end' \
    "$RESULT_DIR/capture.jsonl")"
retained_wal_bytes="$(docker exec -i -e "PGPASSWORD=$PGPASSWORD" "$POSTGRES_CONTAINER" \
    psql --username "$PGUSER" --dbname "$PGDATABASE" --tuples-only --no-align \
    --command "SELECT COALESCE(
        pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint,
        0
    )
    FROM pg_replication_slots
    WHERE slot_name = '$SLOT_NAME'")"
if [[ "$retained_wal_bytes" -le 0 ]]; then
    echo "expected PostgreSQL WAL to remain retained after the rejected write" >&2
    exit 1
fi

echo "disk-pressure boundary passed: capture stopped before PostgreSQL ACK; captured=$captured_events retained_wal_bytes=$retained_wal_bytes results=$RESULT_DIR"
