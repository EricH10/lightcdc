#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DURATION_SECONDS="${DURATION_SECONDS:-60}"
CONSUMER_EXTRA_SECONDS="${CONSUMER_EXTRA_SECONDS:-10}"
CLIENTS="${CLIENTS:-8}"
THREADS="${THREADS:-4}"
RATE="${RATE:-0}"
ROWS_PER_TRANSACTION="${ROWS_PER_TRANSACTION:-1}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-256}"
ACK_EVERY="${ACK_EVERY:-1}"
WORKLOAD="${WORKLOAD:-insert}"
PRELOAD_ROWS="${PRELOAD_ROWS:-100000}"
PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGDATABASE="${PGDATABASE:-lightcdc}"
PGUSER="${PGUSER:-lightcdc}"
PGPASSWORD="${PGPASSWORD:-lightcdc}"
SLOT_NAME="${SLOT_NAME:-lightcdc_benchmark_slot}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-lightcdc-postgres}"
RESULTS_ROOT="${RESULTS_ROOT:-bench/results}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
RESULT_DIR="$RESULTS_ROOT/$RUN_ID"
DATA_DIR="$ROOT/bench/.data"

export PGPASSWORD

for command in cargo; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "required command not found: $command" >&2
        exit 1
    fi
done

USE_DOCKER_CLIENTS=false
if ! command -v pgbench >/dev/null 2>&1 || ! command -v psql >/dev/null 2>&1; then
    if ! command -v docker >/dev/null 2>&1 \
        || ! docker inspect "$POSTGRES_CONTAINER" >/dev/null 2>&1; then
        echo "psql and pgbench are missing, and PostgreSQL container $POSTGRES_CONTAINER is unavailable" >&2
        exit 1
    fi
    USE_DOCKER_CLIENTS=true
fi

db_psql() {
    if [[ "$USE_DOCKER_CLIENTS" == "true" ]]; then
        docker exec -i -e "PGPASSWORD=$PGPASSWORD" "$POSTGRES_CONTAINER" psql "$@"
    else
        psql "$@"
    fi
}

db_pgbench() {
    local script="$1"
    shift
    if [[ "$USE_DOCKER_CLIENTS" == "true" ]]; then
        docker exec -i -e "PGPASSWORD=$PGPASSWORD" "$POSTGRES_CONTAINER" \
            pgbench "$@" --file - <"$script"
    else
        pgbench "$@" --file "$script"
    fi
}

case "$WORKLOAD" in
    insert|update) ;;
    *)
        echo "WORKLOAD must be insert or update" >&2
        exit 1
        ;;
esac

mkdir -p "$RESULT_DIR"
rm -rf "$DATA_DIR"

LIGHTCDC_PID=""
CONSUMER_PID=""
SAMPLER_PID=""

cleanup() {
    for pid in "$SAMPLER_PID" "$CONSUMER_PID" "$LIGHTCDC_PID"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
}
trap cleanup EXIT INT TERM

cat >"$RESULT_DIR/environment.txt" <<EOF
run_id=$RUN_ID
git_sha=$(git rev-parse HEAD)
git_dirty=$(test -n "$(git status --porcelain)" && echo true || echo false)
uname=$(uname -a)
rustc=$(rustc --version)
cargo=$(cargo --version)
postgres=$(db_psql --version)
pgbench=$(db_pgbench /dev/null --version)
postgres_clients_in_docker=$USE_DOCKER_CLIENTS
duration_seconds=$DURATION_SECONDS
clients=$CLIENTS
threads=$THREADS
rate=$RATE
rows_per_transaction=$ROWS_PER_TRANSACTION
payload_bytes=$PAYLOAD_BYTES
ack_every=$ACK_EVERY
workload=$WORKLOAD
preload_rows=$PRELOAD_ROWS
EOF

ACTIVE_PID="$(db_psql \
    --host "$PGHOST" \
    --port "$PGPORT" \
    --username "$PGUSER" \
    --dbname "$PGDATABASE" \
    --tuples-only \
    --no-align \
    --command "SELECT COALESCE(active_pid::text, '')
               FROM pg_replication_slots
               WHERE slot_name = '$SLOT_NAME'")"
if [[ -n "$ACTIVE_PID" ]]; then
    echo "replication slot $SLOT_NAME is already active in PostgreSQL backend $ACTIVE_PID" >&2
    exit 1
fi

db_psql \
    --host "$PGHOST" \
    --port "$PGPORT" \
    --username "$PGUSER" \
    --dbname "$PGDATABASE" \
    --set ON_ERROR_STOP=1 \
    >"$RESULT_DIR/setup.log" \
    <bench/sql/setup.sql

if [[ "$WORKLOAD" == "update" ]]; then
    db_psql \
        --host "$PGHOST" \
        --port "$PGPORT" \
        --username "$PGUSER" \
        --dbname "$PGDATABASE" \
        --set ON_ERROR_STOP=1 \
        --command "ALTER PUBLICATION lightcdc_publication
                       DROP TABLE public.lightcdc_benchmark_events;
                   TRUNCATE TABLE public.lightcdc_benchmark_events RESTART IDENTITY;
                   INSERT INTO public.lightcdc_benchmark_events (producer_id, payload)
                   SELECT 0, repeat('p', $PAYLOAD_BYTES)
                   FROM generate_series(1, $PRELOAD_ROWS);
                   ALTER PUBLICATION lightcdc_publication
                       ADD TABLE public.lightcdc_benchmark_events" \
        >>"$RESULT_DIR/setup.log"
fi

db_psql \
    --host "$PGHOST" \
    --port "$PGPORT" \
    --username "$PGUSER" \
    --dbname "$PGDATABASE" \
    --set ON_ERROR_STOP=1 \
    --command "SELECT pg_drop_replication_slot('$SLOT_NAME')
               WHERE EXISTS (
                   SELECT 1
                   FROM pg_replication_slots
                   WHERE slot_name = '$SLOT_NAME'
               );
               SELECT pg_create_logical_replication_slot('$SLOT_NAME', 'pgoutput');" \
    >>"$RESULT_DIR/setup.log"

TARGET_LSN="$(db_psql \
    --host "$PGHOST" \
    --port "$PGPORT" \
    --username "$PGUSER" \
    --dbname "$PGDATABASE" \
    --tuples-only \
    --no-align \
    --command "SELECT pg_current_wal_lsn()")"

cargo build --release -p lightcdc-cli --bin lightcdc \
    >"$RESULT_DIR/build.log" 2>&1
cargo build --release -p lightcdc-api --example benchmark_consumer \
    >>"$RESULT_DIR/build.log" 2>&1

target/release/lightcdc run \
    --config bench/lightcdc.benchmark.toml \
    --addr 127.0.0.1:50051 \
    --output none \
    --metrics-file "$RESULT_DIR/capture.jsonl" \
    >"$RESULT_DIR/lightcdc.log" 2>&1 &
LIGHTCDC_PID=$!

for _ in $(seq 1 150); do
    if ! kill -0 "$LIGHTCDC_PID" 2>/dev/null; then
        echo "lightcdc exited before reaching the setup LSN; see $RESULT_DIR/lightcdc.log" >&2
        exit 1
    fi
    CAUGHT_UP="$(db_psql \
        --host "$PGHOST" \
        --port "$PGPORT" \
        --username "$PGUSER" \
        --dbname "$PGDATABASE" \
        --tuples-only \
        --no-align \
        --command "SELECT COALESCE(
            (SELECT active_pid IS NOT NULL
                    AND pg_wal_lsn_diff(confirmed_flush_lsn, '$TARGET_LSN') >= 0
             FROM pg_replication_slots
             WHERE slot_name = '$SLOT_NAME'),
            false
        )")"
    if [[ "$CAUGHT_UP" == "t" ]]; then
        break
    fi
    sleep 0.2
done

if [[ "$CAUGHT_UP" != "t" ]]; then
    echo "lightcdc did not catch up to setup LSN $TARGET_LSN" >&2
    exit 1
fi

target/release/examples/benchmark_consumer \
    --endpoint http://127.0.0.1:50051 \
    --stream benchmark \
    --consumer "benchmark-$RUN_ID" \
    --seek latest \
    --duration-seconds "$((DURATION_SECONDS + CONSUMER_EXTRA_SECONDS + 2))" \
    --ack-every "$ACK_EVERY" \
    --metrics-file "$RESULT_DIR/consumer.jsonl" \
    >"$RESULT_DIR/consumer.log" 2>&1 &
CONSUMER_PID=$!

echo "timestamp_ms,current_wal_lsn,confirmed_flush_lsn,retained_wal_bytes,slot_active,postgres_spill_transactions,postgres_spill_count,postgres_spill_bytes" \
    >"$RESULT_DIR/postgres.csv"
echo "timestamp_ms,cpu_percent,rss_kb,data_disk_kb" >"$RESULT_DIR/process.csv"
(
    while true; do
        db_psql \
            --host "$PGHOST" \
            --port "$PGPORT" \
            --username "$PGUSER" \
            --dbname "$PGDATABASE" \
            --csv \
            --set "slot_name=$SLOT_NAME" \
            >>"$RESULT_DIR/postgres.csv" 2>>"$RESULT_DIR/postgres-sampler.log" \
            <bench/sql/postgres_metrics.sql || true
        PROCESS_SAMPLE="$(ps -o %cpu= -o rss= -p "$LIGHTCDC_PID" 2>/dev/null \
            | awk 'NF {print $1 "," $2}' || true)"
        DATA_DISK_KB="$(du -sk "$DATA_DIR" 2>/dev/null | awk '{print $1}')"
        printf '%s,%s,%s\n' \
            "$(date +%s000)" \
            "${PROCESS_SAMPLE:-0,0}" \
            "${DATA_DISK_KB:-0}" \
            >>"$RESULT_DIR/process.csv"
        sleep 1
    done
) &
SAMPLER_PID=$!

sleep 2

PGBENCH_ARGS=(
    --host "$PGHOST"
    --port "$PGPORT"
    --username "$PGUSER"
    --client "$CLIENTS"
    --jobs "$THREADS"
    --time "$DURATION_SECONDS"
    --no-vacuum
    --progress 1
    --progress-timestamp
    --define "rows_per_transaction=$ROWS_PER_TRANSACTION"
    --define "payload_bytes=$PAYLOAD_BYTES"
)

if [[ "$WORKLOAD" == "update" ]]; then
    MAX_ID="$(db_psql \
        --host "$PGHOST" \
        --port "$PGPORT" \
        --username "$PGUSER" \
        --dbname "$PGDATABASE" \
        --tuples-only \
        --no-align \
        --command "SELECT COALESCE(max(id), 0) FROM public.lightcdc_benchmark_events")"
    if [[ "$MAX_ID" -lt 1 ]]; then
        echo "update workload requires existing benchmark rows" >&2
        exit 1
    fi
    PGBENCH_ARGS+=(--define "max_id=$MAX_ID")
fi

if [[ "$RATE" -gt 0 ]]; then
    PGBENCH_ARGS+=(--rate "$RATE")
fi

db_pgbench "bench/sql/$WORKLOAD.sql" "${PGBENCH_ARGS[@]}" "$PGDATABASE" \
    >"$RESULT_DIR/pgbench.log" 2>&1

wait "$CONSUMER_PID"
CONSUMER_PID=""

kill "$SAMPLER_PID" 2>/dev/null || true
wait "$SAMPLER_PID" 2>/dev/null || true
SAMPLER_PID=""

kill "$LIGHTCDC_PID" 2>/dev/null || true
wait "$LIGHTCDC_PID" 2>/dev/null || true
LIGHTCDC_PID=""

trap - EXIT INT TERM
echo "benchmark results: $RESULT_DIR"
