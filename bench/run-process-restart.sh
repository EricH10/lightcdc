#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-process-restart}"
RESULT_DIR="${RESULTS_ROOT:-bench/results}/$RUN_ID"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-lightcdc-postgres}"
PGDATABASE="${PGDATABASE:-lightcdc}"
PGUSER="${PGUSER:-lightcdc}"
PGPASSWORD="${PGPASSWORD:-lightcdc}"
RESTART_AFTER_SECONDS="${RESTART_AFTER_SECONDS:-10}"
PGBENCH_RUN_KEY="${RUN_ID//[^a-zA-Z0-9_.-]/_}"
PGBENCH_PID_FILE="/tmp/lightcdc-pgbench-$PGBENCH_RUN_KEY.pid"

for command in docker jq; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "required command not found: $command" >&2
        exit 1
    fi
done

RUNNER_PID=""
REPLACEMENT_PID=""
stop_container_pgbench() {
    docker exec "$POSTGRES_CONTAINER" sh -c '
        pid_file=$1
        if [ -f "$pid_file" ]; then
            pid=$(cat "$pid_file")
            if [ -r "/proc/$pid/comm" ] && [ "$(cat "/proc/$pid/comm")" = pgbench ]; then
                kill -TERM "$pid" 2>/dev/null || true
            fi
            rm -f "$pid_file"
        fi
    ' sh "$PGBENCH_PID_FILE" >/dev/null 2>&1 || true
}

cleanup() {
    stop_container_pgbench
    for pid in "$RUNNER_PID" "$REPLACEMENT_PID"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
}
trap cleanup EXIT
trap 'exit 130' INT TERM

RUN_ID="$RUN_ID" \
DURATION_SECONDS="${DURATION_SECONDS:-60}" \
ROWS_PER_TRANSACTION="${ROWS_PER_TRANSACTION:-100}" \
RATE="${RATE:-100}" \
ACK_EVERY="${ACK_EVERY:-5000}" \
bench/run-local.sh &
RUNNER_PID=$!

for _ in $(seq 1 1800); do
    inserted="$(docker exec -i -e "PGPASSWORD=$PGPASSWORD" "$POSTGRES_CONTAINER" \
        psql --username "$PGUSER" --dbname "$PGDATABASE" --tuples-only --no-align \
        --command "SELECT count(*) FROM public.lightcdc_benchmark_events" 2>/dev/null \
        || true)"
    if [[ "${inserted:-0}" -gt 0 ]]; then
        break
    fi
    if ! kill -0 "$RUNNER_PID" 2>/dev/null; then
        wait "$RUNNER_PID"
    fi
    sleep 0.1
done
if [[ "${inserted:-0}" -eq 0 ]]; then
    echo "benchmark workload did not start" >&2
    exit 1
fi

sleep "$RESTART_AFTER_SECONDS"
original_pid="$(cat "$RESULT_DIR/lightcdc.pid" 2>/dev/null || true)"
if [[ -z "$original_pid" ]]; then
    echo "could not find benchmark LightCDC process" >&2
    exit 1
fi
printf '%s killed_pid=%s\n' "$(date +%s000)" "$original_pid" >"$RESULT_DIR/faults.log"
kill -KILL "$original_pid"
sleep 0.5

target/release/lightcdc run \
    --config "$RESULT_DIR/lightcdc.toml" \
    --addr 127.0.0.1:50051 \
    --output none \
    >>"$RESULT_DIR/lightcdc.log" 2>&1 &
REPLACEMENT_PID=$!
printf '%s replacement_pid=%s\n' "$(date +%s000)" "$REPLACEMENT_PID" \
    >>"$RESULT_DIR/faults.log"

wait "$RUNNER_PID"
RUNNER_PID=""
kill "$REPLACEMENT_PID" 2>/dev/null || true
wait "$REPLACEMENT_PID" 2>/dev/null || true
REPLACEMENT_PID=""
trap - EXIT INT TERM

source_transactions="$(awk -F: '/number of transactions actually processed/ {
    gsub(/ /, "", $2); print $2
}' "$RESULT_DIR/pgbench.log")"
rows_per_transaction="$(awk -F= '/^rows_per_transaction=/ {print $2}' \
    "$RESULT_DIR/environment.txt")"
generated_events=$((source_transactions * rows_per_transaction))
consumed_events="$(jq -s '.[-1].events_total' "$RESULT_DIR/consumer.jsonl")"
last_sequence="$(target/release/lightcdc inspect \
    --config "$RESULT_DIR/lightcdc.toml" --limit 0 \
    | awk '/^Event range/ {split($3, range, "\\.\\.="); print range[2]}')"

if [[ "$consumed_events" -ne "$generated_events" \
    || "$last_sequence" -ne "$generated_events" ]]; then
    echo "event mismatch: generated=$generated_events stored=$last_sequence consumed=$consumed_events" >&2
    exit 1
fi

redeliveries="$(jq -s '.[-1].redeliveries_total' "$RESULT_DIR/consumer.jsonl")"
echo "process restart passed: events=$generated_events redeliveries=$redeliveries results=$RESULT_DIR"
