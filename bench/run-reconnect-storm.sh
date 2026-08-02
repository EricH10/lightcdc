#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-reconnect-storm}"
RESULT_DIR="${RESULTS_ROOT:-bench/results}/$RUN_ID"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-lightcdc-postgres}"
PGDATABASE="${PGDATABASE:-lightcdc}"
PGUSER="${PGUSER:-lightcdc}"
PGPASSWORD="${PGPASSWORD:-lightcdc}"
SLOT_NAME="${SLOT_NAME:-lightcdc_benchmark_slot}"
TERMINATIONS="${TERMINATIONS:-10}"
TERMINATION_INTERVAL_SECONDS="${TERMINATION_INTERVAL_SECONDS:-2}"
PGBENCH_RUN_KEY="${RUN_ID//[^a-zA-Z0-9_.-]/_}"
PGBENCH_PID_FILE="/tmp/lightcdc-pgbench-$PGBENCH_RUN_KEY.pid"

if ! [[ "$TERMINATIONS" =~ ^[1-9][0-9]*$ ]]; then
    echo "TERMINATIONS must be a positive integer" >&2
    exit 1
fi
for command in docker jq; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "required command not found: $command" >&2
        exit 1
    fi
done

RUNNER_PID=""
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
    if [[ -n "$RUNNER_PID" ]] && kill -0 "$RUNNER_PID" 2>/dev/null; then
        kill "$RUNNER_PID" 2>/dev/null || true
        wait "$RUNNER_PID" 2>/dev/null || true
    fi
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

: >"$RESULT_DIR/faults.log"
for termination in $(seq 1 "$TERMINATIONS"); do
    active_pid=""
    for _ in $(seq 1 150); do
        active_pid="$(docker exec -i -e "PGPASSWORD=$PGPASSWORD" "$POSTGRES_CONTAINER" \
            psql --username "$PGUSER" --dbname "$PGDATABASE" --tuples-only --no-align \
            --command "SELECT COALESCE(active_pid::text, '')
                       FROM pg_replication_slots
                       WHERE slot_name = '$SLOT_NAME'" 2>/dev/null || true)"
        if [[ -n "$active_pid" ]]; then
            break
        fi
        sleep 0.1
    done
    if [[ -z "$active_pid" ]]; then
        echo "replication backend did not reconnect before fault $termination" >&2
        exit 1
    fi
    printf '%s termination=%s backend_pid=%s\n' \
        "$(date +%s000)" "$termination" "$active_pid" >>"$RESULT_DIR/faults.log"
    docker exec -i -e "PGPASSWORD=$PGPASSWORD" "$POSTGRES_CONTAINER" \
        psql --username "$PGUSER" --dbname "$PGDATABASE" --set ON_ERROR_STOP=1 \
        --command "SELECT pg_terminate_backend($active_pid)" \
        >>"$RESULT_DIR/faults.log"
    sleep "$TERMINATION_INTERVAL_SECONDS"
done

wait "$RUNNER_PID"
RUNNER_PID=""
trap - EXIT INT TERM

source_transactions="$(awk -F: '/number of transactions actually processed/ {
    gsub(/ /, "", $2); print $2
}' "$RESULT_DIR/pgbench.log")"
rows_per_transaction="$(awk -F= '/^rows_per_transaction=/ {print $2}' \
    "$RESULT_DIR/environment.txt")"
generated_events=$((source_transactions * rows_per_transaction))
captured_events="$(jq -s '.[-1].events_total' "$RESULT_DIR/capture.jsonl")"
consumed_events="$(jq -s '.[-1].events_total' "$RESULT_DIR/consumer.jsonl")"
reconnects="$(jq -s '.[-1].reconnects_total' "$RESULT_DIR/capture.jsonl")"
dropped_samples="$(jq -s '.[-1].dropped_samples_total' "$RESULT_DIR/capture.jsonl")"

if [[ "$captured_events" -ne "$generated_events" \
    || "$consumed_events" -ne "$generated_events" ]]; then
    echo "event mismatch: generated=$generated_events captured=$captured_events consumed=$consumed_events" >&2
    exit 1
fi
if [[ "$reconnects" -lt "$TERMINATIONS" ]]; then
    echo "expected at least $TERMINATIONS reconnects; measured $reconnects" >&2
    exit 1
fi
if [[ "$dropped_samples" -ne 0 ]]; then
    echo "capture metrics dropped $dropped_samples samples" >&2
    exit 1
fi

echo "reconnect storm passed: events=$generated_events reconnects=$reconnects results=$RESULT_DIR"
