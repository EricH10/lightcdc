#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

EVENT_RATES="${EVENT_RATES:-300}"
ROWS_PER_TRANSACTION_VALUES="${ROWS_PER_TRANSACTION_VALUES:-1 100}"
ACK_EVERY_VALUES="${ACK_EVERY_VALUES:-1 100}"
REPETITIONS="${REPETITIONS:-1}"
RESULTS_ROOT="${RESULTS_ROOT:-bench/results}"
MATRIX_ID="${MATRIX_ID:-$(date -u +%Y%m%dT%H%M%SZ)-cost-matrix}"
SUMMARY="$RESULTS_ROOT/$MATRIX_ID-summary.csv"

mkdir -p "$RESULTS_ROOT"
cargo build --release -p lightcdc-api --example benchmark_summary

first_summary=true
for event_rate in $EVENT_RATES; do
    for rows_per_transaction in $ROWS_PER_TRANSACTION_VALUES; do
        transaction_rate=$(((event_rate + rows_per_transaction - 1) / rows_per_transaction))

        for ack_every in $ACK_EVERY_VALUES; do
            for repetition in $(seq 1 "$REPETITIONS"); do
                run_id="$MATRIX_ID-events-$event_rate-rows-$rows_per_transaction-ack-$ack_every-run-$repetition"

                RUN_ID="$run_id" \
                RESULTS_ROOT="$RESULTS_ROOT" \
                RATE="$transaction_rate" \
                ROWS_PER_TRANSACTION="$rows_per_transaction" \
                ACK_EVERY="$ack_every" \
                    bench/run-local.sh

                summary_args=(
                    --result-dir "$RESULTS_ROOT/$run_id"
                )
                if [[ "$first_summary" == "true" ]]; then
                    summary_args+=(--header)
                    first_summary=false
                fi
                target/release/examples/benchmark_summary "${summary_args[@]}" >>"$SUMMARY"
            done
        done
    done
done

echo "benchmark cost matrix complete: $SUMMARY"
if command -v column >/dev/null 2>&1; then
    column -s, -t "$SUMMARY"
else
    cat "$SUMMARY"
fi
