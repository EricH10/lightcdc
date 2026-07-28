#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RATES="${RATES:-500 1000 2500 5000 10000 0}"
REPETITIONS="${REPETITIONS:-1}"
MATRIX_ID="${MATRIX_ID:-$(date -u +%Y%m%dT%H%M%SZ)-matrix}"

for rate in $RATES; do
    for repetition in $(seq 1 "$REPETITIONS"); do
        if [[ "$rate" == "0" ]]; then
            rate_name="max"
        else
            rate_name="$rate"
        fi
        RUN_ID="$MATRIX_ID-rate-$rate_name-run-$repetition" \
        RATE="$rate" \
            bench/run-local.sh
    done
done

echo "benchmark matrix complete: bench/results/$MATRIX_ID-*"
