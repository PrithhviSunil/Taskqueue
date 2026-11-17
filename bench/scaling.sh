#!/usr/bin/env bash
# Measures how processing throughput scales with worker count.
#
# Runs the API with workers OFF on :8080, preloads the queue, then starts a separate
# worker-only process on :8081 with K workers and times how fast it drains the queue.
# Separating the two means we measure the workers, not how fast we can send HTTP requests.
#
# Usage: bench/scaling.sh [jobs] [type] [sleep_ms] [worker counts...]
#   bench/scaling.sh 20000 noop 0 1 2 4 8 16
#   bench/scaling.sh 1000 sleep 20 1 8
set -euo pipefail
cd "$(dirname "$0")/.."

JOBS=${1:-20000}; TYPE=${2:-noop}; MS=${3:-20}; shift 3 || true
COUNTS=${*:-"1 2 4 8 16"}
JAR=target/taskqueue-0.1.0.jar

wait_up() { for _ in $(seq 1 60); do curl -sf "localhost:$1/actuator/health" >/dev/null && return; sleep 1; done; echo "port $1 never came up"; exit 1; }

WORKERS_ENABLED=false java -jar "$JAR" >/tmp/api.log 2>&1 &
API_PID=$!
trap 'kill $API_PID 2>/dev/null || true' EXIT
wait_up 8080

for K in $COUNTS; do
  echo "=== $K worker(s) ==="
  OUT=$(python3 bench/bench.py --jobs "$JOBS" --type "$TYPE" --ms "$MS" --enqueue-only)
  echo "$OUT" | sed -n 2p
  RANGE=$(echo "$OUT" | grep -oE '[0-9]+\.\.[0-9]+'); FROM=${RANGE%..*}; TO=${RANGE#*..}

  SERVER_PORT=8081 WORKERS=$K java -jar "$JAR" >/tmp/workers.log 2>&1 &
  W_PID=$!
  while :; do
    W=$(curl -s "localhost:8080/stats/window?from=$FROM&to=$TO")
    DONE=$(echo "$W" | python3 -c 'import json,sys; print(json.load(sys.stdin)["done"])')
    [ "$DONE" -ge "$JOBS" ] && break
    sleep 0.5
  done
  echo "$W" | python3 -c '
import json, sys
w = json.load(sys.stdin)
print(f"  processed {w["done"]} in {w["processing_seconds"]:.2f}s -> {w["done"]/w["processing_seconds"]:,.0f} jobs/s"
      f" | claimed more than once: {w["claimed_more_than_once"]}")'
  kill $W_PID; wait $W_PID 2>/dev/null || true
done
