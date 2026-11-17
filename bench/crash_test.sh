#!/usr/bin/env bash
# Kill-a-worker test: proves no job is lost when a worker process dies mid-run.
#
#   1. API on :8080 with workers off. Enqueue JOBS slow jobs.
#   2. Worker process A starts chewing through them.
#   3. After KILL_AFTER seconds, A is killed with SIGKILL: no shutdown hooks, no cleanup,
#      exactly like a machine losing power. Its in-flight jobs are left stuck on RUNNING.
#   4. Worker process B starts. The reaper notices the expired leases and requeues the
#      stuck jobs, and B finishes everything.
#   5. Check: every job is DONE. "Recovered" = jobs that had to be run a second time.
#
# Repeats ROUNDS times so the result isn't a fluke.
# Usage: bench/crash_test.sh [jobs] [job_ms] [kill_after_s] [rounds]
set -euo pipefail
cd "$(dirname "$0")/.."

JOBS=${1:-2000}; MS=${2:-50}; KILL_AFTER=${3:-3}; ROUNDS=${4:-5}
JAR=target/taskqueue-0.1.0.jar
# Short lease so the test doesn't sit around waiting 30s for the reaper.
export LEASE_MS=${LEASE_MS:-3000} REAPER_INTERVAL_MS=${REAPER_INTERVAL_MS:-1000}

wait_up() { for _ in $(seq 1 60); do curl -sf "localhost:$1/actuator/health" >/dev/null && return; sleep 1; done; echo "port $1 never came up"; exit 1; }
window() { curl -s "localhost:8080/stats/window?from=$1&to=$2"; }
field() { python3 -c "import json,sys; print(json.load(sys.stdin)['$1'])"; }

WORKERS_ENABLED=false java -jar "$JAR" >/tmp/api.log 2>&1 &
API_PID=$!
trap 'kill $API_PID 2>/dev/null || true; kill -9 ${A_PID:-} ${B_PID:-} 2>/dev/null || true' EXIT
wait_up 8080

TOTAL_JOBS=0; TOTAL_LOST=0; TOTAL_RECOVERED=0
for R in $(seq 1 "$ROUNDS"); do
  OUT=$(python3 bench/bench.py --jobs "$JOBS" --type sleep --ms "$MS" --enqueue-only)
  RANGE=$(echo "$OUT" | grep -oE '[0-9]+\.\.[0-9]+'); FROM=${RANGE%..*}; TO=${RANGE#*..}

  SERVER_PORT=8081 java -jar "$JAR" >/tmp/worker_a.log 2>&1 &
  A_PID=$!
  wait_up 8081
  sleep "$KILL_AFTER"
  RUNNING=$(curl -s localhost:8080/stats | field RUNNING)
  kill -9 "$A_PID"; wait "$A_PID" 2>/dev/null || true
  BEFORE=$(window "$FROM" "$TO" | field done)

  SERVER_PORT=8082 java -jar "$JAR" >/tmp/worker_b.log 2>&1 &
  B_PID=$!
  while :; do
    W=$(window "$FROM" "$TO")
    FIN=$(echo "$W" | python3 -c 'import json,sys; w=json.load(sys.stdin); print(w["done"]+w["dead"])')
    [ "$FIN" -ge "$JOBS" ] && break
    sleep 0.5
  done
  kill "$B_PID"; wait "$B_PID" 2>/dev/null || true

  DONE=$(echo "$W" | field done); RECOVERED=$(echo "$W" | field claimed_more_than_once)
  LOST=$((JOBS - DONE))
  echo "round $R: killed worker with $RUNNING jobs in flight ($BEFORE/$JOBS done) -> $DONE/$JOBS done, $RECOVERED recovered, $LOST lost"
  TOTAL_JOBS=$((TOTAL_JOBS + JOBS)); TOTAL_LOST=$((TOTAL_LOST + LOST)); TOTAL_RECOVERED=$((TOTAL_RECOVERED + RECOVERED))
done
echo "TOTAL: $ROUNDS kills, $TOTAL_JOBS jobs, $TOTAL_RECOVERED recovered, $TOTAL_LOST lost"
