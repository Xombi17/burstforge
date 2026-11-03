#!/usr/bin/env bash
# The Week 4 drill: three shards, four agents, kill one agent mid-run with
# SIGKILL and watch a spare take its shard over when the lease expires.
#
# Needs a Postgres the agents can share:
#   BURSTFORGE_DB=postgres://user@localhost:5432/burstforge make recovery
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/bin/burstforge"
OUT="${OUT:-$ROOT/results}"
PORT="${PORT:-8080}"
DB="${BURSTFORGE_DB:?set BURSTFORGE_DB to a Postgres URL}"
KILL_AFTER="${KILL_AFTER:-10}"   # seconds after shard 2 is claimed

[ -x "$BIN" ] || { echo "build first: make build" >&2; exit 2; }
mkdir -p "$OUT"

if ! curl -fs "http://localhost:$PORT/healthz" >/dev/null 2>&1; then
  (cd "$ROOT/target" && PORT="$PORT" exec node server.js) >"$OUT/target.log" 2>&1 &
  TARGET_PID=$!
  trap 'kill "$TARGET_PID" 2>/dev/null || true' EXIT
  for _ in $(seq 1 30); do
    curl -fs "http://localhost:$PORT/healthz" >/dev/null 2>&1 && break
    sleep 0.2
  done
fi

LOG="$OUT/recovery.log"
"$BIN" run "$ROOT/scenarios/registration-sustained.yaml" --db "$DB" \
  --shards 3 --agents 4 --lease-ttl 3s --out "$OUT" >"$LOG" 2>&1 &
RUN_PID=$!
tail -f "$LOG" --pid "$RUN_PID" &

for _ in $(seq 1 80); do grep -q "shard 2: claimed by" "$LOG" && break; sleep 0.25; done
sleep "$KILL_AFTER"
victim=$(grep -o "shard 2: claimed by [A-Za-z0-9._-]*" "$LOG" | head -1 | awk '{print $NF}')
# Match on the --name argument; pgrep -f would also match this script.
pid=$(ps -eo pid=,args= | awk -v n="$victim" '$3 == "agent" && $0 ~ ("--name " n "( |$)") { print $1 }')
if [ -n "$pid" ]; then
  printf '\n\033[1m== kill -9 %s (pid %s)\033[0m\n\n' "$victim" "$pid"
  kill -9 "$pid"
else
  echo "could not find the agent holding shard 2; was the run too short?" >&2
fi

set +e
wait "$RUN_PID"
code=$?
set -e
sleep 0.2
exit "$code"
