#!/usr/bin/env bash
# The Week 5 drill: the same proxy-injected faults (slow SSO responses, lost
# registration responses) against the fragile target with naive retries, then
# the hardened target with polite retries. Same seed, so the same requests
# are faulted in both runs.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/bin/burstforge"
OUT="${OUT:-$ROOT/results}"
PORT="${PORT:-8080}"
SEED="${SEED:-21}"

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

step() { printf '\n\033[1m== %s\033[0m\n' "$1"; }

step "1. Fragile target, naive retries, proxy-injected faults."
set +e
"$BIN" run "$ROOT/scenarios/registration-faults.yaml" --seed "$SEED" --out "$OUT/faults-before"
set -e
BEFORE="$(ls -td "$OUT"/faults-before/*/ | head -1)report.json"

step "2. Hardened target, polite retries, the same faults."
"$BIN" run "$ROOT/scenarios/registration-faults-fixed.yaml" --seed "$SEED" --out "$OUT/faults-after"
AFTER="$(ls -td "$OUT"/faults-after/*/ | head -1)report.json"

step "3. Before vs after."
"$BIN" compare "$BEFORE" "$AFTER"
