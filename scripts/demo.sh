#!/usr/bin/env bash
# The BurstForge demo: healthy at 10 users, broken at 500, fixed, proven.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/bin/burstforge"
OUT="${OUT:-$ROOT/results}"
PORT="${PORT:-8080}"

[ -x "$BIN" ] || { echo "build first: make build" >&2; exit 2; }

started=0
if ! curl -fs "http://localhost:$PORT/healthz" >/dev/null 2>&1; then
  (cd "$ROOT/target" && PORT="$PORT" exec node server.js) >"$OUT.target.log" 2>&1 &
  TARGET_PID=$!
  started=1
  trap '[ "$started" = 1 ] && kill "$TARGET_PID" 2>/dev/null || true' EXIT
  for _ in $(seq 1 30); do
    curl -fs "http://localhost:$PORT/healthz" >/dev/null 2>&1 && break
    sleep 0.2
  done
fi

latest() { ls -t "$OUT" | head -1; }
step() { printf '\n\033[1m== %s\033[0m\n' "$1"; }

step "1. Ten users, one at a time. Looks healthy."
"$BIN" run "$ROOT/scenarios/registration-smoke.yaml" --out "$OUT" | sed -n '/VERDICT/,$p'

step "2. 500 synchronized arrivals, slow SSO, naive retries."
set +e
"$BIN" run "$ROOT/scenarios/registration-burst.yaml" --out "$OUT"
set -e
BEFORE="$OUT/$(latest)/report.json"

step "3. Same burst against the hardened target with polite retries."
"$BIN" run "$ROOT/scenarios/registration-burst-fixed.yaml" --out "$OUT"
AFTER="$OUT/$(latest)/report.json"

step "4. Before vs after."
"$BIN" compare "$BEFORE" "$AFTER"
