#!/usr/bin/env bash
# The demos in Docker Compose:
#   1. burst against the fragile target, then the fixed scenario, then compare
#   2. a Postgres-backed run on worker containers; the container holding
#      shard 2 is killed partway through and a spare takes the shard over
#   3. the faults scenario through the proxy service
#
# Needs Docker with the compose plugin. Reports land in ./results.
#   ./scripts/docker-demo.sh            (or: make docker-demo)
#   NO_BUILD=1 ./scripts/docker-demo.sh to reuse the images you have
#   KEEP=1 ./scripts/docker-demo.sh     to leave the containers running
set -euo pipefail

cd "$(dirname "$0")/.."
SEED="${SEED:-42}"
KILL_AFTER="${KILL_AFTER:-10}"   # seconds after shard 2 is claimed
DB=postgres://burstforge:burstforge@postgres:5432/burstforge
dc() { docker compose --profile tools --profile db --profile faults "$@"; }
# Run the agent as the calling user so it can write ./results.
bf() { dc run --rm --no-deps -T -u "$(id -u):$(id -g)" agent "$@"; }
TARGET=(--target http://target:8080 --allow-host target,proxy --out /results)
banner() { printf '\n\033[1m== %s\033[0m\n\n' "$*"; }

mkdir -p results
[ "${NO_BUILD:-}" = 1 ] || dc build
[ "${KEEP:-}" = 1 ] || trap 'dc down -v >/dev/null 2>&1 || true' EXIT
dc up -d --wait target postgres proxy
dc up -d --wait --scale worker=4 worker

set +e
banner "1. burst against the fragile target (expect FAIL)"
bf run /scenarios/registration-burst.yaml "${TARGET[@]}" --seed "$SEED" | tee results/docker-before.log
banner "   same seed, fixed scenario and hardened target (expect PASS)"
bf run /scenarios/registration-burst-fixed.yaml "${TARGET[@]}" --seed "$SEED" | tee results/docker-after.log
before=$(grep -o 'reports written to .*' results/docker-before.log | awk '{print $NF}')
after=$(grep -o 'reports written to .*' results/docker-after.log | awk '{print $NF}')
bf compare "$before/report.json" "$after/report.json"

banner "2. three shards on four worker containers; one is killed partway through"
LOG=results/docker-recovery.log
bf run /scenarios/registration-sustained.yaml "${TARGET[@]}" --seed "$SEED" \
  --db "$DB" --shards 3 --agents 0 </dev/null >"$LOG" 2>&1 &
RUN_PID=$!
tail -f "$LOG" --pid "$RUN_PID" &
for _ in $(seq 1 120); do grep -q "shard 2: claimed by" "$LOG" && break; sleep 0.25; done
sleep "$KILL_AFTER"
# Worker names are <container hostname>-<pid>, and the hostname is the container ID.
victim=$(grep -o "shard 2: claimed by [A-Za-z0-9._-]*" "$LOG" | head -1 | awk '{print $NF}')
if [ -n "$victim" ]; then
  printf '\n\033[1m== docker kill %s\033[0m\n\n' "${victim%-*}"
  docker kill -s KILL "${victim%-*}" >/dev/null
else
  echo "no agent claimed shard 2; are the workers running?" >&2
fi
wait "$RUN_PID"
sleep 0.2

banner "3. faults through the proxy container, fragile then hardened"
bf run /scenarios/registration-faults.yaml "${TARGET[@]}" --seed "$SEED" --fault-proxy http://proxy:8081
bf run /scenarios/registration-faults-fixed.yaml "${TARGET[@]}" --seed "$SEED" --fault-proxy http://proxy:8081
set -e
banner "done; reports are in ./results"
