#!/usr/bin/env bash
# The Week 6 milestone: three organizations run at the same time through
# the control plane. Each has its own staging target, its own quotas and
# its own API keys, and sees only its own runs and metrics.
#
# Starts the control plane, one controller, a pool of agents that only take
# organization runs, and three copies of the demo target. Needs Node and a
# throwaway Postgres database (the demo deletes and recreates the
# organizations acme, globex and initech in it):
#   BURSTFORGE_DB=postgres://user@localhost:5432/burstforge_demo make tenants
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/bin/burstforge"
DB="${BURSTFORGE_DB:?set BURSTFORGE_DB to a throwaway Postgres database}"
OUT="${OUT:-$ROOT/results}/tenants"
CP_PORT="${CP_PORT:-8090}"
API="http://127.0.0.1:$CP_PORT"
AGENTS="${AGENTS:-6}"
METRICS_TOKEN=demo-metrics-token

[ -x "$BIN" ] || { echo "build first: make build" >&2; exit 2; }
mkdir -p "$OUT"
[ -d "$ROOT/control-plane/node_modules" ] || (cd "$ROOT/control-plane" && npm ci --silent)

PIDS=()
cleanup() {
  for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
# jget EXPR: evaluate a JS expression on the JSON read from stdin, as `j`.
jget() { node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const j=JSON.parse(s);const v=new Function("j","return ("+process.argv[1]+")")(j);console.log(typeof v==="object"?JSON.stringify(v):v)})' "$1"; }
# api KEY METHOD PATH [JSON]: prints the body; the status code goes to $OUT/.code
api() {
  curl -s -o "$OUT/.body" -w '%{http_code}' -X "$2" -H "Authorization: Bearer $1" \
    -H 'Content-Type: application/json' ${4:+--data "$4"} "$API$3" >"$OUT/.code"
  cat "$OUT/.body"
}
code() { cat "$OUT/.code"; }
admin() { BURSTFORGE_DB="$DB" node "$ROOT/control-plane/src/admin.js" "$@"; }
wait_http() { for _ in $(seq 1 50); do curl -fs "$1" >/dev/null 2>&1 && return 0; sleep 0.2; done; echo "$1 did not come up" >&2; return 1; }
run_body() {  # run_body SCENARIO_FILE SHARDS
  node -e 'console.log(JSON.stringify({environment:"staging",scenario:require("fs").readFileSync(process.argv[1],"utf8"),shards:+process.argv[2],seed:42}))' "$1" "$2"
}

"$BIN" migrate --db "$DB" >/dev/null
# Clear earlier demo runs first, so the agents don't pick up leftovers.
for org in acme globex initech; do admin delete-org "$org" >/dev/null; done

say "three organizations with different quotas"
declare -A KEY PORT
PORT=([acme]=8091 [globex]=8092 [initech]=8093)
KEY[acme]=$(admin create-org acme --name "Acme" --max-concurrent-runs 1 | awk '/owner key/ {print $NF}')
KEY[globex]=$(admin create-org globex --name "Globex" --max-arrivals 300 --max-concurrent-runs 2 | awk '/owner key/ {print $NF}')
KEY[initech]=$(admin create-org initech --name "Initech" --max-duration-ms 60000 | awk '/owner key/ {print $NF}')
admin list | grep -E '"(acme|globex|initech)"' | sed 's/^/  /'

say "control plane, controller and $AGENTS agents (agents take organization runs only)"
BURSTFORGE_DB="$DB" BURSTFORGE_BIN="$BIN" BURSTFORGE_METRICS_TOKEN="$METRICS_TOKEN" PORT="$CP_PORT" \
  node "$ROOT/control-plane/src/server.js" >"$OUT/control-plane.log" 2>&1 &
PIDS+=($!)
wait_http "$API/healthz"
"$BIN" controller --db "$DB" --name controller-1 >"$OUT/controller.log" 2>&1 &
PIDS+=($!)
for k in $(seq 1 "$AGENTS"); do
  "$BIN" agent --db "$DB" --name "agent-$k" --require-org --trust-proxy 127.0.0.1 >"$OUT/agent-$k.log" 2>&1 &
  PIDS+=($!)
done
echo "  logs in $OUT"

say "each registers its staging target and proves it controls it"
for org in acme globex initech; do
  port=${PORT[$org]}
  token=$(api "${KEY[$org]}" POST /v1/environments "{\"name\":\"staging\",\"base_url\":\"http://127.0.0.1:$port\"}" | jget j.approval_token)
  # The team adds the token to its staging config; here, an env var.
  (cd "$ROOT/target" && PORT=$port APPROVAL_TOKENS="$token" exec node server.js) >"$OUT/target-$org.log" 2>&1 &
  PIDS+=($!)
  wait_http "http://127.0.0.1:$port/healthz"
  verified=$(api "${KEY[$org]}" POST /v1/environments/staging/verify | jget j.verified)
  echo "  $org: http://127.0.0.1:$port verified=$verified"
done
api "${KEY[globex]}" POST /v1/environments '{"name":"acme-target","base_url":"http://127.0.0.1:8091"}' >/dev/null
api "${KEY[globex]}" POST /v1/environments/acme-target/verify >/dev/null
printf "  globex registering acme's target: HTTP %s: %s\n" "$(code)" "$(jget j.message <"$OUT/.body")"

say "quota checks"
api "${KEY[globex]}" POST /v1/runs "$(run_body "$ROOT/scenarios/registration-burst.yaml" 2)" >/dev/null
printf '  globex, 500-arrival burst: HTTP %s: %s\n' "$(code)" "$(jget j.message <"$OUT/.body")"

say "three runs at once"
declare -A RUN
RUN[acme]=$(api "${KEY[acme]}" POST /v1/runs "$(run_body "$ROOT/scenarios/registration-burst-fixed.yaml" 2)" | jget j.id)
echo "  acme     registration-burst-fixed  HTTP $(code)  ${RUN[acme]}"
RUN[globex]=$(api "${KEY[globex]}" POST /v1/runs "$(run_body "$ROOT/scenarios/registration-faults.yaml" 2)" | jget j.id)
echo "  globex   registration-faults       HTTP $(code)  ${RUN[globex]}"
RUN[initech]=$(api "${KEY[initech]}" POST /v1/runs "$(run_body "$ROOT/scenarios/registration-sustained.yaml" 2)" | jget j.id)
echo "  initech  registration-sustained    HTTP $(code)  ${RUN[initech]}"
api "${KEY[acme]}" POST /v1/runs "$(run_body "$ROOT/scenarios/registration-burst-fixed.yaml" 2)" >/dev/null
printf '  acme, a second run while the first is going: HTTP %s: %s\n' "$(code)" "$(jget j.message <"$OUT/.body")"

say "waiting for the runs to finish"
deadline=$((SECONDS + 240))
for org in acme globex initech; do
  while :; do
    state=$(api "${KEY[$org]}" GET "/v1/runs/${RUN[$org]}" | jget j.state)
    case "$state" in completed | rejected) break ;; esac
    [ "$SECONDS" -lt "$deadline" ] || { echo "  $org still $state; giving up" >&2; exit 1; }
    sleep 1
  done
  api "${KEY[$org]}" GET "/v1/runs/${RUN[$org]}" >"$OUT/$org-run.json"
  node -e '
    const r = JSON.parse(require("fs").readFileSync(process.argv[1], "utf8"));
    if (r.state !== "completed") { console.log(`  ${process.argv[2]}: ${r.state}: ${r.reason}`); process.exit(0); }
    const p = r.report, inv = Object.fromEntries(p.invariants.map(i => [i.name, i.observed]));
    const t = (s) => new Date(s).toISOString().slice(11, 23);
    console.log(`  ${process.argv[2].padEnd(8)} ${r.verdict.toUpperCase().padEnd(4)} ran ${t(r.start_at)} to ${t(r.finished_at)}  ` +
      `${p.totals.logical} workflows, ${p.attempts.total} attempts, amplification ${p.attempts.retry_amplification.toFixed(2)}x, ` +
      `duplicates ${inv["no duplicate registrations"]}` + (p.faults ? `, ${p.faults.rules.map(f => f.injected).join("+")} faults injected` : ""));
    for (const f of p.verdict.failures || []) console.log(`           - ${f}`);
  ' "$OUT/$org-run.json" "$org"
done

say "isolation"
api "${KEY[globex]}" GET "/v1/runs/${RUN[acme]}" >/dev/null
echo "  globex reading acme's run: HTTP $(code)"
for org in acme globex initech; do
  n=$(api "${KEY[$org]}" GET /v1/runs | jget j.runs.length)
  orgs=$(api "${KEY[$org]}" GET /v1/metrics | grep -o 'org="[a-z]*"' | sort -u | tr '\n' ' ')
  echo "  $org: sees $n run request(s); its /v1/metrics has series for: $orgs"
done
echo "  operator /metrics (all organizations):"
curl -s -H "Authorization: Bearer $METRICS_TOKEN" "$API/metrics" |
  grep -E '^burstforge_(runs_finished_total|quota_refusals_total|http_attempts_total)' | sed 's/^/    /'
