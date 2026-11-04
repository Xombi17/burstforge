// Needs a throwaway Postgres and a built bin/burstforge:
//   BURSTFORGE_TEST_DB=postgres://postgres@localhost:5433/burstforge npm test
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import pg from 'pg';
import { buildApp } from '../src/app.js';
import { newKey } from '../src/keys.js';

const DB = process.env.BURSTFORGE_TEST_DB;
const BIN = process.env.BURSTFORGE_BIN || fileURLToPath(new URL('../../bin/burstforge', import.meta.url));
const skip = DB ? false : 'BURSTFORGE_TEST_DB not set';

const scenario = (arrivals, window = '1s') => `
name: api-test
target: { base_url: "http://ignored:1", environment: staging }
workflow: { steps: [ { name: a, method: POST, path: /a } ] }
traffic: { profile: burst, arrivals: ${arrivals}, window: ${window} }
`;

let pool, app, approval, served;
const tag = `t${Date.now() % 1e9}`;
const orgs = {};

async function org(id, quotas = {}) {
  const full = `${tag}-${id}`;
  const cols = ['id', 'name', ...Object.keys(quotas)];
  await pool.query(
    `INSERT INTO organizations (${cols.join(', ')}) VALUES (${cols.map((_, i) => '$' + (i + 1)).join(', ')})`,
    [full, id, ...Object.values(quotas)],
  );
  const keys = {};
  for (const role of ['owner', 'operator', 'viewer']) {
    const k = newKey();
    await pool.query('INSERT INTO api_keys (id, organization_id, name, role, secret_sha256) VALUES ($1, $2, $3, $3, $4)', [k.id, full, role, k.hash]);
    keys[role] = k.key;
  }
  orgs[id] = { id: full, keys };
  return orgs[id];
}

const call = (key, method, url, body) =>
  app.inject({ method, url, headers: key ? { authorization: `Bearer ${key}` } : {}, ...(body ? { payload: body } : {}) });

// Creates and verifies an environment pointing at the approval server.
async function verifiedEnv(o, name = 'staging') {
  const r = await call(o.keys.owner, 'POST', '/v1/environments', { name, base_url: `http://127.0.0.1:${approval.address().port}` });
  assert.equal(r.statusCode, 201, r.body);
  served.add(r.json().approval_token);
  const v = await call(o.keys.operator, 'POST', `/v1/environments/${name}/verify`);
  assert.equal(v.statusCode, 200, v.body);
  return r.json();
}

before(async () => {
  if (skip) return;
  execFileSync(BIN, ['migrate', '--db', DB]);
  pool = new pg.Pool({ connectionString: DB });
  served = new Set();
  approval = http.createServer((req, res) => {
    if (req.url !== '/.well-known/burstforge-approval.txt') return res.writeHead(404).end();
    res.writeHead(200).end([...served].join('\n') + '\n');
  });
  await new Promise((r) => approval.listen(0, '127.0.0.1', r));
  app = buildApp({ pool, bin: BIN, metricsToken: 'm-secret' });
  await org('a', { max_concurrent_runs: 1, max_arrivals: 500, max_arrival_rate: 500 });
  await org('b', { max_concurrent_runs: 3 });
  await org('slow', { api_requests_per_min: 3 });
});

after(async () => {
  if (skip) return;
  await pool.query('DELETE FROM organizations WHERE id LIKE $1', [`${tag}-%`]);
  await app.close();
  await pool.end();
  approval.close();
});

test('keys: missing, malformed, wrong and revoked keys are refused', { skip }, async () => {
  assert.equal((await call(null, 'GET', '/v1/me')).statusCode, 401);
  assert.equal((await call('nope', 'GET', '/v1/me')).statusCode, 401);
  const forged = orgs.a.keys.owner.slice(0, -4) + 'AAAA';
  assert.equal((await call(forged, 'GET', '/v1/me')).statusCode, 401);

  const created = await call(orgs.a.keys.owner, 'POST', '/v1/keys', { name: 'ci', role: 'viewer' });
  assert.equal(created.statusCode, 201);
  const { id, key } = created.json();
  assert.equal((await call(key, 'GET', '/v1/me')).json().role, 'viewer');
  assert.equal((await call(orgs.a.keys.owner, 'DELETE', `/v1/keys/${id}`)).statusCode, 204);
  assert.equal((await call(key, 'GET', '/v1/me')).statusCode, 401);
  const stored = await pool.query('SELECT secret_sha256 FROM api_keys WHERE id = $1', [id]);
  assert.ok(!stored.rows[0].secret_sha256.toString().includes(key.split('_')[2]), 'secret stored in the clear');
});

test('roles: viewers read, operators run, owners manage', { skip }, async () => {
  const a = orgs.a.keys;
  assert.equal((await call(a.viewer, 'POST', '/v1/runs', { environment: 'x', scenario: 'x' })).statusCode, 403);
  assert.equal((await call(a.operator, 'POST', '/v1/environments', { name: 'x', base_url: 'http://x' })).statusCode, 403);
  assert.equal((await call(a.operator, 'GET', '/v1/keys')).statusCode, 403);
  assert.equal((await call(a.viewer, 'GET', '/v1/runs')).statusCode, 200);
});

test('environments: verification needs the token served by the target', { skip }, async () => {
  const o = orgs.b;
  const base = `http://127.0.0.1:${approval.address().port}`;
  const bad = await call(o.keys.owner, 'POST', '/v1/environments', { name: 'creds', base_url: 'http://u:p@host' });
  assert.equal(bad.statusCode, 400);
  const r = await call(o.keys.owner, 'POST', '/v1/environments', { name: 'pending', base_url: base });
  assert.equal(r.statusCode, 201);
  assert.match(r.json().approval_token, new RegExp(`^bf-approve-${o.id}-`));
  assert.equal((await call(o.keys.owner, 'POST', '/v1/environments', { name: 'pending', base_url: base })).statusCode, 409);

  const v1 = await call(o.keys.operator, 'POST', '/v1/environments/pending/verify');
  assert.equal(v1.statusCode, 422);
  assert.match(v1.json().message, /doesn't list/);
  // Unverified environments can't be run against.
  const run = await call(o.keys.operator, 'POST', '/v1/runs', { environment: 'pending', scenario: scenario(5) });
  assert.equal(run.statusCode, 409);

  served.add(r.json().approval_token);
  const v2 = await call(o.keys.operator, 'POST', '/v1/environments/pending/verify');
  assert.equal(v2.statusCode, 200);
  assert.equal(v2.json().verified, true);
  served.delete(r.json().approval_token);
  assert.equal((await call(o.keys.operator, 'POST', '/v1/environments/pending/verify')).statusCode, 422);
  const list = (await call(o.keys.viewer, 'GET', '/v1/environments')).json().environments;
  assert.equal(list.find((e) => e.name === 'pending').verified, false, 'a failed check must clear verification');
});

test('runs: invalid scenarios and per-run quotas are refused with reasons', { skip }, async () => {
  const o = orgs.a;
  await verifiedEnv(o);
  const invalid = await call(o.keys.operator, 'POST', '/v1/runs', { environment: 'staging', scenario: 'traffic: { arrivals: 0 }' });
  assert.equal(invalid.statusCode, 422);
  assert.ok(invalid.json().problems.length > 0);

  const big = await call(o.keys.operator, 'POST', '/v1/runs', { environment: 'staging', scenario: scenario(900) });
  assert.equal(big.statusCode, 422);
  assert.deepEqual(
    big.json().quotas.map((q) => q.quota),
    ['max_arrivals', 'max_arrival_rate'],
  );
  const hooks = scenario(5).replace('workflow:', 'hooks: { before: [ { path: "@evil/x" } ] }\nworkflow:');
  const h = await call(o.keys.operator, 'POST', '/v1/runs', { environment: 'staging', scenario: hooks });
  assert.equal(h.statusCode, 422);
  assert.match(JSON.stringify(h.json().problems), /hooks.before\[0\].path/);
});

test('runs: the concurrency quota holds under parallel requests', { skip }, async () => {
  const o = orgs.a; // max_concurrent_runs 1, environment from the previous test
  const results = await Promise.all(
    Array.from({ length: 5 }, () => call(o.keys.operator, 'POST', '/v1/runs', { environment: 'staging', scenario: scenario(50), shards: 2 })),
  );
  const codes = results.map((r) => r.statusCode).sort();
  assert.deepEqual(codes, [202, 429, 429, 429, 429]);
  const refused = results.find((r) => r.statusCode === 429);
  assert.equal(refused.json().quota, 'max_concurrent_runs');
  assert.ok(Number(refused.headers['retry-after']) > 0);
  const accepted = results.find((r) => r.statusCode === 202).json();
  assert.equal(accepted.arrivals, 50);
  assert.equal(accepted.state, 'queued');

  const me = (await call(o.keys.viewer, 'GET', '/v1/me')).json();
  assert.equal(me.usage.active_runs, 1);
  const got = await call(o.keys.viewer, 'GET', `/v1/runs/${accepted.id}`);
  assert.equal(got.json().state, 'queued');
});

test('tenants are isolated: runs, environments and metrics', { skip }, async () => {
  const runs = (await call(orgs.a.keys.viewer, 'GET', '/v1/runs')).json().runs;
  assert.ok(runs.length > 0);
  const theirs = runs[0].id;
  assert.equal((await call(orgs.b.keys.owner, 'GET', `/v1/runs/${theirs}`)).statusCode, 404);
  assert.ok(!(await call(orgs.b.keys.viewer, 'GET', '/v1/runs')).json().runs.some((r) => r.id === theirs));
  // b has no environment called staging, so it can't run against a's.
  assert.equal((await call(orgs.b.keys.operator, 'POST', '/v1/runs', { environment: 'staging', scenario: scenario(5) })).statusCode, 404);

  const mine = (await call(orgs.b.keys.viewer, 'GET', '/v1/metrics')).body;
  assert.match(mine, new RegExp(`org="${orgs.b.id}"`));
  assert.doesNotMatch(mine, new RegExp(`org="${orgs.a.id}"`));

  assert.equal((await app.inject({ method: 'GET', url: '/metrics' })).statusCode, 401);
  const all = await app.inject({ method: 'GET', url: '/metrics', headers: { authorization: 'Bearer m-secret' } });
  assert.equal(all.statusCode, 200);
  assert.match(all.body, new RegExp(`burstforge_runs\\{org="${orgs.a.id}",state="queued"\\} 1`));
  assert.match(all.body, new RegExp(`burstforge_quota_refusals_total\\{org="${orgs.a.id}",quota="max_concurrent_runs"\\} 4`));
});

test('the API rate limit is per organization', { skip }, async () => {
  const codes = [];
  for (let i = 0; i < 5; i++) codes.push((await call(orgs.slow.keys.viewer, 'GET', '/v1/me')).statusCode);
  assert.deepEqual(codes, [200, 200, 200, 429, 429]);
  const r = await call(orgs.slow.keys.viewer, 'GET', '/v1/me');
  assert.equal(r.json().quota, 'api_requests_per_min');
  assert.equal((await call(orgs.b.keys.viewer, 'GET', '/v1/me')).statusCode, 200, "another org's limit leaked");
});

test('an empty JSON body is accepted where none is needed', { skip }, async () => {
  const r = await app.inject({
    method: 'POST',
    url: '/v1/environments/nope/verify',
    headers: { authorization: `Bearer ${orgs.b.keys.operator}`, 'content-type': 'application/json' },
    payload: '',
  });
  assert.equal(r.statusCode, 404);
  const bad = await app.inject({
    method: 'POST',
    url: '/v1/runs',
    headers: { authorization: `Bearer ${orgs.b.keys.operator}`, 'content-type': 'application/json' },
    payload: '{not json',
  });
  assert.equal(bad.statusCode, 400);
});
