// The control plane's HTTP API. It owns organizations, API keys, quotas
// and run requests. It never sends load itself: a `burstforge controller`
// picks up queued requests, and agents run them.
import Fastify from 'fastify';
import { randomBytes, randomInt, timingSafeEqual } from 'node:crypto';
import { parseKey, secretMatches, newKey, approvalToken, ROLES } from './keys.js';
import { RateLimiter } from './ratelimit.js';
import { Metrics } from './metrics.js';
import { validateScenario, checkApproval } from './validate.js';

const ENV_NAME = /^[a-z0-9][a-z0-9-]{0,38}$/;
const KEY_NAME = /^[A-Za-z0-9 ._-]{1,60}$/;

export const QUOTA_FIELDS = [
  'max_concurrent_runs',
  'max_runs_per_hour',
  'max_arrivals',
  'max_arrival_rate',
  'max_duration_ms',
  'api_requests_per_min',
];

export function buildApp({ pool, bin, metricsToken = '', now = () => Date.now(), logger = false, fetchImpl = fetch }) {
  const app = Fastify({ logger, bodyLimit: 1 << 20 });
  const limiter = new RateLimiter(now);
  const touched = new Map(); // key id -> when last_used_at was last written
  const metrics = new Metrics()
    .define('burstforge_api_requests_total', 'counter', 'Authenticated API requests by organization, route and status code.')
    .define('burstforge_api_rate_limited_total', 'counter', 'API requests refused by the per-organization rate limit.')
    .define('burstforge_run_requests_total', 'counter', 'Run requests made through the API, by organization and result.')
    .define('burstforge_quota_refusals_total', 'counter', 'Run requests refused by a quota, by organization and quota.')
    .define('burstforge_runs', 'gauge', 'Run requests by organization and current state.')
    .define('burstforge_runs_finished_total', 'counter', 'Finished runs by organization and verdict.')
    .define('burstforge_workflows_total', 'counter', 'Workflows in finished runs, by organization and result.')
    .define('burstforge_http_attempts_total', 'counter', 'HTTP attempts in finished runs, by organization.');

  const fail = (reply, code, error, message, extra = {}) => reply.code(code).send({ error, message, ...extra });

  // Accept an empty JSON body (curl -X POST -H 'Content-Type: application/json').
  app.removeContentTypeParser('application/json');
  app.addContentTypeParser('application/json', { parseAs: 'string' }, (req, body, done) => {
    if (body.trim() === '') return done(null, undefined);
    try {
      done(null, JSON.parse(body));
    } catch (e) {
      e.statusCode = 400;
      done(e);
    }
  });

  app.setErrorHandler((err, req, reply) => {
    if (err.validation) return fail(reply, 400, 'bad_request', err.message);
    if (err.statusCode && err.statusCode < 500) return fail(reply, err.statusCode, 'bad_request', err.message);
    req.log.error(err);
    return fail(reply, 500, 'internal', 'internal error');
  });

  // ---------------------------------------------------------- auth ---
  app.decorateRequest('tenant', null);
  app.addHook('onRequest', async (req, reply) => {
    if (!req.url.startsWith('/v1/')) return;
    const h = req.headers.authorization || '';
    const parsed = parseKey(h.startsWith('Bearer ') ? h.slice(7).trim() : '');
    if (!parsed) return fail(reply, 401, 'unauthorized', 'send an API key as "Authorization: Bearer bf_..."');
    const { rows } = await pool.query(
      `SELECT k.organization_id, k.role, k.secret_sha256, k.revoked_at, o.name, ${QUOTA_FIELDS.map((f) => 'o.' + f).join(', ')}
       FROM api_keys k JOIN organizations o ON o.id = k.organization_id WHERE k.id = $1`,
      [parsed.id],
    );
    const k = rows[0];
    if (!k || k.revoked_at || !secretMatches(parsed.secret, k.secret_sha256)) {
      return fail(reply, 401, 'unauthorized', 'invalid or revoked API key');
    }
    const quotas = Object.fromEntries(QUOTA_FIELDS.map((f) => [f, k[f]]));
    req.tenant = { org: k.organization_id, name: k.name, role: k.role, keyId: parsed.id, quotas };

    const wait = limiter.take(k.organization_id, k.api_requests_per_min);
    if (wait > 0) {
      metrics.inc('burstforge_api_rate_limited_total', { org: k.organization_id });
      reply.header('Retry-After', String(wait));
      return fail(reply, 429, 'rate_limited', `this organization may make ${k.api_requests_per_min} API requests a minute; retry in ${wait}s`, {
        quota: 'api_requests_per_min',
        limit: k.api_requests_per_min,
      });
    }
    const need = req.routeOptions.config?.role || 'viewer';
    if (ROLES[k.role] < ROLES[need]) {
      return fail(reply, 403, 'forbidden', `this needs the ${need} role; the key has ${k.role}`);
    }
    if (now() - (touched.get(parsed.id) || 0) > 60_000) {
      touched.set(parsed.id, now());
      await pool.query('UPDATE api_keys SET last_used_at = now() WHERE id = $1', [parsed.id]);
    }
  });

  app.addHook('onResponse', async (req, reply) => {
    if (!req.tenant) return;
    metrics.inc('burstforge_api_requests_total', {
      org: req.tenant.org,
      route: `${req.method} ${req.routeOptions.url || 'unknown'}`,
      code: String(reply.statusCode),
    });
  });

  // ------------------------------------------------------- health ---
  app.get('/healthz', async () => {
    await pool.query('SELECT 1');
    return { ok: true };
  });

  // ------------------------------------------------------ metrics ---
  async function refreshRunMetrics() {
    const states = await pool.query(`
      SELECT q.organization_id AS org, CASE WHEN q.status = 'started' THEN r.status ELSE q.status END AS state, count(*)::int AS n
      FROM run_requests q LEFT JOIN runs r ON r.id = q.id GROUP BY 1, 2`);
    metrics.reset('burstforge_runs');
    for (const r of states.rows) metrics.set('burstforge_runs', { org: r.org, state: r.state }, r.n);

    const done = await pool.query(`
      SELECT organization_id AS org,
             count(*) FILTER (WHERE (report->'verdict'->>'pass')::boolean)::int AS pass,
             count(*) FILTER (WHERE NOT (report->'verdict'->>'pass')::boolean)::int AS fail,
             coalesce(sum((report->'totals'->>'succeeded')::bigint), 0)::bigint AS ok,
             coalesce(sum((report->'totals'->>'failed')::bigint), 0)::bigint AS failed,
             coalesce(sum((report->'attempts'->>'total')::bigint), 0)::bigint AS attempts
      FROM runs WHERE organization_id IS NOT NULL AND status = 'completed' GROUP BY 1`);
    for (const f of ['burstforge_runs_finished_total', 'burstforge_workflows_total', 'burstforge_http_attempts_total']) metrics.reset(f);
    for (const r of done.rows) {
      metrics.set('burstforge_runs_finished_total', { org: r.org, verdict: 'pass' }, r.pass);
      metrics.set('burstforge_runs_finished_total', { org: r.org, verdict: 'fail' }, r.fail);
      metrics.set('burstforge_workflows_total', { org: r.org, result: 'ok' }, Number(r.ok));
      metrics.set('burstforge_workflows_total', { org: r.org, result: 'failed' }, Number(r.failed));
      metrics.set('burstforge_http_attempts_total', { org: r.org }, Number(r.attempts));
    }
  }

  // Every organization's metrics, for the operator's Prometheus. Disabled
  // unless a metrics token is configured, since it names every tenant.
  app.get('/metrics', async (req, reply) => {
    const got = Buffer.from(req.headers.authorization || '');
    const want = Buffer.from(`Bearer ${metricsToken}`);
    if (!metricsToken) return fail(reply, 404, 'not_found', 'set BURSTFORGE_METRICS_TOKEN to enable /metrics');
    if (got.length !== want.length || !timingSafeEqual(got, want)) return fail(reply, 401, 'unauthorized', 'bad metrics token');
    await refreshRunMetrics();
    return reply.type('text/plain; version=0.0.4').send(metrics.render());
  });

  // The caller's own organization only.
  app.get('/v1/metrics', async (req, reply) => {
    await refreshRunMetrics();
    return reply.type('text/plain; version=0.0.4').send(metrics.render(req.tenant.org));
  });

  // ----------------------------------------------------------- me ---
  async function usage(db, org) {
    const { rows } = await db.query(
      `SELECT count(*) FILTER (WHERE q.status IN ('queued', 'starting')
                                  OR (q.status = 'started' AND r.status IN ('running', 'finalizing')))::int AS active,
              count(*) FILTER (WHERE q.status <> 'rejected' AND q.created_at > now() - interval '1 hour')::int AS last_hour,
              extract(epoch FROM min(q.created_at) FILTER (WHERE q.status <> 'rejected' AND q.created_at > now() - interval '1 hour')
                                 + interval '1 hour' - now())::float AS hour_frees_in
       FROM run_requests q LEFT JOIN runs r ON r.id = q.id WHERE q.organization_id = $1`,
      [org],
    );
    return rows[0];
  }

  app.get('/v1/me', async (req) => {
    const u = await usage(pool, req.tenant.org);
    return {
      organization: { id: req.tenant.org, name: req.tenant.name },
      role: req.tenant.role,
      key: req.tenant.keyId,
      quotas: req.tenant.quotas,
      usage: { active_runs: u.active, runs_last_hour: u.last_hour },
    };
  });

  // --------------------------------------------------------- keys ---
  app.post(
    '/v1/keys',
    {
      config: { role: 'owner' },
      schema: {
        body: {
          type: 'object',
          required: ['name', 'role'],
          additionalProperties: false,
          properties: { name: { type: 'string' }, role: { enum: Object.keys(ROLES) } },
        },
      },
    },
    async (req, reply) => {
      if (!KEY_NAME.test(req.body.name)) return fail(reply, 400, 'bad_request', 'name: 1-60 letters, digits, spaces, dots, dashes or underscores');
      const k = newKey();
      await pool.query('INSERT INTO api_keys (id, organization_id, name, role, secret_sha256) VALUES ($1, $2, $3, $4, $5)', [
        k.id,
        req.tenant.org,
        req.body.name,
        req.body.role,
        k.hash,
      ]);
      return reply.code(201).send({ id: k.id, key: k.key, name: req.body.name, role: req.body.role, note: 'the key is shown once; store it now' });
    },
  );

  app.get('/v1/keys', { config: { role: 'owner' } }, async (req) => {
    const { rows } = await pool.query(
      `SELECT id, name, role, created_at, last_used_at, revoked_at FROM api_keys WHERE organization_id = $1 ORDER BY created_at`,
      [req.tenant.org],
    );
    return { keys: rows };
  });

  app.delete('/v1/keys/:id', { config: { role: 'owner' } }, async (req, reply) => {
    const r = await pool.query('UPDATE api_keys SET revoked_at = now() WHERE id = $1 AND organization_id = $2 AND revoked_at IS NULL', [
      req.params.id,
      req.tenant.org,
    ]);
    if (r.rowCount === 0) return fail(reply, 404, 'not_found', 'no such active key');
    return reply.code(204).send();
  });

  // ------------------------------------------------- environments ---
  const envView = (e) => ({
    name: e.name,
    base_url: e.base_url,
    approval_token: e.approval_token,
    approval_url: e.base_url.replace(/\/+$/, '') + '/.well-known/burstforge-approval.txt',
    verified: e.verified_at !== null,
    verified_at: e.verified_at,
  });

  app.post(
    '/v1/environments',
    {
      config: { role: 'owner' },
      schema: {
        body: {
          type: 'object',
          required: ['name', 'base_url'],
          additionalProperties: false,
          properties: { name: { type: 'string' }, base_url: { type: 'string', maxLength: 2048 } },
        },
      },
    },
    async (req, reply) => {
      const { name, base_url } = req.body;
      if (!ENV_NAME.test(name)) return fail(reply, 400, 'bad_request', 'name: lower-case letters, digits and dashes, up to 39');
      let u;
      try {
        u = new URL(base_url);
      } catch {
        return fail(reply, 400, 'bad_request', 'base_url is not a URL');
      }
      if (!['http:', 'https:'].includes(u.protocol) || u.username || u.password || u.search || u.hash) {
        return fail(reply, 400, 'bad_request', 'base_url must be a plain http(s) URL without credentials, query or fragment');
      }
      const clean = base_url.replace(/\/+$/, '');
      const r = await pool.query(
        `INSERT INTO environments (organization_id, name, base_url, approval_token) VALUES ($1, $2, $3, $4)
         ON CONFLICT DO NOTHING RETURNING *`,
        [req.tenant.org, name, clean, approvalToken(req.tenant.org)],
      );
      if (r.rowCount === 0) return fail(reply, 409, 'conflict', `environment ${name} already exists`);
      const view = envView(r.rows[0]);
      return reply.code(201).send({
        ...view,
        next: `serve the approval token on a line of its own at ${view.approval_url}, then POST /v1/environments/${name}/verify`,
      });
    },
  );

  app.get('/v1/environments', async (req) => {
    const { rows } = await pool.query('SELECT * FROM environments WHERE organization_id = $1 ORDER BY name', [req.tenant.org]);
    return { environments: rows.map(envView) };
  });

  // Verification reflects the target now: a failed check clears it.
  app.post('/v1/environments/:name/verify', { config: { role: 'operator' } }, async (req, reply) => {
    const { rows } = await pool.query('SELECT * FROM environments WHERE organization_id = $1 AND name = $2', [req.tenant.org, req.params.name]);
    if (!rows[0]) return fail(reply, 404, 'not_found', `no environment ${req.params.name}`);
    const res = await checkApproval(rows[0].base_url, rows[0].approval_token, fetchImpl);
    const upd = await pool.query(
      `UPDATE environments SET verified_at = CASE WHEN $3 THEN now() END WHERE organization_id = $1 AND name = $2 RETURNING *`,
      [req.tenant.org, req.params.name, res.ok],
    );
    if (!res.ok) return fail(reply, 422, 'not_verified', res.reason, { environment: envView(upd.rows[0]) });
    return envView(upd.rows[0]);
  });

  // --------------------------------------------------------- runs ---
  const runView = (r) => ({
    id: r.id,
    environment: r.environment,
    state: r.status === 'started' ? r.run_status : r.status,
    reason: r.reason || undefined,
    seed: Number(r.seed),
    shards: r.shard_count,
    requested_at: r.created_at,
    start_at: r.start_at || undefined,
    finished_at: r.finished_at || undefined,
    verdict: r.report ? (r.report.verdict.pass ? 'pass' : 'fail') : undefined,
  });

  app.post(
    '/v1/runs',
    {
      config: { role: 'operator' },
      schema: {
        body: {
          type: 'object',
          required: ['environment', 'scenario'],
          additionalProperties: false,
          properties: {
            environment: { type: 'string' },
            scenario: { type: 'string', maxLength: 262144 },
            shards: { type: 'integer', minimum: 1, maximum: 64, default: 1 },
            seed: { type: 'integer', minimum: 1, maximum: 9007199254740991 },
          },
        },
      },
    },
    async (req, reply) => {
      const { org } = req.tenant;
      const refused = (result) => metrics.inc('burstforge_run_requests_total', { org, result });
      const env = (await pool.query('SELECT * FROM environments WHERE organization_id = $1 AND name = $2', [org, req.body.environment])).rows[0];
      if (!env) {
        refused('bad_environment');
        return fail(reply, 404, 'not_found', `no environment ${req.body.environment}`);
      }
      if (!env.verified_at) {
        refused('bad_environment');
        return fail(reply, 409, 'not_verified', `environment ${env.name} is not verified; POST /v1/environments/${env.name}/verify first`);
      }
      const seed = req.body.seed ?? randomInt(1, 1_000_000);
      const v = await validateScenario(bin, req.body.scenario, { bind: env.base_url, seed });
      if (!v.ok) {
        refused('invalid');
        return fail(reply, 422, 'invalid_scenario', 'the scenario has problems', { problems: v.errors });
      }
      const shards = req.body.shards;
      if (shards > v.arrivals) {
        refused('invalid');
        return fail(reply, 422, 'invalid_scenario', `shards (${shards}) can't exceed the ${v.arrivals} arrivals`);
      }

      const db = await pool.connect();
      try {
        await db.query('BEGIN');
        // Locking the organization row serializes this org's requests, so
        // two at once can't both slip under the concurrency limit.
        const o = (await db.query('SELECT * FROM organizations WHERE id = $1 FOR UPDATE', [org])).rows[0];
        const over = [];
        if (v.arrivals > o.max_arrivals) over.push({ quota: 'max_arrivals', limit: o.max_arrivals, requested: v.arrivals });
        if (v.peak_per_sec > o.max_arrival_rate) over.push({ quota: 'max_arrival_rate', limit: o.max_arrival_rate, requested: v.peak_per_sec });
        if (v.planned_span_ms > o.max_duration_ms) over.push({ quota: 'max_duration_ms', limit: o.max_duration_ms, requested: v.planned_span_ms });
        if (over.length > 0) {
          await db.query('ROLLBACK');
          for (const q of over) metrics.inc('burstforge_quota_refusals_total', { org, quota: q.quota });
          refused('quota');
          // Retrying won't help, so this is not a 429.
          return fail(reply, 422, 'quota_exceeded', over.map((q) => `${q.quota} is ${q.limit}, the run needs ${q.requested}`).join('; '), {
            quotas: over,
          });
        }
        const u = await usage(db, org);
        let quota = null;
        if (u.active >= o.max_concurrent_runs) {
          quota = { quota: 'max_concurrent_runs', limit: o.max_concurrent_runs, current: u.active, retry: 10 };
        } else if (u.last_hour >= o.max_runs_per_hour) {
          quota = { quota: 'max_runs_per_hour', limit: o.max_runs_per_hour, current: u.last_hour, retry: Math.max(1, Math.ceil(u.hour_frees_in || 60)) };
        }
        if (quota) {
          await db.query('ROLLBACK');
          metrics.inc('burstforge_quota_refusals_total', { org, quota: quota.quota });
          refused('quota');
          reply.header('Retry-After', String(quota.retry));
          return fail(reply, 429, 'quota_exceeded', `${quota.quota} is ${quota.limit} and the organization is at ${quota.current}`, {
            quota: quota.quota,
            limit: quota.limit,
            current: quota.current,
          });
        }
        const id = runId(now());
        await db.query(
          `INSERT INTO run_requests (id, organization_id, environment, scenario_yaml, seed, shard_count) VALUES ($1, $2, $3, $4, $5, $6)`,
          [id, org, env.name, req.body.scenario, seed, shards],
        );
        await db.query('COMMIT');
        metrics.inc('burstforge_run_requests_total', { org, result: 'queued' });
        return reply.code(202).send({
          id,
          state: 'queued',
          environment: env.name,
          seed,
          shards,
          arrivals: v.arrivals,
          peak_per_sec: v.peak_per_sec,
          planned_span_ms: v.planned_span_ms,
        });
      } catch (e) {
        await db.query('ROLLBACK').catch(() => {});
        throw e;
      } finally {
        db.release();
      }
    },
  );

  const runQuery = `
    SELECT q.id, q.environment, q.status, q.reason, q.seed, q.shard_count, q.created_at,
           r.status AS run_status, r.start_at, r.finished_at, r.report
    FROM run_requests q LEFT JOIN runs r ON r.id = q.id`;

  app.get('/v1/runs', async (req) => {
    const { rows } = await pool.query(`${runQuery} WHERE q.organization_id = $1 ORDER BY q.created_at DESC LIMIT 50`, [req.tenant.org]);
    return { runs: rows.map(runView) };
  });

  app.get('/v1/runs/:id', async (req, reply) => {
    const { rows } = await pool.query(`${runQuery} WHERE q.organization_id = $1 AND q.id = $2`, [req.tenant.org, req.params.id]);
    if (!rows[0]) return fail(reply, 404, 'not_found', `no run ${req.params.id}`);
    return { ...runView(rows[0]), report: rows[0].report || undefined };
  });

  return app;
}

function runId(t) {
  const d = new Date(t);
  const p = (n) => String(n).padStart(2, '0');
  return (
    `${d.getUTCFullYear()}${p(d.getUTCMonth() + 1)}${p(d.getUTCDate())}-` +
    `${p(d.getUTCHours())}${p(d.getUTCMinutes())}${p(d.getUTCSeconds())}-${randomBytes(4).toString('hex')}`
  );
}
