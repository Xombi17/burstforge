'use strict';
// Fragile event-registration target for BurstForge.
//
// A deliberately breakable app: SSO with a concurrency cap, a DB pool with a
// seat-counter row lock, a background email queue, and two profiles:
//
//   fragile   - the bugs behind the real outages in docs/decision.md
//   hardened  - the same app with the fixes applied
//
// Switch at runtime with POST /_bf/config {"profile":"hardened"}.
// Zero dependencies on purpose: `node server.js` is all you need.

const http = require('node:http');
const crypto = require('node:crypto');

// ---------------------------------------------------------------- config ---

const PROFILES = {
  fragile: {
    idempotency: false, // ignores Idempotency-Key
    uniqueEmail: false, // check-then-insert race, no unique constraint
    atomicSeats: false, // seat check and decrement are not one transaction
    ssoRetries: 3, // immediate retries when SSO times out
    ssoBackoff: false,
    tokenCache: false, // introspects the token against SSO on every request
    admissionLimit: 0, // no load shedding
    ssoShedQueue: 0, // no SSO queue limit
    ssoCancelQueued: false, // timed-out SSO calls keep running as zombie work
    jobQueueCap: 50, // in-memory queue that silently drops on overflow
  },
  hardened: {
    idempotency: true,
    uniqueEmail: true,
    atomicSeats: true,
    ssoRetries: 2,
    ssoBackoff: true,
    tokenCache: true,
    admissionLimit: 400,
    ssoShedQueue: 300, // shed instead of queueing past this depth
    ssoCancelQueued: true, // abandon queued SSO calls whose caller gave up
    jobQueueCap: Infinity,
  },
};

const DEFAULTS = {
  seats: 300,
  ssoLatencyMs: 40,
  ssoConcurrency: 25,
  ssoTimeoutMs: 800,
  dbPool: 10,
  rowLockMs: 10,
  jobMs: 60,
  jobWorkers: 4,
};

const cfg = { profile: 'fragile', ...DEFAULTS, ...PROFILES.fragile };

// ------------------------------------------------------------ primitives ---

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** FIFO counting semaphore. */
class Semaphore {
  constructor(n) {
    this.capacity = n;
    this.free = n;
    this.waiters = [];
  }
  setCapacity(n) {
    this.free += n - this.capacity;
    this.capacity = n;
    this._drain();
  }
  get waiting() {
    return this.waiters.length;
  }
  acquire() {
    if (this.free > 0) {
      this.free--;
      return Promise.resolve();
    }
    return new Promise((resolve) => this.waiters.push(resolve));
  }
  /** Like acquire(), but the waiter can be withdrawn while it is still queued. */
  acquireCancelable() {
    if (this.free > 0) {
      this.free--;
      return { ready: Promise.resolve(), cancel: () => false };
    }
    let resolve;
    const ready = new Promise((r) => (resolve = r));
    this.waiters.push(resolve);
    return {
      ready,
      cancel: () => {
        const i = this.waiters.indexOf(resolve);
        if (i === -1) return false; // already running: caller must let it finish
        this.waiters.splice(i, 1);
        return true;
      },
    };
  }
  release() {
    this.free++;
    this._drain();
  }
  _drain() {
    while (this.free > 0 && this.waiters.length > 0) {
      this.free--;
      this.waiters.shift()();
    }
  }
  async run(fn) {
    await this.acquire();
    try {
      return await fn();
    } finally {
      this.release();
    }
  }
}

class HttpError extends Error {
  constructor(status, code, headers = {}) {
    super(code);
    this.status = status;
    this.code = code;
    this.headers = headers;
  }
}

// ----------------------------------------------------------------- state ---

let sso, dbPool, seatLock;
let state;
const validTokens = new Set();
const idem = new Map();

function resetState(seats) {
  state = {
    seatsTotal: seats,
    seatsLeft: seats,
    registrations: [], // {id, email}
    jobsQueue: [],
    jobsAccepted: 0,
    jobsCompleted: 0,
    jobsDropped: 0,
    jobsRunning: 0,
    stats: {
      req_refresh: 0,
      req_register: 0,
      sso_calls: 0,
      sso_timeouts: 0,
      db_queries: 0,
      shed_429: 0,
      idempotent_replays: 0,
      responses_5xx: 0,
    },
  };
  validTokens.clear();
  idem.clear();
}

function rebuildResources() {
  sso = new Semaphore(cfg.ssoConcurrency);
  dbPool = new Semaphore(cfg.dbPool);
  seatLock = new Semaphore(1);
}

// ------------------------------------------------------------------- sso ---

/**
 * One SSO round trip. By default a caller that times out still occupies its
 * SSO slot (zombie work). With ssoCancelQueued, a call that never started is
 * withdrawn from the queue instead.
 */
async function ssoCall() {
  state.stats.sso_calls++;
  const slot = sso.acquireCancelable();
  const work = slot.ready.then(async () => {
    try {
      await sleep(cfg.ssoLatencyMs);
    } finally {
      sso.release();
    }
  });
  let timer;
  const deadline = new Promise((_, reject) => {
    timer = setTimeout(() => {
      state.stats.sso_timeouts++;
      if (cfg.ssoCancelQueued) slot.cancel();
      reject(new HttpError(503, 'sso_timeout'));
    }, cfg.ssoTimeoutMs);
  });
  try {
    await Promise.race([work, deadline]);
  } finally {
    clearTimeout(timer);
  }
}

async function ssoWithRetry() {
  if (cfg.ssoShedQueue > 0 && sso.waiting > cfg.ssoShedQueue) {
    state.stats.shed_429++;
    throw new HttpError(429, 'sso_overloaded', { 'Retry-After': '1' });
  }
  let lastErr;
  for (let attempt = 0; attempt < cfg.ssoRetries; attempt++) {
    try {
      return await ssoCall();
    } catch (err) {
      lastErr = err;
      if (cfg.ssoBackoff) await sleep(Math.random() * 100 * 2 ** attempt);
    }
  }
  throw lastErr;
}

// -------------------------------------------------------------------- db ---

async function query(ms) {
  state.stats.db_queries++;
  await sleep(ms);
}

// ------------------------------------------------------------- job queue ---

function enqueueJob(s, regId) {
  if (s.jobsQueue.length >= cfg.jobQueueCap) {
    s.jobsDropped++; // the bug: overflow is silent
    return;
  }
  s.jobsAccepted++;
  s.jobsQueue.push(regId);
  pumpJobs(s);
}

// Work in flight when /_bf/reset runs keeps updating the state it started in,
// so a reset can never corrupt the counters of the next run.
function pumpJobs(s) {
  while (s.jobsRunning < cfg.jobWorkers && s.jobsQueue.length > 0) {
    s.jobsQueue.shift();
    s.jobsRunning++;
    sleep(cfg.jobMs).then(() => {
      s.jobsRunning--;
      s.jobsCompleted++;
      pumpJobs(s);
    });
  }
}

// ------------------------------------------------------------- handlers ---

function bearer(req) {
  const h = req.headers.authorization || '';
  return h.startsWith('Bearer ') ? h.slice(7) : '';
}

async function handleRefresh() {
  state.stats.req_refresh++;
  await ssoWithRetry();
  const token = crypto.randomUUID();
  validTokens.add(token);
  return { status: 200, body: { token } };
}

async function authenticate(req) {
  const token = bearer(req);
  if (!token) throw new HttpError(401, 'missing_token');
  if (cfg.tokenCache) {
    if (!validTokens.has(token)) throw new HttpError(401, 'bad_token');
  } else {
    await ssoWithRetry(); // introspect on every request
  }
}

function shedIfOverloaded() {
  if (cfg.admissionLimit > 0 && dbPool.waiting + seatLock.waiting > cfg.admissionLimit) {
    state.stats.shed_429++;
    throw new HttpError(429, 'overloaded', { 'Retry-After': '1' });
  }
}

async function commitRegistration(email, name) {
  const s = state; // pin: a reset mid-flight must not leak into the next run
  const insert = () => {
    const id = crypto.randomUUID();
    s.registrations.push({ id, email, name });
    enqueueJob(s, id);
    return { status: 201, body: { id, email } };
  };

  return dbPool.run(async () => {
    if (cfg.atomicSeats) {
      if (s.seatsLeft <= 0) return { status: 409, body: { error: 'sold_out' } };
      return seatLock.run(async () => {
        await query(5); // SELECT ... FOR UPDATE
        if (cfg.uniqueEmail && s.registrations.some((r) => r.email === email)) {
          return { status: 409, body: { error: 'already_registered' } };
        }
        if (s.seatsLeft <= 0) return { status: 409, body: { error: 'sold_out' } };
        await query(cfg.rowLockMs); // INSERT + UPDATE seats under the row lock
        s.seatsLeft--;
        return insert();
      });
    }

    // Fragile path: read, read, write, lock. The awaits are where races live.
    await query(5);
    if (s.registrations.some((r) => r.email === email)) {
      return { status: 409, body: { error: 'already_registered' } };
    }
    await query(5);
    if (s.seatsLeft <= 0) return { status: 409, body: { error: 'sold_out' } };
    await query(30); // slow commit: the window the race lives in
    const result = insert();
    await seatLock.run(async () => {
      await query(cfg.rowLockMs); // UPDATE seats SET left = left - 1
      s.seatsLeft--;
    });
    return result;
  });
}

async function handleRegister(req, body) {
  state.stats.req_register++;
  shedIfOverloaded();
  await authenticate(req);

  const email = String(body.email || '').toLowerCase();
  if (!email.includes('@')) throw new HttpError(400, 'invalid_email');

  const key = req.headers['idempotency-key'];
  if (cfg.idempotency && key) {
    if (idem.has(key)) {
      state.stats.idempotent_replays++;
      return idem.get(key);
    }
    const pending = commitRegistration(email, body.name);
    idem.set(key, pending);
    pending.catch(() => idem.delete(key)); // do not cache failures
    return pending;
  }
  return commitRegistration(email, body.name);
}

function invariants() {
  const emails = new Set(state.registrations.map((r) => r.email));
  const regs = state.registrations.length;
  const pending = state.jobsQueue.length + state.jobsRunning;
  return {
    seats_total: state.seatsTotal,
    seats_left: state.seatsLeft,
    registrations_total: regs,
    registrations_unique: emails.size,
    duplicate_registrations: regs - emails.size,
    oversold: Math.max(0, emails.size - state.seatsTotal),
    jobs_accepted: state.jobsAccepted,
    jobs_completed: state.jobsCompleted,
    jobs_pending: pending,
    jobs_dropped: state.jobsDropped,
    jobs_lost: Math.max(0, regs - state.jobsCompleted - pending),
  };
}

// ---------------------------------------------------------------- router ---

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      if (chunks.length === 0) return resolve({});
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')));
      } catch {
        reject(new HttpError(400, 'invalid_json'));
      }
    });
    req.on('error', reject);
  });
}

function send(res, status, body, headers = {}) {
  if (res.destroyed || res.writableEnded) return;
  if (status >= 500) state.stats.responses_5xx++;
  const payload = JSON.stringify(body);
  res.writeHead(status, {
    'content-type': 'application/json',
    'content-length': Buffer.byteLength(payload),
    ...headers,
  });
  res.end(payload);
}

function applyConfig(patch) {
  if (patch.profile) {
    if (!PROFILES[patch.profile]) throw new HttpError(400, 'unknown_profile');
    Object.assign(cfg, PROFILES[patch.profile], { profile: patch.profile });
  }
  for (const [k, v] of Object.entries(patch)) {
    if (k === 'profile') continue;
    if (!(k in cfg)) throw new HttpError(400, `unknown_option:${k}`);
    cfg[k] = v === 'Infinity' ? Infinity : v;
  }
  rebuildResources();
}

async function route(req, res) {
  const { method } = req;
  const path = new URL(req.url, 'http://x').pathname;

  if (method === 'GET' && path === '/healthz') return send(res, 200, { ok: true });
  // Organizations allowed to test this target through a BurstForge control
  // plane, one approval token per line (APPROVAL_TOKENS, comma-separated).
  if (method === 'GET' && path === '/.well-known/burstforge-approval.txt' && approvalTokens.length > 0) {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    return res.end(approvalTokens.join('\n') + '\n');
  }
  if (method === 'GET' && path === '/_bf/config') return send(res, 200, cfg);
  if (method === 'GET' && path === '/_bf/stats') {
    return send(res, 200, {
      ...state.stats,
      db_waiting: dbPool.waiting,
      seat_lock_waiting: seatLock.waiting,
      sso_waiting: sso.waiting,
    });
  }
  if (method === 'GET' && path === '/_bf/invariants') return send(res, 200, invariants());

  if (method === 'POST' && path === '/_bf/config') {
    applyConfig(await readBody(req));
    return send(res, 200, cfg);
  }
  if (method === 'POST' && path === '/_bf/reset') {
    const body = await readBody(req);
    resetState(body.seats || cfg.seats);
    rebuildResources();
    return send(res, 200, { ok: true, seats: state.seatsTotal });
  }

  if (method === 'POST' && path === '/auth/refresh') {
    const r = await handleRefresh();
    return send(res, r.status, r.body);
  }
  if (method === 'POST' && path === '/register') {
    const body = await readBody(req);
    const r = await handleRegister(req, body);
    return send(res, r.status, r.body);
  }
  throw new HttpError(404, 'not_found');
}

const approvalTokens = (process.env.APPROVAL_TOKENS || '').split(',').map((t) => t.trim()).filter(Boolean);

const server = http.createServer((req, res) => {
  route(req, res).catch((err) => {
    if (err instanceof HttpError) return send(res, err.status, { error: err.code }, err.headers);
    console.error(err);
    send(res, 500, { error: 'internal' });
  });
});
server.keepAliveTimeout = 30_000;
server.headersTimeout = 35_000;

resetState(cfg.seats);
rebuildResources();

const port = Number(process.env.PORT || 8080);
const profile = process.env.PROFILE || 'fragile';
applyConfig({ profile });
server.listen(port, () => {
  console.log(`bf-target listening on :${port} (profile=${cfg.profile}, seats=${cfg.seats})`);
});
