// Starts the control plane.
//
//   BURSTFORGE_DB=postgres://... node src/server.js
//
// Environment:
//   BURSTFORGE_DB             Postgres URL (required)
//   BURSTFORGE_BIN            the burstforge binary (default: ../bin/burstforge, then PATH)
//   HOST, PORT                where to listen (default 127.0.0.1:8090)
//   BURSTFORGE_METRICS_TOKEN  enables /metrics for every organization behind this bearer token
import { existsSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import pg from 'pg';
import { buildApp } from './app.js';

const db = process.env.BURSTFORGE_DB;
if (!db) {
  console.error('set BURSTFORGE_DB to a Postgres URL');
  process.exit(2);
}
const local = fileURLToPath(new URL('../../bin/burstforge', import.meta.url));
const bin = process.env.BURSTFORGE_BIN || (existsSync(local) ? local : 'burstforge');

// The schema belongs to the Go side; apply it the same way agents do.
try {
  execFileSync(bin, ['migrate', '--db', db], { stdio: ['ignore', 'ignore', 'inherit'] });
} catch (e) {
  console.error(`could not migrate with ${bin}: ${e.message}`);
  process.exit(2);
}

const pool = new pg.Pool({ connectionString: db, max: 10 });
const app = buildApp({ pool, bin, metricsToken: process.env.BURSTFORGE_METRICS_TOKEN || '', logger: { level: 'info' } });
const port = Number(process.env.PORT || 8090);
const host = process.env.HOST || '127.0.0.1';
await app.listen({ port, host });

for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, async () => {
    await app.close();
    await pool.end();
    process.exit(0);
  });
}
