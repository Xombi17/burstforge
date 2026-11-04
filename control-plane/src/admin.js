// Operator commands that talk to the database directly. Creating an
// organization prints its first owner key; everything else goes through
// the API with that key.
//
//   node src/admin.js create-org acme --name "Acme" [--max-arrivals 2000 ...]
//   node src/admin.js set-quota acme --max-concurrent-runs 2
//   node src/admin.js create-key acme --name ci --role operator
//   node src/admin.js list
import { parseArgs } from 'node:util';
import pg from 'pg';
import { newKey, ROLES } from './keys.js';
import { QUOTA_FIELDS } from './app.js';

const usage = `usage:
  admin.js create-org <id> --name NAME [quota flags]
  admin.js set-quota <id> [quota flags]
  admin.js create-key <id> --name NAME --role owner|operator|viewer
  admin.js delete-org <id>     (also deletes its keys, environments and runs)
  admin.js list

quota flags: ${QUOTA_FIELDS.map((f) => '--' + f.replaceAll('_', '-') + ' N').join(' ')}
(BURSTFORGE_DB must be set)`;

const options = { name: { type: 'string' }, role: { type: 'string' } };
for (const f of QUOTA_FIELDS) options[f.replaceAll('_', '-')] = { type: 'string' };

async function main() {
  let parsed;
  try {
    parsed = parseArgs({ args: process.argv.slice(2), options, allowPositionals: true });
  } catch (e) {
    console.error(e.message + '\n\n' + usage);
    return 2;
  }
  const { values, positionals } = parsed;
  const [cmd, org] = positionals;
  if (!process.env.BURSTFORGE_DB || !cmd) {
    console.error(usage);
    return 2;
  }
  const quotas = {};
  for (const f of QUOTA_FIELDS) {
    const v = values[f.replaceAll('_', '-')];
    if (v === undefined) continue;
    if (!/^\d+$/.test(v)) {
      console.error(`--${f.replaceAll('_', '-')} must be a whole number`);
      return 2;
    }
    quotas[f] = Number(v);
  }
  const db = new pg.Client({ connectionString: process.env.BURSTFORGE_DB });
  await db.connect();
  try {
    switch (cmd) {
      case 'create-org': {
        if (!org || !values.name) break;
        const cols = ['id', 'name', ...Object.keys(quotas)];
        const vals = [org, values.name, ...Object.values(quotas)];
        await db.query('BEGIN');
        await db.query(`INSERT INTO organizations (${cols.join(', ')}) VALUES (${cols.map((_, i) => '$' + (i + 1)).join(', ')})`, vals);
        const k = newKey();
        await db.query(`INSERT INTO api_keys (id, organization_id, name, role, secret_sha256) VALUES ($1, $2, 'owner', 'owner', $3)`, [
          k.id,
          org,
          k.hash,
        ]);
        await db.query('COMMIT');
        console.log(`created organization ${org}`);
        console.log(`owner key (shown once): ${k.key}`);
        return 0;
      }
      case 'set-quota': {
        if (!org || Object.keys(quotas).length === 0) break;
        const sets = Object.keys(quotas).map((f, i) => `${f} = $${i + 2}`);
        const r = await db.query(`UPDATE organizations SET ${sets.join(', ')} WHERE id = $1`, [org, ...Object.values(quotas)]);
        if (r.rowCount === 0) {
          console.error(`no organization ${org}`);
          return 1;
        }
        console.log(`updated ${org}: ${JSON.stringify(quotas)}`);
        return 0;
      }
      case 'create-key': {
        if (!org || !values.name || !ROLES[values.role]) break;
        const k = newKey();
        await db.query(`INSERT INTO api_keys (id, organization_id, name, role, secret_sha256) VALUES ($1, $2, $3, $4, $5)`, [
          k.id,
          org,
          values.name,
          values.role,
          k.hash,
        ]);
        console.log(`${values.role} key for ${org} (shown once): ${k.key}`);
        return 0;
      }
      case 'delete-org': {
        if (!org) break;
        const r = await db.query('DELETE FROM organizations WHERE id = $1', [org]);
        console.log(r.rowCount ? `deleted ${org}` : `no organization ${org}`);
        return 0;
      }
      case 'list': {
        const { rows } = await db.query(`SELECT id, name, ${QUOTA_FIELDS.join(', ')} FROM organizations ORDER BY id`);
        for (const r of rows) console.log(JSON.stringify(r));
        return 0;
      }
    }
    console.error(usage);
    return 2;
  } catch (e) {
    await db.query('ROLLBACK').catch(() => {});
    console.error(e.message);
    return 1;
  } finally {
    await db.end();
  }
}

process.exitCode = await main();
