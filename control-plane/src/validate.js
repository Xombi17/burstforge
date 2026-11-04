// Scenario validation is done by the Go binary, so the control plane, the
// controller and the agents apply exactly the same rules.
import { spawn } from 'node:child_process';

export function validateScenario(bin, yaml, { bind, seed }) {
  return new Promise((resolve, reject) => {
    const args = ['validate', '--json', '--seed', String(seed)];
    if (bind) args.push('--bind', bind);
    args.push('-');
    const child = spawn(bin, args, { stdio: ['pipe', 'pipe', 'pipe'] });
    let out = '';
    let err = '';
    const timer = setTimeout(() => child.kill('SIGKILL'), 10_000);
    child.stdout.on('data', (d) => (out += d));
    child.stderr.on('data', (d) => (err += d));
    child.on('error', (e) => {
      clearTimeout(timer);
      reject(new Error(`running ${bin}: ${e.message}`));
    });
    child.on('close', () => {
      clearTimeout(timer);
      try {
        resolve(JSON.parse(out));
      } catch {
        reject(new Error(`${bin} validate gave no JSON: ${err.trim() || out.trim()}`));
      }
    });
    child.stdin.on('error', () => {}); // the child may exit before reading everything
    child.stdin.end(yaml);
  });
}

// checkApproval fetches the target's approval file and looks for the token
// on a line of its own, like the Go agents do. No redirects, 5s, 64 KB.
export async function checkApproval(baseURL, token, fetchImpl = fetch) {
  const url = baseURL.replace(/\/+$/, '') + '/.well-known/burstforge-approval.txt';
  let res;
  try {
    res = await fetchImpl(url, { redirect: 'manual', signal: AbortSignal.timeout(5000) });
  } catch (e) {
    return { ok: false, reason: `fetching ${url}: ${e.cause?.message || e.message}` };
  }
  if (res.status !== 200) {
    return { ok: false, reason: `${url} returned HTTP ${res.status}; the target must serve the approval token there` };
  }
  const reader = res.body.getReader();
  let text = '';
  while (text.length < 65536) {
    const { done, value } = await reader.read();
    if (done) break;
    text += Buffer.from(value).toString('utf8');
  }
  await reader.cancel().catch(() => {});
  const found = text
    .slice(0, 65536)
    .split('\n')
    .some((line) => line.trim() === token);
  return found ? { ok: true } : { ok: false, reason: `${url} doesn't list this organization's approval token` };
}
