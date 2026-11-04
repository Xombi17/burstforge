// API keys look like bf_k1a2b3c4d5e6_<32 url-safe characters>. The id is
// stored in the clear and identifies the key; only a SHA-256 of the secret
// is stored, so a database leak doesn't leak working keys.
import { createHash, randomBytes, timingSafeEqual } from 'node:crypto';

const KEY_RE = /^bf_(k[0-9a-f]{12})_([A-Za-z0-9_-]{32})$/;

export function newKey() {
  const id = 'k' + randomBytes(6).toString('hex');
  const secret = randomBytes(24).toString('base64url');
  return { id, key: `bf_${id}_${secret}`, hash: sha256(secret) };
}

export function parseKey(raw) {
  const m = KEY_RE.exec(raw || '');
  return m ? { id: m[1], secret: m[2] } : null;
}

export function sha256(s) {
  return createHash('sha256').update(s).digest();
}

export function secretMatches(secret, hash) {
  const h = sha256(secret);
  return Buffer.isBuffer(hash) && h.length === hash.length && timingSafeEqual(h, hash);
}

// The token a target serves at /.well-known/burstforge-approval.txt to let
// this organization test it.
export function approvalToken(org) {
  return `bf-approve-${org}-${randomBytes(12).toString('hex')}`;
}

export const ROLES = { viewer: 1, operator: 2, owner: 3 };
