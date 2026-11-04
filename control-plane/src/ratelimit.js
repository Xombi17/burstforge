// A token bucket per key, refilled continuously. It lives in this process,
// so with several control-plane instances each enforces its own budget; the
// plan's Redis-backed limiter is not built yet.
export class RateLimiter {
  constructor(now = () => Date.now()) {
    this.now = now;
    this.buckets = new Map();
  }

  // take spends one token. It returns 0 if the request may proceed, or the
  // whole seconds until a token is available.
  take(key, perMinute) {
    if (perMinute <= 0) return 60;
    const t = this.now();
    const perMs = perMinute / 60_000;
    let b = this.buckets.get(key);
    if (!b) {
      b = { tokens: perMinute, at: t };
      this.buckets.set(key, b);
    }
    b.tokens = Math.min(perMinute, b.tokens + (t - b.at) * perMs);
    b.at = t;
    if (b.tokens >= 1) {
      b.tokens -= 1;
      return 0;
    }
    return Math.max(1, Math.ceil((1 - b.tokens) / perMs / 1000));
  }
}
