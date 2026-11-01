import { createHmac, timingSafeEqual } from "node:crypto";

// Dev-only default so the demo runs with zero setup. A real deployment would
// require HERMES_SIGNING_SECRET and refuse to start without it.
const SECRET = process.env.HERMES_SIGNING_SECRET ?? "hermes-dev-secret-do-not-use-in-prod";

export function sign(rawBody: string): string {
  return createHmac("sha256", SECRET).update(rawBody).digest("hex");
}

// Constant-time comparison: a naive === lets an attacker learn the signature
// one correct byte at a time by timing failed guesses.
export function verify(rawBody: string, signature: string): boolean {
  const expected = sign(rawBody);
  const expectedBuf = Buffer.from(expected, "hex");
  const actualBuf = Buffer.from(signature, "hex");
  if (expectedBuf.length !== actualBuf.length) return false;
  return timingSafeEqual(expectedBuf, actualBuf);
}
