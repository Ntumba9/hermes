import { test } from "node:test";
import assert from "node:assert/strict";
import { sign, verify } from "./signing.js";

test("verify accepts a signature produced by sign", () => {
  const body = JSON.stringify({ amount: 42 });
  assert.equal(verify(body, sign(body)), true);
});

test("verify rejects a tampered body", () => {
  const signature = sign(JSON.stringify({ amount: 42 }));
  assert.equal(verify(JSON.stringify({ amount: 43 }), signature), false);
});

test("verify rejects a garbage signature", () => {
  const body = JSON.stringify({ amount: 42 });
  assert.equal(verify(body, "not-a-real-signature"), false);
});
