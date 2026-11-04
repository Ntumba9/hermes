import { test } from "node:test";
import assert from "node:assert/strict";
import { backoffSeconds } from "./worker.js";

// No DB dependency - pure function test.

test("backoff stays within [0, cap] and the cap grows exponentially", async () => {
  // attempt 1: cap = base * 2^0 = 2
  for (let i = 0; i < 200; i++) {
    const v = backoffSeconds(1);
    assert.ok(v >= 0 && v <= 2, `attempt 1 sample ${v} out of [0,2]`);
  }
  // attempt 3: cap = base * 2^2 = 8
  for (let i = 0; i < 200; i++) {
    const v = backoffSeconds(3);
    assert.ok(v >= 0 && v <= 8, `attempt 3 sample ${v} out of [0,8]`);
  }
});

test("backoff is capped, not unbounded, for high attempt numbers", async () => {
  // base=2, cap=60 by construction: 2*2^(20-1) would be huge without the cap
  for (let i = 0; i < 50; i++) {
    const v = backoffSeconds(20);
    assert.ok(v >= 0 && v <= 60, `expected capped at 60, got ${v}`);
  }
});

test("backoff is randomized, not a fixed delay", async () => {
  const samples = Array.from({ length: 50 }, () => backoffSeconds(4));
  const distinctValues = new Set(samples.map((v) => v.toFixed(6)));
  assert.ok(distinctValues.size > 1, "expected jitter to produce varying values, not a fixed delay");
});
