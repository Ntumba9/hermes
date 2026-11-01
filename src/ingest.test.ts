import { test } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { pool } from "./db.js";

// Exercises the real Postgres instance (docker compose up) and the exact
// INSERT ... ON CONFLICT the API uses, not a mock, since the whole point of
// the idempotency guarantee lives in that one query.

async function upsertEvent(idempotencyKey: string, payload: unknown) {
  const id = randomUUID();
  const result = await pool.query<{ id: string }>(
    `INSERT INTO events (id, idempotency_key, payload)
     VALUES ($1, $2, $3)
     ON CONFLICT (idempotency_key) DO UPDATE SET idempotency_key = events.idempotency_key
     RETURNING id`,
    [id, idempotencyKey, payload]
  );
  return { attemptedId: id, returnedId: result.rows[0].id };
}

test("same idempotency key never produces two rows", async () => {
  const key = `test-${randomUUID()}`;

  const first = await upsertEvent(key, { n: 1 });
  const second = await upsertEvent(key, { n: 2 });

  assert.equal(first.attemptedId, first.returnedId, "first insert should be the new row");
  assert.notEqual(second.attemptedId, second.returnedId, "second insert should hit the conflict path");
  assert.equal(second.returnedId, first.returnedId, "both calls resolve to the same row");

  const { rows } = await pool.query("SELECT count(*) FROM events WHERE idempotency_key = $1", [key]);
  assert.equal(rows[0].count, "1");
});

test.after(async () => {
  await pool.end();
});
