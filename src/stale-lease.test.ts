import { test } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { pool } from "./db.js";

// Exercises lease-token fencing directly with raw SQL, mirroring exactly
// what claimOne/markDelivered in worker.ts do - no DB dependency beyond a
// live Postgres (docker compose up).

async function insertPendingEvent(): Promise<string> {
  const id = randomUUID();
  await pool.query(
    `INSERT INTO events (id, idempotency_key, payload) VALUES ($1, $2, $3)`,
    [id, `stale-lease-${id}`, { n: 1 }]
  );
  return id;
}

test("a write using a superseded lease token affects zero rows", async () => {
  const id = await insertPendingEvent();

  // Worker A claims it.
  const leaseA = randomUUID();
  await pool.query(
    `UPDATE events SET status = 'claimed', lease_token = $1, lease_expires_at = now() - interval '1 second'
     WHERE id = $2`,
    [leaseA, id]
  );

  // Its lease is already expired (set in the past above), so worker B
  // reclaims the same row - exactly what claimOne's WHERE clause does.
  const leaseB = randomUUID();
  const reclaim = await pool.query(
    `UPDATE events SET lease_token = $1, lease_expires_at = now() + interval '5 seconds'
     WHERE id = $2 AND status = 'claimed' AND lease_expires_at <= now()
     RETURNING id`,
    [leaseB, id]
  );
  assert.equal(reclaim.rowCount, 1, "worker B should have reclaimed the row");

  // Worker A, unaware its lease was superseded, finally finishes and tries
  // to mark the job delivered using its now-stale lease token A.
  const staleWrite = await pool.query(
    `UPDATE events SET status = 'delivered' WHERE id = $1 AND lease_token = $2`,
    [id, leaseA]
  );
  assert.equal(staleWrite.rowCount, 0, "a write with a superseded lease token must affect zero rows");

  const { rows } = await pool.query("SELECT status, lease_token FROM events WHERE id = $1", [id]);
  assert.equal(rows[0].status, "claimed", "worker B's claim must not have been clobbered");
  assert.equal(rows[0].lease_token, leaseB, "the current lease token must still be B's");
});

test.after(async () => {
  await pool.end();
});
