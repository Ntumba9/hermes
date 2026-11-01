import { randomUUID } from "node:crypto";
import { pool } from "./db.js";

const RECEIVER_URL = "http://127.0.0.1:4000/receive"; // local-only allowlisted target
const LEASE_SECONDS = 30;
const POLL_INTERVAL_MS = 500;

type ClaimedEvent = {
  id: string;
  idempotency_key: string;
  payload: unknown;
  lease_token: string;
};

// Claim one pending job: lock it, stamp a fresh lease token, commit, then let the
// caller do the network call outside this transaction. SKIP LOCKED means a second
// worker polling concurrently just moves on to the next row instead of blocking.
async function claimOne(): Promise<ClaimedEvent | null> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const { rows } = await client.query<ClaimedEvent>(
      `SELECT id, idempotency_key, payload
       FROM events
       WHERE status = 'pending' AND next_attempt_at <= now()
       ORDER BY created_at
       LIMIT 1
       FOR UPDATE SKIP LOCKED`
    );
    if (rows.length === 0) {
      await client.query("ROLLBACK");
      return null;
    }

    const leaseToken = randomUUID();
    const row = rows[0];
    await client.query(
      `UPDATE events
       SET status = 'claimed', lease_token = $1, lease_expires_at = now() + interval '${LEASE_SECONDS} seconds'
       WHERE id = $2`,
      [leaseToken, row.id]
    );
    await client.query("COMMIT");
    return { ...row, lease_token: leaseToken };
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

async function deliver(event: ClaimedEvent): Promise<boolean> {
  try {
    const res = await fetch(RECEIVER_URL, {
      method: "POST",
      headers: { "content-type": "application/json", "x-event-id": event.id },
      body: JSON.stringify(event.payload),
    });
    return res.ok;
  } catch {
    return false;
  }
}

// Only update the row if lease_token still matches what we claimed with. If the
// lease expired and another worker reclaimed this job in the meantime, its token
// is different, the WHERE matches zero rows, and this stale write is a no-op.
async function markDelivered(event: ClaimedEvent): Promise<void> {
  await pool.query(
    `UPDATE events SET status = 'delivered', attempts = attempts + 1
     WHERE id = $1 AND lease_token = $2`,
    [event.id, event.lease_token]
  );
}

async function markFailed(event: ClaimedEvent): Promise<void> {
  await pool.query(
    `UPDATE events
     SET status = 'pending', attempts = attempts + 1, next_attempt_at = now() + interval '2 seconds'
     WHERE id = $1 AND lease_token = $2`,
    [event.id, event.lease_token]
  );
}

async function runOnce(): Promise<boolean> {
  const event = await claimOne();
  if (!event) return false;

  const ok = await deliver(event);
  if (ok) {
    await markDelivered(event);
  } else {
    await markFailed(event);
  }
  return true;
}

async function main() {
  console.log("HERMES worker started");
  for (;;) {
    const didWork = await runOnce();
    if (!didWork) {
      await new Promise((r) => setTimeout(r, POLL_INTERVAL_MS));
    }
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
