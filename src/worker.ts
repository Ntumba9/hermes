import { randomUUID } from "node:crypto";
import { pool } from "./db.js";
import { sign } from "./signing.js";

// local-only allowlisted target; override for Docker, where services address
// each other by name instead of 127.0.0.1
const RECEIVER_URL = process.env.RECEIVER_URL ?? "http://127.0.0.1:4000/receive";
const LEASE_SECONDS = Number(process.env.LEASE_SECONDS ?? "5");
const POLL_INTERVAL_MS = 500;
const MAX_ATTEMPTS = 8;
const BACKOFF_BASE_SECONDS = 2;
const BACKOFF_CAP_SECONDS = 60;
const CONCURRENCY = Math.max(1, Number(process.env.WORKER_CONCURRENCY ?? "1"));

// Demo/test-only fault injection: the first claimed job after process start
// that reaches this boundary makes the worker exit immediately, standing in
// for a real SIGKILL at a precise, reproducible point. Unset by default -
// zero effect on normal operation or the chaos test.
type CrashPoint = "claim" | "deliver";
const CRASH_AFTER = process.env.HERMES_CRASH_AFTER as CrashPoint | undefined;
let crashArmed = CRASH_AFTER !== undefined;

function maybeCrash(point: CrashPoint): void {
  if (crashArmed && CRASH_AFTER === point) {
    crashArmed = false;
    console.log(`HERMES_CRASH_AFTER=${point}: exiting now (simulated crash)`);
    process.exit(137);
  }
}

type ClaimedEvent = {
  id: string;
  idempotency_key: string;
  payload: unknown;
  attempts: number;
  lease_token: string;
};

// Full jitter: pick uniformly between 0 and the exponential cap, so a batch of
// jobs that failed together don't all retry on the same tick.
export function backoffSeconds(nextAttemptNumber: number): number {
  const cap = Math.min(BACKOFF_CAP_SECONDS, BACKOFF_BASE_SECONDS * 2 ** (nextAttemptNumber - 1));
  return Math.random() * cap;
}

// Claim one job: either a fresh pending one, or a 'claimed' one whose lease
// expired (its worker died after claiming but before marking a result - that
// row's status never moved back to 'pending' on its own, so this is the only
// path back to delivery). Lock it, stamp a fresh lease token, commit, then let
// the caller do the network call outside this transaction. SKIP LOCKED means a
// second worker polling concurrently just moves on to the next row instead of
// blocking.
async function claimOne(): Promise<ClaimedEvent | null> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const { rows } = await client.query<ClaimedEvent>(
      `SELECT id, idempotency_key, payload, attempts
       FROM events
       WHERE (status = 'pending' AND next_attempt_at <= now())
          OR (status = 'claimed' AND lease_expires_at <= now())
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

type DeliverResult = { ok: boolean; httpStatus: number | null };

async function deliver(event: ClaimedEvent): Promise<DeliverResult> {
  const body = JSON.stringify(event.payload);
  try {
    const res = await fetch(RECEIVER_URL, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-event-id": event.id,
        "x-hermes-signature": sign(body),
      },
      body,
    });
    return { ok: res.ok, httpStatus: res.status };
  } catch {
    return { ok: false, httpStatus: null };
  }
}

// Best-effort observability write, separate from the status update that
// actually governs delivery. A failure here must never affect delivery
// correctness, so errors are swallowed.
async function logAttempt(
  event: ClaimedEvent,
  attemptNumber: number,
  outcome: "delivered" | "failed",
  httpStatus: number | null
): Promise<void> {
  try {
    await pool.query(
      `INSERT INTO activity_log (kind, event_id, attempt_number, outcome, http_status)
       VALUES ('delivery_attempt', $1, $2, $3, $4)`,
      [event.id, attemptNumber, outcome, httpStatus]
    );
  } catch (err) {
    console.error("failed to write activity_log row (non-fatal):", err);
  }
}

// Only update the row if lease_token still matches what we claimed with. If the
// lease expired and another worker reclaimed this job in the meantime, its token
// is different, the WHERE matches zero rows, and this stale write is a no-op.
async function markDelivered(event: ClaimedEvent): Promise<void> {
  await pool.query(
    `UPDATE events SET status = 'delivered', attempts = $1
     WHERE id = $2 AND lease_token = $3`,
    [event.attempts + 1, event.id, event.lease_token]
  );
}

async function markFailed(event: ClaimedEvent): Promise<void> {
  const nextAttemptNumber = event.attempts + 1;

  if (nextAttemptNumber >= MAX_ATTEMPTS) {
    await pool.query(
      `UPDATE events SET status = 'dead', attempts = $1
       WHERE id = $2 AND lease_token = $3`,
      [nextAttemptNumber, event.id, event.lease_token]
    );
    return;
  }

  await pool.query(
    `UPDATE events
     SET status = 'pending', attempts = $1,
         next_attempt_at = now() + ($2 * interval '1 second')
     WHERE id = $3 AND lease_token = $4`,
    [nextAttemptNumber, backoffSeconds(nextAttemptNumber), event.id, event.lease_token]
  );
}

async function runOnce(): Promise<boolean> {
  const event = await claimOne();
  if (!event) return false;
  maybeCrash("claim");

  const result = await deliver(event);
  maybeCrash("deliver");
  const attemptNumber = event.attempts + 1;
  if (result.ok) {
    await markDelivered(event);
  } else {
    await markFailed(event);
  }
  await logAttempt(event, attemptNumber, result.ok ? "delivered" : "failed", result.httpStatus);
  return true;
}

// shuttingDown is checked between jobs, never mid-job: SIGTERM lets whatever
// a loop already claimed finish (deliver + mark result) before that loop
// exits, so a graceful stop never orphans a lease the way SIGKILL can.
let shuttingDown = false;

async function runLoop(loopId: number): Promise<void> {
  console.log(`HERMES worker loop ${loopId} started`);
  while (!shuttingDown) {
    const didWork = await runOnce();
    if (!didWork) {
      await new Promise((r) => setTimeout(r, POLL_INTERVAL_MS));
    }
  }
  console.log(`HERMES worker loop ${loopId} stopped`);
}

async function main() {
  console.log(`HERMES worker started (concurrency=${CONCURRENCY})`);
  const loops = Array.from({ length: CONCURRENCY }, (_, i) => runLoop(i));

  process.on("SIGTERM", () => {
    console.log("SIGTERM received, finishing in-flight work before exit");
    shuttingDown = true;
  });

  await Promise.all(loops);
  await pool.end();
  process.exit(0);
}

// Only auto-run when executed directly (`npm run worker`, or spawned as a
// child process); importing this module for its exports (tests,
// worker-supervisor's type-only usage) must not also start the loop.
import { pathToFileURL } from "node:url";
if (import.meta.url === pathToFileURL(process.argv[1] ?? "").href) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
