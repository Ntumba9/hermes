import { test } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import { pool } from "./db.js";

// Deterministic version of what the random-SIGKILL chaos test exercises
// stochastically: a worker that dies the instant after its claim commits -
// after status flips to 'claimed' and the lease is set, before any HTTP call
// reaches the receiver. Proves that exact boundary specifically (not "some
// failure window, maybe this one").

const RECEIVER_TOTAL_URL = "http://127.0.0.1:4000/total";
const LEASE_SECONDS = 1; // short on purpose, so the test doesn't sit around waiting

function spawnTsx(entry: string, env: Record<string, string> = {}): ChildProcess {
  return spawn(process.execPath, ["--import", "tsx", entry], {
    stdio: "inherit",
    env: { ...process.env, ...env },
  });
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function waitForHttp(url: string, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      await fetch(url);
      return;
    } catch {
      await sleep(100);
    }
  }
  throw new Error(`timed out waiting for ${url}`);
}

function waitForExit(child: ChildProcess): Promise<number | null> {
  return new Promise((resolve) => child.on("exit", (code) => resolve(code)));
}

async function insertPendingEvent(): Promise<string> {
  const id = randomUUID();
  await pool.query(
    `INSERT INTO events (id, idempotency_key, payload) VALUES ($1, $2, $3)`,
    [id, `boundary-${id}`, { amount: 1 }]
  );
  return id;
}

test("worker crashing immediately after claim-commit: lease is reclaimed, event delivered exactly once", async () => {
  await pool.query("TRUNCATE events, received_events, activity_log");
  await pool.query("UPDATE receiver_total SET total = 0 WHERE id = true");

  const receiver = spawnTsx("src/receiver.ts");
  try {
    await waitForHttp(RECEIVER_TOTAL_URL);

    const eventId = await insertPendingEvent();

    // First worker: self-exits the instant its claim commits, before any
    // HTTP call. Give it a very short lease so the reclaim window is short.
    const crashingWorker = spawnTsx("src/worker.ts", {
      HERMES_CRASH_AFTER: "claim",
      LEASE_SECONDS: String(LEASE_SECONDS),
    });
    const exitCode = await waitForExit(crashingWorker);
    assert.equal(exitCode, 137, "worker should have self-exited with the simulated-crash code");

    const { rows: afterCrash } = await pool.query(
      "SELECT status, lease_expires_at FROM events WHERE id = $1",
      [eventId]
    );
    assert.equal(afterCrash[0].status, "claimed", "claim must have committed before the crash");

    const { rows: receiverRows } = await pool.query(
      "SELECT count(*) FROM received_events WHERE event_id = $1",
      [eventId]
    );
    assert.equal(Number(receiverRows[0].count), 0, "the receiver must never have seen this event before the crash");

    // Wait past the lease, then run a normal worker to reclaim and deliver.
    await sleep((LEASE_SECONDS + 1) * 1000);
    const recoveryWorker = spawnTsx("src/worker.ts", { LEASE_SECONDS: String(LEASE_SECONDS) });
    try {
      const deadline = Date.now() + 10_000;
      let status = "claimed";
      while (status !== "delivered") {
        if (Date.now() > deadline) throw new Error("event never reached delivered after reclaim");
        const { rows } = await pool.query("SELECT status FROM events WHERE id = $1", [eventId]);
        status = rows[0].status;
        if (status !== "delivered") await sleep(200);
      }
    } finally {
      recoveryWorker.kill("SIGKILL");
    }

    const totalRes = await fetch(RECEIVER_TOTAL_URL);
    const { total } = (await totalRes.json()) as { total: number };
    assert.equal(total, 1, "side effect must have run exactly once despite the crash-and-reclaim");
  } finally {
    receiver.kill("SIGKILL");
  }
});

test.after(async () => {
  await pool.end();
});
