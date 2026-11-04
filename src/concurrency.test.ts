import { test } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import { pool } from "./db.js";

// WORKER_CONCURRENCY=4, no kills: proves that concurrent claim loops never
// double-deliver the same event's side effect, even though retries/duplicate
// HTTP attempts are allowed by design (a job can legitimately take longer
// than the lease and get reclaimed while the original call is still
// in-flight). The receiver's own dedup is what makes that safe - this test
// checks the outcome, not the absence of duplicate attempts.

const NUM_EVENTS = 60;
const API_URL = "http://127.0.0.1:3500/events";
const RECEIVER_TOTAL_URL = "http://127.0.0.1:4000/total";
const TIMEOUT_MS = 30_000;

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

async function terminalCount(): Promise<number> {
  const { rows } = await pool.query(
    `SELECT count(*) FROM events WHERE status IN ('delivered', 'dead')`
  );
  return Number(rows[0].count);
}

test("concurrent delivery loops deliver each event's side effect exactly once", async () => {
  await pool.query("TRUNCATE events, received_events, activity_log");
  await pool.query("UPDATE receiver_total SET total = 0 WHERE id = true");

  const receiver = spawnTsx("src/receiver.ts");
  const api = spawnTsx("src/api.ts");
  const worker = spawnTsx("src/worker.ts", { WORKER_CONCURRENCY: "4" });

  try {
    await waitForHttp(RECEIVER_TOTAL_URL);
    await waitForHttp(API_URL.replace("/events", "/events?status=pending"));

    for (let i = 0; i < NUM_EVENTS; i++) {
      const res = await fetch(API_URL, {
        method: "POST",
        headers: { "content-type": "application/json", "idempotency-key": `conc-${randomUUID()}` },
        body: JSON.stringify({ amount: 1 }),
      });
      assert.ok(res.ok, `event ${i} failed to ingest`);
    }

    const deadline = Date.now() + TIMEOUT_MS;
    while ((await terminalCount()) < NUM_EVENTS) {
      if (Date.now() > deadline) {
        throw new Error("timed out waiting for all events to reach a terminal state");
      }
      await sleep(200);
    }

    const { rows: deadRows } = await pool.query(`SELECT count(*) FROM events WHERE status = 'dead'`);
    assert.equal(Number(deadRows[0].count), 0, "no events should have gone dead against a healthy local receiver");

    const totalRes = await fetch(RECEIVER_TOTAL_URL);
    const { total } = (await totalRes.json()) as { total: number };
    assert.equal(total, NUM_EVENTS, "receiver side effect must have run exactly once per event despite 4 concurrent loops");
  } finally {
    receiver.kill("SIGKILL");
    api.kill("SIGKILL");
    worker.kill("SIGKILL");
  }
});

test.after(async () => {
  await pool.end();
});
