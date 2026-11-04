import { randomUUID } from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import { pool } from "./db.js";

// Fires NUM_EVENTS webhooks, then repeatedly SIGKILLs the worker mid-flight
// and restarts it, until every event reaches a terminal state. Asserts zero
// lost events and that the receiver's side effect ran exactly once per event.
const NUM_EVENTS = 50;
const API_URL = "http://127.0.0.1:3500/events";
const RECEIVER_TOTAL_URL = "http://127.0.0.1:4000/total";
const KILL_MIN_MS = 400; // tsx cold start + DB connect takes real time; much below this and kills land before any claim happens
const KILL_MAX_MS = 2500;
const POLL_INTERVAL_MS = 300;
const STALL_TIMEOUT_MS = 25_000; // > worker's lease window, so a stuck claim has time to be reclaimed

function spawnTsx(entry: string): ChildProcess {
  return spawn(process.execPath, ["--import", "tsx", entry], {
    stdio: "inherit",
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

async function resetState(): Promise<void> {
  await pool.query("TRUNCATE events, received_events, activity_log");
  await pool.query("UPDATE receiver_total SET total = 0 WHERE id = true");
}

async function sendEvents(): Promise<void> {
  for (let i = 0; i < NUM_EVENTS; i++) {
    const res = await fetch(API_URL, {
      method: "POST",
      headers: { "content-type": "application/json", "idempotency-key": `chaos-${randomUUID()}` },
      body: JSON.stringify({ amount: 1 }),
    });
    if (!res.ok) throw new Error(`event ${i} failed to ingest: ${res.status}`);
  }
}

async function terminalCount(): Promise<number> {
  const { rows } = await pool.query(
    `SELECT count(*) FROM events WHERE status IN ('delivered', 'dead')`
  );
  return Number(rows[0].count);
}

async function runChaosLoop(): Promise<void> {
  let lastProgressAt = Date.now();
  let lastTerminal = 0;

  while (true) {
    const done = await terminalCount();
    if (done > lastTerminal) {
      lastTerminal = done;
      lastProgressAt = Date.now();
    }
    if (done >= NUM_EVENTS) return;
    if (Date.now() - lastProgressAt > STALL_TIMEOUT_MS) {
      throw new Error(`stalled: ${done}/${NUM_EVENTS} terminal after ${STALL_TIMEOUT_MS}ms with no progress`);
    }

    const worker = spawnTsx("src/worker.ts");
    const killAfter = KILL_MIN_MS + Math.random() * (KILL_MAX_MS - KILL_MIN_MS);
    await sleep(killAfter);
    worker.kill("SIGKILL");
    await sleep(POLL_INTERVAL_MS);
  }
}

async function main() {
  console.log(`Resetting state and sending ${NUM_EVENTS} events...`);
  await resetState();

  const receiver = spawnTsx("src/receiver.ts");
  const api = spawnTsx("src/api.ts");
  try {
    await waitForHttp(RECEIVER_TOTAL_URL);
    await waitForHttp(API_URL.replace("/events", "/events?status=pending"));

    await sendEvents();
    console.log("Events sent. Starting kill loop...");
    await runChaosLoop();

    const { rows: deadRows } = await pool.query(`SELECT count(*) FROM events WHERE status = 'dead'`);
    const dead = Number(deadRows[0].count);
    if (dead > 0) {
      throw new Error(`${dead} events went dead - expected the local receiver to always succeed`);
    }

    const totalRes = await fetch(RECEIVER_TOTAL_URL);
    const { total } = (await totalRes.json()) as { total: number };
    if (total !== NUM_EVENTS) {
      throw new Error(`side effect ran ${total} times, expected exactly ${NUM_EVENTS} (one per event)`);
    }

    console.log(`PASS: ${NUM_EVENTS}/${NUM_EVENTS} delivered, side effect total == ${total}`);
  } finally {
    receiver.kill("SIGKILL");
    api.kill("SIGKILL");
    await pool.end();
  }
}

main().catch((err) => {
  console.error("CHAOS TEST FAILED:", err);
  process.exit(1);
});
