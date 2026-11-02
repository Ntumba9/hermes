import { spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { pool } from "./db.js";

// Throughput benchmark, no kills: fire NUM_EVENTS events, start one worker,
// time from send-start to the last event reaching a terminal state.
const NUM_EVENTS = 1000;
const API_URL = "http://127.0.0.1:3500/events";
const RECEIVER_TOTAL_URL = "http://127.0.0.1:4000/total";
const POLL_INTERVAL_MS = 100;

function spawnTsx(entry: string): ChildProcess {
  return spawn(process.execPath, ["--import", "tsx", entry], { stdio: "inherit" });
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
  await pool.query("TRUNCATE events");
  await pool.query("TRUNCATE received_events");
  await pool.query("UPDATE receiver_total SET total = 0 WHERE id = true");
}

async function sendEvents(): Promise<void> {
  for (let i = 0; i < NUM_EVENTS; i++) {
    const res = await fetch(API_URL, {
      method: "POST",
      headers: { "content-type": "application/json", "idempotency-key": `bench-${randomUUID()}` },
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

async function main() {
  console.log(`Resetting state and sending ${NUM_EVENTS} events...`);
  await resetState();

  const receiver = spawnTsx("src/receiver.ts");
  const api = spawnTsx("src/api.ts");
  const worker = spawnTsx("src/worker.ts");
  try {
    await waitForHttp(RECEIVER_TOTAL_URL);
    await waitForHttp(API_URL.replace("/events", "/events?status=pending"));

    await sendEvents();
    console.log("Events sent. Timing delivery...");
    const start = Date.now();

    while ((await terminalCount()) < NUM_EVENTS) {
      await sleep(POLL_INTERVAL_MS);
    }
    const elapsedMs = Date.now() - start;
    const eventsPerSecond = NUM_EVENTS / (elapsedMs / 1000);

    console.log(`\n${NUM_EVENTS} events delivered in ${elapsedMs}ms`);
    console.log(`Throughput: ${eventsPerSecond.toFixed(1)} events/sec (single worker, single poller)`);
  } finally {
    receiver.kill("SIGKILL");
    api.kill("SIGKILL");
    worker.kill("SIGKILL");
    await pool.end();
  }
}

main().catch((err) => {
  console.error("BENCHMARK FAILED:", err);
  process.exit(1);
});
