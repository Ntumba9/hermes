import { spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { pool } from "./db.js";

// Throughput benchmark, no kills: fire NUM_EVENTS events, start the api and
// receiver once, then run one pass per concurrency level, each against a
// freshly reset queue, timing send-start to the last event reaching a
// terminal state. Prints a markdown table - real measured numbers only.
const NUM_EVENTS = 1000;
const CONCURRENCY_LEVELS = [1, 2, 4];
const API_URL = "http://127.0.0.1:3500/events";
const RECEIVER_TOTAL_URL = "http://127.0.0.1:4000/total";
const POLL_INTERVAL_MS = 100;

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

async function resetState(): Promise<void> {
  await pool.query("TRUNCATE events, received_events, activity_log");
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

function waitForExit(child: ChildProcess): Promise<void> {
  return new Promise((resolve) => child.on("exit", () => resolve()));
}

async function runPass(concurrency: number): Promise<number> {
  console.log(`\n--- concurrency=${concurrency}: resetting and sending ${NUM_EVENTS} events ---`);
  await resetState();

  const worker = spawnTsx("src/worker.ts", { WORKER_CONCURRENCY: String(concurrency) });
  try {
    await sendEvents();
    const start = Date.now();

    while ((await terminalCount()) < NUM_EVENTS) {
      await sleep(POLL_INTERVAL_MS);
    }
    const elapsedMs = Date.now() - start;
    const eventsPerSecond = NUM_EVENTS / (elapsedMs / 1000);
    console.log(`concurrency=${concurrency}: ${NUM_EVENTS} events in ${elapsedMs}ms -> ${eventsPerSecond.toFixed(1)} events/sec`);
    return eventsPerSecond;
  } finally {
    worker.kill("SIGKILL");
    await waitForExit(worker);
  }
}

async function main() {
  const receiver = spawnTsx("src/receiver.ts");
  const api = spawnTsx("src/api.ts");
  const results: Array<{ concurrency: number; eventsPerSecond: number }> = [];

  try {
    await waitForHttp(RECEIVER_TOTAL_URL);
    await waitForHttp(API_URL.replace("/events", "/events?status=pending"));

    for (const concurrency of CONCURRENCY_LEVELS) {
      const eventsPerSecond = await runPass(concurrency);
      results.push({ concurrency, eventsPerSecond });
    }

    console.log("\n| WORKER_CONCURRENCY | events/sec |");
    console.log("|---|---|");
    for (const r of results) {
      console.log(`| ${r.concurrency} | ${r.eventsPerSecond.toFixed(1)} |`);
    }
  } finally {
    receiver.kill("SIGKILL");
    api.kill("SIGKILL");
    await pool.end();
  }
}

main().catch((err) => {
  console.error("BENCHMARK FAILED:", err);
  process.exit(1);
});
