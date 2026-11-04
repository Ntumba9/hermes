import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import { pool } from "./db.js";

// Opt-in demo orchestrator: owns the real worker.ts as a child process so it
// can SIGKILL/respawn it on demand, and exposes the "break it live" controls
// the dashboard's Control Room panel talks to. Every destructive route is
// gated behind DEMO_MODE and origin-checked - this process is never started
// as part of normal operation (plain `npm run worker` is unaffected by any
// of this file).
const DEMO_MODE = process.env.DEMO_MODE === "true";
const BIND_HOST = process.env.BIND_HOST ?? "127.0.0.1";
const CONTROL_PORT = Number(process.env.CONTROL_PORT ?? "4700");
const API_URL = process.env.API_URL ?? "http://127.0.0.1:3500/events";
const RESPAWN_DELAY_MS = 300;
const MAX_SEED_COUNT = 200;

const allowedOrigins = new Set([
  `http://127.0.0.1:${CONTROL_PORT}`,
  `http://localhost:${CONTROL_PORT}`,
  `http://127.0.0.1:${process.env.API_PORT ?? "3500"}`,
  `http://localhost:${process.env.API_PORT ?? "3500"}`,
]);

let child: ChildProcess | null = null;
let shuttingDown = false;

function spawnWorker(env: Record<string, string> = {}): ChildProcess {
  const proc = spawn(process.execPath, ["--import", "tsx", "src/worker.ts"], {
    stdio: "inherit",
    env: { ...process.env, ...env },
  });
  proc.on("exit", (code) => onWorkerExit(code));
  return proc;
}

async function logEvent(kind: string, detail: string): Promise<void> {
  try {
    await pool.query(`INSERT INTO activity_log (kind, detail) VALUES ($1, $2)`, [kind, detail]);
  } catch (err) {
    console.error("failed to write activity_log row (non-fatal):", err);
  }
}

function onWorkerExit(code: number | null): void {
  if (shuttingDown) return;
  const crashed = code !== 0;
  console.log(`worker child exited (code=${code}), respawning in ${RESPAWN_DELAY_MS}ms`);
  logEvent(
    crashed ? "worker_crashed" : "worker_restarted",
    code === 137 ? "simulated crash (HERMES_CRASH_AFTER)" : `exit code ${code}`
  );
  setTimeout(() => {
    child = spawnWorker();
  }, RESPAWN_DELAY_MS);
}

function checkOrigin(req: import("node:http").IncomingMessage): boolean {
  const origin = req.headers.origin;
  if (!origin) return true; // no Origin header: curl/server-to-server, not a browser CSRF vector
  return allowedOrigins.has(origin);
}

async function readJsonBody(req: import("node:http").IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  const raw = Buffer.concat(chunks).toString("utf8");
  if (!raw) return {};
  try {
    return JSON.parse(raw);
  } catch {
    return {};
  }
}

const server = createServer(async (req, res) => {
  res.setHeader("content-type", "application/json");

  // The dashboard (served by api.ts on a different port) calls this server
  // directly from the browser, so its JSON responses need CORS headers, not
  // just the server-side origin check below (which stops the request being
  // acted on; CORS is what lets the browser hand the response back to the
  // page at all). Only echoed back for origins already on the allowlist -
  // never a wildcard.
  const origin = req.headers.origin;
  if (origin && allowedOrigins.has(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Access-Control-Allow-Methods", "GET, POST");
    res.setHeader("Access-Control-Allow-Headers", "content-type");
  }

  if (req.method === "OPTIONS") {
    res.writeHead(204);
    res.end();
    return;
  }

  if (req.method === "GET" && req.url === "/api/demo/mode") {
    res.writeHead(200);
    res.end(JSON.stringify({ enabled: DEMO_MODE }));
    return;
  }

  if (!DEMO_MODE) {
    res.writeHead(404);
    res.end(JSON.stringify({ error: "not found" }));
    return;
  }

  if (!checkOrigin(req)) {
    res.writeHead(403);
    res.end(JSON.stringify({ error: "origin not allowed" }));
    return;
  }

  try {
    if (req.method === "POST" && req.url === "/api/demo/crash") {
      const body = (await readJsonBody(req)) as { point?: string };
      const point = body.point ?? "random";

      if (point === "random") {
        if (child) child.kill("SIGKILL");
        res.writeHead(200);
        res.end(JSON.stringify({ message: "worker killed at a random point" }));
        return;
      }

      if (point === "after-claim" || point === "after-deliver") {
        if (child) child.kill("SIGKILL");
        // the respawned child self-exits the first time it hits that
        // boundary, then onWorkerExit's normal respawn path takes over
        child = spawnWorker({ HERMES_CRASH_AFTER: point === "after-claim" ? "claim" : "deliver" });
        res.writeHead(200);
        res.end(JSON.stringify({ message: `worker will crash after its next ${point.replace("after-", "")}` }));
        return;
      }

      res.writeHead(400);
      res.end(JSON.stringify({ error: "point must be random, after-claim, or after-deliver" }));
      return;
    }

    if (req.method === "POST" && req.url === "/api/demo/restart") {
      if (child) child.kill("SIGKILL");
      else child = spawnWorker();
      res.writeHead(200);
      res.end(JSON.stringify({ message: "worker restarted" }));
      return;
    }

    if (req.method === "POST" && req.url === "/api/demo/seed") {
      const body = (await readJsonBody(req)) as { count?: number };
      const count = Math.min(MAX_SEED_COUNT, Math.max(1, Math.floor(body.count ?? 20)));
      for (let i = 0; i < count; i++) {
        await fetch(API_URL, {
          method: "POST",
          headers: { "content-type": "application/json", "idempotency-key": `demo-${randomUUID()}` },
          body: JSON.stringify({ amount: 1 }),
        });
      }
      await logEvent("demo_seed", `sent ${count} synthetic events`);
      res.writeHead(200);
      res.end(JSON.stringify({ message: `sent ${count} events` }));
      return;
    }

    if (req.method === "POST" && req.url === "/api/demo/reset") {
      await pool.query("TRUNCATE events, activity_log, received_events");
      await pool.query("UPDATE receiver_total SET total = 0 WHERE id = true");
      await logEvent("demo_reset", "queue and receiver state reset");
      res.writeHead(200);
      res.end(JSON.stringify({ message: "demo state reset" }));
      return;
    }

    res.writeHead(404);
    res.end(JSON.stringify({ error: "not found" }));
  } catch (err) {
    console.error(err);
    res.writeHead(500);
    res.end(JSON.stringify({ error: "internal error" }));
  }
});

server.on("error", (err) => {
  console.error(`Worker supervisor control server failed to start: ${err.message}`);
  process.exit(1);
});

server.listen(CONTROL_PORT, BIND_HOST, () => {
  console.log(
    `Worker supervisor control server on http://${BIND_HOST}:${CONTROL_PORT} (DEMO_MODE=${DEMO_MODE})`
  );
  child = spawnWorker();
});

process.on("SIGTERM", () => {
  shuttingDown = true;
  if (child) child.kill("SIGTERM");
  server.close(() => process.exit(0));
});
