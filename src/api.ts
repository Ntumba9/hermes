import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { pool } from "./db.js";

const PORT = Number(process.env.PORT ?? "3500");
// Default 127.0.0.1 for local dev; Docker overrides to 0.0.0.0 inside the
// container and restricts host access via its own port-publish instead.
const HOST = process.env.BIND_HOST ?? "127.0.0.1";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PUBLIC_DIR = join(__dirname, "..", "public");

async function readBody(req: import("node:http").IncomingMessage): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  return Buffer.concat(chunks).toString("utf8");
}

const server = createServer(async (req, res) => {
  try {
    if (req.method === "POST" && req.url === "/events") {
      return await handleCreateEvent(req, res);
    }
    if (req.method === "GET" && req.url?.startsWith("/events")) {
      return await handleListEvents(req, res);
    }
    if (req.method === "GET" && req.url === "/api/state") {
      return await handleDashboardState(req, res);
    }
    if (req.method === "GET" && req.url?.startsWith("/api/events/")) {
      return await handleEventDetail(req, res);
    }
    if (req.method === "GET" && (req.url === "/" || req.url === "/dashboard")) {
      return await serveStatic(res, "dashboard.html", "text/html");
    }
    res.writeHead(404, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "not found" }));
  } catch (err) {
    console.error(err);
    res.writeHead(500, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "internal error" }));
  }
});

async function serveStatic(
  res: import("node:http").ServerResponse,
  filename: string,
  contentType: string
) {
  try {
    const body = await readFile(join(PUBLIC_DIR, filename));
    res.writeHead(200, { "content-type": contentType });
    res.end(body);
  } catch {
    res.writeHead(404, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "not found" }));
  }
}

// Dashboard summary: counts by status, the receiver's own side-effect total
// (same Postgres instance, no cross-service call needed), and the most
// recent activity for the timeline.
async function handleDashboardState(
  req: import("node:http").IncomingMessage,
  res: import("node:http").ServerResponse
) {
  const [counts, receiverTotal, recentActivity] = await Promise.all([
    pool.query<{ status: string; count: string }>(
      `SELECT status, count(*) FROM events GROUP BY status`
    ),
    pool.query<{ total: string }>(`SELECT total FROM receiver_total WHERE id = true`),
    pool.query(
      `SELECT id, kind, event_id, attempt_number, outcome, http_status, detail, created_at
       FROM activity_log ORDER BY created_at DESC LIMIT 50`
    ),
  ]);

  const statusCounts: Record<string, number> = { pending: 0, claimed: 0, delivered: 0, dead: 0 };
  for (const row of counts.rows) {
    statusCounts[row.status] = Number(row.count);
  }

  res.writeHead(200, { "content-type": "application/json" });
  res.end(
    JSON.stringify({
      statusCounts,
      receiverTotal: Number(receiverTotal.rows[0]?.total ?? 0),
      recentActivity: recentActivity.rows,
    })
  );
}

// Event inspector: one event's record plus its full activity_log history
// (every attempt, outcome, and http_status - never signing keys or
// credentials, which never enter this table in the first place).
async function handleEventDetail(
  req: import("node:http").IncomingMessage,
  res: import("node:http").ServerResponse
) {
  const id = req.url?.split("/api/events/")[1]?.split("?")[0];
  if (!id) {
    res.writeHead(400, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "event id required" }));
    return;
  }

  const [eventResult, activityResult] = await Promise.all([
    pool.query(
      `SELECT id, idempotency_key, payload, status, attempts, next_attempt_at,
              lease_expires_at, created_at
       FROM events WHERE id = $1`,
      [id]
    ),
    pool.query(
      `SELECT kind, attempt_number, outcome, http_status, detail, created_at
       FROM activity_log WHERE event_id = $1 ORDER BY created_at ASC`,
      [id]
    ),
  ]);

  if (eventResult.rows.length === 0) {
    res.writeHead(404, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "not found" }));
    return;
  }

  res.writeHead(200, { "content-type": "application/json" });
  res.end(JSON.stringify({ event: eventResult.rows[0], activity: activityResult.rows }));
}

async function handleCreateEvent(
  req: import("node:http").IncomingMessage,
  res: import("node:http").ServerResponse
) {
  const idempotencyKey = req.headers["idempotency-key"];
  if (typeof idempotencyKey !== "string" || idempotencyKey.length === 0) {
    res.writeHead(400, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "Idempotency-Key header is required" }));
    return;
  }

  let payload: unknown;
  try {
    payload = JSON.parse(await readBody(req));
  } catch {
    res.writeHead(400, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "body must be valid JSON" }));
    return;
  }

  // Insert-or-return-existing on the idempotency key, in one round trip, so two
  // callers racing with the same key can never produce two rows.
  const id = randomUUID();
  const result = await pool.query<{ id: string; idempotency_key: string; payload: unknown; status: string }>(
    `INSERT INTO events (id, idempotency_key, payload)
     VALUES ($1, $2, $3)
     ON CONFLICT (idempotency_key) DO UPDATE SET idempotency_key = events.idempotency_key
     RETURNING id, idempotency_key, payload, status`,
    [id, idempotencyKey, payload]
  );

  const event = result.rows[0];
  res.writeHead(event.id === id ? 201 : 200, { "content-type": "application/json" });
  res.end(JSON.stringify(event));
}

async function handleListEvents(
  req: import("node:http").IncomingMessage,
  res: import("node:http").ServerResponse
) {
  const url = new URL(req.url ?? "/events", `http://${req.headers.host}`);
  const status = url.searchParams.get("status");

  const result = status
    ? await pool.query(
        `SELECT id, idempotency_key, status, attempts, created_at FROM events WHERE status = $1 ORDER BY created_at`,
        [status]
      )
    : await pool.query(
        `SELECT id, idempotency_key, status, attempts, created_at FROM events ORDER BY created_at`
      );

  res.writeHead(200, { "content-type": "application/json" });
  res.end(JSON.stringify(result.rows));
}

server.on("error", (err) => {
  console.error(`API failed to start: ${err.message}`);
  process.exit(1);
});

server.listen(PORT, HOST, () => {
  console.log(`HERMES API listening on http://${HOST}:${PORT}`);
});
