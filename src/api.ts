import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import { pool } from "./db.js";

const PORT = 3500;
const HOST = "127.0.0.1";

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
    res.writeHead(404, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "not found" }));
  } catch (err) {
    console.error(err);
    res.writeHead(500, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "internal error" }));
  }
});

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
