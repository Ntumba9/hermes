import { createServer } from "node:http";
import { pool } from "./db.js";
import { verify } from "./signing.js";

const PORT = 4000;
const HOST = "127.0.0.1";

async function readBody(req: import("node:http").IncomingMessage): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  return Buffer.concat(chunks).toString("utf8");
}

const server = createServer(async (req, res) => {
  if (req.method === "POST" && req.url === "/receive") {
    return handleReceive(req, res);
  }
  if (req.method === "GET" && req.url === "/total") {
    const { rows } = await pool.query("SELECT total FROM receiver_total WHERE id = true");
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ total: Number(rows[0].total) }));
    return;
  }
  res.writeHead(404);
  res.end();
});

async function handleReceive(
  req: import("node:http").IncomingMessage,
  res: import("node:http").ServerResponse
) {
  const eventId = req.headers["x-event-id"];
  const signature = req.headers["x-hermes-signature"];
  const rawBody = await readBody(req);

  if (typeof eventId !== "string" || typeof signature !== "string") {
    res.writeHead(400, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "x-event-id and x-hermes-signature headers are required" }));
    return;
  }

  // Verify over the exact bytes received, before trusting anything in the body.
  if (!verify(rawBody, signature)) {
    res.writeHead(401, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "invalid signature" }));
    return;
  }

  let payload: { amount?: number } = {};
  try {
    payload = JSON.parse(rawBody);
  } catch {
    // tolerate non-JSON/empty bodies; there's nothing to apply as a side effect
  }
  const amount = typeof payload.amount === "number" ? payload.amount : 0;

  // Record the event id and apply its side effect in one transaction. If the
  // id was already recorded, the insert is a no-op and we skip the UPDATE, so
  // a redelivered event never moves the total twice.
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const inserted = await client.query(
      `INSERT INTO received_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING RETURNING event_id`,
      [eventId]
    );
    const isNew = inserted.rows.length > 0;
    if (isNew) {
      await client.query(`UPDATE receiver_total SET total = total + $1 WHERE id = true`, [amount]);
    }
    await client.query("COMMIT");
    console.log(`received event-id=${eventId} new=${isNew}`);
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }

  res.writeHead(200, { "content-type": "application/json" });
  res.end(JSON.stringify({ ok: true }));
}

server.listen(PORT, HOST, () => {
  console.log(`Demo receiver listening on http://${HOST}:${PORT}`);
});
