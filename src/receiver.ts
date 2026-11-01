import { createServer } from "node:http";

// Tonight's slice: just acks every delivery so the worker can prove end-to-end
// flow. Signature verification + durable dedup land in week core.
const PORT = 4000;
const HOST = "127.0.0.1";

let received = 0;

const server = createServer(async (req, res) => {
  if (req.method === "POST" && req.url === "/receive") {
    for await (const _chunk of req) {
      // drain the body
    }
    received++;
    console.log(`received #${received} event-id=${req.headers["x-event-id"]}`);
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: true }));
    return;
  }
  if (req.method === "GET" && req.url === "/count") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ received }));
    return;
  }
  res.writeHead(404);
  res.end();
});

server.listen(PORT, HOST, () => {
  console.log(`Demo receiver listening on http://${HOST}:${PORT}`);
});
