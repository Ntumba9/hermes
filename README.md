# HERMES

A crash-tested webhook delivery service. Postgres as the queue, no Kafka/Redis/RabbitMQ,
no ORM. The point is proving delivery survives a worker crash, not feature count.

## Run it

```bash
docker compose up -d
npm install
npm run migrate
```

In three terminals:

```bash
npm run receiver   # demo delivery target on :4000
npm run api        # ingest API on :3000
npm run worker     # claims and delivers pending events
```

Send an event:

```bash
curl -X POST http://127.0.0.1:3000/events \
  -H "Idempotency-Key: demo-1" \
  -H "Content-Type: application/json" \
  -d '{"hello":"world"}'
```

Check dead letters: `curl http://127.0.0.1:3000/events?status=dead`

## Status

Tonight's slice: ingest with idempotency, single worker claiming via
`SELECT ... FOR UPDATE SKIP LOCKED` + lease token, delivery to a local demo receiver.
Retries/backoff, HMAC signing, receiver-side dedup, and the chaos test are week-core work.

## What works / limitations (updated each session)

See commit history for the running log. Tonight: ingest + single-worker delivery proven
manually (50 events, worker killed mid-run, restarted, all 50 eventually delivered;
duplicates expected and accepted — exactly-once lands with receiver dedup in week core).
