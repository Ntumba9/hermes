# HERMES

A crash-tested webhook delivery service. Postgres as the queue, no Kafka/Redis/RabbitMQ,
no ORM. The point is proving delivery survives a worker crash, not feature count.

## Architecture

```
POST /events ──► [events table]
(idempotency        │  status: pending → claimed → delivered
 key, dedup on       │                           └→ dead (after 8 attempts)
 insert)             │
                     ▼
              worker (polling loop)
   1. SELECT ... FOR UPDATE SKIP LOCKED, LIMIT 1   (claim: own transaction, commits)
   2. stamp a fresh lease_token + lease_expires_at
   3. ── commit ──
   4. HTTP POST to receiver, X-Event-Id + X-Hermes-Signature (HMAC-SHA256)  (no tx open)
   5. UPDATE events SET status = ... WHERE id = $1 AND lease_token = $2
      (stale worker whose lease was reclaimed: this WHERE matches 0 rows, no-op)
                     │
                     ▼
              demo receiver (:4000)
   verify signature → INSERT received_events (event_id) ON CONFLICT DO NOTHING
                    → side effect runs only if that insert was new, same transaction
```

## The failure this survives

Kill the worker at any point — mid-claim, mid-delivery, right after the HTTP call
returns but before the status write lands — and restart it. No event is lost: the
lease token means a half-finished claim can't be double-written by two workers, and
the receiver's own dedup means a redelivered event never applies its side effect
twice. At-least-once delivery + idempotent processing = effectively-once overall.

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
  -d '{"amount":1}'
```

Check dead letters: `curl http://127.0.0.1:3000/events?status=dead`

Check the receiver's side-effect total (should only ever move once per event,
even if delivery repeats): `curl http://127.0.0.1:4000/total`

Automated proof — fires 50 events, SIGKILLs the worker repeatedly mid-flight,
restarts it, and asserts zero lost events + an exactly-once side effect:

```bash
npm run chaos
```

`npm test` runs the dependency-free unit tests (HMAC sign/verify).

An optional `HERMES_SIGNING_SECRET` env var overrides the dev-only default secret
used to sign/verify delivery payloads; set the same value for anything that needs
to verify HERMES's signatures.

## Status

All of tonight's slice and week-core mechanism is written: idempotent ingest,
SKIP LOCKED + lease-token claiming, exponential backoff with jitter and
dead-lettering after 8 attempts, HMAC-SHA256 signing, and durable receiver-side
dedup (event id + side effect in one transaction). The chaos test and its GitHub
Actions workflow are written too. Benchmark numbers and the "what I learned"
writeup are still open.

## What works / limitations (updated each session)

See commit history for the running log, including which commits are marked
UNVERIFIED.

**Current honest limitation:** none of the Postgres-dependent code has actually
been run. Docker Desktop on this machine is stuck behind an orphaned Windows
AF_UNIX socket reparse point (`sailor-ingest.sock` / `.stale` in
`%LOCALAPPDATA%\Docker\run`) that nothing — not killing the process, not
`wsl --shutdown`, not deleting the file — can clear short of a reboot. Everything
above is `tsc --noEmit`-clean and reviewed by hand, but the actual delivery
guarantee this project exists to prove has not yet been demonstrated end to end.
First thing to do once Docker is back: `npm run migrate && npm run chaos`.
