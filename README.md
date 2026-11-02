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
npm run api        # ingest API on :3500
npm run worker     # claims and delivers pending events
```

Send an event:

```bash
curl -X POST http://127.0.0.1:3500/events \
  -H "Idempotency-Key: demo-1" \
  -H "Content-Type: application/json" \
  -d '{"amount":1}'
```

Check dead letters: `curl http://127.0.0.1:3500/events?status=dead`

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

Tonight's slice and week-core mechanism are built and verified end to end:
idempotent ingest, SKIP LOCKED + lease-token claiming (including reclaiming a
job whose worker died mid-flight), exponential backoff with jitter and
dead-lettering after 8 attempts, HMAC-SHA256 signing, and durable receiver-side
dedup (event id + side effect in one transaction). `npm run chaos` passes
consistently: 50/50 events delivered, zero lost, side-effect total exactly 50
across repeated random SIGKILLs of the worker, with observed redeliveries
correctly deduped. Benchmark numbers and the "what I learned" writeup are
still open.

## What works / limitations (updated each session)

See commit history for the running log.

**What works (verified against a real Postgres, not just type-checked):**
idempotent ingest, claim/lease/deliver/mark-result, expired-lease reclaim,
backoff + dead-letter, HMAC sign/verify, receiver dedup. `npm test` (4/4) and
`npm run chaos` both pass.

**How to run the proof:** `docker compose up -d && npm run migrate && npm run chaos`.

**Honest limitation:** the chaos test's kill timing (400ms-2.5s between
SIGKILLs) was tuned by hand against one machine's `tsx` cold-start overhead,
not derived from first principles - a much faster or slower machine might need
different numbers to actually interrupt mid-flight work instead of mostly
killing process startup. The first run of the chaos test also caught a real
bug (expired leases were never reclaimed, so a worker killed right after
claiming a job orphaned it forever) - a reminder that "written and
type-checked" and "actually correct" are different claims, which is the whole
point of this project. Benchmark numbers (events/sec) are not yet measured.
