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

Throughput benchmark — sends 1000 events, runs one worker with no kills, times
send-start to the last event reaching a terminal state:

```bash
npm run benchmark
```

An optional `HERMES_SIGNING_SECRET` env var overrides the dev-only default secret
used to sign/verify delivery payloads; set the same value for anything that needs
to verify HERMES's signatures.

## Benchmark

Single worker, single poller, local receiver, no concurrency — measures the
mechanism's floor, not an optimized ceiling:

| Events | Elapsed | Throughput |
|--------|---------|------------|
| 1000   | 35.8s   | 27.9 events/sec |

That number is bound by the worker doing one job fully sequentially — claim
(transaction + commit), HTTP POST, mark-result (another write) — before
polling for the next one, with a 500ms poll delay whenever it finds nothing
to do. Running multiple worker processes against the same table is the
obvious next lever: `SELECT ... FOR UPDATE SKIP LOCKED` is exactly what makes
that safe without any coordination between them, since two workers polling
concurrently just skip each other's locked rows. Not benchmarked here —
tonight's slice was proving correctness under crash, not peak throughput.

## Status

Tonight's slice and week-core mechanism are built and verified end to end:
idempotent ingest, SKIP LOCKED + lease-token claiming (including reclaiming a
job whose worker died mid-flight), exponential backoff with jitter and
dead-lettering after 8 attempts, HMAC-SHA256 signing, and durable receiver-side
dedup (event id + side effect in one transaction). `npm run chaos` passes
consistently: 50/50 events delivered, zero lost, side-effect total exactly 50
across repeated random SIGKILLs of the worker, with observed redeliveries
correctly deduped. Benchmarked at 27.9 events/sec with a single worker.

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
killing process startup. The benchmark (27.9 events/sec) is a single
unoptimized worker, not a ceiling - it was never the point of this project to
push that number up.

## What I learned

**The chaos test found a bug the type checker and unit tests couldn't.**
`claimOne()` only ever selected `status = 'pending'`. A worker SIGKILLed
right after committing a row to `'claimed'`, but before it could write a
result, left that row permanently stuck - nothing anywhere ever reset it,
despite `lease_expires_at` existing for exactly this purpose. Everything
compiled, every unit test passed, and the bug still shipped, because none of
that exercises concurrent crash timing. Only throwing real SIGKILLs at a real
worker against a real Postgres instance surfaced it. That's the actual thesis
of this project, not a thing I wrote in the README to sound reflective: you
cannot reason your way to "this survives a crash," you have to crash it and
check.

**The lease-token pattern is a cheap way to turn "which worker still owns
this job" into one `WHERE` clause.** I expected to need something heavier -
a separate leases table, maybe a watchdog process - to handle a worker dying
mid-flight. Instead, every status write already carries `WHERE id = $1 AND
lease_token = $2`; a stale worker's write just silently matches zero rows.
The reclaim logic (broadening the claim query to also pick up
`status = 'claimed' AND lease_expires_at <= now()`) was the one piece I had
to add by hand, and it's three lines.

**At-least-once delivery is the easy half of "exactly-once behavior."**
Making the worker resend was never the hard part - not committing the status
write inside the same transaction as the HTTP call is what forces at-least-
once, and that's a one-line decision. The hard part was making redelivery
harmless on the receiving end: the receiver's own `INSERT ... ON CONFLICT DO
NOTHING RETURNING` has to run in the same transaction as the side effect it
gates, or a crash between those two statements reopens the exact bug this
whole design exists to prevent.

**Full jitter mattered more than I expected for something I almost skipped.**
Early on I nearly used plain exponential backoff (no randomization) since it
"obviously" fixes retry storms. It doesn't, on its own - a batch of events
that all fail together still retries in lockstep, just on a longer delay.
`Math.random() * cap` instead of a fixed delay per attempt is what actually
spreads the batch out.
