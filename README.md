# HERMES

A crash-tested webhook delivery service, with a live dashboard that shows
the delivery mechanism working — and breaking, and recovering — in real
time. Postgres as the queue, no Kafka/Redis/RabbitMQ, no ORM. The point is
proving delivery survives a worker crash, not feature count.

## Architecture

```
POST /events ──► [events table]
(idempotency        │  status: pending → claimed → delivered
 key, dedup on       │                           └→ dead (after 8 attempts)
 insert)             │
                     ▼
         worker (1..N concurrent claim loops)
   1. SELECT ... FOR UPDATE SKIP LOCKED, LIMIT 1   (claim: own transaction, commits)
   2. stamp a fresh lease_token + lease_expires_at
   3. ── commit ──
   4. HTTP POST to receiver, X-Event-Id + X-Hermes-Signature (HMAC-SHA256)  (no tx open)
   5. UPDATE events SET status = ... WHERE id = $1 AND lease_token = $2
      (stale worker whose lease was reclaimed: this WHERE matches 0 rows, no-op)
   6. INSERT activity_log row (best-effort - feeds the dashboard, never gates delivery)
                     │
                     ▼
              demo receiver (:4000)
   verify signature → INSERT received_events (event_id) ON CONFLICT DO NOTHING
                    → side effect runs only if that insert was new, same transaction

dashboard (served by api.ts, polls /api/state every 1.5s)
   reads events + activity_log + receiver_total directly - same Postgres,
   no cross-service calls. worker-supervisor.ts (opt-in, DEMO_MODE=true) owns
   the worker as a child process and exposes crash/restart/seed/reset.
```

## The failure this survives

Kill the worker at any point — mid-claim, mid-delivery, right after the HTTP call
returns but before the status write lands — and restart it. No event is lost: the
lease token means a half-finished claim can't be double-written by two workers, and
the receiver's own dedup means a redelivered event never applies its side effect
twice. At-least-once delivery + idempotent processing = effectively-once overall.

## Control room demo

One command brings up Postgres, the receiver, the API + dashboard, and a
demo-mode worker supervisor:

```bash
docker compose up
```

Open **http://127.0.0.1:3500** — that's the dashboard. A reproducible
sequence to see the whole mechanism:

1. Click **Seed 20 events** — watch them go `pending → delivered` in the
   queue-state counts and the activity timeline, receiver total climbing in
   lockstep.
2. Click **Crash after claim-commit** — the worker is killed the instant
   after its *next* claim commits (a deterministic stand-in for the random
   SIGKILL the chaos test throws at it). The timeline logs `worker_crashed`.
3. Watch the stuck row reappear a few seconds later as the lease expires and
   gets reclaimed — queue state drains back to all-delivered, receiver total
   still exactly matches the event count. Nothing was lost or double-applied.
4. Click any timeline row to open the **event inspector**: payload, every
   delivery attempt, its HTTP status, and the recovery history for that one
   event.
5. Click **Reset demo** to clear the queue and run it again.

The control port (`127.0.0.1:4700`) only accepts requests when
`DEMO_MODE=true` and only from the dashboard's own origin - never published
except to host loopback, and off by default everywhere else (plain
`npm run worker` has none of this).

### Demo screenshots

Screenshots from a local run using synthetic data. The dashboard refreshes
every 1.5 seconds; each image shows a sampled state.

![Processing: 686 pending, 1 claimed and 242 delivered; receiver total 242.](https://github.com/user-attachments/assets/a0616070-5b4f-4cc2-b303-7b813561a2b4)

![Recovery: after the simulated crash, delivered and receiver totals both reach 2,005.](https://github.com/user-attachments/assets/433365ac-9bbc-482c-a8b9-aba93ac54c40)

<details>
<summary>More screenshots</summary>

![Clean start: queue counters and demo controls after reset.](https://github.com/user-attachments/assets/01642cf3-e6bf-46e8-baaf-20b7034c83c4)

![Crash: one event remains claimed while the timeline records the worker crash.](https://github.com/user-attachments/assets/cccdae95-c7b1-4708-ab30-b9cf170fc935)

![Event inspector: delivered event payload, attempt count and recorded history.](https://github.com/user-attachments/assets/87351cdd-db4b-4749-90c9-1367c7781c71)

![Dead-letter state: with the receiver stopped, one event exhausts its retries and moves to dead.](https://github.com/user-attachments/assets/b87b2beb-b7dd-4c65-9829-65756abceab2)

![Failed-event inspector: all eight failed attempts shown in the event history.](https://github.com/user-attachments/assets/c1f2ec9e-7379-4062-83fe-766c6a8fb185)

</details>

## Run it without Docker

```bash
docker compose up -d postgres
npm install
npm run migrate
```

In three terminals:

```bash
npm run receiver   # demo delivery target on :4000
npm run api        # ingest API + dashboard on :3500
npm run worker     # claims and delivers pending events (set WORKER_CONCURRENCY=N for parallel loops)
```

For the crash/restart/seed/reset controls without Docker, run the supervisor
instead of the plain worker (bash: `DEMO_MODE=true npm run worker:demo`;
PowerShell: `$env:DEMO_MODE='true'; npm run worker:demo`).

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

`npm test` runs the full suite (10 tests) against a live Postgres: HMAC
sign/verify, idempotent ingest, lease fencing, concurrent delivery, and the
deterministic crash-after-claim-commit boundary. Test files run sequentially
(`--test-concurrency=1`) since several of them `TRUNCATE` shared tables
against the same database.

Throughput benchmark — sends 1000 events per pass, no kills, at
`WORKER_CONCURRENCY` 1/2/4, times send-start to the last event reaching a
terminal state:

```bash
npm run benchmark
```

An optional `HERMES_SIGNING_SECRET` env var overrides the dev-only default secret
used to sign/verify delivery payloads; set the same value for anything that needs
to verify HERMES's signatures.

## Benchmark

1000 events per pass, local receiver, no kills, same machine and conditions
for every row — real measured numbers, one `npm run benchmark` run:

| WORKER_CONCURRENCY | events/sec |
|---|---|
| 1 | 31.0 |
| 2 | 49.3 |
| 4 | 254.2 |

The 1→4 jump is bigger than linear, and that's explained by where the time
actually goes, not by magic: each job is ~7 sequential round trips (claim's
`BEGIN`/`SELECT ... FOR UPDATE`/`UPDATE`/`COMMIT`, one HTTP POST, the
mark-result write, the activity-log write), none of them pipelined. At
concurrency 1, 100% of wall-clock time is that serial chain with nothing
else happening. At concurrency 4, four claim loops have requests in flight
at once, so one loop's wait on a round trip overlaps with another loop
actually doing work - Node's non-blocking I/O fills in gaps that a single
loop leaves idle. `SELECT ... FOR UPDATE SKIP LOCKED` is exactly what makes
this safe with zero coordination: concurrent loops just skip rows another
transaction already has locked instead of blocking on them. Run-to-run
variance is real (a repeat run measured 28.0 / 58.0 / 400.2) - these are
live numbers against a real Postgres instance, not a controlled benchmark
environment, and should be read as "concurrency clearly helps, and by more
than you'd naively guess," not as precise throughput guarantees.

## Design tradeoffs

**Polling over SSE for the dashboard.** Both were reasonable. Polling every
1.5s is fewer moving parts and no reconnect/keep-alive logic to get wrong
live - the data doesn't need sub-second latency to make the point.

**Process-level crash control, not a Docker-engine control port.** "Crash
the worker" doesn't shell out to the Docker API or mount a Docker socket -
that's a real dependency and a real security surface for what's fundamentally
a demo. Instead `worker-supervisor.ts` owns the worker as a normal OS child
process (the same `child_process.spawn` pattern the chaos test already uses)
and can `SIGKILL`/respawn it directly. The control port is still
loopback-only and demo-mode-gated in Docker - same safety property, without
a socket mount.

## Status

Mechanism, concurrency, and the live control room are built and verified end
to end against a real Postgres instance: idempotent ingest, SKIP LOCKED +
lease-token claiming with N concurrent claim loops, expired-lease reclaim,
exponential backoff with jitter, dead-lettering after 8 attempts, HMAC-SHA256
signing, durable receiver-side dedup, a live dashboard, and an on-demand
crash-and-recover demo (both a random SIGKILL and a deterministic
crash-after-claim-commit, each one triggerable from the dashboard and each
one auto-recovered and visible in the timeline). `npm test` (10/10) and
`npm run chaos` (50/50, zero lost) both pass. `docker compose up` was run and
verified live: all four services built and started, the dashboard loaded,
seeding/crashing/reclaiming/inspecting all worked through the real browser UI.

## What works / limitations (updated each session)

See commit history for the running log.

**What works (verified against a real Postgres, not just type-checked):**
idempotent ingest, claim/lease/deliver/mark-result at concurrency 1-4,
expired-lease reclaim (both random-timing and the deterministic
claim-commit boundary), backoff + dead-letter, HMAC sign/verify, receiver
dedup under concurrent delivery, the dashboard's live state/timeline/
inspector, and the crash/restart/seed/reset control routes (demo-mode-gated,
origin-checked, loopback-only). `npm test` (10/10) and `npm run chaos` both
pass.

**How to run the proof:** `docker compose up` then open
`http://127.0.0.1:3500` and use the Crash after claim-commit button - or,
without Docker, `docker compose up -d postgres && npm run migrate && npm run chaos`
for the automated version.

**Honest limitations:**
- HERMES guarantees **at-least-once delivery plus receiver-side
  idempotency** - not exactly-once delivery and not a zero-loss guarantee in
  any absolute sense. Duplicate concurrent delivery attempts for the same
  event are possible and expected (a job can legitimately outlive its lease
  and get reclaimed while the original HTTP call is still in flight); the
  receiver's own dedup transaction is what makes that safe, not the absence
  of duplicates.
- The chaos test's kill timing (400ms-2.5s between SIGKILLs) was tuned by
  hand against one machine's `tsx` cold-start overhead, not derived from
  first principles - a much faster or slower machine might need different
  numbers to actually interrupt mid-flight work instead of mostly killing
  process startup. The deterministic `HERMES_CRASH_AFTER` boundary test
  doesn't have this problem, by construction - it only proves that one exact
  boundary, which is the honest tradeoff for determinism.
- Benchmark numbers are real but noisy (see Benchmark section) - read as
  directional, not as a performance SLA.

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

**Integration tests that share a real database need explicit isolation, and
I found that out the hard way.** The first time I ran the full suite with
the new concurrency and crash-boundary tests added, two tests failed with
bizarre, non-deterministic-looking errors - an idempotency check that should
never fail, and a crash-recovery test that saw its event vanish mid-check.
The cause wasn't a mechanism bug: Node's test runner runs separate test
*files* concurrently by default, and several of my new files `TRUNCATE` the
same tables against the same live Postgres instance. Two files running at
once meant one test's cleanup wiped another test's fixture out from under
it, mid-assertion. Forcing `--test-concurrency=1` fixed it immediately. The
lesson generalizes past this project: process isolation (separate Node
processes per test file) is not the same thing as data isolation, and tests
that share a stateful external resource need to say so explicitly rather
than relying on accidents of scheduling.

**A feature isn't verified until it's run through the actual UI, in a real
browser.** `tsc` was clean and the Dockerized stack came up cleanly on the
first try, but the dashboard's demo-control panel silently never appeared -
the browser was blocking the cross-origin fetch from the dashboard's origin
to the control server's port, and my server never sent an
`Access-Control-Allow-Origin` header, so the request failed before my
same-origin check on the server side ever ran. Nothing in `tsc`, `npm test`,
or a `curl` smoke test would have caught that - only a real browser enforces
CORS. Fixed by echoing back the origin when (and only when) it's on the same
allowlist the server-side check already used.
