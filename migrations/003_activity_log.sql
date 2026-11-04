-- Append-only log of both per-event delivery attempts and control-room
-- actions (crash/restart/seed/reset). Lets the dashboard show a timeline
-- without inferring history from the events table's current-state-only rows.
CREATE TABLE IF NOT EXISTS activity_log (
  id              bigserial PRIMARY KEY,
  kind            text NOT NULL
                  CHECK (kind IN ('delivery_attempt', 'worker_crashed', 'worker_restarted', 'demo_reset', 'demo_seed')),
  event_id        uuid NULL REFERENCES events(id),
  attempt_number  integer NULL,
  outcome         text NULL,
  http_status     integer NULL,
  detail          text NULL,
  created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS activity_log_event_idx ON activity_log (event_id, created_at);
CREATE INDEX IF NOT EXISTS activity_log_kind_idx ON activity_log (kind, created_at);
