-- The receiver's own durable dedup: event ids it has already applied a side
-- effect for. Separate from the sender's `events` table on purpose - in
-- reality this would live in the receiving application's own database.
CREATE TABLE IF NOT EXISTS received_events (
  event_id    uuid PRIMARY KEY,
  received_at timestamptz NOT NULL DEFAULT now()
);

-- A stand-in side effect: a running total that must only move once per
-- event, even if HERMES redelivers it.
CREATE TABLE IF NOT EXISTS receiver_total (
  id    boolean PRIMARY KEY DEFAULT true CHECK (id),
  total bigint NOT NULL DEFAULT 0
);
INSERT INTO receiver_total (id, total) VALUES (true, 0) ON CONFLICT DO NOTHING;
