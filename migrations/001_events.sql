CREATE TABLE IF NOT EXISTS events (
  id              uuid PRIMARY KEY,
  idempotency_key text NOT NULL UNIQUE,
  payload         jsonb NOT NULL,
  status          text NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending', 'claimed', 'delivered', 'dead')),
  attempts        integer NOT NULL DEFAULT 0,
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  lease_expires_at timestamptz,
  lease_token     uuid,
  created_at      timestamptz NOT NULL DEFAULT now()
);

-- the worker's claim query filters on (status, next_attempt_at); keep that lookup indexed
CREATE INDEX IF NOT EXISTS events_claim_idx ON events (status, next_attempt_at);
