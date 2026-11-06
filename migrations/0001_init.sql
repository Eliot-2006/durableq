CREATE TYPE job_state AS ENUM ('queued', 'running', 'succeeded', 'dead');

CREATE TABLE jobs (
  id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  kind             text        NOT NULL,
  payload          jsonb       NOT NULL DEFAULT '{}',
  state            job_state   NOT NULL DEFAULT 'queued',
  attempt          int         NOT NULL DEFAULT 0,
  max_attempts     int         NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
  run_at           timestamptz NOT NULL DEFAULT clock_timestamp(),
  lease_owner      text,
  lease_expires_at timestamptz,
  idempotency_key  text,
  last_error       text,
  created_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
  finished_at      timestamptz,
  CHECK ((state = 'running') = (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL))
);

CREATE UNIQUE INDEX jobs_idempotency_key ON jobs (idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX jobs_queued_run_at   ON jobs (run_at, id)       WHERE state = 'queued';
CREATE INDEX jobs_running_expiry  ON jobs (lease_expires_at) WHERE state = 'running';

CREATE TABLE job_attempts (
  job_id      bigint      NOT NULL REFERENCES jobs(id),
  attempt     int         NOT NULL,
  worker_id   text        NOT NULL,
  claimed_at  timestamptz NOT NULL,
  finished_at timestamptz,
  outcome     text CHECK (outcome IN ('succeeded', 'failed', 'dead', 'released')),
  error       text,
  PRIMARY KEY (job_id, attempt)
);

-- Demo only. Deliberately no unique constraint on job_id: duplicates must be
-- visible to the invariant checker rather than hidden by a constraint.
CREATE TABLE effects (
  id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  job_id     bigint      NOT NULL REFERENCES jobs(id),
  attempt    int         NOT NULL,
  applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
