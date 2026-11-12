package durableq

// All SQL lives here. Every lease check uses clock_timestamp() (the database
// clock at statement time), never now(), which is frozen at transaction start.
// $attempt is the fencing token: a transition commits only if it still matches
// the row and the lease is unexpired at the moment the row is locked.

const qEnqueue = `
INSERT INTO jobs (kind, payload, max_attempts, run_at, idempotency_key)
VALUES ($1, $2, $3, clock_timestamp() + $4 * interval '1 microsecond', $5)
ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
RETURNING id`

const qByKey = `SELECT id FROM jobs WHERE idempotency_key = $1`

// qClaim also reclaims running jobs whose lease expired (no separate reaper).
// $4 is the kinds this worker handles: a worker never claims (and so never
// burns attempts on) a kind it cannot run.
const qClaim = `
WITH next AS (
  SELECT id, state = 'running' AS reclaimed FROM jobs
  WHERE kind = ANY($4)
    AND ((state = 'queued'  AND run_at <= clock_timestamp())
      OR (state = 'running' AND lease_expires_at < clock_timestamp()))
  ORDER BY run_at, id
  LIMIT $1
  FOR UPDATE SKIP LOCKED
), claimed AS (
  UPDATE jobs j
  SET state = 'running', attempt = j.attempt + 1, lease_owner = $2,
      lease_expires_at = clock_timestamp() + $3 * interval '1 microsecond',
      updated_at = clock_timestamp()
  FROM next WHERE j.id = next.id
  RETURNING j.id, j.kind, j.payload, j.attempt, j.max_attempts,
      extract(epoch FROM clock_timestamp() - j.run_at)::double precision AS queue_wait, next.reclaimed
), hist AS (
  INSERT INTO job_attempts (job_id, attempt, worker_id, claimed_at)
  SELECT id, attempt, $2, clock_timestamp() FROM claimed
)
SELECT id, kind, payload, attempt, max_attempts, queue_wait, reclaimed FROM claimed`

const fenced = ` WHERE id = $1 AND attempt = $2 AND state = 'running' AND lease_expires_at > clock_timestamp()`

const qHeartbeat = `
UPDATE jobs SET lease_expires_at = clock_timestamp() + $3 * interval '1 microsecond',
                updated_at = clock_timestamp()` + fenced

const qComplete = `
UPDATE jobs SET state = 'succeeded', lease_owner = NULL, lease_expires_at = NULL,
                finished_at = clock_timestamp(), updated_at = clock_timestamp()` + fenced

// qFail: $3 = permanent, $4 = backoff (µs), $5 = error. Returns the new state.
const qFail = `
UPDATE jobs SET
  state = CASE WHEN $3 OR attempt >= max_attempts THEN 'dead'::job_state ELSE 'queued'::job_state END,
  run_at = CASE WHEN $3 OR attempt >= max_attempts THEN run_at
                ELSE clock_timestamp() + $4 * interval '1 microsecond' END,
  finished_at = CASE WHEN $3 OR attempt >= max_attempts THEN clock_timestamp() END,
  lease_owner = NULL, lease_expires_at = NULL, last_error = $5, updated_at = clock_timestamp()` + fenced + `
RETURNING state`

// qRelease returns the job to the queue immediately. The attempt number stays
// consumed (it is the fencing token and never decreases, D11), so the budget is
// raised by one instead: a graceful shutdown must not push a job toward dead.
const qRelease = `
UPDATE jobs SET state = 'queued', run_at = clock_timestamp(), max_attempts = max_attempts + 1,
                lease_owner = NULL, lease_expires_at = NULL, updated_at = clock_timestamp()` + fenced

const qFinishAttempt = `
UPDATE job_attempts SET outcome = $3, error = $4, finished_at = clock_timestamp()
WHERE job_id = $1 AND attempt = $2`

// Sample the DB clock once so a job becoming due during the scan is counted once.
const qStats = `WITH snapshot AS MATERIALIZED (SELECT clock_timestamp() AS at)
 SELECT
 (SELECT count(*) FROM jobs WHERE state='queued' AND run_at <= snapshot.at),
 (SELECT count(*) FROM jobs WHERE state='queued' AND run_at > snapshot.at),
 (SELECT count(*) FROM jobs WHERE state='running' AND lease_expires_at > snapshot.at),
 (SELECT count(*) FROM jobs WHERE state='running' AND lease_expires_at <= snapshot.at),
 (SELECT count(*) FROM jobs WHERE state='dead')
 FROM snapshot`

const qJob = `SELECT j.id, j.kind, j.state::text, j.attempt, j.max_attempts,
 j.run_at, j.created_at, j.finished_at, j.last_error,
 coalesce((SELECT jsonb_agg(jsonb_build_object(
 'attempt', a.attempt, 'worker_id', a.worker_id, 'claimed_at', a.claimed_at,
 'finished_at', a.finished_at, 'outcome', a.outcome, 'error', a.error) ORDER BY a.attempt)
 FROM job_attempts a WHERE a.job_id=j.id), '[]'::jsonb)
 FROM jobs j WHERE j.id=$1`
