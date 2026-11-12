# DurableQ — Product Requirements & Implementation Plan

A PostgreSQL-backed durable job queue in Go. Workers claim jobs with `FOR UPDATE SKIP LOCKED`, hold database-timed leases renewed by heartbeats, and recover work from crashed workers. Correctness is demonstrated by SIGKILL-based chaos tests that check invariants directly in the database.

---

## 1. Goals and non-goals

### Goals
- Durable job submission with optional idempotency keys, including enqueue inside a caller's transaction.
- Many worker processes claiming jobs concurrently without double ownership.
- Lease-based ownership on the database clock, heartbeat renewal, and automatic reclaim of expired leases.
- Fencing so stale workers cannot commit state transitions.
- Retries with exponential backoff and jitter; dead-letter state for jobs that exhaust attempts.
- A CLI for migrating, enqueueing, running workers, inspecting state, checking invariants, and running the demo.
- Chaos tests that SIGKILL real worker processes and terminate real database connections.
- Reproducible benchmarks with honestly documented results.

### Non-goals
- Exactly-once execution of arbitrary side effects.
- PostgreSQL high availability, replication, or surviving permanent database loss.
- Redis, Kafka, gRPC, a custom web dashboard, or any queue infrastructure beyond Postgres. (Kubernetes deployment, Prometheus metrics, and Grafana dashboards were added in §16.)
- Named queues, priorities, cron/scheduled recurring jobs, job cancellation, workflows/DAGs (all stretch at most).
- Multi-tenant auth.

---

## 2. Decisions taken (revisit if you disagree)

| # | Decision | Rationale |
|---|----------|-----------|
| D1 | **Library + CLI.** Root package `durableq` exposes `Client` and `Worker`; `cmd/durableq` is the CLI. | Library API reads as a real system; CLI is a thin layer over it. |
| D2 | **Lease model, not lock-held-for-duration.** Claim is a short transaction; ownership is a lease row field. | Doesn't pin one connection per running job or create long transactions. The alternative is documented in `design.md`. |
| D3 | **No separate reaper.** The claim query also picks `running` jobs whose lease expired. | One less process and one less failure mode. |
| D4 | **`attempt` increments at claim time** and is the fencing token. | A job that crashes its worker every time still burns attempts and reaches `dead`. |
| D5 | **Lease expiry counts as an attempt.** | Follows from D4; prevents poison jobs looping forever. |
| D6 | **Handlers can fail permanently** via `durableq.Permanent(err)` → straight to `dead`. | Retrying a malformed payload is pointless. |
| D7 | **Idempotency keys deduplicate forever** (unique partial index). | Simplest rule to state and test. Time-windowed dedupe is stretch. |
| D8 | **All lease checks use `clock_timestamp()`, not `now()`.** | `now()` is frozen at transaction start; a long transaction would compare against a stale time. |
| D9 | **Polling for MVP.** `LISTEN/NOTIFY` is stretch, measured as a before/after latency change. | Polling is simpler and robust to dropped notifications. |
| D10 | **Driver: `pgx/v5` (`pgxpool`). CLI: stdlib `flag`. Migrations: embedded SQL + tiny runner.** | One real dependency. No cobra, no migration framework. |
| D11 | **`attempt` is never decremented**, including when manually retrying a dead job (raise `max_attempts` instead). | Decrementing would let an old token become valid again and break fencing. |

---

## 3. Guarantees and fault model

### Guarantees (stated in README and `design.md`)
1. **At-least-once execution.** Every committed job is eventually executed until it succeeds or is dead-lettered, as long as at least one worker is running and the database is available.
2. **At most one committed terminal transition per job.** A job transitions to `succeeded` or `dead` exactly once.
3. **Fenced transitions.** A state transition (heartbeat, complete, fail, release) commits only if the caller's `attempt` matches the row and its lease is unexpired by the database clock at the moment the row is locked.
4. **Exactly-once effects for transactional work.** Effects written in the same transaction as completion (via the handler's `onCommit` hook) are applied exactly once per job.
5. **Bounded attempts.** No job executes more than `max_attempts` times. A reclaimed job whose attempt exceeds the budget is marked `dead` without running.

### Not guaranteed
- External side effects (HTTP calls, emails) may happen more than once. Handlers must be idempotent for those.
- Ordering. Jobs are claimed roughly by `run_at`, but there is no FIFO guarantee under concurrency or retries.
- Liveness while Postgres is down. Workers retry connecting; nothing progresses.

### Fault model covered
- Worker SIGKILL at any point (including mid-commit).
- Worker graceful shutdown (SIGTERM).
- Worker database connection terminated (`pg_terminate_backend`).
- Lease expiry while the worker is still alive (slow job, GC pause, network stall).
- Postgres restart (stretch test; must not lose committed jobs, since Postgres is durable).

---

## 4. Data model

### `jobs`
```sql
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
```

### `job_attempts` (history, drives metrics and invariant checks)
```sql
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
```
`outcome IS NULL` on an attempt that is not the job's current attempt means the attempt was **abandoned** (crash, lease loss, killed connection).

### `effects` (demo only)
```sql
CREATE TABLE effects (
  id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  job_id     bigint      NOT NULL REFERENCES jobs(id),
  attempt    int         NOT NULL,
  applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
```
**There is deliberately no unique constraint on `effects.job_id`.** A unique constraint would hide fencing bugs; without it, a bug shows up as duplicate rows, which the invariant checker detects.

### State machine
```
            enqueue
               │
               ▼
   ┌──────► queued ──claim──► running ──complete──► succeeded
   │           ▲                │  │
   │  retry    │ release/       │  └──fail (attempts exhausted | permanent)──► dead
   │ (backoff) │ lease expiry   │
   └───────────┴──── fail ──────┘
```
- `running → running` also happens on reclaim: an expired `running` row is claimed directly by another worker, with `attempt` incremented.
- `succeeded` and `dead` are terminal (except manual dead-job retry, stretch, per D11).

---

## 5. Core operations (SQL contracts)

Each operation below is one statement or one short transaction. `$attempt` is the fencing token held by the worker.

### Enqueue
```sql
INSERT INTO jobs (kind, payload, max_attempts, run_at, idempotency_key)
VALUES ($1, $2, $3, clock_timestamp() + $4, $5)
ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
RETURNING id;
-- no row returned → SELECT id FROM jobs WHERE idempotency_key = $5; report existed=true
```
`Client.Enqueue(ctx, ...)` and `Client.EnqueueTx(ctx, tx, ...)`. The Tx variant lets an application enqueue atomically with its own writes (the transactional outbox pattern).

### Claim (batch of up to N)
```sql
WITH next AS (
  SELECT id FROM jobs
  WHERE (state = 'queued'  AND run_at <= clock_timestamp())
     OR (state = 'running' AND lease_expires_at < clock_timestamp())
  ORDER BY run_at, id
  LIMIT $n
  FOR UPDATE SKIP LOCKED
), claimed AS (
  UPDATE jobs j
  SET state = 'running', attempt = j.attempt + 1, lease_owner = $worker,
      lease_expires_at = clock_timestamp() + $lease, updated_at = clock_timestamp()
  FROM next WHERE j.id = next.id
  RETURNING j.*
), hist AS (
  INSERT INTO job_attempts (job_id, attempt, worker_id, claimed_at)
  SELECT id, attempt, $worker, clock_timestamp() FROM claimed
)
SELECT * FROM claimed;
```
After claiming, if `attempt > max_attempts`, the worker immediately runs the fenced **dead** transition without executing the handler (guarantee 5).

### Heartbeat
```sql
UPDATE jobs SET lease_expires_at = clock_timestamp() + $lease, updated_at = clock_timestamp()
WHERE id = $id AND attempt = $attempt AND state = 'running' AND lease_expires_at > clock_timestamp();
```
0 rows → lease lost → cancel the job's context and abandon it. Do not try to complete.

### Complete (one transaction, **fenced update first**)
```sql
BEGIN;
UPDATE jobs SET state = 'succeeded', lease_owner = NULL, lease_expires_at = NULL,
                finished_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE id = $id AND attempt = $attempt AND state = 'running' AND lease_expires_at > clock_timestamp();
-- 0 rows → ROLLBACK, report stale completion rejected
-- handler's onCommit(tx) runs here (e.g. INSERT INTO effects)
UPDATE job_attempts SET outcome = 'succeeded', finished_at = clock_timestamp() WHERE job_id = $id AND attempt = $attempt;
COMMIT;
```
The fenced update must come first. It takes the row lock, so from that point a reclaimer's `SKIP LOCKED` scan cannot take the job. The lease check is therefore valid for the rest of the transaction.

### Fail
Same fenced `WHERE`. Sets `state = 'dead'` if `attempt >= max_attempts` or the error is permanent, else `state = 'queued', run_at = clock_timestamp() + $backoff`. Clears the lease, records `last_error`, and updates `job_attempts` in the same transaction.

### Release (graceful shutdown)
Fenced update to `state = 'queued', run_at = clock_timestamp()`, clearing the lease. The attempt stays consumed (D5).

### Backoff
`delay = rand(0, min(cap, base * 2^(attempt-1)))` (full jitter). Defaults: base 1s, cap 10m. Pure function, unit-tested.

---

## 6. Worker design

```
Worker
 ├─ poll loop: free slots > 0 → Claim(min(free, batch)); nothing claimed → sleep(poll ± jitter)
 ├─ per job goroutine:
 │    ctx = WithTimeout(jobTimeout) + cancel on lease loss
 │    heartbeat ticker (every lease/3)
 │    onCommit, err := handler(ctx, job)
 │    lease lost?  → drop (no transition)
 │    err == nil   → Complete(onCommit)
 │    err != nil   → Fail(err, permanent?)
 └─ shutdown (SIGTERM): stop claiming → wait up to grace → Release unfinished → exit
```

**Handler API**
```go
type Job struct { ID int64; Kind string; Payload json.RawMessage; Attempt, MaxAttempts int }
type CommitFunc func(ctx context.Context, tx pgx.Tx) error
type HandlerFunc func(ctx context.Context, job Job) (CommitFunc, error)

w := durableq.NewWorker(pool, durableq.WorkerConfig{...})
w.Handle("demo.effect", demoHandler)
w.Run(ctx)
```
Handlers do their slow or external work outside any transaction and return an optional `CommitFunc`, which runs inside the fenced completion transaction.

**Local lease deadline.** A worker cannot read the database clock when heartbeats fail. It tracks `localDeadline = sendTimeOfLastSuccessfulHeartbeat + lease − heartbeatInterval` using Go's monotonic clock. Using the send time (not the ack time) is conservative. Once the local deadline passes, the worker cancels the job's context even if the database is unreachable.

**Worker identity.** `hostname-pid-random`, also set as the Postgres `application_name` so chaos tests can target its connections.

**Defaults:** lease 30s, heartbeat 10s, poll 1s, concurrency 10, batch 10, job timeout 5m, shutdown grace 10s. Chaos runs use lease 2s and heartbeat 500ms.

---

## 7. Demo job kinds

| Kind | Behavior | Purpose |
|------|----------|---------|
| `demo.effect` | Sleeps `sleep_ms` (with jitter), fails with probability `fail_rate`, permanently fails if `permanent: true`; on success, `CommitFunc` inserts into `effects`. | The invariant-checkable workload. |
| `demo.noop` | Returns immediately. | Throughput benchmarks. |

---

## 8. CLI

Single binary, stdlib `flag` subcommands, `DATABASE_URL` from the environment.

| Command | Purpose |
|---------|---------|
| `durableq migrate` | Apply embedded migrations. |
| `durableq enqueue --kind K --payload JSON [--key K] [--max-attempts N] [--delay D] [--count N]` | Submit jobs. |
| `durableq worker [--concurrency N] [--lease D] [--heartbeat D] [--poll D]` | Run a worker with demo handlers registered. |
| `durableq status [job-id]` | Counts by state, or one job plus its attempt history. |
| `durableq check` | Run invariant queries (§9), print pass/fail, exit non-zero on violation. |
| `durableq demo [--jobs N] [--workers N] [--kills N] [--seed S]` | One-command demo (§10). |
| `durableq bench [...]` | Benchmark runner (§11). |

---

## 9. Invariants (`durableq check`)

Run when the queue is drained (no `queued` or `running` jobs remain).

| ID | Invariant | Query sketch |
|----|-----------|--------------|
| I1 | No lost jobs: every job is terminal. | `count(*) WHERE state NOT IN ('succeeded','dead') = 0` |
| I2 | No duplicate effects. | `SELECT job_id FROM effects GROUP BY 1 HAVING count(*) > 1` is empty |
| I3 | Every succeeded `demo.effect` job has exactly one effect. | anti-join both directions |
| I4 | Dead jobs have no effects. | join `effects` ↔ `jobs WHERE state='dead'` is empty |
| I5 | Effect was written by the final owner. | `effects.attempt = jobs.attempt` for all rows |
| I6 | Attempt budget respected. | count of `job_attempts` with `outcome IS NOT NULL` ≤ `max_attempts` per job |
| I7 | At most one terminal attempt per job. | `count(*) FILTER (WHERE outcome IN ('succeeded','dead')) ≤ 1` per job |

Reported metrics (not invariants): duplicate execution attempts (abandoned attempts on jobs that later ran again), stale transitions rejected (worker-side counter), retries, and dead count.

---

## 10. Chaos testing and demo

### Kill mechanisms
1. **Random-time SIGKILL.** The harness spawns K worker processes and, at random intervals, SIGKILLs one and respawns it.
2. **Deterministic failpoints.** `DURABLEQ_FAILPOINTS="after_claim=0.02,in_complete_tx=0.05"`. At each named point the worker SIGKILLs itself with the given probability. These are no-ops when the variable is unset.
   - `after_claim`: claimed, handler not started
   - `during_run`: inside the handler
   - `before_complete`: handler returned, transaction not begun
   - `in_complete_tx`: fenced update and effect written, before COMMIT
   - `after_commit`: committed, before any further bookkeeping
3. **Connection kills.** `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name LIKE 'durableq-worker-%'` at random intervals.
4. **Lease stalls.** A payload flag makes the handler stop heartbeating and sleep past the lease. This proves the stale worker's completion is rejected and the job is completed exactly once by the new owner.

### Harness
- `chaos/` package, build tag `chaos`, run with `go test -tags chaos ./chaos/...`.
- Seeded randomness. The seed is printed on failure so the run can be reproduced.
- Flow: migrate a fresh schema → enqueue N `demo.effect` jobs with a fail rate → start K workers → inject faults until done → stop faults → wait for drain (with timeout) → run invariant checks → print report.

### `durableq demo` (one command)
`docker compose run --rm demo` builds the image, starts Postgres, and runs the harness in-process with readable progress output:
```
Enqueued 10,000 jobs · 6 workers · lease 2s
[00:03] SIGKILL worker-3 (pid 412) … respawned
[00:04] terminated 2 worker connections
...
Drained in 48.2s
  Jobs succeeded       9,812   dead 188
  Worker kills         120     connection kills 30
  Abandoned attempts   97      stale transitions rejected 14
  Duplicate effects    0       Lost jobs 0
All 7 invariants PASSED
```

---

## 11. Benchmarks

`durableq bench` writes CSV to `bench/results/` and a summary to stdout. Each run records the Postgres version, settings, CPU and RAM, and git SHA.

| Metric | Source |
|--------|--------|
| Throughput (jobs/s) | `demo.noop`; sweep processes {1,2,4,8} × concurrency {1,10,50} |
| Queue latency p50/p95/p99 | `job_attempts.claimed_at − jobs.run_at` for first attempts |
| Crash recovery latency | SIGKILL time → next attempt's `claimed_at`; reported against lease length |
| Contention | `pg_stat_activity` wait events sampled during the run; `pg_locks` counts |
| Connections used | pool stats + `pg_stat_activity` |
| Table bloat | `n_dead_tup` from `pg_stat_user_tables` before and after |
| Chaos reliability | N seeded chaos runs → % passing all invariants (expected 100%) |

`docs/results.md` reports numbers with the setup, the exact reproduction command, and known caveats (laptop Docker, single Postgres, etc.).

---

## 12. Repository layout

```
durableq/
├─ go.mod                     module github.com/Eliot-2006/durableq
├─ client.go                  Enqueue, EnqueueTx
├─ worker.go                  Worker, poll loop, job runner, heartbeat, shutdown
├─ queries.go                 all SQL in one place
├─ backoff.go, errors.go      backoff, Permanent()
├─ *_test.go                  unit + integration tests beside the code
├─ migrations/0001_init.sql   embedded via go:embed
├─ internal/migrate/          ~40-line runner with schema_migrations table
├─ internal/failpoint/
├─ internal/demo/             demo handlers, invariant queries, chaos harness core
├─ cmd/durableq/              CLI
├─ chaos/                     chaos tests (build tag)
├─ bench/                     benchmark runner + results/
├─ docs/PRD.md, design.md, results.md
├─ docker-compose.yml, Dockerfile, Makefile
└─ .github/workflows/ci.yml
```

---

## 13. Testing strategy

- **Integration tests use real Postgres** (Docker Compose locally, a `services: postgres` container in CI). Each test gets an isolated schema via `search_path`. No database mocks.
- **Lease expiry is tested deterministically** by setting `lease_expires_at = clock_timestamp() - interval '1s'` directly rather than sleeping.
- All tests run with `-race`.
- **CI:** lint (`go vet`, `staticcheck`) → unit + integration → a short chaos run (1,000 jobs, ~20 kills, < 2 min) on every push. A nightly workflow runs a long chaos run (10,000 jobs, 20 seeds).

---

## 14. Milestones and acceptance criteria

### M0 — Skeleton
- Go module, `docker-compose.yml` (Postgres 17), `Makefile`, `durableq migrate`, CI running `go vet` and tests.
- ✅ `make up && make migrate` creates the schema; CI is green on an empty test suite.

### M1 — Core queue
Enqueue (+ idempotency, `EnqueueTx`), claim, complete with `CommitFunc`, fail with backoff, dead-letter, `Permanent`, worker poll loop with concurrency, CLI `enqueue` / `worker` / `status`.
- ✅ Enqueueing the same idempotency key twice (including concurrently from 20 goroutines) yields one job.
- ✅ A job enqueued with `EnqueueTx` in a rolled-back transaction never runs.
- ✅ 50 goroutine workers claiming 10,000 jobs: every job is claimed exactly once (no duplicate `(job_id, attempt=1)` rows; every job claimed).
- ✅ A job failing every time ends `dead` after exactly `max_attempts` attempts, with backoff gaps ≥ the computed minimum.
- ✅ A `Permanent` error goes to `dead` after one attempt.
- ✅ A job with `run_at` in the future is not claimed early.
- ✅ Backoff unit tests: bounds, cap, monotone upper bound.

### M2 — Leases, fencing, recovery
Heartbeat, lease-loss cancellation, local lease deadline, reclaim of expired leases, fenced transitions everywhere, attempt-budget-exceeded → dead, SIGTERM release.
- ✅ A job whose lease expired is reclaimed by another worker with `attempt + 1`.
- ✅ The stale worker's complete, fail, and heartbeat calls all affect 0 rows, and its `CommitFunc` effect is rolled back.
- ✅ Heartbeats keep a job running past several lease durations without reclaim.
- ✅ With the database connection killed, the worker cancels the job context by its local deadline.
- ✅ A job that kills its worker every time ends `dead` after `max_attempts` claims and is never executed beyond the budget.
- ✅ After SIGTERM, in-flight jobs are released and immediately claimable; the worker exits within the grace period.
- ✅ Concurrent heartbeat-vs-reclaim race test (1,000 iterations) never yields two successful terminal transitions.

### M3 — Chaos + demo (resume-ready)
Failpoints, harness, connection kills, lease stalls, `durableq check`, `durableq demo`, Docker demo service.
- ✅ `docker compose run --rm demo` works from a clean clone with only Docker installed.
- ✅ 10,000 jobs, ≥ 100 SIGKILLs, ≥ 20 connection kills, all failpoints enabled: all invariants I1–I7 pass.
- ✅ The same run passes for 20 consecutive seeds.
- ✅ A deliberately injected bug (removing the `attempt` check from complete) makes I2 or I5 fail. This proves the checker can catch a fencing bug. Document it in `results.md`, then revert.

### M4 — Benchmarks + docs
`durableq bench`, `docs/design.md`, `docs/results.md`, README.
- ✅ `design.md` covers: state machine, every SQL contract with its reasoning, lease vs. lock-held tradeoff, `now()` vs `clock_timestamp()`, fencing-token limits for external effects, the claim-time attempt decision, and known limitations.
- ✅ `results.md` has throughput, latency, and recovery tables with reproduction commands and hardware specs.
- ✅ README: one-paragraph pitch, guarantees, quickstart, demo output, links to the docs.

### Stretch (pick by interest)
- `LISTEN/NOTIFY` wakeups, measured as the queue-latency change.
- Same benchmark against River, reported honestly.
- `durableq dead retry <id>` (raises `max_attempts`, per D11).
- Retention/cleanup of finished jobs; table partitioning by `finished_at`.
- Postgres-restart chaos scenario.
- Priorities or named queues.
- Batching completions.

---

## 15. Risks

| Risk | Mitigation |
|------|------------|
| The `OR` in the claim query defeats index use under load | Benchmark it; fallback is two claim queries (expired first, then queued). Document whichever wins. |
| Dead-tuple bloat on `jobs` under high churn | Measure `n_dead_tup`; tune autovacuum for the table; retention as stretch. |
| Flaky chaos tests | Seeded randomness, drain timeout with diagnostic dump, deterministic failpoint tests alongside random ones. |
| Fencing bugs hidden by constraints | No unique constraint on `effects.job_id` (§4); injected-bug test (M3). |

---

## 16. Kubernetes extension (2026-11-12)

Adds a containerized HTTP API, worker health and metrics endpoints, a Helm chart, queue-depth autoscaling, and Kubernetes-level chaos and benchmarks. Plan, interface contract, and delegation: [`docs/kubernetes-plan.md`](kubernetes-plan.md). The queue semantics (D2–D9, D11) are unchanged.

| # | Decision | Rationale |
|---|----------|-----------|
| D12 | **Second dependency: `prometheus/client_golang`, used only by `cmd/` and `internal/`.** The root package exposes an optional event callback (`WorkerConfig.OnEvent`) and does not import Prometheus. | `/metrics` in the standard format, while library users don't inherit the dependency. Revises D10. |
| D13 | **One binary and one image; `durableq serve` is the HTTP API.** API and worker are the same image with different args. | One build, one scan, one tag to roll. |
| D14 | **Queue-wide gauges (pending, running, dead…) are exported only by the API**, computed with one SQL query per scrape. Workers export per-process counters and histograms. | N workers exporting the same DB-derived gauge would multiply it under `sum()`. |
| D15 | **Autoscaling: KEDA's PostgreSQL scaler on eligible backlog** (queued and due, plus expired leases), with `AverageValue` semantics (backlog per replica). | Scales on the source of truth, with no Prometheus in the scaling path. Trade-offs are in `docs/kubernetes.md`. |
| D16 | **In-chart Postgres is a dev-only single-replica StatefulSet on the official `postgres:17` image with a PVC**, switched off when using an external database. | Bitnami images are no longer freely published, and an operator (CloudNativePG) is more than local dev needs. The app stays compatible with any external Postgres through `DATABASE_URL`. |
| D17 | **Migrations run as an init container** (`durableq migrate`, advisory-locked and idempotent) on API and worker pods, not as a Helm hook. | A pre-install hook would run before the in-chart Postgres exists. |

