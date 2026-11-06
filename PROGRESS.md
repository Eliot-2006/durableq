# Progress

## Done
- PRD committed (`0aea558`, branch `git-identity-setup-d5a8d2de`, pushed).
- M0: `migrations/0001_init.sql` (PRD §4 verbatim), `internal/migrate` (single tx + advisory lock), `cmd/durableq migrate`, `docker-compose.yml`, `Makefile`, `.github/workflows/ci.yml`.
- M0 commit `9733620`, M1 commit `0a7cd9f`.
- M2: `lease_test.go` covers every PRD M2 criterion. The DB-unreachable test cuts a TCP proxy between the worker and Postgres.
- M1: `client.go`, `worker.go`, `queries.go`, `backoff.go`, `errors.go`, `internal/demo`, CLI `enqueue`/`worker`/`status`.

## Test results (2026-11-05, local Homebrew PG 17.11, `go test -race -count=1 ./...`)
All pass: migrate idempotent + concurrent; backoff bounds/cap/monotone; enqueue validation; idempotency key ×20 concurrent → 1 job; EnqueueTx rollback never runs; 10,000 jobs × 50 concurrent claimers → each claimed exactly once; always-failing → dead after exactly 3 attempts with backoff respected; Permanent → dead after 1; delayed job not claimed early; CommitFunc effects atomic.
CLI smoke test: 200 demo.effect jobs at 30% fail rate, SIGTERM after 4s → 199 effects = 199 succeeded, 0 mismatches.

M2 (same setup), all pass under `-race`, full suite at `-count=2`: expired lease reclaimed with attempt+1; stale heartbeat/complete/fail/release all rejected and the stale effect rolled back; heartbeats keep a job alive for 5 lease durations; DB unreachable → job ctx cancelled within lease−heartbeat; crashing job → dead after max_attempts with no execution beyond budget; SIGTERM → in-flight jobs released and claimable within grace; heartbeat-vs-reclaim race ×1000 (≈720 contested stale completions rejected per run, 0 double terminals, 0 duplicate effects).

## Codex review (after M2), results
- Fixed: a handler that ignores ctx no longer blocks shutdown or keeps renewing its lease (`run` stops waiting once ctx is done and discards a late result). Regression test: `TestShutdownBoundedWithUncooperativeHandler`.
- Fixed: `Heartbeat >= Lease/2` is now rejected, because the local deadline would pass before the first renewal. Regression test: `TestHeartbeatMustBeUnderHalfLease`.
- Documented, not changed: a fenced UPDATE evaluates its lease predicate before waiting on a row lock, and Postgres re-checks only if the row was modified meanwhile. A reclaim always bumps `attempt`, so it forces the re-check. An unmodified row means no other owner exists, so a late commit is still single-owner. This only arises if an external session holds `FOR UPDATE` on a job row. Goes into design.md (M4).

## Known issues / notes
- A handler that ignores cancellation keeps running in its goroutine after the worker gives up on it. Its result is discarded and it no longer occupies a slot. External side effects from it can still happen (at-least-once).
- Unknown job kind = retryable failure (not permanent), so a rolling deploy doesn't dead-letter jobs.
- Not yet run: CI, Docker Compose, staticcheck.

## Next
1. M3: failpoints (`internal/failpoint`), invariant checks (`durableq check`), chaos harness with SIGKILL + `pg_terminate_backend` + lease stalls (`chaos/`, build tag), `durableq demo`, Dockerfile.
2. M3 injected-bug test (remove the attempt check from complete → I2/I5 must fail).
