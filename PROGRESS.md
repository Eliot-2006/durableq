# Progress

## Done
- PRD committed (`0aea558`, branch `git-identity-setup-d5a8d2de`, pushed).
- M0: `migrations/0001_init.sql` (PRD §4 verbatim), `internal/migrate` (single tx + advisory lock), `cmd/durableq migrate`, `docker-compose.yml`, `Makefile`, `.github/workflows/ci.yml`.
- M0 commit `9733620`, M1 commit `0a7cd9f`.
- M2: `lease_test.go` covers every PRD M2 criterion. The DB-unreachable test cuts a TCP proxy between the worker and Postgres.
- M1: `client.go`, `worker.go`, `queries.go`, `backoff.go`, `errors.go`, `internal/demo`, CLI `enqueue`/`worker`/`status`.

- M3: `internal/failpoint`, `internal/demo/{check,chaos,schema}.go`, `chaos/` (build tag `chaos`), CLI `check`/`demo`, Dockerfile + compose `demo` service, CI short chaos run + nightly long run.
- M4 (partial): `internal/demo/bench.go` + CLI `bench`, `docs/design.md`, README.

## M3 acceptance (2026-11-05, same machine)
- 20 consecutive seeds (1000–1019) × 10,000 jobs, all failpoints at 0.2%: every run had 100 SIGKILLs, 92–116 failpoint self-kills, 20 connection-kill rounds (205–396 connections), and 5 SIGSTOP stalls. **20/20 passed I1–I7.** Drain time 24.5–34.8s. Command is in `chaos/chaos_test.go`'s header.
- Each failpoint isolated at 3%: all 5 fire and pass.
- Injected bug (attempt check removed from complete): I5 fails (8 and 4 violations on seeds 4242/4243, 11 in `TestStalledWorkerFenced`). The correct code passes the same workload. Reverted; `TestStalledWorkerFenced` now uses that workload permanently.
- Lease stall uses SIGSTOP/SIGCONT of a worker process instead of the PRD's payload flag. A payload flag can't stop heartbeats, which run outside the handler. Resumed workers mostly abandon their jobs on the local deadline; the rest attempt completions that the fence rejects.

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
- Workers claim only kinds they have handlers for (since the audit). The earlier note here, that unknown kinds were retried "so a rolling deploy doesn't dead-letter jobs", was wrong: they were dead-lettered after max_attempts.
- Run benchmarks on an otherwise idle database: wait-event and connection sampling, and the chaos connection kills, match every `durableq-worker-%` session in the database.

## M4
- `durableq bench` run at `8fa4341` (clean tree). Results are in `bench/results/20261106T050117Z/`, written up in `docs/results.md`: peak about 10.9k jobs/s (8 × 50, 80 conns), queue latency p50/p95/p99 21/70/83ms at 200 jobs/s, crash recovery about 1.99s with a 2s lease, chaos 5/5.

## CI fix (after first push)
- The first GitHub CI run failed in `TestChaos`: the runner drained 1,000 jobs in 4.6s, before 27 time-scheduled faults landed. Root cause: faults were scheduled on wall time while throughput depends on the machine. Fix: faults now fire at seeded random points of queue progress (2–95% complete), so the configured counts always land. The `-fault-every` flag is removed. Verified with a zero-sleep workload draining in 4.2s: exact fault counts, all invariants pass.
- `TestLocalDeadlineCancelsJobWhenDatabaseUnreachable` failed 11/30 locally: the test read `LeaseLost` before the runner recorded it. That's a test race, not a worker bug. The test now waits up to 1s for the counter; 30/30 pass.
- `docker compose run --rm demo` verified (Docker 29.9.2): 10,000 jobs, 120 SIGKILLs, 30 conn-kill rounds, 5 stalls, all invariants pass. The compose host port is overridable via `DURABLEQ_PG_PORT`.

## Audit (2026-11-05, branch `audit-fixes`)
Self-review plus an independent Codex review found no critical issues and 8 majors. All are fixed with regression proof:
- **M1:** release on a job's last attempt dead-lettered it and broke I6. Reproduced (`max_attempts=1`, SIGTERM mid-run: dead, I6 1 violation). Fix: release raises `max_attempts` by 1. Proof: `TestReleaseOnLastAttemptDoesNotDeadLetter`; `TestGracefulRestartsKeepBudget` fails with the fix reverted (I6 163 violations) and passes with it.
- **M2:** workers claimed kinds they had no handler for, then dead-lettered them. Fix: the claim filters `kind = ANY($4)`. Proof: `TestWorkersClaimOnlyRegisteredKinds` (failed before the fix).
- **M3:** invariants were checked before stalled or exiting workers stopped. Fix: the harness stops and reaps all workers, then checks. The injected fencing bug now shows 50 I5 violations in `TestStalledWorkerFenced` (11 before the fix).
- **M4:** infrastructure faults dead-letter healthy jobs (D5), which the docs didn't say. Reproduced with `fail_rate=0`. Documented; the report now splits dead jobs by cause.
- **M5:** a `CommitFunc` panic killed the worker process. It is now recovered and the job retried. The blocking-callback contract is documented.
- **M6:** connection kills and bench sampling hit every DurableQ worker in the database. Now scoped by a per-run `--tag`.
- **M7:** fault counters counted signals that didn't land, and could target dead processes. They now count only effective faults on live workers and fire every fault that's due; `Shortfall` is asserted to be 0.
- **M8:** the race test could pass with completion broken. It now asserts every job succeeds with exactly one effect, and that both sides of the race won.
- Minor: claims aren't cancelled by shutdown; README says "at most one terminal transition"; the shutdown bound with the DB down is documented. Added `--grace`, `--tag`, `--restarts` flags.

## Next
- Stretch items (PRD §14) only if asked: LISTEN/NOTIFY, River comparison, `dead retry`, retention, Postgres-restart chaos.
