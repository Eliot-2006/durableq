# Progress

## Done
- PRD committed (`0aea558`, branch `git-identity-setup-d5a8d2de`, pushed).
- M0: `migrations/0001_init.sql` (PRD §4 verbatim), `internal/migrate` (single tx + advisory lock), `cmd/durableq migrate`, `docker-compose.yml`, `Makefile`, `.github/workflows/ci.yml`.
- M1: `client.go`, `worker.go`, `queries.go`, `backoff.go`, `errors.go`, `internal/demo`, CLI `enqueue`/`worker`/`status`.

## Test results (2026-11-05, local Homebrew PG 17.11, `go test -race -count=1 ./...`)
All pass: migrate idempotent + concurrent; backoff bounds/cap/monotone; enqueue validation; idempotency key ×20 concurrent → 1 job; EnqueueTx rollback never runs; 10,000 jobs × 50 concurrent claimers → each claimed exactly once; always-failing → dead after exactly 3 attempts with backoff respected; Permanent → dead after 1; delayed job not claimed early; CommitFunc effects atomic.
CLI smoke test: 200 demo.effect jobs at 30% fail rate, SIGTERM after 4s → 199 effects = 199 succeeded, 0 mismatches.

## Known issues / notes
- If a handler ignores ctx cancellation, `Run` blocks past ShutdownGrace until it returns. Its lease stays alive through heartbeats in the meantime.
- Unknown job kind = retryable failure (not permanent), so a rolling deploy doesn't dead-letter jobs.
- Not yet run: CI, Docker Compose, staticcheck.

## Next
1. M2 acceptance tests (PRD §14 M2), including the 1,000-iteration heartbeat-vs-reclaim race and the DB-unreachable local deadline.
2. Codex review of the fencing/lease code.
3. M3 failpoints + chaos harness.
