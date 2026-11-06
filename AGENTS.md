# AGENTS.md

Instructions for any agent working on DurableQ. Spec: `docs/PRD.md` (decisions D1–D11 are binding unless revisited there).

## Git
- Identity is set in the repo-local config (shared by all worktrees): `Eliot Boda <eliotboda@gmail.com>`. Check with `git var GIT_AUTHOR_IDENT` before committing.
- Commit or push only when the owner explicitly asks. No `Co-authored-by` or AI attribution trailers. Never rewrite history.

## Environment (local)
- Go 1.27, PostgreSQL 17 via Homebrew (`brew services start postgresql@17`; binaries in `$(brew --prefix postgresql@17)/bin`).
- `DATABASE_URL=postgres://localhost:5432/durableq?sslmode=disable` (Homebrew) or the Makefile default (Docker Compose).

## Conventions
- All SQL in `queries.go`. Lease checks use `clock_timestamp()`, never `now()`.
- Every state transition is fenced on `(id, attempt, state='running', lease unexpired)`; the fenced UPDATE comes first in its transaction.
- `attempt` is never decremented. No unique constraint on `effects.job_id`.
- Dependencies: `pgx/v5` only. CLI uses stdlib `flag`.

## Testing
- Integration tests hit real Postgres via `internal/testdb.New(t)` (isolated schema per test). No DB mocks.
- Run: `go vet ./... && go test -race -count=1 ./...`. Tests skip without `DATABASE_URL`, so check that they actually ran (`-v`).
- Test lease expiry deterministically: set `lease_expires_at` in the past rather than sleeping.
- Never weaken a test to make it pass.
