# Implementation Plan

Milestones and acceptance criteria are in `docs/PRD.md` §14; this file tracks status.

| Milestone | Scope | Status |
|-----------|-------|--------|
| M0 Skeleton | module, migrations + runner, `migrate`, compose, Makefile, CI | Done (CI and Docker not yet run; Docker isn't installed) |
| M1 Core queue | enqueue/idempotency/EnqueueTx, claim, complete+CommitFunc, fail/backoff/dead, Permanent, worker loop, CLI enqueue/worker/status | Done |
| M2 Leases & recovery | heartbeat, lease-loss cancel, local deadline, reclaim, budget→dead, SIGTERM release, and their tests | Done; Codex-reviewed, findings fixed |
| M3 Chaos + demo | failpoints, harness, connection kills, lease stalls, `check`, `demo`, Dockerfile | Done except `docker compose run --rm demo` (Docker not installed, so untested) |
| M4 Bench + docs | `bench`, `design.md`, `results.md`, README | Done |

Dependencies: M2 tests → M3 (the harness relies on fenced recovery) → M4 (bench reuses the harness).

Reviews: Codex static review of queries/worker/client after M2 (no critical findings, 2 majors fixed, 1 documented). Next review: the chaos harness at M3.
