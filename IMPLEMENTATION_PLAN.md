# Implementation Plan

Milestones and acceptance criteria are in `docs/PRD.md` §14; this file tracks status.

| Milestone | Scope | Status |
|-----------|-------|--------|
| M0 Skeleton | module, migrations + runner, `migrate`, compose, Makefile, CI | Done (CI and Docker not yet run; Docker isn't installed) |
| M1 Core queue | enqueue/idempotency/EnqueueTx, claim, complete+CommitFunc, fail/backoff/dead, Permanent, worker loop, CLI enqueue/worker/status | Done |
| M2 Leases & recovery | heartbeat, lease-loss cancel, local deadline, reclaim, budget→dead, SIGTERM release, and their tests | Code written in M1; **acceptance tests pending** |
| M3 Chaos + demo | failpoints, harness, connection kills, lease stalls, `check`, `demo`, Dockerfile | Not started |
| M4 Bench + docs | `bench`, `design.md`, `results.md`, README | Not started |

Dependencies: M2 tests → M3 (the harness relies on fenced recovery) → M4 (bench reuses the harness).

Review plan: send `worker.go` and `queries.go` to Codex for an independent concurrency/fencing review once the M2 tests are in.
