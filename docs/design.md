# DurableQ design

DurableQ is a job queue whose only infrastructure is PostgreSQL. This document explains the state machine, every SQL contract and why it is shaped the way it is, and what the system does and does not guarantee. Requirements and decisions D1–D11 are in [PRD.md](PRD.md); measurements are in [results.md](results.md).

## 1. Guarantees

| Guaranteed | Not guaranteed |
|---|---|
| **At-least-once execution.** A committed job runs until it succeeds or is dead-lettered, as long as some worker runs and Postgres is up. | Exactly-once *external* side effects (HTTP calls, emails). The handler may run more than once. |
| **One terminal transition per job.** `succeeded` or `dead` commits once. | Ordering. Jobs are claimed roughly by `run_at`, with no FIFO under concurrency or retries. |
| **Fenced transitions.** Heartbeat/complete/fail/release commit only for the current attempt with an unexpired lease. | Progress while Postgres is down (workers retry; nothing advances). |
| **Exactly-once transactional effects.** Writes made in a handler's `CommitFunc` commit iff that attempt's completion commits. | Survival of permanent database loss (no replication; out of scope). |
| **Bounded attempts.** No job is *claimed for execution* more than `max_attempts` times. | |

Three different events are easy to conflate, so the code keeps them separate:

- **Execution:** the handler runs. This can happen more than once: after a crash, a lease loss, or a kill in the middle of a commit.
- **Acknowledgment:** the fenced terminal transition commits. This happens at most once per job.
- **Effect:** a durable change to the outside world. If the effect is written through `CommitFunc`, it shares the acknowledgment's transaction, so it also happens at most once and only if the acknowledgment happens. Any other effect inherits at-least-once and must be idempotent on the receiving side, for example by keying on `(job_id)`. The attempt number can serve as a fencing token there, but only if the external system checks it.

"Duplicate processing" is therefore measured precisely. Abandoned attempts (re-executions) are expected and reported as a metric. Duplicate *acknowledgments* or *transactional effects* are bugs, and invariants I2, I5 and I7 detect them.

## 2. State machine

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

`running → running` also happens: the claim query takes an expired `running` row directly and increments `attempt`. A table CHECK enforces `state = 'running'` ⇔ lease fields are set, so a transition that forgets to clear or set the lease fails loudly.

## 3. Leases, not long transactions (D2)

A worker could hold `SELECT … FOR UPDATE` open for the job's whole duration. Then a crash would release the lock automatically, and no lease logic would be needed. DurableQ doesn't do that, for these reasons:

- It pins one connection and one open transaction per running job. A pool of 20 connections would cap concurrency at 20, and five-minute jobs would mean five-minute transactions. Those block vacuum and bloat every table.
- Network partitions still need a timeout. A half-open TCP connection keeps the lock until the kernel's keepalive gives up, which can take minutes.
- Handler work, often an external call, would happen inside a database transaction.

Instead, claiming is a short transaction that writes ownership into the row: `lease_owner`, `lease_expires_at`, and `attempt` as the fencing token. The tradeoff is that DurableQ has to implement expiry, renewal and fencing itself. Sections 4–6 cover those.

## 4. SQL contracts

All SQL lives in `queries.go`. Every time comparison uses **`clock_timestamp()`**, the database clock at the moment the statement runs. `now()` is frozen at transaction start, so inside a long transaction it would compare leases against a stale time (D8). Using the database clock everywhere means worker clocks can't skew lease decisions. The worker uses its own clock only for the conservative local deadline in §5.

**Enqueue:** `INSERT … ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING RETURNING id`. If no row comes back, the key already existed, so the code selects the existing id. When two enqueues with the same key race, the second waits on the unique index until the first commits, then gets its row. Keys deduplicate forever (D7). `EnqueueTx` runs the same statement in the caller's transaction, which gives the transactional-outbox pattern. Each duplicate insert consumes an identity value, so ids can have gaps.

**Claim:** one statement made of three CTEs.
1. `next` selects due rows, either `queued` with `run_at <= now` or `running` with an expired lease. It locks them with `FOR UPDATE SKIP LOCKED`, so concurrent claimers split the rows instead of queueing behind each other.
2. `claimed` sets state `running`, increments `attempt`, and sets the lease.
3. `hist` inserts one `job_attempts` row per claim.

The whole statement is atomic. If a claimer crashes before COMMIT, the row is unchanged and nothing leaks. No separate reaper process exists (D3): expired leases are reclaimed by the same query that claims new work.

**Fenced transitions:** heartbeat, complete, fail and release all end in

```sql
WHERE id = $1 AND attempt = $2 AND state = 'running' AND lease_expires_at > clock_timestamp()
```

Zero affected rows means the caller no longer owns the job. The worker then rolls back and counts a *stale transition rejected*.

**Complete:** one transaction, and the **fenced UPDATE runs first**. It takes the row lock, and a reclaimer's `SKIP LOCKED` scan can't take a locked row, so ownership can't change for the rest of the transaction. Next the handler's `CommitFunc` runs (writing `effects` in the demo), then the attempt row is closed, then COMMIT. If the process dies anywhere before COMMIT, the whole thing rolls back, and the lease later expires and the job is reclaimed. If COMMIT itself fails ambiguously (the connection drops mid-commit), the worker records a failure, which is also fenced. If the commit did land, that failure matches 0 rows and is rejected, so neither outcome can produce a second terminal transition.

**Fail:** the same fence. In one statement it moves to `dead` if `attempt >= max_attempts` or the error is `Permanent` (D6), and otherwise to `queued` with `run_at = clock_timestamp() + backoff`. Backoff is full jitter, `rand[0, min(cap, base·2^(attempt−1))]` with base 1s and cap 10m. Jitter keeps a batch of failures from all retrying at the same moment.

**Release (graceful shutdown):** the same fence, with `queued` and `run_at = now`. The attempt stays consumed (D5).

### Lock-wait subtlety (from the independent review)

A fenced UPDATE evaluates its WHERE clause, including `clock_timestamp()`, when it reaches the row. If the row is locked, the UPDATE then waits. After the wait, Postgres re-checks the clause only if the row was *modified* in the meantime (EvalPlanQual). In theory, then, a lease can expire while the transition waits and the transition still commits.

This can't violate a guarantee. Every way to take ownership away is a claim, and a claim modifies the row by incrementing `attempt`, so the re-check runs against the new attempt and fails. If the row was not modified, nobody else claimed the job, so the late transition is still by the only owner the job ever had in that window. Within DurableQ, every session that locks a job row also modifies it, so this needs an *external* session holding `FOR UPDATE` on a job row. We document it rather than spend an extra round trip on every transition.

## 5. Worker

```
poll loop:  free slots → claim(min(free, batch)); nothing due → sleep(poll ± 20%), or until a slot frees
per job:    ctx = timeout(JobTimeout), cancelled on lease loss or shutdown
            heartbeat goroutine every Heartbeat
            handler(ctx, job) → (CommitFunc, err)
            lease lost → drop silently (no transition; the new owner decides)
            err == nil → complete(CommitFunc)      err != nil → fail(err)      shutdown → release
```

**Local lease deadline.** A worker that can't reach the database can't read the database clock either. It therefore keeps its own deadline on Go's monotonic clock: `deadline = send time of the last successful heartbeat + Lease − Heartbeat`. The send time is used rather than the reply time because the database extended the lease no earlier than the send. When the deadline passes, the job's context is cancelled. This works even while the database is unreachable, and even right after the process resumes from a stall (a SIGSTOP or GC pause), because the monotonic clock kept advancing. `Heartbeat` must be less than `Lease/2`; otherwise the deadline would pass before the next renewal is even sent. `NewWorker` and the CLI reject such configurations.

**Handlers that ignore cancellation.** The runner waits for whichever comes first: the handler's result or the context finishing. If the context finishes first, the runner stops heartbeating, records the transition (release on shutdown, fail on timeout, nothing on lease loss), and discards the handler's late result. Shutdown therefore finishes within `ShutdownGrace` plus one release round trip, even if a handler never returns. That handler's goroutine keeps running until it returns, and any external effects it produces fall under at-least-once.

**Attempt budget (D4/D5).** `attempt` increments at claim time, so a job that crashes its worker every time still uses up attempts. When a claim returns `attempt > max_attempts`, the previous attempt was abandoned (a crash or a lost lease). The worker moves the job to `dead` without running the handler. `attempt` is never decremented (D11), because decrementing would make an old fencing token valid again.

**Identity.** Worker ids look like `durableq-worker-<host>-<pid>-<random>`. The id is also the Postgres `application_name`, which lets the chaos harness find and terminate a worker's connections.

## 6. Failure model and what each failure does

| Failure | Effect |
|---|---|
| SIGKILL after claim / during handler / before complete | No transition. The lease expires and the job is reclaimed with `attempt+1`. The handler may re-run (at-least-once). |
| SIGKILL inside the completion transaction | Postgres aborts the transaction, so the effect and the completion both roll back. Then the job is reclaimed as above. |
| SIGKILL after COMMIT | The job is already `succeeded`. Nothing is left to redo. |
| Connection terminated (`pg_terminate_backend`) | The in-flight statement fails. A failed heartbeat retries on a new pooled connection. A failed transition is fenced, so retrying or reclaiming is safe. |
| Worker stalls past its lease (SIGSTOP, GC pause, network stall) | Another worker reclaims. On resume, the stalled worker either sees its local deadline passed and drops the job, or attempts a completion that the fence rejects. |
| SIGTERM | The worker stops claiming, waits up to the grace period, releases unfinished jobs (immediately claimable), and exits. |
| Postgres down | Nothing progresses. Workers keep retrying. Committed state is durable. |

## 7. Verification

- **Integration tests on real Postgres.** Each test gets an isolated schema, and there are no mocks. They cover:
  - every PRD M1/M2 acceptance criterion;
  - a 1,000-iteration race between an owner completing near expiry and a reclaimer, where most iterations produce a contested completion that gets rejected;
  - a TCP proxy that cuts the database connection, to test the local deadline.
- **Chaos (`go test -tags chaos ./chaos/...`).** Real worker processes, random SIGKILL, `pg_terminate_backend`, SIGSTOP stalls, and five in-process failpoints (`after_claim`, `during_run`, `before_complete`, `in_complete_tx`, `after_commit`). Each failpoint is also run in isolation at high probability. Once the queue drains, invariants I1–I7 are checked in SQL.
- **The checker can catch real bugs.** Removing the `attempt` check from completion makes I2/I5 fail (see [results.md](results.md)). `effects` deliberately has no unique constraint on `job_id`, because a constraint would hide exactly this kind of bug.

## 8. Known limitations

- Idempotency keys never expire. Retention and cleanup of finished jobs are not implemented, so `jobs` and `job_attempts` grow without bound.
- Workers poll; there is no LISTEN/NOTIFY. Idle queue latency is up to `Poll` (default 1s).
- The claim query's `OR` (queued-and-due vs. running-and-expired) is served by two partial indexes. Its cost under contention is measured in results.md.
- Ordering is best-effort.
- The chaos seed controls the harness's fault schedule. Workers' own randomness (failure rate, failpoints, jitter) is not seeded, so a seed reproduces the fault pattern, not the exact interleaving.
- A handler that ignores its context can keep running in the background after the worker has given up on it.
