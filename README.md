# durableq

[![ci](https://github.com/Eliot-2006/durableq/actions/workflows/ci.yml/badge.svg)](https://github.com/Eliot-2006/durableq/actions/workflows/ci.yml)

A durable job queue in Go with PostgreSQL as its only dependency. Workers claim jobs with `FOR UPDATE SKIP LOCKED` and hold leases timed on the database clock, renewing them with heartbeats. When a worker crashes, its leases expire and other workers reclaim the jobs. Every state transition is fenced by an attempt number, so a stale or partitioned worker cannot complete a job it no longer owns. Correctness is demonstrated rather than asserted: chaos tests SIGKILL real worker processes, restart them gracefully mid-run, terminate their database connections and freeze them past their leases. Then seven invariants are checked directly in SQL.

**Results** (laptop, [details](docs/results.md)): about 11,000 jobs/s at peak. A crashed worker's jobs are back in about 2s, one lease. 20 of 20 chaos runs over 200,000 jobs (100 SIGKILLs each, plus connection kills, stalls and restarts) passed with 0 lost jobs and 0 duplicate effects. Postgres crash-restarts mid-run pass too.

## Guarantees

- **At-least-once execution.** Every committed job runs until it succeeds or is dead-lettered.
- **At most one terminal transition per job.** A job reaches `succeeded` or `dead` at most once, never both and never twice.
- **Fenced transitions.** Heartbeat, complete, fail and release succeed only for the current attempt, with a lease that is unexpired by the database clock.
- **Exactly-once transactional effects.** Writes made in a handler's `CommitFunc` commit if and only if that attempt's completion commits.
- **Bounded attempts.** A job's handler runs at most `max_attempts` times, plus once per graceful release (a deploy never dead-letters a job). A job that keeps crashing its worker still ends up `dead`. So, by design, can a healthy job whose workers crash `max_attempts` times in a row.

**Not guaranteed:** exactly-once *external* side effects. HTTP calls and emails may happen more than once, so handlers must make them idempotent. Ordering is also not guaranteed. See [docs/design.md](docs/design.md) for the reasoning behind each guarantee and its limits.

## Quickstart

Requires Go 1.27+ and PostgreSQL 17 (Docker Compose, or a local install).

```sh
make up                                   # Postgres 17 in Docker (or use your own and set DATABASE_URL)
                                          # 5432 taken? DURABLEQ_PG_PORT=55432 make up, and use :55432 below
export DATABASE_URL=postgres://durableq:durableq@localhost:5432/durableq?sslmode=disable
go run ./cmd/durableq migrate
go run ./cmd/durableq enqueue --kind demo.effect --payload '{"sleep_ms":100,"fail_rate":0.2}' --count 1000
go run ./cmd/durableq worker --concurrency 20      # Ctrl-C releases in-flight jobs and exits
go run ./cmd/durableq status                       # counts by state;  status <id> shows attempt history
go run ./cmd/durableq check                        # invariants I1–I7 (exit 1 on violation)
```

### Library

```go
client := durableq.NewClient(pool)
client.Enqueue(ctx, durableq.EnqueueParams{Kind: "email.send", Payload: payload, IdempotencyKey: "order-42"})
client.EnqueueTx(ctx, tx, params) // atomic with your own writes (transactional outbox)

w := durableq.NewWorker(pool, durableq.WorkerConfig{Concurrency: 10})
w.Handle("email.send", func(ctx context.Context, job durableq.Job) (durableq.CommitFunc, error) {
	if err := send(ctx, job.Payload); err != nil { // external effect: may repeat, so make it idempotent
		return nil, err // retried with exponential backoff; durableq.Permanent(err) dead-letters
	}
	return func(ctx context.Context, tx pgx.Tx) error { // runs in the fenced completion transaction
		_, err := tx.Exec(ctx, `UPDATE orders SET emailed = true WHERE id = $1`, orderID(job))
		return err
	}, nil
})
w.Run(ctx) // until ctx is cancelled; then drains for ShutdownGrace and releases the rest
```

## Chaos demo

```sh
docker compose run --rm demo            # Postgres + 10,000 jobs, 6 workers, 120 SIGKILLs
# port 5432 already taken by a local Postgres?  DURABLEQ_PG_PORT=55432 docker compose run --rm demo
# or, against your own Postgres:
go run ./cmd/durableq demo --jobs 10000 --workers 6 --kills 120 --seed 7
```

Real output (`durableq demo --seed 7`, Apple M3 Pro, PostgreSQL 17.11):

```
Enqueued 10000 jobs · 6 workers × 10 · lease 2s · failpoints "after_claim=0.002,during_run=0.002,before_complete=0.002,in_complete_tx=0.002,after_commit=0.002"
[00:01] SIGKILL worker-4 (pid 66498) … respawned
[00:01] SIGKILL worker-5 (pid 66441) … respawned
[00:01] SIGKILL worker-5 (pid 66548) … respawned
[00:01] SIGKILL worker-3 (pid 66500) … respawned
[00:01] SIGKILL worker-2 (pid 66438) … respawned
…
Drained in 40.4s (seed 7, schema chaos_7_1793947041373928000)
  Jobs succeeded       9996     dead 4 (2 after a failed final attempt, 2 with the budget exhausted by abandoned attempts)
  Worker kills         120      self-kills (failpoints) 103
  Connection kills     30       (578 connections terminated)
  Worker stalls        5        graceful restarts 10 (jobs released 74)
  Abandoned attempts   2346     jobs possibly executed more than once 1941
  Retries              622      stale transitions rejected 24
  Leases lost (worker abandoned job on its local deadline) 23
  I1 PASS no lost jobs: every job is terminal
  I2 PASS no duplicate effects
  I3 PASS every succeeded demo.effect job has exactly one effect, and every effect's job succeeded
  I4 PASS dead jobs have no effects
  I5 PASS each effect was written by the job's final owner
  I6 PASS attempt budget respected
  I7 PASS at most one terminal attempt per job
All 7 invariants PASSED
```

"Jobs possibly executed more than once" counts at-least-once re-executions. Those are expected. Duplicate *effects* or acknowledgments would be bugs, and I2, I5 and I7 catch them. To show the checker works, removing the attempt check from completion makes I5 fail (see [results.md](docs/results.md#the-checker-catches-a-fencing-bug)).

## Tests

```sh
go test -race ./...                       # unit + integration (real Postgres via DATABASE_URL)
go test -tags chaos ./chaos/...           # chaos: SIGKILL, SIGTERM restarts, pg_terminate_backend, SIGSTOP, all failpoints
DURABLEQ_PG_RESTART_CMD='docker restart -t 0 <postgres-container>' \
  go test -tags chaos ./chaos/ -run TestPostgresRestart   # also crash-restart Postgres mid-run
```

## Docs

- [docs/PRD.md](docs/PRD.md): requirements and decisions
- [docs/design.md](docs/design.md): state machine, SQL contracts, lease and fencing reasoning, failure model, limitations
- [docs/results.md](docs/results.md): benchmarks and chaos results, with hardware and reproduction commands

## License

MIT. See [LICENSE](LICENSE).
