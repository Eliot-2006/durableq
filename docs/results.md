# Results

All numbers below were measured, not estimated. Raw CSVs are in [`bench/results/20261106T050117Z/`](../bench/results/20261106T050117Z/), and `env.txt` there records the environment.

## Setup

| | |
|---|---|
| Machine | Apple M3 Pro, 11 CPUs, 18 GiB RAM, macOS (darwin/arm64), laptop on AC power |
| PostgreSQL | 17.11 (Homebrew), on the same machine as the workers. Default settings: `shared_buffers=160MB`, `max_connections=100`, `synchronous_commit=on`, `fsync=on` |
| Go | 1.27.3 |
| Code | git `8fa4341` (clean tree) |
| Workers | Separate OS processes (`durableq worker`), `--poll 100ms`. Pool size per process = `min(concurrency+2, 80/procs)` |

**Reproduce:** `DATABASE_URL=… go run ./cmd/durableq bench` (defaults shown here), on an otherwise idle database. Each benchmark runs in its own fresh schema. The connection and wait-event sampling matches every `durableq-worker-%` session in the database, so concurrent runs would pollute each other.

**Caveats:** this is a single laptop, with Postgres and workers sharing CPU, and none of it is a dedicated server. `synchronous_commit=on` means every job transition waits for a WAL flush to the laptop SSD. One run per cell, without repetitions or confidence intervals. Treat the figures as orders of magnitude and relative comparisons, not capacity planning numbers.

## Throughput

The `demo.noop` handler on a pre-enqueued backlog of 20,000 jobs. Throughput is `jobs / (last finished_at − first claimed_at)`, both from the database, so process startup is excluded. Each job costs at least two committed transactions: the claim (batched, up to 10 per statement) and the completion.

| procs × concurrency | pool/proc | jobs/s | peak worker conns | top wait events (sampled every 100ms) |
|---|---|---:|---:|---|
| 1 × 1 | 3 | 398 | 1 | running 62%, ClientRead 30%, WalWrite 8% |
| 1 × 10 | 12 | 2,423 | 10 | ClientRead 81%, running 15% |
| 1 × 50 | 52 | 4,514 | 21 | ClientRead 86%, running 10% |
| 2 × 1 | 3 | 770 | 2 | running 57%, ClientRead 35% |
| 2 × 10 | 12 | 4,848 | 20 | ClientRead 80%, running 15% |
| 2 × 50 | 40 | 8,623 | 80 | ClientRead 92%, running 5% |
| 4 × 1 | 3 | 1,577 | 4 | running 56%, ClientRead 32% |
| 4 × 10 | 12 | 7,710 | 40 | ClientRead 78%, running 15% |
| 4 × 50 | 20 | 10,523 | 80 | ClientRead 83%, LWLock:BufferContent 5% |
| 8 × 1 | 3 | 2,093 | 8 | running 72%, ClientRead 12%, LWLock:WALWrite 6% |
| 8 × 10 | 10 | 8,701 | 80 | ClientRead 82%, LWLock:BufferContent 3% |
| 8 × 50 | 10 | 10,946 | 80 | ClientRead 60%, LWLock:BufferContent 10% |

What the numbers show:
- **Single-claimer throughput is bound by commit latency.** At 1 × 1, each job is a serial sequence of round trips plus WAL flushes, at about 2.5ms per job. Adding single-concurrency processes scales linearly up to 4 (398 → 770 → 1,577/s). Beyond that, scaling is sublinear: 8 × 1 reaches 2,093/s, and 4 × 10 (40 concurrent) reaches 7,710/s, about 19× the single claimer for 40× the concurrency.
- **It plateaus around 10–11k jobs/s from 200 concurrent jobs upward,** at the 80-connection budget. Beyond that point, more concurrency doesn't help: 4 × 50 and 8 × 50 are within 4% of each other.
- **`SKIP LOCKED` keeps claimers from serializing.** Even at 400 concurrent jobs there is no heavyweight `Lock:tuple` or `Lock:transactionid` wait in the top three. The contention that does appear is `LWLock:BufferContent`, which grows from 3% to 10% between 8 × 10 and 8 × 50. That is many backends touching the same few heap and index pages at the head of the queue. This is the first scalability limit, and it's the one the PRD's "OR in the claim query" risk anticipates. We have not split the claim query, because at this scale the measured cost is still small.
- `ClientRead` (a backend waiting for the client's next statement) dominates every cell from 10 concurrency upward. Much of the time goes to round trips and the Go side, not to Postgres work. Its share drops to 60% at 8 × 50 as `BufferContent` contention rises. We didn't profile CPU split between processes, so we can't say which side saturates first on this laptop.

**Table bloat.** `n_dead_tup` on `jobs` after a run is 40,000 for 20,000 jobs in 10 of 12 cells. That's exactly two dead tuples per job (the claim UPDATE and the completion UPDATE). In the two longest runs (1 × 1 at 50s and 2 × 1 at 26s), autovacuum ran mid-run and the counts were 26,549 and 20,362. The schema doesn't tune autovacuum per table, and there is no retention, which is a stretch goal. A long-lived deployment would need both.

## Queue latency

Open-loop arrivals: 200 jobs/s for 10s (2,000 jobs), with 4 processes × 10, poll 100ms. Latency is `job_attempts.claimed_at − jobs.run_at` for first attempts, measured on the database clock.

| p50 | p95 | p99 |
|---:|---:|---:|
| 21.5 ms | 70.4 ms | 83.4 ms |

Polling sets these numbers. An idle worker sleeps `poll ± 20%`, so a job that arrives while every worker sleeps waits up to about 120ms. Four workers polling independently cut the expected wait. `LISTEN/NOTIFY` (stretch) would remove most of it.

## Crash recovery

20 long `demo.effect` jobs are held by one worker (lease 2s, heartbeat 500ms). It is SIGKILLed, and a fresh worker starts. Recovery is the time from the kill (database clock, read right after the process is reaped) to each job's next `claimed_at`.

| jobs | lease | p50 | max |
|---:|---:|---:|---:|
| 20 | 2s | 1,992 ms | 1,994 ms |

Recovery is bounded by the lease, as designed: the lease runs from the last successful heartbeat, which was up to 500ms *before* the kill, so the remaining lease is at most 2s. The reclaiming worker then has its next 100ms poll on top. This lands just under one lease length. The lease is the main setting for trading recovery speed against the heartbeat load and false expiries under stalls.

## Chaos reliability

| Run | Jobs | Faults per run | Result |
|---|---:|---|---|
| `bench` (seeds 1–5) | 1,000 | 20 SIGKILL, 5 conn-kill rounds, 2 stalls, all failpoints at 0.2% | **5/5 passed** I1–I7 |
| M3 acceptance (seeds 1000–1019) | 10,000 | 100 SIGKILL, 92–116 failpoint self-kills, 20 conn-kill rounds (205–396 connections), 5 stalls | **20/20 passed** I1–I7 |
| `durableq demo --seed 7` (README) | 10,000 | 120 SIGKILL, 109 self-kills, 30 conn-kill rounds, 5 stalls | passed |

Across the 20 acceptance runs:
- 40,428 attempts were abandoned (at-least-once re-executions on 33,466 jobs), 41 jobs went dead, and 199,959 succeeded.
- Duplicate effects: 0. Duplicate terminal transitions: 0. Lost jobs: 0.
- Drain time was 24.5–34.8s.

Reproduce with:

```sh
go test -tags chaos ./chaos/ -run 'TestChaos$' -timeout 0 -v -args \
  -jobs 10000 -kills 100 -conn-kills 20 -stalls 5 -sleep-ms 100 -seeds 20 -seed 1000
```

The seed fixes the harness's fault schedule. Worker-side randomness (failure rate, failpoints, jitter) is not seeded.

## The checker catches a fencing bug

To prove the invariants can fail, we temporarily removed `AND attempt = $2` from the completion's WHERE clause, keeping the state and lease checks. Then we ran a workload where a stalled worker's jobs outlast its stall: 600 jobs of 1–3s each, 4 workers, 15 SIGSTOPs of 3s with a 2s lease, and no other faults.

| Build | Seed | Result |
|---|---|---|
| Bug injected | 4242 | **I5 FAIL (8 violations)**: effects written by a stale attempt |
| Bug injected | 4243 | **I5 FAIL (4 violations)** |
| Bug injected | `TestStalledWorkerFenced` | **I5 FAIL (11 violations)** |
| Correct | 4242 | PASS: 19 stale completions rejected by the fence |

In this workload, the stale worker's completion overwrites the job while the new owner is still running. The new owner's own completion then fails the state check, so exactly one effect remains (I2 and I3 still pass). But that effect carries the stale attempt number, and I5 catches it. This is why `effects` has no unique constraint on `job_id`, and why I5 compares attempt numbers instead of only counting rows.

The first attempt at this experiment used 300ms jobs and **did not** catch the bug. New owners always finished before stalled workers resumed, so the state check alone rejected every stale completion. The longer-job workload is now permanent in `TestStalledWorkerFenced`, which fails with the bug and passes without it. The code was reverted, and `queries.go` matches the commit.
