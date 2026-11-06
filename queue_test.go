package durableq_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Eliot-2006/durableq"
	"github.com/Eliot-2006/durableq/internal/testdb"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func noop(context.Context, durableq.Job) (durableq.CommitFunc, error) { return nil, nil }

// startWorkers runs n workers until the test ends.
func startWorkers(t *testing.T, pool *pgxpool.Pool, n int, cfg durableq.WorkerConfig, kind string, h durableq.HandlerFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	t.Cleanup(func() { cancel(); wg.Wait() })
	if cfg.Poll == 0 {
		cfg.Poll = 20 * time.Millisecond
	}
	cfg.Logger = quiet
	for range n {
		w := durableq.NewWorker(pool, cfg)
		w.Handle(kind, h)
		wg.Go(func() { w.Run(ctx) })
	}
}

// drain waits until no job is queued or running.
func drain(t *testing.T, pool *pgxpool.Pool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		var n int
		if err := pool.QueryRow(context.Background(),
			`SELECT count(*) FROM jobs WHERE state IN ('queued','running')`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("queue not drained after %v: %d jobs pending", timeout, n)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func count(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestEnqueueValidation(t *testing.T) {
	c := durableq.NewClient(nil) // validation fails before touching the pool
	for _, p := range []durableq.EnqueueParams{
		{},
		{Kind: "k", Payload: []byte("{bad")},
		{Kind: "k", MaxAttempts: -1},
		{Kind: "k", Delay: -time.Second},
	} {
		if _, err := c.Enqueue(context.Background(), p); err == nil {
			t.Errorf("expected error for %+v", p)
		}
	}
}

func TestIdempotencyKeyConcurrent(t *testing.T) {
	pool := testdb.New(t, durableq.Migrations)
	c := durableq.NewClient(pool)
	var wg sync.WaitGroup
	ids := make(chan int64, 20)
	for range 20 {
		wg.Go(func() {
			r, err := c.Enqueue(context.Background(), durableq.EnqueueParams{Kind: "k", IdempotencyKey: "same"})
			if err != nil {
				t.Error(err)
				return
			}
			ids <- r.ID
		})
	}
	wg.Wait()
	close(ids)
	seen := map[int64]bool{}
	for id := range ids {
		seen[id] = true
	}
	if len(seen) != 1 || count(t, pool, `SELECT count(*) FROM jobs`) != 1 {
		t.Fatalf("expected exactly one job, got ids %v", seen)
	}
	r, err := c.Enqueue(context.Background(), durableq.EnqueueParams{Kind: "k", IdempotencyKey: "same"})
	if err != nil || !r.Existed {
		t.Fatalf("second enqueue: existed=%v err=%v", r.Existed, err)
	}
}

func TestEnqueueTxRollbackNeverRuns(t *testing.T) {
	pool := testdb.New(t, durableq.Migrations)
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := durableq.NewClient(pool).EnqueueTx(ctx, tx, durableq.EnqueueParams{Kind: "k"}); err != nil {
		t.Fatal(err)
	}
	tx.Rollback(ctx)

	startWorkers(t, pool, 1, durableq.WorkerConfig{}, "k", func(context.Context, durableq.Job) (durableq.CommitFunc, error) {
		t.Error("rolled-back job ran")
		return nil, nil
	})
	time.Sleep(200 * time.Millisecond)
	if n := count(t, pool, `SELECT count(*) FROM jobs`); n != 0 {
		t.Fatalf("rolled-back job exists: %d", n)
	}
}

func TestEveryJobClaimedExactlyOnce(t *testing.T) {
	pool := testdb.New(t, durableq.Migrations)
	ctx := context.Background()
	const n = 10_000
	if _, err := pool.Exec(ctx, `INSERT INTO jobs (kind) SELECT 'noop' FROM generate_series(1, $1)`, n); err != nil {
		t.Fatal(err)
	}
	// 5 workers × concurrency 10 = 50 concurrent claimers.
	startWorkers(t, pool, 5, durableq.WorkerConfig{Concurrency: 10, BatchSize: 10}, "noop", noop)
	drain(t, pool, 60*time.Second)

	if got := count(t, pool, `SELECT count(*) FROM job_attempts`); got != n {
		t.Fatalf("attempt rows = %d, want %d (a job was claimed twice or not at all)", got, n)
	}
	if got := count(t, pool, `SELECT count(*) FROM jobs WHERE state = 'succeeded' AND attempt = 1`); got != n {
		t.Fatalf("succeeded on first attempt = %d, want %d", got, n)
	}
}

func TestAlwaysFailingJobDeadAfterMaxAttempts(t *testing.T) {
	pool := testdb.New(t, durableq.Migrations)
	const gap = 100 * time.Millisecond
	r, err := durableq.NewClient(pool).Enqueue(context.Background(), durableq.EnqueueParams{Kind: "fail", MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	startWorkers(t, pool, 2, durableq.WorkerConfig{Backoff: func(int) time.Duration { return gap }}, "fail",
		func(context.Context, durableq.Job) (durableq.CommitFunc, error) { return nil, errors.New("boom") })
	drain(t, pool, 10*time.Second)

	if s := count(t, pool, `SELECT count(*) FROM jobs WHERE id = $1 AND state = 'dead' AND attempt = 3 AND last_error = 'boom'`, r.ID); s != 1 {
		t.Fatal("job not dead after exactly 3 attempts")
	}
	if n := count(t, pool, `SELECT count(*) FROM job_attempts WHERE job_id = $1`, r.ID); n != 3 {
		t.Fatalf("attempts = %d, want 3", n)
	}
	// Each retry was claimed no earlier than backoff after the previous failure.
	short := count(t, pool, `
		SELECT count(*) FROM (
		  SELECT claimed_at - lag(finished_at) OVER (ORDER BY attempt) AS g FROM job_attempts WHERE job_id = $1
		) x WHERE g < $2 * interval '1 microsecond'`, r.ID, gap.Microseconds())
	if short != 0 {
		t.Fatalf("%d retries ran before the backoff elapsed", short)
	}
}

func TestPermanentErrorDeadAfterOneAttempt(t *testing.T) {
	pool := testdb.New(t, durableq.Migrations)
	if _, err := durableq.NewClient(pool).Enqueue(context.Background(), durableq.EnqueueParams{Kind: "perm"}); err != nil {
		t.Fatal(err)
	}
	startWorkers(t, pool, 1, durableq.WorkerConfig{}, "perm",
		func(context.Context, durableq.Job) (durableq.CommitFunc, error) {
			return nil, durableq.Permanent(errors.New("bad payload"))
		})
	drain(t, pool, 5*time.Second)
	if n := count(t, pool, `SELECT count(*) FROM jobs WHERE state = 'dead' AND attempt = 1`); n != 1 {
		t.Fatal("permanent failure was retried")
	}
}

func TestFutureJobNotClaimedEarly(t *testing.T) {
	pool := testdb.New(t, durableq.Migrations)
	c := durableq.NewClient(pool)
	if _, err := c.Enqueue(context.Background(), durableq.EnqueueParams{Kind: "noop", Delay: time.Hour}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(context.Background(), durableq.EnqueueParams{Kind: "noop", Delay: 300 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	startWorkers(t, pool, 1, durableq.WorkerConfig{}, "noop", noop)
	time.Sleep(150 * time.Millisecond)
	if n := count(t, pool, `SELECT count(*) FROM job_attempts`); n != 0 {
		t.Fatalf("delayed job claimed early (%d attempts)", n)
	}
	time.Sleep(500 * time.Millisecond)
	if n := count(t, pool, `SELECT count(*) FROM jobs WHERE state = 'succeeded'`); n != 1 {
		t.Fatalf("succeeded = %d, want only the 300ms job", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM jobs WHERE state = 'queued'`); n != 1 {
		t.Fatal("1h job should still be queued")
	}
}

func TestCommitFuncAtomicWithCompletion(t *testing.T) {
	pool := testdb.New(t, durableq.Migrations)
	c := durableq.NewClient(pool)
	for i := range 20 {
		if _, err := c.Enqueue(context.Background(), durableq.EnqueueParams{Kind: "eff", Payload: []byte(fmt.Sprint(i))}); err != nil {
			t.Fatal(err)
		}
	}
	startWorkers(t, pool, 2, durableq.WorkerConfig{}, "eff", func(_ context.Context, j durableq.Job) (durableq.CommitFunc, error) {
		return func(ctx context.Context, tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `INSERT INTO effects (job_id, attempt) VALUES ($1, $2)`, j.ID, j.Attempt)
			return err
		}, nil
	})
	drain(t, pool, 10*time.Second)
	if n := count(t, pool, `SELECT count(*) FROM effects e JOIN jobs j ON j.id = e.job_id AND j.attempt = e.attempt AND j.state = 'succeeded'`); n != 20 {
		t.Fatalf("effects = %d, want 20", n)
	}
}
