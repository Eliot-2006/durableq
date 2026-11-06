package durableq

// Internal tests for leases, fencing, and recovery (PRD §14 M2). They drive
// claim/run/transitions directly so each interleaving is deterministic.

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Eliot-2006/durableq/internal/testdb"
)

var quietLog = slog.New(slog.NewTextHandler(io.Discard, nil))

func newTestWorker(pool *pgxpool.Pool, cfg WorkerConfig) *Worker {
	cfg.Logger = quietLog
	if cfg.Poll == 0 {
		cfg.Poll = 20 * time.Millisecond
	}
	return NewWorker(pool, cfg)
}

func mustExec(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatal(err)
	}
}

func scalar[T any](t *testing.T, pool *pgxpool.Pool, sql string, args ...any) T {
	t.Helper()
	var v T
	if err := pool.QueryRow(context.Background(), sql, args...).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func claimOne(t *testing.T, w *Worker) Job {
	t.Helper()
	jobs, err := w.claim(context.Background(), 1)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("claim: got %d jobs, err %v", len(jobs), err)
	}
	return jobs[0]
}

func expireLeases(t *testing.T, pool *pgxpool.Pool) {
	mustExec(t, pool, `UPDATE jobs SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE state = 'running'`)
}

func effectCommit(j Job) CommitFunc {
	return func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO effects (job_id, attempt) VALUES ($1, $2)`, j.ID, j.Attempt)
		return err
	}
}

func enqueueOne(t *testing.T, pool *pgxpool.Pool, maxAttempts int) int64 {
	t.Helper()
	r, err := NewClient(pool).Enqueue(context.Background(), EnqueueParams{Kind: "k", MaxAttempts: maxAttempts})
	if err != nil {
		t.Fatal(err)
	}
	return r.ID
}

func TestExpiredLeaseReclaimedAndStaleWorkerFenced(t *testing.T) {
	pool := testdb.New(t, Migrations)
	id := enqueueOne(t, pool, 5)
	stale := newTestWorker(pool, WorkerConfig{ID: "stale"})
	fresh := newTestWorker(pool, WorkerConfig{ID: "fresh"})

	j1 := claimOne(t, stale)
	expireLeases(t, pool)

	// Expired but not yet reclaimed: the database clock already rejects the stale owner.
	if tag, _ := pool.Exec(context.Background(), qHeartbeat, id, j1.Attempt, int64(1e6)); tag.RowsAffected() != 0 {
		t.Fatal("heartbeat on an expired lease succeeded")
	}

	j2 := claimOne(t, fresh)
	if j2.ID != id || j2.Attempt != j1.Attempt+1 {
		t.Fatalf("reclaim: got job %d attempt %d, want job %d attempt %d", j2.ID, j2.Attempt, id, j1.Attempt+1)
	}
	if owner := scalar[string](t, pool, `SELECT lease_owner FROM jobs WHERE id = $1`, id); owner != "fresh" {
		t.Fatalf("lease owner = %q", owner)
	}

	// Every transition from the stale attempt affects 0 rows, and its effect rolls back.
	if tag, _ := pool.Exec(context.Background(), qHeartbeat, id, j1.Attempt, int64(1e6)); tag.RowsAffected() != 0 {
		t.Fatal("stale heartbeat succeeded")
	}
	stale.complete(j1, effectCommit(j1), quietLog)
	stale.fail(j1, errors.New("x"), quietLog)
	stale.release(j1, quietLog)
	if got := stale.Stats().StaleRejected; got != 3 {
		t.Fatalf("stale rejections = %d, want 3", got)
	}
	if n := scalar[int](t, pool, `SELECT count(*) FROM effects`); n != 0 {
		t.Fatal("stale CommitFunc effect was committed")
	}
	if s := scalar[string](t, pool, `SELECT state::text FROM jobs WHERE id = $1`, id); s != "running" {
		t.Fatalf("stale transition changed state to %s", s)
	}

	// The new owner still completes normally, exactly once.
	fresh.complete(j2, effectCommit(j2), quietLog)
	if n := scalar[int](t, pool, `SELECT count(*) FROM effects WHERE attempt = $1`, j2.Attempt); n != 1 {
		t.Fatal("owner's completion did not write its effect")
	}
}

func TestHeartbeatKeepsLongJobAlive(t *testing.T) {
	pool := testdb.New(t, Migrations)
	id := enqueueOne(t, pool, 5)
	cfg := WorkerConfig{Lease: 300 * time.Millisecond, Heartbeat: 100 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	defer func() { cancel(); wg.Wait() }()
	for range 3 { // other workers are polling and would reclaim an expired lease
		w := newTestWorker(pool, cfg)
		w.Handle("k", func(ctx context.Context, j Job) (CommitFunc, error) {
			select { // 5 lease durations
			case <-time.After(1500 * time.Millisecond):
				return effectCommit(j), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		})
		wg.Go(func() { w.Run(ctx) })
	}
	deadline := time.Now().Add(5 * time.Second)
	for scalar[string](t, pool, `SELECT state::text FROM jobs WHERE id = $1`, id) != "succeeded" {
		if time.Now().After(deadline) {
			t.Fatal("job did not succeed")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if a := scalar[int](t, pool, `SELECT attempt FROM jobs WHERE id = $1`, id); a != 1 {
		t.Fatalf("job was reclaimed despite heartbeats (attempt %d)", a)
	}
}

// proxy forwards TCP to target until cut, after which it refuses connections:
// from the worker's point of view the database becomes unreachable.
type proxy struct {
	ln    net.Listener
	mu    sync.Mutex
	conns []net.Conn
}

func newProxy(t *testing.T, target string) *proxy {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &proxy{ln: ln}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			d, err := net.Dial("tcp", target)
			if err != nil {
				c.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, c, d)
			p.mu.Unlock()
			go func() { io.Copy(d, c); d.Close() }()
			go func() { io.Copy(c, d); c.Close() }()
		}
	}()
	t.Cleanup(p.cut)
	return p
}

func (p *proxy) cut() {
	p.ln.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		c.Close()
	}
}

func TestLocalDeadlineCancelsJobWhenDatabaseUnreachable(t *testing.T) {
	var px *proxy
	pool := testdb.NewWithConfig(t, Migrations, func(c *pgxpool.Config) {
		px = newProxy(t, net.JoinHostPort(c.ConnConfig.Host, strconv.Itoa(int(c.ConnConfig.Port))))
		addr := px.ln.Addr().(*net.TCPAddr)
		c.ConnConfig.Host, c.ConnConfig.Port = addr.IP.String(), uint16(addr.Port)
		c.ConnConfig.ConnectTimeout = 100 * time.Millisecond
	})
	enqueueOne(t, pool, 5)
	const lease, hb = 900 * time.Millisecond, 300 * time.Millisecond
	w := newTestWorker(pool, WorkerConfig{Lease: lease, Heartbeat: hb})
	started, cancelled := make(chan struct{}), make(chan time.Time, 1)
	w.Handle("k", func(ctx context.Context, j Job) (CommitFunc, error) {
		close(started)
		<-ctx.Done()
		cancelled <- time.Now()
		return nil, ctx.Err()
	})
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	defer func() { stop(); <-done }()

	<-started
	time.Sleep(hb + hb/2) // let at least one heartbeat succeed
	cutAt := time.Now()
	px.cut()

	select {
	case at := <-cancelled:
		// The last successful heartbeat was sent before the cut, so the local
		// deadline (send + lease - hb) is at most lease - hb after it.
		if el := at.Sub(cutAt); el > lease-hb+100*time.Millisecond {
			t.Fatalf("cancelled %v after cut; want <= %v", el, lease-hb)
		}
	case <-time.After(3 * lease):
		t.Fatal("job context not cancelled while database unreachable")
	}
	// The handler sees cancellation before the runner records the lease loss.
	for deadline := time.Now().Add(time.Second); w.Stats().LeaseLost == 0 && time.Now().Before(deadline); {
		time.Sleep(5 * time.Millisecond)
	}
	if got := w.Stats().LeaseLost; got != 1 {
		t.Fatalf("lease lost = %d, want 1", got)
	}
}

func TestCrashingJobDeadAfterBudgetWithoutExtraExecution(t *testing.T) {
	pool := testdb.New(t, Migrations)
	const max = 3
	id := enqueueOne(t, pool, max)
	executions := 0
	w := newTestWorker(pool, WorkerConfig{})
	w.Handle("k", func(context.Context, Job) (CommitFunc, error) {
		executions++
		return nil, nil
	})
	// Each attempt "kills its worker": claimed and executed, but no transition is recorded.
	for range max {
		claimOne(t, w)
		executions++
		expireLeases(t, pool)
	}
	j := claimOne(t, w)
	if j.Attempt != max+1 {
		t.Fatalf("attempt = %d", j.Attempt)
	}
	w.run(context.Background(), j, time.Now())
	if executions != max {
		t.Fatalf("handler ran beyond budget: %d executions", executions)
	}
	if s := scalar[string](t, pool, `SELECT state::text FROM jobs WHERE id = $1`, id); s != "dead" {
		t.Fatalf("state = %s, want dead", s)
	}
	if n := scalar[int](t, pool, `SELECT count(*) FROM job_attempts WHERE job_id = $1 AND outcome IS NOT NULL`, id); n > max {
		t.Fatalf("%d recorded outcomes exceed budget", n)
	}
}

func TestShutdownReleasesInFlightJobs(t *testing.T) {
	pool := testdb.New(t, Migrations)
	for range 3 {
		enqueueOne(t, pool, 5)
	}
	const grace = 300 * time.Millisecond
	w := newTestWorker(pool, WorkerConfig{ShutdownGrace: grace})
	var running atomic.Int32
	w.Handle("k", func(ctx context.Context, j Job) (CommitFunc, error) {
		running.Add(1)
		<-ctx.Done() // slow job: only stops when told to
		return nil, ctx.Err()
	})
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	for running.Load() < 3 {
		time.Sleep(10 * time.Millisecond)
	}
	stop()
	t0 := time.Now()
	<-done
	if el := time.Since(t0); el > grace+500*time.Millisecond {
		t.Fatalf("worker took %v to exit; grace %v", el, grace)
	}
	if n := scalar[int](t, pool, `SELECT count(*) FROM jobs WHERE state = 'queued' AND run_at <= clock_timestamp() AND lease_owner IS NULL`); n != 3 {
		t.Fatalf("released & immediately claimable = %d, want 3", n)
	}
	if n := scalar[int](t, pool, `SELECT count(*) FROM job_attempts WHERE outcome = 'released'`); n != 3 {
		t.Fatalf("released attempts = %d", n)
	}
	other := newTestWorker(pool, WorkerConfig{})
	if jobs, err := other.claim(context.Background(), 10); err != nil || len(jobs) != 3 {
		t.Fatalf("released jobs not claimable: %d, %v", len(jobs), err)
	}
}

// Races the owner's (optional) heartbeat + complete against a reclaim +
// complete by another worker, with the lease expiring at a random moment.
func TestHeartbeatVsReclaimRace(t *testing.T) {
	pool := testdb.New(t, Migrations)
	a := newTestWorker(pool, WorkerConfig{ID: "a", Lease: time.Second, Heartbeat: 100 * time.Millisecond})
	b := newTestWorker(pool, WorkerConfig{ID: "b", Lease: time.Second, Heartbeat: 100 * time.Millisecond})
	const iterations = 1000
	for range iterations {
		enqueueOne(t, pool, 10)
		j := claimOne(t, a)
		mustExec(t, pool, `UPDATE jobs SET lease_expires_at = clock_timestamp() + $2 * interval '1 microsecond' WHERE id = $1`,
			j.ID, rand.Int64N(3000))
		var wg sync.WaitGroup
		wg.Go(func() {
			// The owner may or may not get a heartbeat in, then finishes its
			// handler and completes, racing the lease expiry either way.
			time.Sleep(time.Duration(rand.Int64N(3000)) * time.Microsecond)
			if rand.IntN(2) == 0 {
				pool.Exec(context.Background(), qHeartbeat, j.ID, j.Attempt, int64(1e6))
			}
			a.complete(j, effectCommit(j), quietLog)
		})
		wg.Go(func() {
			time.Sleep(time.Duration(rand.Int64N(3000)) * time.Microsecond)
			if jobs, err := b.claim(context.Background(), 1); err == nil && len(jobs) == 1 {
				b.complete(jobs[0], effectCommit(jobs[0]), quietLog)
			}
		})
		wg.Wait()
	}
	if n := scalar[int](t, pool, `SELECT count(*) FROM (SELECT job_id FROM job_attempts WHERE outcome = 'succeeded' GROUP BY 1 HAVING count(*) > 1) x`); n != 0 {
		t.Fatalf("%d jobs have two successful terminal transitions", n)
	}
	if n := scalar[int](t, pool, `SELECT count(*) FROM (SELECT job_id FROM effects GROUP BY 1 HAVING count(*) > 1) x`); n != 0 {
		t.Fatalf("%d jobs have duplicate effects", n)
	}
	if n := scalar[int](t, pool, `SELECT count(*) FROM effects e JOIN jobs j ON j.id = e.job_id WHERE j.state <> 'succeeded' OR j.attempt <> e.attempt`); n != 0 {
		t.Fatalf("%d effects not written by the final owner", n)
	}
	t.Logf("succeeded=%d (by a=%d, b=%d), stale rejected a=%d b=%d",
		scalar[int](t, pool, `SELECT count(*) FROM jobs WHERE state = 'succeeded'`),
		a.Stats().Succeeded, b.Stats().Succeeded, a.Stats().StaleRejected, b.Stats().StaleRejected)
}

func TestHeartbeatMustBeUnderHalfLease(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("lease 10s / heartbeat 6s accepted: local deadline (4s) would pass before the first renewal")
		}
	}()
	NewWorker(nil, WorkerConfig{Lease: 10 * time.Second, Heartbeat: 6 * time.Second})
}

func TestShutdownBoundedWithUncooperativeHandler(t *testing.T) {
	pool := testdb.New(t, Migrations)
	id := enqueueOne(t, pool, 5)
	const grace = 200 * time.Millisecond
	w := newTestWorker(pool, WorkerConfig{ShutdownGrace: grace})
	started, unblock := make(chan struct{}), make(chan struct{})
	defer close(unblock)
	w.Handle("k", func(context.Context, Job) (CommitFunc, error) {
		close(started)
		<-unblock // ignores ctx entirely
		return nil, nil
	})
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	<-started
	stop()
	select {
	case <-done:
	case <-time.After(grace + time.Second):
		t.Fatal("Run blocked on a handler that ignores cancellation")
	}
	if s := scalar[string](t, pool, `SELECT state::text FROM jobs WHERE id = $1`, id); s != "queued" {
		t.Fatalf("state = %s, want released to queued", s)
	}
}
