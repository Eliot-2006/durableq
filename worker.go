package durableq

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	mrand "math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Job is what a handler receives. Attempt is the fencing token (1-based).
type Job struct {
	ID          int64
	Kind        string
	Payload     json.RawMessage
	Attempt     int
	MaxAttempts int
}

// CommitFunc runs inside the fenced completion transaction. Writes made with tx
// commit if and only if the job is marked succeeded by this attempt.
type CommitFunc func(ctx context.Context, tx pgx.Tx) error

// HandlerFunc does the job's work outside any transaction. A nil error
// completes the job (running the optional CommitFunc); a non-nil error retries
// it with backoff, or dead-letters it if attempts are exhausted or the error is
// Permanent.
type HandlerFunc func(ctx context.Context, job Job) (CommitFunc, error)

type WorkerConfig struct {
	ID            string        // default hostname-pid-random
	Concurrency   int           // default 10
	BatchSize     int           // default 10
	Lease         time.Duration // default 30s
	Heartbeat     time.Duration // default 10s; must be < Lease
	Poll          time.Duration // default 1s
	JobTimeout    time.Duration // default 5m
	ShutdownGrace time.Duration // default 10s
	// Backoff returns the retry delay after a failed attempt. Default: full
	// jitter with base 1s, cap 10m.
	Backoff func(attempt int) time.Duration
	Logger  *slog.Logger
}

// Stats are worker-side counters.
type Stats struct {
	Succeeded, Failed, Dead, Released, LeaseLost, StaleRejected int64
}

type Worker struct {
	pool     *pgxpool.Pool
	cfg      WorkerConfig
	handlers map[string]HandlerFunc
	log      *slog.Logger

	succeeded, failed, dead, released, leaseLost, staleRejected atomic.Int64
}

// WorkerID returns a unique id of the form durableq-worker-host-pid-random.
func WorkerID() string {
	host, _ := os.Hostname()
	b := make([]byte, 4)
	rand.Read(b)
	return fmt.Sprintf("durableq-worker-%s-%d-%s", host, os.Getpid(), hex.EncodeToString(b))
}

func NewWorker(pool *pgxpool.Pool, cfg WorkerConfig) *Worker {
	def := func(d *time.Duration, v time.Duration) {
		if *d <= 0 {
			*d = v
		}
	}
	if cfg.ID == "" {
		cfg.ID = WorkerID()
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 10
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 10
	}
	def(&cfg.Lease, 30*time.Second)
	def(&cfg.Heartbeat, 10*time.Second)
	def(&cfg.Poll, time.Second)
	def(&cfg.JobTimeout, 5*time.Minute)
	def(&cfg.ShutdownGrace, 10*time.Second)
	if cfg.Heartbeat >= cfg.Lease {
		panic("durableq: heartbeat must be shorter than lease")
	}
	if cfg.Backoff == nil {
		cfg.Backoff = func(a int) time.Duration { return FullJitter(a, time.Second, 10*time.Minute) }
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Worker{pool: pool, cfg: cfg, handlers: map[string]HandlerFunc{}, log: cfg.Logger.With("worker", cfg.ID)}
}

// Handle registers h for kind. Call before Run.
func (w *Worker) Handle(kind string, h HandlerFunc) { w.handlers[kind] = h }

func (w *Worker) ID() string { return w.cfg.ID }

func (w *Worker) Stats() Stats {
	return Stats{w.succeeded.Load(), w.failed.Load(), w.dead.Load(), w.released.Load(), w.leaseLost.Load(), w.staleRejected.Load()}
}

// Run claims and executes jobs until ctx is cancelled, then stops claiming,
// waits up to ShutdownGrace for in-flight jobs, releases the rest, and returns.
func (w *Worker) Run(ctx context.Context) error {
	jobsCtx, cancelJobs := context.WithCancel(context.Background())
	defer cancelJobs()
	slots := make(chan struct{}, w.cfg.Concurrency)
	freed := make(chan struct{}, 1) // wakes the poll loop when a slot frees
	var wg sync.WaitGroup

	for ctx.Err() == nil {
		free := cap(slots) - len(slots)
		want := min(free, w.cfg.BatchSize)
		claimed := 0
		if want > 0 {
			sent := time.Now()
			jobs, err := w.claim(ctx, want)
			if err != nil && ctx.Err() == nil {
				w.log.Warn("claim failed", "err", err)
			}
			claimed = len(jobs)
			for _, j := range jobs {
				slots <- struct{}{}
				wg.Go(func() {
					defer func() {
						<-slots
						select {
						case freed <- struct{}{}:
						default:
						}
					}()
					w.run(jobsCtx, j, sent)
				})
			}
		}
		if claimed > 0 && claimed == want {
			continue // full batch: likely more work, claim again immediately
		}
		// Sleep poll ± 20% so idle workers don't poll in lockstep.
		wait := time.Duration(float64(w.cfg.Poll) * (0.8 + 0.4*mrand.Float64()))
		select {
		case <-ctx.Done():
		case <-time.After(wait):
		case <-freed:
			if want > 0 {
				// Slots were free and the queue was empty; a finishing job
				// doesn't mean new work, so keep sleeping.
				select {
				case <-ctx.Done():
				case <-time.After(wait):
				}
			}
		}
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(w.cfg.ShutdownGrace):
		cancelJobs() // remaining jobs observe shutdown and release themselves
		<-done
	}
	return nil
}

func (w *Worker) claim(ctx context.Context, n int) ([]Job, error) {
	rows, err := w.pool.Query(ctx, qClaim, n, w.cfg.ID, w.cfg.Lease.Microseconds())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Job, error) {
		var j Job
		err := r.Scan(&j.ID, &j.Kind, &j.Payload, &j.Attempt, &j.MaxAttempts)
		return j, err
	})
}

// run executes one claimed job. claimSent is when the claim query was sent;
// the lease cannot have started earlier, so it anchors the local deadline.
func (w *Worker) run(shutdown context.Context, j Job, claimSent time.Time) {
	log := w.log.With("job", j.ID, "attempt", j.Attempt)
	if j.Attempt > j.MaxAttempts {
		// Reclaimed after its last allowed attempt was abandoned: dead-letter without running.
		w.fail(j, Permanent(errors.New("attempt budget exhausted")), log)
		return
	}
	h, ok := w.handlers[j.Kind]
	if !ok {
		w.fail(j, fmt.Errorf("no handler registered for kind %q", j.Kind), log)
		return
	}

	ctx, cancel := context.WithTimeout(shutdown, w.cfg.JobTimeout)
	defer cancel()
	var lost atomic.Bool
	hbDone := make(chan struct{})
	hbCtx, stopHB := context.WithCancel(context.Background())
	go func() {
		defer close(hbDone)
		if !w.heartbeat(hbCtx, j, claimSent) {
			lost.Store(true)
			cancel()
		}
	}()

	commit, err := safeCall(ctx, h, j)
	stopHB()
	<-hbDone

	switch {
	case lost.Load():
		w.leaseLost.Add(1)
		log.Warn("lease lost; abandoning job without transition")
	case err != nil && shutdown.Err() != nil:
		w.release(j, log)
	case err != nil:
		w.fail(j, err, log)
	default:
		w.complete(j, commit, log)
	}
}

// heartbeat renews the lease until ctx is cancelled (returns true) or the lease
// is lost (returns false): either the database rejects the renewal, or the
// local deadline passes without a successful renewal (e.g. database
// unreachable). The local deadline is conservative: it is measured from when
// the last successful renewal was *sent*, minus one heartbeat interval.
func (w *Worker) heartbeat(ctx context.Context, j Job, sent time.Time) bool {
	deadline := sent.Add(w.cfg.Lease - w.cfg.Heartbeat)
	tick := time.NewTicker(w.cfg.Heartbeat)
	defer tick.Stop()
	for {
		expire := time.NewTimer(time.Until(deadline))
		select {
		case <-ctx.Done():
			expire.Stop()
			return true
		case <-expire.C:
			return false
		case <-tick.C:
			expire.Stop()
		}
		now := time.Now()
		qctx, cancel := context.WithDeadline(ctx, deadline)
		tag, err := w.pool.Exec(qctx, qHeartbeat, j.ID, j.Attempt, w.cfg.Lease.Microseconds())
		cancel()
		switch {
		case err == nil && tag.RowsAffected() == 1:
			deadline = now.Add(w.cfg.Lease - w.cfg.Heartbeat)
		case err == nil:
			return false // fenced out: lease expired or job reclaimed
		case ctx.Err() != nil:
			return true
		case time.Now().After(deadline):
			return false
		default:
			w.log.Warn("heartbeat failed", "job", j.ID, "err", err)
		}
	}
}

func safeCall(ctx context.Context, h HandlerFunc, j Job) (c CommitFunc, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	return h(ctx, j)
}

var errStale = errors.New("stale transition rejected")

// transition runs fn in a short transaction on a fresh context: the job's own
// context may already be cancelled, but the outcome must still be recorded.
func (w *Worker) transition(fn func(ctx context.Context, tx pgx.Tx) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), w.cfg.Lease)
	defer cancel()
	err := pgx.BeginFunc(ctx, w.pool, func(tx pgx.Tx) error { return fn(ctx, tx) })
	if errors.Is(err, errStale) {
		w.staleRejected.Add(1)
	}
	return err
}

func exec1(ctx context.Context, tx pgx.Tx, sql string, args ...any) error {
	tag, err := tx.Exec(ctx, sql, args...)
	if err == nil && tag.RowsAffected() == 0 {
		return errStale
	}
	return err
}

func (w *Worker) complete(j Job, commit CommitFunc, log *slog.Logger) {
	err := w.transition(func(ctx context.Context, tx pgx.Tx) error {
		// Fenced update first: it row-locks the job, so no reclaimer can take
		// it for the rest of this transaction.
		if err := exec1(ctx, tx, qComplete, j.ID, j.Attempt); err != nil {
			return err
		}
		if commit != nil {
			if err := commit(ctx, tx); err != nil {
				return fmt.Errorf("commit func: %w", err)
			}
		}
		return exec1(ctx, tx, qFinishAttempt, j.ID, j.Attempt, "succeeded", nil)
	})
	if err == nil {
		w.succeeded.Add(1)
		return
	}
	log.Warn("complete failed", "err", err)
	if !errors.Is(err, errStale) {
		// The CommitFunc or the connection failed; record a failure so the job
		// retries instead of waiting for lease expiry.
		w.fail(j, err, log)
	}
}

func (w *Worker) fail(j Job, cause error, log *slog.Logger) {
	permanent := IsPermanent(cause)
	backoff := w.cfg.Backoff(j.Attempt)
	var state string
	err := w.transition(func(ctx context.Context, tx pgx.Tx) error {
		err := tx.QueryRow(ctx, qFail, j.ID, j.Attempt, permanent, backoff.Microseconds(), cause.Error()).Scan(&state)
		if errors.Is(err, pgx.ErrNoRows) {
			return errStale
		}
		if err != nil {
			return err
		}
		outcome := "failed"
		if state == "dead" {
			outcome = "dead"
		}
		return exec1(ctx, tx, qFinishAttempt, j.ID, j.Attempt, outcome, cause.Error())
	})
	switch {
	case err != nil:
		log.Warn("fail transition failed", "err", err)
	case state == "dead":
		w.dead.Add(1)
		log.Info("job dead", "err", cause)
	default:
		w.failed.Add(1)
		log.Info("job failed; retrying", "err", cause, "backoff", backoff)
	}
}

func (w *Worker) release(j Job, log *slog.Logger) {
	err := w.transition(func(ctx context.Context, tx pgx.Tx) error {
		if err := exec1(ctx, tx, qRelease, j.ID, j.Attempt); err != nil {
			return err
		}
		return exec1(ctx, tx, qFinishAttempt, j.ID, j.Attempt, "released", nil)
	})
	if err != nil {
		log.Warn("release failed", "err", err)
		return
	}
	w.released.Add(1)
}
