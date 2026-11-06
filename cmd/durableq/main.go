// Command durableq operates a DurableQ queue. DATABASE_URL selects the database.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Eliot-2006/durableq"
	"github.com/Eliot-2006/durableq/internal/demo"
	"github.com/Eliot-2006/durableq/internal/migrate"
)

const usage = `usage: durableq <command> [flags]

commands:
  migrate    apply embedded migrations
  enqueue    submit jobs
  worker     run a worker with the demo handlers
  status     job counts by state, or one job with its attempts`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "migrate":
		err = runMigrate(ctx)
	case "enqueue":
		err = runEnqueue(ctx, os.Args[2:])
	case "worker":
		err = runWorker(ctx, os.Args[2:])
	case "status":
		err = runStatus(ctx, os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n%s\n", os.Args[1], usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func connect(ctx context.Context) (*pgxpool.Pool, error) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return nil, fmt.Errorf("DATABASE_URL is not set")
	}
	return pgxpool.New(ctx, url)
}

func runMigrate(ctx context.Context) error {
	pool, err := connect(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	applied, err := migrate.Up(ctx, pool, durableq.Migrations, "migrations")
	if err != nil {
		return err
	}
	if len(applied) == 0 {
		fmt.Println("schema up to date")
	}
	for _, v := range applied {
		fmt.Println("applied", v)
	}
	return nil
}

func runEnqueue(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("enqueue", flag.ExitOnError)
	kind := fs.String("kind", "", "job kind (required)")
	payload := fs.String("payload", "{}", "JSON payload")
	key := fs.String("key", "", "idempotency key (with --count > 1, suffixed -1..-N)")
	maxAttempts := fs.Int("max-attempts", 5, "attempt budget")
	delay := fs.Duration("delay", 0, "delay before the job becomes runnable")
	n := fs.Int("count", 1, "number of jobs")
	fs.Parse(args)

	pool, err := connect(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	c := durableq.NewClient(pool)
	existed := 0
	for i := 1; i <= *n; i++ {
		p := durableq.EnqueueParams{Kind: *kind, Payload: []byte(*payload), MaxAttempts: *maxAttempts, Delay: *delay, IdempotencyKey: *key}
		if *key != "" && *n > 1 {
			p.IdempotencyKey = *key + "-" + strconv.Itoa(i)
		}
		r, err := c.Enqueue(ctx, p)
		if err != nil {
			return err
		}
		if r.Existed {
			existed++
		}
		if *n == 1 {
			fmt.Printf("job %d (existed=%v)\n", r.ID, r.Existed)
		}
	}
	if *n > 1 {
		fmt.Printf("enqueued %d jobs (%d already existed)\n", *n-existed, existed)
	}
	return nil
}

func runWorker(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	cfg := durableq.WorkerConfig{}
	fs.IntVar(&cfg.Concurrency, "concurrency", 10, "max concurrent jobs")
	fs.DurationVar(&cfg.Lease, "lease", 30*time.Second, "lease duration")
	fs.DurationVar(&cfg.Heartbeat, "heartbeat", 10*time.Second, "heartbeat interval (< lease/2)")
	fs.DurationVar(&cfg.Poll, "poll", time.Second, "idle poll interval")
	fs.Parse(args)
	if 2*cfg.Heartbeat >= cfg.Lease {
		return errors.New("--heartbeat must be less than half of --lease")
	}

	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return errors.New("DATABASE_URL is not set")
	}
	pcfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return err
	}
	cfg.ID = durableq.WorkerID()
	pcfg.ConnConfig.RuntimeParams["application_name"] = cfg.ID // lets chaos tests target this worker's connections
	pcfg.MaxConns = int32(cfg.Concurrency + 2)
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return err
	}
	defer pool.Close()

	w := durableq.NewWorker(pool, cfg)
	demo.Register(w)
	fmt.Fprintln(os.Stderr, "worker", w.ID(), "started")
	err = w.Run(ctx)
	fmt.Fprintf(os.Stderr, "worker %s stopped: %+v\n", w.ID(), w.Stats())
	return err
}

func runStatus(ctx context.Context, args []string) error {
	pool, err := connect(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()

	if len(args) == 0 {
		rows, err := pool.Query(ctx, `SELECT state::text, count(*) FROM jobs GROUP BY 1 ORDER BY 1`)
		if err != nil {
			return err
		}
		var state string
		var n int64
		_, err = pgx.ForEachRow(rows, []any{&state, &n}, func() error {
			fmt.Printf("%-10s %d\n", state, n)
			return nil
		})
		return err
	}

	id, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid job id %q", args[0])
	}
	var kind, state, payload string
	var attempt, maxAttempts int
	var lastErr *string
	err = pool.QueryRow(ctx, `SELECT kind, state::text, payload::text, attempt, max_attempts, last_error FROM jobs WHERE id = $1`, id).
		Scan(&kind, &state, &payload, &attempt, &maxAttempts, &lastErr)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("job %d not found", id)
	}
	if err != nil {
		return err
	}
	fmt.Printf("job %d  kind=%s  state=%s  attempt=%d/%d\npayload: %s\n", id, kind, state, attempt, maxAttempts, payload)
	if lastErr != nil {
		fmt.Println("last error:", *lastErr)
	}
	rows, err := pool.Query(ctx, `SELECT attempt, worker_id, claimed_at, coalesce(outcome, '-'), coalesce(error, '')
		FROM job_attempts WHERE job_id = $1 ORDER BY attempt`, id)
	if err != nil {
		return err
	}
	var a int
	var worker, outcome, msg string
	var claimed time.Time
	_, err = pgx.ForEachRow(rows, []any{&a, &worker, &claimed, &outcome, &msg}, func() error {
		fmt.Printf("  #%d  %s  %s  %-9s %s\n", a, claimed.Format(time.RFC3339Nano), worker, outcome, msg)
		return nil
	})
	return err
}
