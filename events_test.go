package durableq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Eliot-2006/durableq/internal/testdb"
	"github.com/jackc/pgx/v5"
)

func TestEvents(t *testing.T) {
	for _, outcome := range []string{"succeeded", "retried", "dead", "released", "stale", "error", "lease_lost", "budget", "commit_retry"} {
		t.Run(outcome, func(t *testing.T) {
			pool := testdb.New(t, Migrations)
			var events []Event
			w := NewWorker(pool, WorkerConfig{Lease: time.Second, Heartbeat: 100 * time.Millisecond, Logger: quietLog, OnEvent: func(e Event) { events = append(events, e) }})
			w.Handle("event", func(ctx context.Context, j Job) (CommitFunc, error) {
				switch outcome {
				case "retried":
					return nil, errors.New("retry")
				case "dead":
					return nil, Permanent(errors.New("dead"))
				case "released", "lease_lost":
					<-ctx.Done()
					return nil, ctx.Err()
				case "stale":
					_, err := pool.Exec(ctx, `UPDATE jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, j.ID)
					if err != nil {
						panic(err)
					}
					return nil, nil
				case "error":
					pool.Close()
					return nil, errors.New("offline")
				case "commit_retry":
					return func(context.Context, pgx.Tx) error { return errors.New("commit") }, nil
				}
				return nil, nil
			})
			r, err := NewClient(pool).Enqueue(context.Background(), EnqueueParams{Kind: "event"})
			if err != nil {
				t.Fatal(err)
			}
			if outcome == "budget" {
				_, err = pool.Exec(context.Background(), `UPDATE jobs SET attempt=max_attempts, state='running',lease_owner='abandoned',lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, r.ID)
				if err != nil {
					t.Fatal(err)
				}
			}
			j := claimOne(t, w)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if outcome == "released" {
				cancel()
			}
			if outcome == "lease_lost" {
				_, err = pool.Exec(ctx, `UPDATE jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, r.ID)
				if err != nil {
					t.Fatal(err)
				}
			}
			w.run(ctx, j, time.Now())
			want := outcome
			if want == "budget" {
				want = "dead"
			}
			if want == "commit_retry" {
				want = "retried"
			}
			if len(events) != 2 {
				t.Fatalf("events: %+v", events)
			}
			if events[0].Type != "claimed" || events[1].Type != want {
				t.Fatalf("events: %+v", events)
			}
			for _, e := range events {
				if e.Kind != "event" || e.Attempt != j.Attempt {
					t.Fatalf("event: %+v", e)
				}
			}
			if events[1].Duration <= 0 || w.Active() != 0 {
				t.Fatalf("duration/active: %+v %d", events, w.Active())
			}
		})
	}
}

func TestClaimObservations(t *testing.T) {
	pool := testdb.New(t, Migrations)
	var events []Event
	w := NewWorker(pool, WorkerConfig{OnEvent: func(e Event) { events = append(events, e) }})
	w.Handle("event", func(context.Context, Job) (CommitFunc, error) { return nil, nil })
	c := NewClient(pool)
	r, err := c.Enqueue(context.Background(), EnqueueParams{Kind: "event", Delay: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	// Queue wait measures lateness after run_at, not the scheduling delay itself.
	_, err = pool.Exec(context.Background(), `UPDATE jobs SET run_at=clock_timestamp()-interval '1 second' WHERE id=$1`, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	claimOne(t, w)
	_, err = pool.Exec(context.Background(), `UPDATE jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	j := claimOne(t, w)
	w.run(context.Background(), j, time.Now())
	if len(events) != 3 || events[0].Reclaimed || !events[1].Reclaimed || events[1].Attempt != 2 || events[1].QueueWait < time.Second {
		t.Fatalf("events: %+v", events)
	}
	info, err := c.Job(context.Background(), r.ID)
	if err != nil || len(info.Attempts) != 2 {
		t.Fatalf("job: %+v %v", info, err)
	}
	stats, err := c.Stats(context.Background())
	if err != nil || stats != (QueueStats{}) {
		t.Fatalf("stats: %+v %v", stats, err)
	}
	if _, err = c.Job(context.Background(), -1); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}

func TestTransitionErrorEvents(t *testing.T) {
	for _, method := range []string{"complete", "fail", "release"} {
		for _, outcome := range []string{"stale", "error"} {
			t.Run(method+"/"+outcome, func(t *testing.T) {
				pool := testdb.New(t, Migrations)
				var events []Event
				w := NewWorker(pool, WorkerConfig{Logger: quietLog, OnEvent: func(e Event) { events = append(events, e) }})
				w.Handle("event", func(context.Context, Job) (CommitFunc, error) { return nil, nil })
				if _, err := NewClient(pool).Enqueue(context.Background(), EnqueueParams{Kind: "event"}); err != nil {
					t.Fatal(err)
				}
				j := claimOne(t, w)
				if outcome == "stale" {
					if _, err := pool.Exec(context.Background(), `UPDATE jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, j.ID); err != nil {
						t.Fatal(err)
					}
				} else {
					pool.Close()
				}
				switch method {
				case "complete":
					w.complete(j, nil, quietLog)
				case "fail":
					w.fail(j, errors.New("failure"), quietLog)
				case "release":
					w.release(j, quietLog)
				}
				if len(events) != 2 || events[0].Type != "claimed" || events[1].Type != outcome || events[1].Kind != j.Kind || events[1].Attempt != j.Attempt {
					t.Fatalf("events: %+v", events)
				}
			})
		}
	}
}

func TestActiveDuringHandler(t *testing.T) {
	pool := testdb.New(t, Migrations)
	entered, finish := make(chan struct{}), make(chan struct{})
	w := NewWorker(pool, WorkerConfig{Logger: quietLog})
	w.Handle("active", func(context.Context, Job) (CommitFunc, error) { close(entered); <-finish; return nil, nil })
	if _, err := NewClient(pool).Enqueue(context.Background(), EnqueueParams{Kind: "active"}); err != nil {
		t.Fatal(err)
	}
	j := claimOne(t, w)
	done := make(chan struct{})
	go func() { w.run(context.Background(), j, time.Now()); close(done) }()
	<-entered
	active := w.Active()
	close(finish)
	<-done
	if active != 1 || w.Active() != 0 {
		t.Fatalf("active during=%d after=%d", active, w.Active())
	}
}
