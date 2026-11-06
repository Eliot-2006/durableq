// Package demo holds the demo job kinds used by the CLI, chaos tests, and benchmarks.
package demo

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Eliot-2006/durableq"
	"github.com/Eliot-2006/durableq/internal/failpoint"
)

// EffectPayload configures a demo.effect job.
type EffectPayload struct {
	SleepMS   int     `json:"sleep_ms"`  // base work time; ±50% jitter
	FailRate  float64 `json:"fail_rate"` // probability of a retryable failure
	Permanent bool    `json:"permanent"` // fail permanently
}

// Register adds demo.effect and demo.noop to w.
func Register(w *durableq.Worker) {
	w.Handle("demo.noop", func(context.Context, durableq.Job) (durableq.CommitFunc, error) { return nil, nil })
	w.Handle("demo.effect", Effect)
}

// Effect sleeps, maybe fails, and on success records one row in effects
// inside the completion transaction.
func Effect(ctx context.Context, j durableq.Job) (durableq.CommitFunc, error) {
	var p EffectPayload
	if err := json.Unmarshal(j.Payload, &p); err != nil {
		return nil, durableq.Permanent(err)
	}
	if p.Permanent {
		return nil, durableq.Permanent(errors.New("payload requested permanent failure"))
	}
	if p.SleepMS > 0 {
		d := time.Duration(float64(p.SleepMS)*(0.5+rand.Float64())) * time.Millisecond
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(d):
		}
	}
	failpoint.Hit("during_run")
	if rand.Float64() < p.FailRate {
		return nil, errors.New("injected failure")
	}
	return func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO effects (job_id, attempt) VALUES ($1, $2)`, j.ID, j.Attempt)
		return err
	}, nil
}
