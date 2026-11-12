package durableq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Client submits jobs.
type Client struct{ pool *pgxpool.Pool }

func NewClient(pool *pgxpool.Pool) *Client { return &Client{pool} }

// EnqueueParams describes a job. Zero values take defaults: payload {}, 5 attempts, no delay.
type EnqueueParams struct {
	Kind           string
	Payload        json.RawMessage
	MaxAttempts    int
	Delay          time.Duration
	IdempotencyKey string // empty = no deduplication
}

type EnqueueResult struct {
	ID      int64
	Existed bool // a job with this idempotency key already existed
}

// Enqueue inserts a job in its own transaction.
func (c *Client) Enqueue(ctx context.Context, p EnqueueParams) (EnqueueResult, error) {
	return enqueue(ctx, c.pool, p)
}

// EnqueueTx inserts a job inside the caller's transaction: it becomes visible
// to workers only if tx commits (transactional outbox).
func (c *Client) EnqueueTx(ctx context.Context, tx pgx.Tx, p EnqueueParams) (EnqueueResult, error) {
	return enqueue(ctx, tx, p)
}

type queryRower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func enqueue(ctx context.Context, db queryRower, p EnqueueParams) (EnqueueResult, error) {
	if p.Kind == "" {
		return EnqueueResult{}, errors.New("durableq: kind is required")
	}
	if p.MaxAttempts < 0 || p.Delay < 0 {
		return EnqueueResult{}, errors.New("durableq: max attempts and delay must be non-negative")
	}
	if p.MaxAttempts == 0 {
		p.MaxAttempts = 5
	}
	if len(p.Payload) == 0 {
		p.Payload = json.RawMessage("{}")
	}
	if !json.Valid(p.Payload) {
		return EnqueueResult{}, errors.New("durableq: payload is not valid JSON")
	}
	var key *string
	if p.IdempotencyKey != "" {
		key = &p.IdempotencyKey
	}

	var r EnqueueResult
	err := db.QueryRow(ctx, qEnqueue, p.Kind, string(p.Payload), p.MaxAttempts, p.Delay.Microseconds(), key).Scan(&r.ID)
	if errors.Is(err, pgx.ErrNoRows) && key != nil {
		// ON CONFLICT waited for the conflicting insert to commit; it is visible now.
		r.Existed = true
		err = db.QueryRow(ctx, qByKey, *key).Scan(&r.ID)
	}
	if err != nil {
		return EnqueueResult{}, fmt.Errorf("durableq: enqueue: %w", err)
	}
	return r, nil
}

// ErrNotFound indicates that the requested job does not exist.
var ErrNotFound = errors.New("durableq: job not found")

type QueueStats struct {
	Pending      int64 `json:"pending"`
	Scheduled    int64 `json:"scheduled"`
	Running      int64 `json:"running"`
	LeaseExpired int64 `json:"lease_expired"`
	Succeeded    int64 `json:"succeeded"`
	Dead         int64 `json:"dead"`
}

// Stats reads the queue counts in one query.
func (c *Client) Stats(ctx context.Context) (QueueStats, error) {
	var s QueueStats
	err := c.pool.QueryRow(ctx, qStats).Scan(&s.Pending, &s.Scheduled, &s.Running, &s.LeaseExpired, &s.Succeeded, &s.Dead)
	return s, err
}

type AttemptInfo struct {
	Attempt    int        `json:"attempt"`
	WorkerID   string     `json:"worker_id"`
	ClaimedAt  time.Time  `json:"claimed_at"`
	FinishedAt *time.Time `json:"finished_at"`
	Outcome    *string    `json:"outcome"`
	Error      *string    `json:"error"`
}

type JobInfo struct {
	ID          int64         `json:"id"`
	Kind        string        `json:"kind"`
	State       string        `json:"state"`
	Attempt     int           `json:"attempt"`
	MaxAttempts int           `json:"max_attempts"`
	RunAt       time.Time     `json:"run_at"`
	CreatedAt   time.Time     `json:"created_at"`
	FinishedAt  *time.Time    `json:"finished_at"`
	LastError   *string       `json:"last_error"`
	Attempts    []AttemptInfo `json:"attempts"`
}

// Job reads a job and its ordered attempt history in one query.
func (c *Client) Job(ctx context.Context, id int64) (JobInfo, error) {
	var j JobInfo
	var attempts []byte
	err := c.pool.QueryRow(ctx, qJob, id).Scan(&j.ID, &j.Kind, &j.State, &j.Attempt, &j.MaxAttempts, &j.RunAt, &j.CreatedAt, &j.FinishedAt, &j.LastError, &attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return JobInfo{}, ErrNotFound
	}
	if err != nil {
		return JobInfo{}, err
	}
	err = json.Unmarshal(attempts, &j.Attempts)
	return j, err
}
