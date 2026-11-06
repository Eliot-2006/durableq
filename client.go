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
