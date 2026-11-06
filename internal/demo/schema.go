package demo

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Eliot-2006/durableq"
	"github.com/Eliot-2006/durableq/internal/migrate"
)

// Schema is a fresh, migrated schema for one harness run, so runs never see
// each other's jobs.
type Schema struct {
	Name  string
	URL   string        // DATABASE_URL with search_path set, for worker processes
	Pool  *pgxpool.Pool // pool inside the schema
	Admin *pgx.Conn     // plain connection, for pg_stat_* and pg_terminate_backend
}

func NewSchema(ctx context.Context, dbURL, name string) (*Schema, error) {
	u, err := url.Parse(dbURL)
	if err != nil || u.Scheme == "" {
		return nil, fmt.Errorf("DATABASE_URL must be a postgres:// URL (got %q)", dbURL)
	}
	admin, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		return nil, err
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{name}.Sanitize()); err != nil {
		admin.Close(ctx)
		return nil, err
	}
	q := u.Query()
	q.Set("search_path", name)
	u.RawQuery = q.Encode()
	s := &Schema{Name: name, URL: u.String(), Admin: admin}
	if s.Pool, err = pgxpool.New(ctx, s.URL); err == nil {
		_, err = migrate.Up(ctx, s.Pool, durableq.Migrations, "migrations")
	}
	if err != nil {
		s.Close(true)
		return nil, err
	}
	return s, nil
}

// Reconnect waits up to timeout for Postgres to accept connections again
// (after a server restart), replacing Admin and every pooled connection.
func (s *Schema) Reconnect(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		admin, err := pgx.Connect(ctx, s.URL)
		if err == nil {
			s.Admin.Close(ctx)
			s.Admin = admin
			s.Pool.Reset()
			if _, err = s.Pool.Exec(ctx, "SELECT 1"); err == nil {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("postgres not back after %v: %w", timeout, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Close releases connections, dropping the schema if drop is set.
func (s *Schema) Close(drop bool) {
	ctx := context.Background()
	if s.Pool != nil {
		s.Pool.Close()
	}
	if drop {
		s.Admin.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{s.Name}.Sanitize()+" CASCADE")
	}
	s.Admin.Close(ctx)
}
