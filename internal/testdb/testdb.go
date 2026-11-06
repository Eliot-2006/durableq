// Package testdb gives each test an isolated, migrated schema on a real Postgres.
package testdb

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Eliot-2006/durableq"
	"github.com/Eliot-2006/durableq/internal/migrate"
)

// New returns a pool whose search_path is a fresh schema, migrated unless
// raw is true. The schema is dropped when the test ends. Tests are skipped
// when DATABASE_URL is unset.
func New(t testing.TB, raw ...bool) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set; skipping Postgres integration test")
	}
	ctx := context.Background()
	schema := fmt.Sprintf("t_%s_%d", sanitize(t.Name()), time.Now().UnixNano())

	admin, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}

	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		c, err := pgx.Connect(ctx, url)
		if err != nil {
			t.Error(err)
			return
		}
		defer c.Close(ctx)
		if _, err := c.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	if len(raw) == 0 || !raw[0] {
		if _, err := migrate.Up(ctx, pool, durableq.Migrations, "migrations"); err != nil {
			t.Fatal(err)
		}
	}
	return pool
}

func sanitize(name string) string {
	name = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			return r
		}
		return '_'
	}, strings.ToLower(name))
	return name[:min(len(name), 30)]
}
