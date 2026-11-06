// Package testdb gives each test an isolated, migrated schema on a real Postgres.
package testdb

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Eliot-2006/durableq/internal/migrate"
)

// New returns a pool whose search_path is a fresh schema, migrated with
// migrations (pass nil for an empty schema). The schema is dropped when the
// test ends. Tests are skipped when DATABASE_URL is unset. It takes the
// migrations as a parameter so package durableq's own tests can use it.
func New(t testing.TB, migrations fs.FS) *pgxpool.Pool {
	return NewWithConfig(t, migrations, nil)
}

// NewWithConfig is New with a hook to adjust the pool config (e.g. dial through a proxy).
func NewWithConfig(t testing.TB, migrations fs.FS, adjust func(*pgxpool.Config)) *pgxpool.Pool {
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
	if adjust != nil {
		adjust(cfg)
	}
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
	if migrations != nil {
		if _, err := migrate.Up(ctx, pool, migrations, "migrations"); err != nil {
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
