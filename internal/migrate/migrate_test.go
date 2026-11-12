package migrate_test

import (
	"context"
	"io/fs"
	"path"
	"sync"
	"testing"

	"github.com/Eliot-2006/durableq"
	"github.com/Eliot-2006/durableq/internal/migrate"
	"github.com/Eliot-2006/durableq/internal/testdb"
)

func TestUpIsIdempotentAndConcurrencySafe(t *testing.T) {
	pool := testdb.New(t, nil)
	ctx := context.Background()

	var wg sync.WaitGroup
	total := make(chan []string, 5)
	for range 5 {
		wg.Go(func() {
			applied, err := migrate.Up(ctx, pool, durableq.Migrations, "migrations")
			if err != nil {
				t.Error(err)
			}
			total <- applied
		})
	}
	wg.Wait()
	close(total)
	expected, err := fs.Glob(durableq.Migrations, "migrations/*.sql")
	if err != nil {
		t.Fatal(err)
	}
	if len(expected) == 0 {
		t.Fatal("no embedded migrations")
	}
	counts := map[string]int{}
	for applied := range total {
		for _, version := range applied {
			counts[version]++
		}
	}
	if len(counts) != len(expected) {
		t.Fatalf("expected %d distinct migrations, got %v", len(expected), counts)
	}
	for _, file := range expected {
		if counts[path.Base(file)] != 1 {
			t.Fatalf("expected %s applied exactly once across migrators, got %v", file, counts)
		}
	}
	applied, err := migrate.Up(ctx, pool, durableq.Migrations, "migrations")
	if err != nil || len(applied) != 0 {
		t.Fatalf("repeat migrate: applied=%v err=%v", applied, err)
	}

	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM jobs`).Scan(&n); err != nil {
		t.Fatalf("jobs table missing: %v", err)
	}
}
