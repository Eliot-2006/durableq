package migrate_test

import (
	"context"
	"sync"
	"testing"

	"github.com/Eliot-2006/durableq"
	"github.com/Eliot-2006/durableq/internal/migrate"
	"github.com/Eliot-2006/durableq/internal/testdb"
)

func TestUpIsIdempotentAndConcurrencySafe(t *testing.T) {
	pool := testdb.New(t, true)
	ctx := context.Background()

	var wg sync.WaitGroup
	total := make(chan int, 5)
	for range 5 {
		wg.Go(func() {
			applied, err := migrate.Up(ctx, pool, durableq.Migrations, "migrations")
			if err != nil {
				t.Error(err)
			}
			total <- len(applied)
		})
	}
	wg.Wait()
	close(total)
	sum := 0
	for n := range total {
		sum += n
	}
	if sum != 1 {
		t.Fatalf("expected 0001_init applied exactly once across migrators, got %d", sum)
	}

	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM jobs`).Scan(&n); err != nil {
		t.Fatalf("jobs table missing: %v", err)
	}
}
