// Command durableq operates a DurableQ queue. DATABASE_URL selects the database.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Eliot-2006/durableq"
	"github.com/Eliot-2006/durableq/internal/migrate"
)

const usage = `usage: durableq <command> [flags]

commands:
  migrate    apply embedded migrations`

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
