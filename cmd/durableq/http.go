package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/Eliot-2006/durableq/internal/api"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

func jsonLogger() *slog.Logger {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	slog.SetDefault(logger)
	return logger
}

func runServe(ctx context.Context, args []string) error {
	jsonLogger()
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", ":8080", "HTTP listen address")
	maxConns := fs.Int("max-conns", 5, "connection pool size")
	timeout := fs.Duration("shutdown-timeout", 10*time.Second, "HTTP drain timeout")
	fs.Parse(args)
	if *maxConns < 1 || *maxConns > math.MaxInt32 || *timeout <= 0 {
		return errors.New("max-conns and shutdown-timeout must be positive (max-conns <= 2147483647)")
	}
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return errors.New("DATABASE_URL is not set")
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return err
	}
	cfg.MaxConns = int32(*maxConns)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer pool.Close()
	return serve(ctx, pool, *addr, *timeout)
}

func serve(ctx context.Context, pool *pgxpool.Pool, addr string, timeout time.Duration) error {
	h := api.New(pool, prometheus.NewRegistry())
	s, done, err := startHTTP(addr, h)
	if err != nil {
		return err
	}
	slog.Info("API started", "addr", addr)
	select {
	case err = <-done:
	case <-ctx.Done():
	}
	h.BeginShutdown()
	shutdownErr := shutdownHTTP(s, timeout)
	slog.Info("API stopped")
	return errors.Join(err, shutdownErr)
}

func startHTTP(addr string, h http.Handler) (*http.Server, <-chan error, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	s := &http.Server{Addr: addr, Handler: h, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
	done := make(chan error, 1)
	go func() {
		err := s.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		done <- err
	}()
	return s, done, nil
}

func shutdownHTTP(s *http.Server, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	err := s.Shutdown(ctx)
	if err != nil {
		s.Close()
	}
	return err
}

func runHealthcheck(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("healthcheck", flag.ExitOnError)
	url := fs.String("url", "", "health endpoint URL (required)")
	fs.Parse(args)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, *url, nil)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("healthcheck: HTTP %d", response.StatusCode)
	}
	return nil
}
