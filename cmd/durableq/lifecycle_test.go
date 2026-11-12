package main

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/Eliot-2006/durableq"
	"github.com/Eliot-2006/durableq/internal/testdb"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func freeAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition did not become true within 5s")
}

func status(client *http.Client, endpoint string) int {
	r, err := client.Get(endpoint)
	if err != nil {
		return 0
	}
	defer r.Body.Close()
	io.Copy(io.Discard, r.Body)
	return r.StatusCode
}

func waitStopped(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("process did not stop")
	}
}

func TestWorkerLifecycleReleasesDuringShutdown(t *testing.T) {
	pool := testdb.New(t, durableq.Migrations)
	dsn, err := url.Parse(os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	params := dsn.Query()
	params.Set("search_path", pool.Config().ConnConfig.RuntimeParams["search_path"])
	dsn.RawQuery = params.Encode()
	t.Setenv("DATABASE_URL", dsn.String())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr := freeAddress(t)
	done := make(chan error, 1)
	go func() {
		done <- runWorker(ctx, []string{"--http-addr", addr, "--concurrency", "1", "--poll", "5ms", "--grace", "1s"})
	}()
	// Always wait for the worker before testdb's pool/schema cleanup.
	t.Cleanup(func() { cancel(); waitStopped(t, done) })
	client := &http.Client{Timeout: time.Second}
	defer client.CloseIdleConnections()
	endpoint := "http://" + addr
	eventually(t, func() bool { return status(client, endpoint+"/readyz") == 200 })
	c := durableq.NewClient(pool)
	job, err := c.Enqueue(context.Background(), durableq.EnqueueParams{Kind: "demo.effect", Payload: json.RawMessage(`{"sleep_ms":60000}`), MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { j, err := c.Job(context.Background(), job.ID); return err == nil && j.State == "running" })
	cancel()
	eventually(t, func() bool { return status(client, endpoint+"/readyz") == 503 })
	if code := status(client, endpoint+"/healthz"); code != 200 {
		t.Fatalf("healthz during drain: %d", code)
	}
	select {
	case err := <-done:
		done <- err
		t.Fatalf("worker exited before drain window: %v", err)
	default:
	}
	// Cleanup owns receiving done; observing the closed listener proves shutdown finished.
	eventually(t, func() bool {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err != nil {
			return true
		}
		conn.Close()
		return false
	})
	j, err := c.Job(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if j.State != "queued" || j.Attempt != 1 || j.MaxAttempts != 2 || len(j.Attempts) != 1 || j.Attempts[0].Outcome == nil || *j.Attempts[0].Outcome != "released" {
		t.Fatalf("released job: %+v", j)
	}
}

// An acquire trace lets the test cancel only after /readyz is inside Ping.
// The database and HTTP server are real; holding the pool delays that request.
type acquireTrace struct{ started chan struct{} }

func (t *acquireTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	return ctx
}
func (t *acquireTrace) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}
func (t *acquireTrace) TraceAcquireStart(ctx context.Context, _ *pgxpool.Pool, _ pgxpool.TraceAcquireStartData) context.Context {
	t.started <- struct{}{}
	return ctx
}
func (t *acquireTrace) TraceAcquireEnd(context.Context, *pgxpool.Pool, pgxpool.TraceAcquireEndData) {}

func TestServeLifecycleRejectsReadinessDuringShutdown(t *testing.T) {
	trace := &acquireTrace{started: make(chan struct{}, 10)}
	pool := testdb.NewWithConfig(t, durableq.Migrations, func(cfg *pgxpool.Config) { cfg.MaxConns = 1; cfg.ConnConfig.Tracer = trace })
	// Discard migration acquires before exercising the server.
	for len(trace.started) > 0 {
		<-trace.started
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr := freeAddress(t)
	done := make(chan error, 1)
	go func() { done <- serve(ctx, pool, addr, 3*time.Second) }()
	t.Cleanup(func() { cancel(); waitStopped(t, done) })
	client := &http.Client{Timeout: 3 * time.Second}
	defer client.CloseIdleConnections()
	endpoint := "http://" + addr
	eventually(t, func() bool { return status(client, endpoint+"/healthz") == 200 })
	held, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()
	<-trace.started
	ready := make(chan int, 1)
	go func() { ready <- status(client, endpoint+"/readyz") }()
	select {
	case <-trace.started:
	case <-time.After(time.Second):
		t.Fatal("readyz did not enter Ping")
	}
	cancel()
	// Shutdown closes the listener before waiting for this in-flight readiness request.
	eventually(t, func() bool {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err != nil {
			return true
		}
		conn.Close()
		return false
	})
	held.Release()
	select {
	case code := <-ready:
		if code != 503 {
			t.Fatalf("readyz during shutdown: %d", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("readyz did not return")
	}
}
