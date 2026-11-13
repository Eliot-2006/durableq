package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHealthcheck(t *testing.T) {
	for _, code := range []int{200, 204, 302, 503} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(code) }))
			defer s.Close()
			err := runHealthcheck(context.Background(), []string{"--url", s.URL})
			if (err == nil) != (code >= 200 && code < 300) {
				t.Fatalf("code=%d err=%v", code, err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runHealthcheck(ctx, []string{"--url", "http://127.0.0.1:1"}); err == nil {
		t.Fatal("expected failure")
	}
}

func TestHTTPListenerAndShutdown(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if _, _, err := startHTTP(listener.Addr().String(), http.NotFoundHandler()); err == nil {
		t.Fatal("expected bind failure")
	}
	s, done, err := startHTTP("127.0.0.1:0", http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	if err := shutdownHTTP(s, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
