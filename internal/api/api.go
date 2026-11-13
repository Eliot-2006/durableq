// Package api serves the queue API and process health endpoints.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/Eliot-2006/durableq"
	"github.com/Eliot-2006/durableq/internal/metrics"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

type Handler struct {
	mux      *http.ServeMux
	pool     *pgxpool.Pool
	stopping atomic.Bool
	metrics  *metrics.APIMetrics
}

// Health serves only health, readiness and metrics, for worker processes.
func Health(pool *pgxpool.Pool, reg *prometheus.Registry) *Handler {
	h := &Handler{mux: http.NewServeMux(), pool: pool}
	h.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") })
	h.mux.HandleFunc("GET /readyz", h.ready)
	h.mux.Handle("GET /metrics", metrics.Handler(reg))
	return h
}

func New(pool *pgxpool.Pool, reg *prometheus.Registry) *Handler {
	h := Health(pool, reg)
	c := durableq.NewClient(pool)
	h.metrics = metrics.API(reg, c)
	h.mux.HandleFunc("POST /v1/jobs", func(w http.ResponseWriter, r *http.Request) { h.enqueue(c, w, r) })
	h.mux.HandleFunc("GET /v1/jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil {
			writeError(w, 400, "id must be an integer")
			return
		}
		j, err := c.Job(r.Context(), id)
		if errors.Is(err, durableq.ErrNotFound) {
			writeError(w, 404, "job not found")
			return
		}
		if err != nil {
			internalError(w, err)
			return
		}
		writeJSON(w, 200, j)
	})
	h.mux.HandleFunc("GET /v1/stats", func(w http.ResponseWriter, r *http.Request) {
		s, err := c.Stats(r.Context())
		if err != nil {
			internalError(w, err)
			return
		}
		writeJSON(w, 200, s)
	})
	return h
}

// BeginShutdown removes readiness before draining HTTP requests or jobs.
func (h *Handler) BeginShutdown() { h.stopping.Store(true) }
func (h *Handler) ready(w http.ResponseWriter, r *http.Request) {
	if h.stopping.Load() {
		writeError(w, 503, "shutting down")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if err := h.pool.Ping(ctx); err != nil {
		writeError(w, 503, "database unavailable")
		return
	}
	if h.stopping.Load() {
		writeError(w, 503, "shutting down")
		return
	}
	io.WriteString(w, "ok")
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	sw := &statusWriter{ResponseWriter: w, code: 200}
	h.mux.ServeHTTP(sw, r)
	if h.metrics != nil {
		h.metrics.Requests.WithLabelValues(r.Pattern, strconv.Itoa(sw.code)).Inc()
	}
}

type statusWriter struct {
	http.ResponseWriter
	code     int
	wrote    bool
	suppress bool
}

func (w *statusWriter) WriteHeader(code int) {
	if w.wrote {
		return
	}
	w.wrote = true
	w.code = code
	// The mux's own 404/405 errors must obey the JSON error contract too.
	if (code == 404 || code == 405) && w.Header().Get("Content-Type") != "application/json" {
		w.Header().Set("Content-Type", "application/json")
		w.ResponseWriter.WriteHeader(code)
		json.NewEncoder(w.ResponseWriter).Encode(map[string]string{"error": http.StatusText(code)})
		w.suppress = true
		return
	}
	w.ResponseWriter.WriteHeader(code)
}
func (w *statusWriter) Write(p []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(200)
	}
	if w.suppress {
		return len(p), nil
	}
	return w.ResponseWriter.Write(p)
}

func (h *Handler) enqueue(c *durableq.Client, w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		var limit *http.MaxBytesError
		if errors.As(err, &limit) {
			writeError(w, 413, "body exceeds 1 MiB")
		} else {
			writeError(w, 400, "cannot read body")
		}
		return
	}
	var p struct {
		Kind           string          `json:"kind"`
		Payload        json.RawMessage `json:"payload"`
		MaxAttempts    *int            `json:"max_attempts"`
		DelayMS        *int64          `json:"delay_ms"`
		IdempotencyKey *string         `json:"idempotency_key"`
	}
	attempts := 5
	var delayMS int64
	var key string
	p.MaxAttempts, p.DelayMS, p.IdempotencyKey = &attempts, &delayMS, &key
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if err := d.Decode(&p); err != nil {
		writeError(w, 400, "invalid JSON body")
		return
	}
	if err := d.Decode(new(any)); err != io.EOF {
		writeError(w, 400, "body must contain one JSON object")
		return
	}
	if p.MaxAttempts == nil || p.DelayMS == nil || p.IdempotencyKey == nil {
		writeError(w, 400, "job parameters must not be null")
		return
	}
	payload := bytes.TrimSpace(p.Payload)
	if utf8.RuneCountInString(p.Kind) < 1 || utf8.RuneCountInString(p.Kind) > 200 || utf8.RuneCountInString(key) > 200 || attempts < 1 || attempts > 100 || delayMS < 0 || delayMS > math.MaxInt64/int64(time.Millisecond) || (len(payload) > 0 && payload[0] != '{') {
		writeError(w, 400, "invalid job parameters")
		return
	}
	result, err := c.Enqueue(r.Context(), durableq.EnqueueParams{Kind: p.Kind, Payload: p.Payload, MaxAttempts: attempts, Delay: time.Duration(delayMS) * time.Millisecond, IdempotencyKey: key})
	if err != nil {
		// Valid JSON may still exceed PostgreSQL's text or numeric representation.
		var dataErr *pgconn.PgError
		if errors.As(err, &dataErr) && strings.HasPrefix(dataErr.Code, "22") {
			writeError(w, 400, "job parameters cannot be represented in PostgreSQL")
			return
		}
		internalError(w, err)
		return
	}
	code := 201
	if result.Existed {
		code = 200
	} else {
		h.metrics.Enqueued.Inc()
	}
	writeJSON(w, code, struct {
		ID      int64 `json:"id"`
		Existed bool  `json:"existed"`
	}{result.ID, result.Existed})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
func writeError(w http.ResponseWriter, code int, message string) {
	writeJSON(w, code, map[string]string{"error": message})
}
func internalError(w http.ResponseWriter, err error) {
	slog.Error("API database operation failed", "err", err)
	writeError(w, 500, "internal server error")
}
