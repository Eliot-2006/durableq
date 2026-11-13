package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Eliot-2006/durableq"
	"github.com/Eliot-2006/durableq/internal/testdb"
	"github.com/prometheus/client_golang/prometheus"
)

func TestAPI(t *testing.T) {
	pool := testdb.New(t, durableq.Migrations)
	h := New(pool, prometheus.NewRegistry())
	request := func(method, path, body string, code int) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRecorder()
		h.ServeHTTP(r, httptest.NewRequest(method, path, strings.NewReader(body)))
		if r.Code != code {
			t.Fatalf("%s %s: %d %s", method, path, r.Code, r.Body)
		}
		if code >= 400 {
			var body map[string]string
			if err := json.Unmarshal(r.Body.Bytes(), &body); err != nil || body["error"] == "" {
				t.Fatalf("expected JSON error: %s (%v)", r.Body, err)
			}
		}
		return r
	}
	for _, body := range []string{`{}`, `{`, `{"kind":"x","wat":1}`, `{"kind":"x","max_attempts":0}`, `{"kind":"x","max_attempts":101}`, `{"kind":"x","delay_ms":-1}`, `{"kind":"x","max_attempts":null}`, `{"kind":"x","delay_ms":null}`, `{"kind":"x","idempotency_key":null}`, `{"kind":"x","payload":[]}`, `{"kind":"x","payload":null}`, `{"kind":"bad\u0000kind"}`, `{"kind":"x","idempotency_key":"bad\u0000key"}`, `{"kind":"x","payload":{"value":"\u0000"}}`, `{"kind":"x","payload":{"value":1e1000000000}}`, `{"kind":"x","delay_ms":9223372036854775807}`, `{"kind":"x"} {}`, `{"kind":"` + strings.Repeat("x", 201) + `"}`, `{"kind":"x","idempotency_key":"` + strings.Repeat("x", 201) + `"}`} {
		request("POST", "/v1/jobs", body, 400)
	}
	request("POST", "/v1/jobs", `{"kind":"x","payload":{"huge":"`+strings.Repeat("x", 1<<20)+`"}}`, 413)
	r := request("POST", "/v1/jobs", `{"kind":"x","idempotency_key":"same"}`, 201)
	var first struct {
		ID      int64 `json:"id"`
		Existed bool  `json:"existed"`
	}
	if err := json.Unmarshal(r.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	if first.ID <= 0 || first.Existed {
		t.Fatal(first)
	}
	r = request("POST", "/v1/jobs", `{"kind":"x","idempotency_key":"same"}`, 200)
	if !strings.Contains(r.Body.String(), `"existed":true`) {
		t.Fatal(r.Body)
	}
	r = request("GET", fmt.Sprintf("/v1/jobs/%d", first.ID), "", 200)
	for _, field := range []string{`"finished_at":null`, `"last_error":null`, `"attempts":[]`} {
		if !strings.Contains(r.Body.String(), field) {
			t.Fatalf("missing %s: %s", field, r.Body)
		}
	}
	w := durableq.NewWorker(pool, durableq.WorkerConfig{Poll: time.Millisecond})
	w.Handle("x", func(context.Context, durableq.Job) (durableq.CommitFunc, error) { return nil, nil })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		j, err := durableq.NewClient(pool).Job(ctx, first.ID)
		if err != nil {
			t.Fatal(err)
		}
		if j.State == "succeeded" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("job did not complete")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	r = request("GET", fmt.Sprintf("/v1/jobs/%d", first.ID), "", 200)
	var j durableq.JobInfo
	if err := json.Unmarshal(r.Body.Bytes(), &j); err != nil {
		t.Fatal(err)
	}
	if len(j.Attempts) != 1 || j.Attempts[0].Outcome == nil || *j.Attempts[0].Outcome != "succeeded" {
		t.Fatal(j)
	}
	request("GET", "/unknown/path", "", 404)
	request("DELETE", "/v1/jobs/1", "", 405)
	request("GET", "/v1/jobs/nope", "", 400)
	request("GET", "/v1/jobs/99999999", "", 404)
	c := durableq.NewClient(pool)
	for _, p := range []durableq.EnqueueParams{{Kind: "x"}, {Kind: "x", Delay: time.Hour}, {Kind: "x"}, {Kind: "x"}, {Kind: "x"}} {
		if _, err := c.Enqueue(context.Background(), p); err != nil {
			t.Fatal(err)
		}
	}
	_, err := pool.Exec(context.Background(), `UPDATE jobs SET state='running',lease_owner='test',lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=(SELECT max(id)-2 FROM jobs)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(context.Background(), `UPDATE jobs SET state='running',lease_owner='test',lease_expires_at=clock_timestamp()+interval '1 hour' WHERE id=(SELECT max(id)-1 FROM jobs)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(context.Background(), `UPDATE jobs SET state='dead',finished_at=clock_timestamp() WHERE id=(SELECT max(id) FROM jobs)`)
	if err != nil {
		t.Fatal(err)
	}
	r = request("GET", "/v1/stats", "", 200)
	if strings.Contains(r.Body.String(), `"succeeded"`) {
		t.Fatalf("unbounded history count: %s", r.Body)
	}
	var stats durableq.QueueStats
	if err := json.Unmarshal(r.Body.Bytes(), &stats); err != nil {
		t.Fatal(err)
	}
	if stats != (durableq.QueueStats{Pending: 1, Scheduled: 1, Running: 1, LeaseExpired: 1, Dead: 1}) {
		t.Fatal(stats)
	}
	request("GET", "/readyz", "", 200)
	h.BeginShutdown()
	request("GET", "/readyz", "", 503)
	request("GET", "/healthz", "", 200)
	r = request("GET", "/metrics", "", 200)
	for _, name := range []string{"jobs_enqueued_total counter", "http_requests_total counter", "jobs_pending gauge", "jobs_scheduled gauge", "jobs_running gauge", "jobs_lease_expired gauge", "jobs_dead gauge"} {
		if !strings.Contains(r.Body.String(), "# TYPE durableq_"+name) {
			t.Errorf("missing %s", name)
		}
	}
	if strings.Contains(r.Body.String(), "durableq_jobs_succeeded") {
		t.Fatal("succeeded gauge must not be exported")
	}
	for _, sample := range []string{"durableq_jobs_enqueued_total 1", "durableq_jobs_pending 1", "durableq_jobs_scheduled 1", "durableq_jobs_running 1", "durableq_jobs_lease_expired 1", "durableq_jobs_dead 1"} {
		if !strings.Contains(r.Body.String(), sample) {
			t.Errorf("missing %s", sample)
		}
	}
	if !strings.Contains(r.Body.String(), `route="GET /v1/jobs/{id}"`) || strings.Contains(r.Body.String(), `route="/v1/jobs/99999999"`) {
		t.Fatal(r.Body)
	}
	pool.Close()
	request("GET", "/readyz", "", 503)
	request("GET", "/healthz", "", 200)
	r = request("GET", "/metrics", "", 200)
	if !strings.Contains(r.Body.String(), "go_goroutines") {
		t.Fatal("ContinueOnError lost process metrics")
	}
}
