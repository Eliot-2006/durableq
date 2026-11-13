package durableq

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Eliot-2006/durableq/internal/testdb"
)

func TestStatsAvoidsHistoryScan(t *testing.T) {
	pool := testdb.New(t, Migrations)
	ctx := context.Background()
	for _, sql := range []string{
		`INSERT INTO jobs(kind,state,finished_at) SELECT 'history','succeeded',clock_timestamp() FROM generate_series(1,200000)`,
		`INSERT INTO jobs(kind,state,run_at) VALUES ('pending','queued',clock_timestamp()-interval '1 hour'),('scheduled','queued',clock_timestamp()+interval '1 hour')`,
		`INSERT INTO jobs(kind,state,lease_owner,lease_expires_at) VALUES ('running','running','worker',clock_timestamp()+interval '1 hour'),('expired','running','worker',clock_timestamp()-interval '1 hour')`,
		`INSERT INTO jobs(kind,state,finished_at) VALUES ('dead','dead',clock_timestamp())`,
		`ANALYZE jobs`,
	} {
		if _, err := pool.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := pool.Query(ctx, "EXPLAIN "+qStats)
	if err != nil {
		t.Fatal(err)
	}
	var plan strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(line + "\n")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	t.Log("200000 succeeded + 5 live/dead rows, after ANALYZE:\n" + plan.String())
	if strings.Contains(plan.String(), "Seq Scan on jobs") {
		t.Fatal("stats scans history")
	}
	for _, index := range []string{"jobs_queued_run_at", "jobs_running_expiry", "jobs_dead"} {
		if !strings.Contains(plan.String(), index) {
			t.Errorf("missing partial index %s", index)
		}
	}
	stats, err := NewClient(pool).Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(stats)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != `{"pending":1,"scheduled":1,"running":1,"lease_expired":1,"dead":1}` {
		t.Fatalf("stats: %s", body)
	}
}
