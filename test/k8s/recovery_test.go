//go:build k8s

package k8s

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Eliot-2006/durableq"
	"github.com/Eliot-2006/durableq/internal/testdb"
)

func TestRecoveryExcludesCompletedSnapshot(t *testing.T) {
	pool := testdb.New(t, durableq.Migrations)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, qRecoveryFixture); err != nil {
		t.Fatal(err)
	}
	query := recoveryQuery([]claim{{1, 1}, {2, 1}, {3, 1}})
	for _, pending := range []int{1, 0} {
		var raw []byte
		if err := pool.QueryRow(ctx, query).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var got recovery
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		if got.Pending != pending || len(got.Abandoned) != 2 || len(got.Claims) != 2-pending {
			t.Fatalf("completed snapshot should not need another claim: %s", raw)
		}
		for _, c := range got.Claims {
			if c.ClaimedAt.IsZero() {
				t.Fatal("missing recovery timestamp")
			}
			if c.ID == 1 {
				t.Fatal("completed job included as recovered")
			}
		}
		if pending == 1 {
			if _, err := pool.Exec(ctx, qRecoveryFixtureNext); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestKilledClaimsIncludesPostSnapshotClaims(t *testing.T) {
	pool := testdb.New(t, durableq.Migrations)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, qRecoveryFixture); err != nil {
		t.Fatal(err)
	}
	// Job 1 closed after the snapshot; job 3 was claimed by that same process.
	// Job 2's old abandoned attempt was already reclaimed before this kill.
	query := killedClaimsQuery(claim{ID: 1, Attempt: 1}, time.Now().UTC().Format(time.RFC3339Nano))
	var raw []byte
	if err := pool.QueryRow(ctx, query).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var got []claim
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != 3 {
		t.Fatalf("expected all current killed claims, not just snapshot or older abandoned attempts: %s", raw)
	}
}
