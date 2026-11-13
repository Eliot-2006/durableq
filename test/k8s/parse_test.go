//go:build k8s

package k8s

import "testing"

func TestParseChecks(t *testing.T) {
	valid := "  I1 PASS no lost jobs\n  I2 FAIL (2 violations) duplicates\n  I3 PASS effects\n  I4 PASS dead\n  I5 PASS owner\n  I6 PASS budget\n  I7 PASS terminal\n"
	got, err := parseChecks(valid)
	if err != nil || len(got) != 7 || got["I2"] != 2 {
		t.Fatalf("checks=%v err=%v", got, err)
	}
	for _, bad := range []string{"", valid[:30], valid + "  I1 PASS repeated\n"} {
		if _, err := parseChecks(bad); err == nil {
			t.Fatalf("accepted incomplete or repeated checks: %q", bad)
		}
	}
}

func TestStatusDrain(t *testing.T) {
	for _, tc := range []struct {
		text string
		open int
		bad  bool
	}{
		{"succeeded 10\ndead 2\n", 0, false},
		{"queued 2\nrunning 3\nsucceeded 4\n", 5, false},
		{"", 0, true}, {"unknown 1\n", 0, true}, {"running nope\n", 0, true},
	} {
		got, err := openJobs(tc.text)
		if (err != nil) != tc.bad || !tc.bad && got != tc.open {
			t.Fatalf("%q: got %d, %v", tc.text, got, err)
		}
	}
}

func TestContainerTime(t *testing.T) {
	for _, s := range []string{"1794526110536195944", "2026-11-12T23:28:30.536195944Z"} {
		got, err := containerTime(s)
		if err != nil || got.UnixNano() != 1794526110536195944 {
			t.Fatalf("%q: %v %v", s, got, err)
		}
	}
}

func TestResources(t *testing.T) {
	r, err := resources("durableq-api-a 10m 20Mi\ndurableq-worker-a 150m 30Mi\ndurableq-worker-b 250m 40Mi\ndurableq-postgres-0 1 1Gi")
	if err != nil || r.WorkerCPU != 400 || r.WorkerMiB != 70 || r.PostgresCPU != 1000 || r.PostgresMiB != 1024 || r.Workers != 2 || r.Postgres != 1 {
		t.Fatalf("%+v %v", r, err)
	}
	if _, err := resources("durableq-worker-a bad 1Mi"); err == nil {
		t.Fatal("accepted invalid CPU")
	}
}
