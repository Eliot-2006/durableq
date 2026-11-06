package durableq_test

import (
	"testing"
	"time"

	"github.com/Eliot-2006/durableq"
)

func TestBackoff(t *testing.T) {
	base, cap := time.Second, 10*time.Minute
	if got := durableq.BackoffCeiling(1, base, cap); got != base {
		t.Fatalf("attempt 1 ceiling = %v, want %v", got, base)
	}
	if got := durableq.BackoffCeiling(4, base, cap); got != 8*time.Second {
		t.Fatalf("attempt 4 ceiling = %v, want 8s", got)
	}
	prev := time.Duration(0)
	for a := 1; a <= 200; a++ { // far past overflow of base<<a
		c := durableq.BackoffCeiling(a, base, cap)
		if c < prev || c > cap {
			t.Fatalf("attempt %d: ceiling %v not monotone in [%v, cap]", a, c, prev)
		}
		prev = c
		for range 50 {
			if d := durableq.FullJitter(a, base, cap); d < 0 || d > c {
				t.Fatalf("attempt %d: jitter %v outside [0, %v]", a, d, c)
			}
		}
	}
	if prev != cap {
		t.Fatalf("ceiling never reached cap: %v", prev)
	}
}
