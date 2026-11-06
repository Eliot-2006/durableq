package durableq

import (
	"math/rand/v2"
	"time"
)

// BackoffCeiling is min(cap, base * 2^(attempt-1)), overflow-safe.
func BackoffCeiling(attempt int, base, cap time.Duration) time.Duration {
	k := max(attempt-1, 0)
	if k >= 62 || base > cap>>k {
		return cap
	}
	return base << k
}

// FullJitter returns a delay uniformly drawn from [0, BackoffCeiling].
func FullJitter(attempt int, base, cap time.Duration) time.Duration {
	return time.Duration(rand.Int64N(int64(BackoffCeiling(attempt, base, cap)) + 1))
}
