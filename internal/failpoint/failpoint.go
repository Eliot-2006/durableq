// Package failpoint kills the current process at named points with a
// configured probability, for chaos testing:
//
//	DURABLEQ_FAILPOINTS="after_claim=0.02,in_complete_tx=0.05"
//
// Hit is a no-op when the variable is unset.
package failpoint

import (
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
)

// Names lists the valid failpoints, in execution order.
var Names = []string{"after_claim", "during_run", "before_complete", "in_complete_tx", "after_commit"}

var points = mustParse(os.Getenv("DURABLEQ_FAILPOINTS"))

// Parse validates a failpoint spec and returns name → probability.
func Parse(spec string) (map[string]float64, error) {
	m := map[string]float64{}
	for _, kv := range strings.Split(spec, ",") {
		if kv = strings.TrimSpace(kv); kv == "" {
			continue
		}
		name, val, ok := strings.Cut(kv, "=")
		p, err := strconv.ParseFloat(val, 64)
		if !ok || err != nil || p < 0 || p > 1 || !valid(name) {
			return nil, fmt.Errorf("failpoint: bad entry %q (want name=probability, names %v)", kv, Names)
		}
		m[name] = p
	}
	return m, nil
}

func valid(name string) bool {
	for _, n := range Names {
		if n == name {
			return true
		}
	}
	return false
}

func mustParse(spec string) map[string]float64 {
	m, err := Parse(spec)
	if err != nil {
		panic(err)
	}
	return m
}

// Hit SIGKILLs the process with the configured probability for name.
func Hit(name string) {
	if p := points[name]; p > 0 && rand.Float64() < p {
		self, _ := os.FindProcess(os.Getpid())
		self.Kill()
		select {} // never proceed past a failpoint that fired
	}
}
