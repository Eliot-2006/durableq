package durableq

import (
	"context"
	"testing"
)

func TestWorkerKindsCopy(t *testing.T) {
	w := NewWorker(nil, WorkerConfig{})
	if len(w.Kinds()) != 0 {
		t.Fatal(w.Kinds())
	}
	h := func(context.Context, Job) (CommitFunc, error) { return nil, nil }
	w.Handle("one", h)
	w.Handle("two", h)
	w.Handle("one", h)
	kinds := w.Kinds()
	if len(kinds) != 2 || kinds[0] != "one" || kinds[1] != "two" {
		t.Fatal(kinds)
	}
	kinds[0] = "changed"
	if w.Kinds()[0] != "one" {
		t.Fatal("Kinds exposes worker storage")
	}
}
