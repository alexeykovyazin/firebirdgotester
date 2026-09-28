package profile

import (
	"context"
	"database/sql"
	"sync"
	"testing"

	"fb-loadgen/ops"
)

func nopOp(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error { return nil }

// TestWeightedSelectorConcurrent hammers Select from many goroutines: one
// selector is shared by all workers of a run, so unsynchronized state fails
// under `go test -race`.
func TestWeightedSelectorConcurrent(t *testing.T) {
	ws := NewWeightedSelector([]OpWeight{
		{Weight: 25, Op: nopOp, Name: "op1"},
		{Weight: 50, Op: nopOp, Name: "op2"},
		{Weight: 25, Op: nopOp, Name: "op3"},
	})

	const goroutines = 8
	const iterations = 5000
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				op, name := ws.Select()
				if op == nil || name == "" {
					t.Errorf("Select returned nil op or empty name")
					return
				}
			}
		}()
	}
	wg.Wait()
}
