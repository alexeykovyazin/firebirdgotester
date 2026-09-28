package emul

import (
	"sync"
	"testing"
)

// TestSelectorConcurrent hammers Pick from many goroutines: one Selector is
// shared by all oltp-emul workers of a session, so unsynchronized state fails
// under `go test -race`.
func TestSelectorConcurrent(t *testing.T) {
	s := NewSelector([]Unit{
		{Name: "SP_ORDER", Mode: "stock", Kind: "creation", Weight: 30},
		{Name: "SP_PAY", Mode: "payments", Kind: "state_next", Weight: 40},
		{Name: "SP_SERVICE", Mode: "service", Kind: "service", Weight: 30},
	})

	allowed := map[string]bool{"creation": true, "state_next": true}

	const goroutines = 8
	const iterations = 5000
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				u, ok := s.Pick(allowed)
				if !ok {
					t.Errorf("Pick returned !ok on a non-empty registry")
					return
				}
				if u.Name == "" {
					t.Errorf("Pick returned an empty unit name")
					return
				}
			}
		}()
	}
	wg.Wait()
}
