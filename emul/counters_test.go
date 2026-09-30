package emul

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestObserveInvariant(t *testing.T) {
	s := &EmulState{startedAt: time.Now()}
	s.ObserveInvariant(InvOK, "")
	s.ObserveInvariant(InvLockConflict, "lock conflict on no wait transaction")
	s.ObserveInvariant(InvBeginFail, "")
	s.ObserveInvariant(InvHard, "failed (attempt 1 of 3)")
	s.ObserveInvariant(InvDisabled, "disabled: invariant check failed 3 times in a row")

	st := s.InvariantStats()
	if st.Checks != 5 {
		t.Fatalf("Checks = %d, want 5", st.Checks)
	}
	if st.OK != 1 || st.Transient != 2 || st.Hard != 1 || !st.Disabled {
		t.Fatalf("counters wrong: %+v", st)
	}
	if st.LastReason != "disabled: invariant check failed 3 times in a row" {
		t.Fatalf("LastReason = %q", st.LastReason)
	}
}

func TestBumpMax(t *testing.T) {
	var v atomic.Int64
	bumpMax(&v, 5)
	if v.Load() != 5 {
		t.Fatalf("after bumpMax(5): %d", v.Load())
	}
	bumpMax(&v, 3)
	if v.Load() != 5 {
		t.Fatalf("bumpMax lowered the value: %d", v.Load())
	}
	bumpMax(&v, 7)
	if v.Load() != 7 {
		t.Fatalf("after bumpMax(7): %d", v.Load())
	}
}
