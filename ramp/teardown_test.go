package ramp

import (
	"context"
	"database/sql"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fb-loadgen/ops"
)

// A zero-length run (Warmup=Main=Cooldown=0) completes naturally on the first
// ticks: phase spans must record warmup → cooldown with real boundaries.
func TestTeardownPhaseSpansOnNaturalCompletion(t *testing.T) {
	s := testScheduler(t, &stubConnector{t: t}, &stubProfile{})
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		select {
		case <-s.Done():
			goto done
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("scheduler did not complete within 5s")
		}
		time.Sleep(10 * time.Millisecond)
	}
done:
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	spans := s.PhaseSpans()
	if len(spans) < 2 {
		t.Fatalf("got %d phase span(s), want >= 2 (warmup + cooldown): %+v", len(spans), spans)
	}
	if spans[0].Phase != "warmup" || spans[1].Phase != "cooldown" {
		t.Fatalf("span phases = %s, %s; want warmup, cooldown", spans[0].Phase, spans[1].Phase)
	}
	for _, sp := range spans {
		// Zero-length phases legitimately produce zero-length spans.
		if sp.End.Before(sp.Start) {
			t.Fatalf("span %s: End %v before Start %v", sp.Phase, sp.End, sp.Start)
		}
	}

	if td := s.Teardown(); td.StopFailures != 0 || td.Reaped != 0 {
		t.Fatalf("clean run produced StopFailures=%d Reaped=%d, want 0/0", td.StopFailures, td.Reaped)
	}
}

// Workers whose Stop times out (an in-flight op that ignores ctx cancel) must
// land in the teardown counters — the D5 acceptance metric.
func TestTeardownStopFailuresCounted(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })

	var inFlight atomic.Int64
	conn := &stubConnector{t: t}
	prof := &stubProfile{op: func(context.Context, *sql.Tx, *ops.Cache) error {
		inFlight.Add(1)
		<-release
		return nil
	}}

	s := testScheduler(t, conn, prof)
	s.workerStopTimeout = 100 * time.Millisecond

	// Ramp up directly (the zero-length run loop would complete before any
	// op starts): two workers, then park both inside the op.
	if err := s.ensureWorkerCount(2); err != nil {
		t.Fatalf("ensureWorkerCount up: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for inFlight.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("workers never started their operations")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Both workers are parked inside the op: their Stops must time out.
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	td := s.Teardown()
	if td.StopFailures != 2 {
		t.Fatalf("StopFailures = %d, want 2", td.StopFailures)
	}
	if td.DrainDuration <= 0 {
		t.Fatalf("DrainDuration = %v, want > 0 after a timed-out drain", td.DrainDuration)
	}

	releaseOnce.Do(func() { close(release) })
}

// Stop without Start must not fabricate phase spans.
func TestTeardownNoStartNoSpans(t *testing.T) {
	s := testScheduler(t, &stubConnector{t: t}, &stubProfile{})
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if spans := s.PhaseSpans(); len(spans) != 0 {
		t.Fatalf("never-started scheduler produced %d span(s)", len(spans))
	}
}
