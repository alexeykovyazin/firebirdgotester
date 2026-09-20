package emul

import (
	"context"
	"errors"
	"testing"
	"time"
)

// RunSidecars must stop all sidecar goroutines promptly after the run
// context is cancelled — the session engine relies on this on every finish
// path.
func TestRunSidecarsStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	counts := func() (int64, int64, string) { return 0, 0, "main" }
	state := RunSidecars(ctx, nil, 0, 0, 20*time.Millisecond, counts, nil)
	time.Sleep(80 * time.Millisecond) // let a few series ticks run
	cancel()

	done := make(chan struct{})
	go func() {
		state.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("sidecar goroutines did not exit within 2s of cancel")
	}
}

// With a nil db the monitor and invariant sidecars must not start at all
// (the series ticker alone runs).
func TestRunSidecarsNilDbOnlyTicker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	// phase "main" — only main-phase units are scored (upstream semantics)
	counts := func() (int64, int64, string) { return 7, 9, "main" }
	state := RunSidecars(ctx, nil, 0, 0, 20*time.Millisecond, counts, nil)
	time.Sleep(70 * time.Millisecond)
	cancel()
	state.Wait()

	_, ok, total, _, _, _ := state.Final()
	if ok != 7 || total != 9 {
		t.Errorf("counts not propagated: ok=%d total=%d", ok, total)
	}
	// the score is per-interval; the last interval may be zero — assert that
	// at least one recorded series point carried a positive score
	js := state.JSON(nil, nil)
	sawPositive := false
	for _, p := range js.Series {
		if p.ScorePerMin > 0 {
			sawPositive = true
		}
	}
	if !sawPositive {
		t.Errorf("no series point with a positive score: %+v", js.Series)
	}
}

func TestInvariantFailureClassification(t *testing.T) {
	tests := []struct {
		name      string
		err       string
		streak    int
		permanent bool
		wantSub   string
	}{
		{
			name:      "fb3 driver limitation disables immediately",
			err:       "EX_SNAPSHOT_ISOLATION_REQUIRED: operation must run only in TIL = SNAPSHOT",
			streak:    0,
			permanent: true,
			wantSub:   "disabled: server requires a snapshot+nowait transaction",
		},
		{
			name:      "nowait requirement also disables",
			err:       "EX_NOWAIT_OR_TIMEOUT_REQUIRED: transaction must start in NO WAIT mode",
			streak:    2,
			permanent: true,
			wantSub:   "disabled: server requires a snapshot+nowait transaction",
		},
		{
			name:      "transient failure retries before the limit",
			err:       "connection refused",
			streak:    0,
			permanent: false,
			wantSub:   "failed (attempt 1 of 3, will retry)",
		},
		{
			name:      "third consecutive failure gives up",
			err:       "connection refused",
			streak:    2,
			permanent: true,
			wantSub:   "disabled: invariant check failed 3 times in a row",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			permanent, msg := invariantFailure(errors.New(tt.err), tt.streak)
			if permanent != tt.permanent {
				t.Errorf("permanent = %v, want %v (msg=%q)", permanent, tt.permanent, msg)
			}
			if len(msg) < len(tt.wantSub) || !containsAny(msg, tt.wantSub) {
				t.Errorf("msg %q does not contain %q", msg, tt.wantSub)
			}
		})
	}
}
