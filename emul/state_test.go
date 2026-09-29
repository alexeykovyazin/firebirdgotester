package emul

import (
	"context"
	"database/sql"
	"errors"
	"os"
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

// The soak on ubuntu-5 (2026-09-29, D1) saw FB 4.0.8 surface deferred
// semaphores-record contention inside SRV_MAKE_INVNT_SALDO as
// "can`t lock semaphores.id=3, deferred" — a transient lock conflict by
// nature, but with none of the words the pre-fix classifier matched, so
// three consecutive checks disabled the invariant loop for the rest of the
// run. Golden strings: the exact soak message, the same message with a
// straight apostrophe (client version skew), a classic lock conflict, and a
// genuine hard failure that must stay hard.
func TestInvariantLockConflictSemaphoresDeferred(t *testing.T) {
	soakMsg := "emul: invariant SRV_MAKE_INVNT_SALDO failed: exception 8\n" +
		"EX_CANT_LOCK_SEMAPHORE_RECORD\n" +
		"2026-09-29T18:39:48.6140 ATT_179 TRA_10056 can`t lock semaphores.id=3, deferred\n" +
		"At procedure 'SRV_MAKE_INVNT_SALDO' line: 107, col: 9\n" +
		"At procedure 'SRV_MAKE_INVNT_SALDO' line: 241, col: 9"
	tests := []struct {
		name string
		err  string
		want bool
	}{
		{"exact soak message is transient", soakMsg, true},
		{"straight apostrophe variant is transient",
			"exception 8 EX_CANT_LOCK_SEMAPHORE_RECORD can't lock semaphores.id=1, deferred", true},
		{"classic lock conflict stays transient",
			"lock conflict on no wait transaction", true},
		{"deadlock stays transient", "deadlock", true},
		{"limbo-stuck record under own limbo load is transient",
			"emul: invariant SRV_MAKE_INVNT_SALDO failed: record from transaction 43811 is stuck in limbo", true},
		{"connection refused stays hard", "connection refused", false},
		{"semaphore exception without lock text stays hard",
			"exception 8 EX_CANT_LOCK_SEMAPHORE_RECORD something else", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := invariantLockConflict(errors.New(tt.err)); got != tt.want {
				t.Errorf("invariantLockConflict(%q) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// The monitor pins one pooled connection for its whole lifetime; with the
// factory's max-1 pool that starved the invariant loop's BeginTx silently
// (it blocked until the run context was cancelled). RunSidecars must size
// the shared sidecar pool for all its consumers. Regression needs a live
// server: a second sql.Conn must be obtainable while the monitor holds the
// first one.
func TestRunSidecarsPoolsEnoughConnections(t *testing.T) {
	dsn := os.Getenv("FIREBIRD_TEST_DSN")
	if dsn == "" {
		t.Skip("FIREBIRD_TEST_DSN not set")
	}
	db, err := sql.Open("firebirdsql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1) // simulate the factory cap that starved the loop
	if err := db.Ping(); err != nil {
		t.Skipf("server unreachable: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	state := RunSidecars(ctx, db, 30*time.Millisecond, 0, time.Second, nil, nil)
	// cancel must precede Wait: the monitor goroutine exits only on ctx.Done.
	defer func() {
		cancel()
		state.Wait()
	}()

	// The monitor's first sample happens immediately, so its pinned conn is
	// taken by the time we get here.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		c2, err := db.Conn(ctx)
		if err == nil {
			_ = c2.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("pool not sized for a second connection while the monitor holds one: invariant loop would starve")
}
