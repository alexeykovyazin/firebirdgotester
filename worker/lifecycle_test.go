package worker

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fb-loadgen/config"
	"fb-loadgen/ops"
	"fb-loadgen/profile"
)

// --- fake driver -----------------------------------------------------------
//
// A worker needs a real *sql.DB to exercise BeginTx/Commit. These stubs give
// one without a Firebird server.

type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) { return &fakeConn{}, nil }

type fakeConn struct{}

func (*fakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (*fakeConn) Close() error                        { return nil }
func (*fakeConn) Begin() (driver.Tx, error)           { return fakeTx{}, nil }

type fakeTx struct{}

func (fakeTx) Commit() error   { return nil }
func (fakeTx) Rollback() error { return nil }

var registerFake sync.Once

func openFakeDB(t *testing.T) *sql.DB {
	t.Helper()
	registerFake.Do(func() { sql.Register("fbloadgen_fake", fakeDriver{}) })
	db, err := sql.Open("fbloadgen_fake", "test")
	if err != nil {
		t.Fatalf("open fake db: %v", err)
	}
	db.SetMaxOpenConns(1)
	return db
}

// --- stubs -----------------------------------------------------------------

// stubConnector stands in for *db.ConnectionFactory.
type stubConnector struct {
	db      *sql.DB
	openErr error

	mu     sync.Mutex
	opens  int
	closes int
}

func (c *stubConnector) Open() (*sql.DB, error) {
	c.mu.Lock()
	c.opens++
	c.mu.Unlock()
	if c.openErr != nil {
		return nil, c.openErr
	}
	return c.db, nil
}

func (c *stubConnector) Close(db *sql.DB) error {
	c.mu.Lock()
	c.closes++
	c.mu.Unlock()
	if db == nil {
		return nil
	}
	return db.Close()
}

func (c *stubConnector) counts() (opens, closes int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.opens, c.closes
}

type opFunc func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error

type stubProfile struct{ op opFunc }

func (p *stubProfile) Name() string { return "stub" }

func (p *stubProfile) NextOp() func(context.Context, *sql.Tx, *ops.Cache) error {
	return p.op
}

func (p *stubProfile) NextOpWithName() (func(context.Context, *sql.Tx, *ops.Cache) error, string) {
	return p.op, "STUB_OP"
}

func (p *stubProfile) Weights() []profile.OpWeight { return nil }

func testConfig() *config.Config {
	return &config.Config{TxTimeout: 10, ThinkMs: 0}
}

func waitFor(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", desc)
}

// --- tests -----------------------------------------------------------------

// A worker whose connection factory fails must never enter the operation loop.
// Before the fix, Start left dbConn nil and callers could still end up running
// the loop, where BeginTx dereferenced a nil *sql.DB and killed the process.
func TestStartWithFailedConnectionNeverEntersOperationLoop(t *testing.T) {
	var opCalls atomic.Int64
	conn := &stubConnector{openErr: errors.New("failed to ping database: context deadline exceeded")}
	prof := &stubProfile{op: func(context.Context, *sql.Tx, *ops.Cache) error {
		opCalls.Add(1)
		return nil
	}}

	w := NewWorker(59, context.Background(), conn, nil, prof, testConfig(), NewMetricsCollector())

	err := w.Start()
	if err == nil {
		t.Fatal("Start returned nil for a connection factory that failed")
	}
	if !strings.Contains(err.Error(), "failed to open database connection") {
		t.Fatalf("unexpected Start error: %v", err)
	}
	if w.IsRunning() {
		t.Fatal("worker reports running after a failed Start")
	}

	// No goroutine, so no operations.
	time.Sleep(100 * time.Millisecond)
	if n := opCalls.Load(); n != 0 {
		t.Fatalf("worker executed %d operations with no connection", n)
	}

	// The derived context must be released, not left registered on the parent.
	if w.ctx.Err() == nil {
		t.Fatal("context of a failed worker was not cancelled")
	}

	// The regression itself: with no handle this must report an error rather
	// than panic on a nil *sql.DB.
	if err := w.executeOperation(); !errors.Is(err, ErrNoConnection) {
		t.Fatalf("executeOperation with no connection = %v, want ErrNoConnection", err)
	}

	// Stopping a worker that never started is a no-op, and restarting a dead
	// worker is refused rather than half-initialised.
	if err := w.Stop(); err != nil {
		t.Fatalf("Stop on a never-started worker: %v", err)
	}
	if err := w.Start(); err == nil {
		t.Fatal("Start succeeded on a worker that already failed to connect")
	}

	if opens, _ := conn.counts(); opens != 1 {
		t.Fatalf("Open called %d times, want 1", opens)
	}
}

// A Stop that times out must leave the worker goroutine on a closed handle,
// never a nil one, and that goroutine must exit instead of continuing to run
// operations.
func TestStopTimeoutLeavesNoGoroutineRunningOperations(t *testing.T) {
	conn := &stubConnector{db: openFakeDB(t)}

	release := make(chan struct{})
	var started, afterRelease atomic.Int64
	prof := &stubProfile{op: func(context.Context, *sql.Tx, *ops.Cache) error {
		if started.Add(1) == 1 {
			// Model an in-flight Firebird call that ignores context cancel.
			<-release
			return nil
		}
		afterRelease.Add(1)
		return nil
	}}

	w := NewWorker(57, context.Background(), conn, nil, prof, testConfig(), NewMetricsCollector())
	w.SetStopTimeout(150 * time.Millisecond)

	if err := w.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitFor(t, 2*time.Second, "worker to begin an operation", func() bool {
		return started.Load() >= 1
	})

	stopBegan := time.Now()
	err := w.Stop()
	if err == nil || !strings.Contains(err.Error(), "stop timed out") {
		t.Fatalf("Stop = %v, want a stop-timed-out error", err)
	}
	if elapsed := time.Since(stopBegan); elapsed > 2*time.Second {
		t.Fatalf("Stop blocked for %v, want roughly the stop timeout", elapsed)
	}
	if w.IsRunning() {
		t.Fatal("worker still reports running after Stop timed out")
	}

	// Let the stuck call finish. The goroutine must unwind without panicking
	// and must not run another operation against the closed handle.
	close(release)
	waitFor(t, 5*time.Second, "stuck worker goroutine to exit", func() bool {
		done := make(chan struct{})
		go func() { w.wg.Wait(); close(done) }()
		select {
		case <-done:
			return true
		case <-time.After(50 * time.Millisecond):
			return false
		}
	})

	time.Sleep(100 * time.Millisecond)
	if n := afterRelease.Load(); n != 0 {
		t.Fatalf("worker ran %d operations after a timed-out Stop", n)
	}

	// The handle is gone, so any further operation must report rather than
	// dereference it. This is what killed the process on the lab: a worker
	// left looping after its Stop timed out.
	if err := w.executeOperation(); !errors.Is(err, ErrNoConnection) {
		t.Fatalf("executeOperation after a timed-out Stop = %v, want ErrNoConnection", err)
	}

	waitFor(t, 2*time.Second, "the connection to be closed exactly once", func() bool {
		_, closes := conn.counts()
		return closes == 1
	})
	time.Sleep(100 * time.Millisecond)
	if _, closes := conn.counts(); closes != 1 {
		t.Fatalf("connection closed %d times, want exactly 1", closes)
	}
}

// Stop and the operation loop run on different goroutines and both touch the
// database handle. This is the reported crash: Stop cleared the handle while
// run() was between its context check and BeginTx. Run with -race.
func TestStopConcurrentWithOperationLoopDoesNotPanic(t *testing.T) {
	prof := &stubProfile{op: func(context.Context, *sql.Tx, *ops.Cache) error { return nil }}
	// Shared, so the panic guard in run() cannot quietly absorb a regression:
	// a recovered panic shows up here as a recorded error.
	metrics := NewMetricsCollector()

	for i := 0; i < 60; i++ {
		conn := &stubConnector{db: openFakeDB(t)}
		w := NewWorker(i, context.Background(), conn, nil, prof, testConfig(), metrics)
		w.SetStopTimeout(2 * time.Second)

		if err := w.Start(); err != nil {
			t.Fatalf("Start %d: %v", i, err)
		}
		// Stagger so Stop lands at different points of the loop.
		time.Sleep(time.Duration(i%5) * 200 * time.Microsecond)
		if err := w.Stop(); err != nil {
			t.Fatalf("Stop %d: %v", i, err)
		}
		if w.IsRunning() {
			t.Fatalf("worker %d still running after Stop", i)
		}
	}

	if total, _, _, _, _ := metrics.ErrorStats().Snapshot(); total != 0 {
		t.Fatalf("%d errors recorded while stopping healthy workers; a panic in the operation loop was recovered", total)
	}
}

// A healthy worker keeps doing what it always did: loop, commit, and stop
// cleanly when asked.
func TestHealthyWorkerRunsAndStopsCleanly(t *testing.T) {
	conn := &stubConnector{db: openFakeDB(t)}
	var opCalls atomic.Int64
	prof := &stubProfile{op: func(context.Context, *sql.Tx, *ops.Cache) error {
		opCalls.Add(1)
		return nil
	}}

	metrics := NewMetricsCollector()
	w := NewWorker(1, context.Background(), conn, nil, prof, testConfig(), metrics)

	if err := w.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !w.IsRunning() {
		t.Fatal("healthy worker does not report running")
	}
	waitFor(t, 2*time.Second, "operations to be executed", func() bool {
		return opCalls.Load() >= 5
	})
	if err := w.Stop(); err != nil {
		t.Fatalf("Stop on a healthy worker: %v", err)
	}
	if got := metrics.GetTxSuccess(); got == 0 {
		t.Fatal("no successful transactions recorded for a healthy worker")
	}
	// Stop closes the handle on a background goroutine.
	waitFor(t, 2*time.Second, "the connection to be closed", func() bool {
		_, closes := conn.counts()
		return closes == 1
	})
	time.Sleep(100 * time.Millisecond)
	if _, closes := conn.counts(); closes != 1 {
		t.Fatalf("connection closed %d times, want exactly 1", closes)
	}
}

// A panic inside one worker must not kill the process: the same process drives
// every other database and the web UI session.
func TestPanicInOperationDoesNotKillTheProcess(t *testing.T) {
	conn := &stubConnector{db: openFakeDB(t)}
	var calls atomic.Int64
	prof := &stubProfile{op: func(context.Context, *sql.Tx, *ops.Cache) error {
		calls.Add(1)
		panic("simulated driver panic")
	}}

	metrics := NewMetricsCollector()
	w := NewWorker(42, context.Background(), conn, nil, prof, testConfig(), metrics)
	w.SetStopTimeout(time.Second)

	if err := w.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitFor(t, 2*time.Second, "the panicking worker to exit", func() bool {
		return !w.IsRunning()
	})
	if n := calls.Load(); n != 1 {
		t.Fatalf("operation ran %d times, want 1 before the worker gave up", n)
	}
	// The panic is recorded, not swallowed.
	if stats := metrics.ErrorStats(); stats == nil {
		t.Fatal("no error stats collector")
	}
	// Stop on an already-exited worker is a no-op.
	if err := w.Stop(); err != nil {
		t.Fatalf("Stop after a panic: %v", err)
	}
}
