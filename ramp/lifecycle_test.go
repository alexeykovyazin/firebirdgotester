package ramp

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fb-loadgen/config"
	"fb-loadgen/ops"
	"fb-loadgen/profile"
	"fb-loadgen/worker"
)

// --- fake driver -----------------------------------------------------------

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

// --- stubs -----------------------------------------------------------------

// stubConnector stands in for *db.ConnectionFactory. It hands each worker its
// own *sql.DB, so closing one worker's handle does not disturb the others.
type stubConnector struct {
	t *testing.T

	mu       sync.Mutex
	failNext bool
	opens    int
}

func (c *stubConnector) Open() (*sql.DB, error) {
	c.mu.Lock()
	c.opens++
	fail := c.failNext
	c.mu.Unlock()

	if fail {
		return nil, errors.New("failed to ping database: context deadline exceeded")
	}

	registerFake.Do(func() { sql.Register("fbloadgen_fake_ramp", fakeDriver{}) })
	db, err := sql.Open("fbloadgen_fake_ramp", "test")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

func (c *stubConnector) Close(db *sql.DB) error {
	if db == nil {
		return nil
	}
	return db.Close()
}

func (c *stubConnector) openCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.opens
}

type stubProfile struct {
	op func(context.Context, *sql.Tx, *ops.Cache) error
}

func (p *stubProfile) Name() string { return "stub" }

func (p *stubProfile) NextOp() func(context.Context, *sql.Tx, *ops.Cache) error {
	return p.op
}

func (p *stubProfile) NextOpWithName() (func(context.Context, *sql.Tx, *ops.Cache) error, string) {
	return p.op, "STUB_OP"
}

func (p *stubProfile) Weights() []profile.OpWeight { return nil }

func testScheduler(t *testing.T, conn worker.Connector, prof profile.Profile) *Scheduler {
	t.Helper()
	cfg := &config.Config{ConnMin: 1, ConnMax: 8, TxTimeout: 10, ThinkMs: 0}
	s := NewScheduler(cfg, conn, nil, prof, worker.NewMetricsCollector())
	t.Cleanup(func() {
		s.cancel()
		_ = s.drainWorkers()
	})
	return s
}

// workerIDs returns the IDs currently in the scheduler's set.
func (s *Scheduler) workerIDs() []int {
	s.workerMutex.RLock()
	defer s.workerMutex.RUnlock()
	ids := make([]int, 0, len(s.workers))
	for _, w := range s.workers {
		ids = append(ids, w.GetID())
	}
	return ids
}

// --- tests -----------------------------------------------------------------

// A worker that cannot open a connection must never enter the scheduler's set.
// Counting it would make the scheduler believe it has a worker that holds no
// database handle.
func TestEnsureWorkerCountSkipsWorkersThatCannotConnect(t *testing.T) {
	conn := &stubConnector{t: t, failNext: true}
	prof := &stubProfile{op: func(context.Context, *sql.Tx, *ops.Cache) error { return nil }}
	s := testScheduler(t, conn, prof)

	if err := s.ensureWorkerCount(4); err != nil {
		t.Fatalf("ensureWorkerCount: %v", err)
	}
	if got := s.GetCurrentWorkerCount(); got != 0 {
		t.Fatalf("worker count = %d after every connection failed, want 0", got)
	}
	// One failed worker ends the tick. Retrying all four in-line would stack
	// four connect timeouts into a single 500ms tick.
	if got := conn.openCount(); got != 1 {
		t.Fatalf("Open attempted %d times in one tick, want 1", got)
	}

	// The next tick retries.
	if err := s.ensureWorkerCount(4); err != nil {
		t.Fatalf("ensureWorkerCount (retry): %v", err)
	}
	if got := conn.openCount(); got != 2 {
		t.Fatalf("Open attempted %d times over two ticks, want 2", got)
	}

	// Once the database accepts connections again, the ramp recovers.
	conn.mu.Lock()
	conn.failNext = false
	conn.mu.Unlock()

	if err := s.ensureWorkerCount(3); err != nil {
		t.Fatalf("ensureWorkerCount (recovered): %v", err)
	}
	if got := s.GetCurrentWorkerCount(); got != 3 {
		t.Fatalf("worker count = %d after recovery, want 3", got)
	}
}

// A worker whose Stop times out must still leave the scheduler's set. Keeping
// it pinned the worker count, so the ramp could never shrink and every later
// tick retried the same doomed removal.
func TestEnsureWorkerCountRemovesWorkersWhoseStopTimesOut(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })

	var inFlight atomic.Int64
	conn := &stubConnector{t: t}
	prof := &stubProfile{op: func(context.Context, *sql.Tx, *ops.Cache) error {
		// Model an in-flight Firebird call that ignores context cancel.
		inFlight.Add(1)
		<-release
		return nil
	}}

	s := testScheduler(t, conn, prof)
	s.workerStopTimeout = 200 * time.Millisecond

	const n = 5
	if err := s.ensureWorkerCount(n); err != nil {
		t.Fatalf("ensureWorkerCount up: %v", err)
	}
	if got := s.GetCurrentWorkerCount(); got != n {
		t.Fatalf("worker count = %d after ramp up, want %d", got, n)
	}
	waitFor(t, 2*time.Second, "workers to start operations", func() bool {
		return inFlight.Load() >= n
	})

	began := time.Now()
	if err := s.ensureWorkerCount(0); err != nil {
		t.Fatalf("ensureWorkerCount down: %v", err)
	}
	elapsed := time.Since(began)

	if got := s.GetCurrentWorkerCount(); got != 0 {
		t.Fatalf("worker count = %d after ramping down, want 0; workers whose Stop timed out were left in the set", got)
	}
	// Stops run in parallel and outside the worker lock. Serially this would
	// take n * the stop timeout.
	if elapsed > 700*time.Millisecond {
		t.Fatalf("ramp down took %v for %d workers with a 200ms stop timeout, want parallel stops", elapsed, n)
	}

	// The scheduler can ramp back up immediately; it is not blocked by the
	// goroutines still unwinding.
	if err := s.ensureWorkerCount(2); err != nil {
		t.Fatalf("ensureWorkerCount up again: %v", err)
	}
	if got := s.GetCurrentWorkerCount(); got != 2 {
		t.Fatalf("worker count = %d after ramping back up, want 2", got)
	}
}

// Worker IDs must not be reused while an older worker with that ID is still
// unwinding, or the error log attributes two workers to one ID.
func TestWorkerIDsAreNotReusedAfterTimedOutStop(t *testing.T) {
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

	if err := s.ensureWorkerCount(3); err != nil {
		t.Fatalf("ensureWorkerCount up: %v", err)
	}
	waitFor(t, 2*time.Second, "workers to start operations", func() bool {
		return inFlight.Load() >= 3
	})
	if got := s.workerIDs(); len(got) != 3 || got[0] != 0 || got[1] != 1 || got[2] != 2 {
		t.Fatalf("initial worker IDs = %v, want [0 1 2]", got)
	}

	// Workers 1 and 2 are stuck, so their Stop times out; they leave the set
	// but their goroutines are still alive.
	if err := s.ensureWorkerCount(1); err != nil {
		t.Fatalf("ensureWorkerCount down: %v", err)
	}
	if err := s.ensureWorkerCount(3); err != nil {
		t.Fatalf("ensureWorkerCount up again: %v", err)
	}

	got := s.workerIDs()
	if len(got) != 3 {
		t.Fatalf("worker IDs = %v, want 3 workers", got)
	}
	seen := make(map[int]bool, len(got))
	for _, id := range got {
		if seen[id] {
			t.Fatalf("duplicate worker ID in %v", got)
		}
		seen[id] = true
	}
	if got[1] <= 2 || got[2] <= 2 {
		t.Fatalf("worker IDs = %v, want replacements above the still-unwinding IDs 1 and 2", got)
	}
}

// Healthy workers still ramp up and down exactly as before.
func TestHealthyWorkersRampUpAndDown(t *testing.T) {
	var opCalls atomic.Int64
	conn := &stubConnector{t: t}
	prof := &stubProfile{op: func(context.Context, *sql.Tx, *ops.Cache) error {
		opCalls.Add(1)
		return nil
	}}

	s := testScheduler(t, conn, prof)

	if err := s.ensureWorkerCount(4); err != nil {
		t.Fatalf("ensureWorkerCount up: %v", err)
	}
	if got := s.GetCurrentWorkerCount(); got != 4 {
		t.Fatalf("worker count = %d, want 4", got)
	}
	waitFor(t, 2*time.Second, "operations to run", func() bool { return opCalls.Load() > 0 })

	if err := s.ensureWorkerCount(2); err != nil {
		t.Fatalf("ensureWorkerCount down: %v", err)
	}
	if got := s.GetCurrentWorkerCount(); got != 2 {
		t.Fatalf("worker count = %d after shrinking, want 2", got)
	}

	if err := s.ensureWorkerCount(0); err != nil {
		t.Fatalf("ensureWorkerCount to zero: %v", err)
	}
	if got := s.GetCurrentWorkerCount(); got != 0 {
		t.Fatalf("worker count = %d after draining, want 0", got)
	}
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
