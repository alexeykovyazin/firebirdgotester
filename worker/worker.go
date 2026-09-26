package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"fb-loadgen/config"
	"fb-loadgen/emul"
	"fb-loadgen/errlog"
	"fb-loadgen/ops"
	"fb-loadgen/opslog"
	"fb-loadgen/profile"
)

// DebugEnabled controls debug output. The constructor writes it while worker
// goroutines from previous constructions may still read it, so it is atomic.
var DebugEnabled atomic.Bool

// defaultStopTimeout bounds how long Stop waits for the worker goroutine.
const defaultStopTimeout = 5 * time.Second

// ErrNoConnection reports a worker with no usable database handle, either
// because the connection was never opened or because Stop closed it. The run
// loop treats it as fatal: the worker exits and the ramp scheduler is free to
// start a replacement.
var ErrNoConnection = errors.New("worker has no usable database connection")

// Connector opens and closes the per-worker database handle.
// *db.ConnectionFactory satisfies it; tests substitute a stub.
type Connector interface {
	Open() (*sql.DB, error)
	Close(db *sql.DB) error
}

// Worker represents a single worker goroutine that executes database operations
type Worker struct {
	id            int
	ctx           context.Context
	cancel        context.CancelFunc
	connFactory   Connector
	cache         *ops.Cache
	profile       profile.Profile
	config        *config.Config
	metrics       *MetricsCollector
	pauseGate     *PauseGate
	thinkDuration time.Duration
	txTimeout     time.Duration
	stopTimeout   time.Duration

	// mu guards the fields below. Stop runs on the caller's goroutine while
	// run/cleanup run on the worker goroutine, and both touch dbConn.
	mu      sync.Mutex
	dbConn  *sql.DB
	closed  bool
	running bool

	wg sync.WaitGroup
}

// NewWorker creates a new worker instance
func NewWorker(id int, ctx context.Context, connFactory Connector, cache *ops.Cache, profile profile.Profile, config *config.Config, metrics *MetricsCollector) *Worker {
	return NewWorkerWithPause(id, ctx, connFactory, cache, profile, config, metrics, nil)
}

// NewWorkerWithPause creates a worker that honors an optional pause gate.
func NewWorkerWithPause(id int, ctx context.Context, connFactory Connector, cache *ops.Cache, profile profile.Profile, config *config.Config, metrics *MetricsCollector, pause *PauseGate) *Worker {
	workerCtx, cancel := context.WithCancel(ctx)

	DebugEnabled.Store(config.Debug)

	return &Worker{
		id:            id,
		ctx:           workerCtx,
		cancel:        cancel,
		connFactory:   connFactory,
		cache:         cache,
		profile:       profile,
		config:        config,
		metrics:       metrics,
		pauseGate:     pause,
		thinkDuration: config.GetThinkDuration(),
		txTimeout:     config.GetTxTimeout(),
		stopTimeout:   defaultStopTimeout,
		running:       false,
	}
}

// SetStopTimeout overrides how long Stop waits for the worker goroutine to
// exit before reporting a timeout. Values <= 0 restore the default.
func (w *Worker) SetStopTimeout(d time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if d <= 0 {
		d = defaultStopTimeout
	}
	w.stopTimeout = d
}

// conn returns the current database handle, or nil once Stop has closed it.
// Callers must treat nil as fatal rather than dereferencing it.
func (w *Worker) conn() *sql.DB {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	return w.dbConn
}

// Start opens the database connection and, only if that succeeds, starts the
// worker goroutine. A worker that cannot connect is marked dead so it never
// enters the operation loop without a handle.
func (w *Worker) Start() error {
	w.mu.Lock()
	switch {
	case w.running:
		w.mu.Unlock()
		return fmt.Errorf("worker %d is already running", w.id)
	case w.closed:
		w.mu.Unlock()
		return fmt.Errorf("worker %d has already been stopped", w.id)
	}
	w.mu.Unlock()

	dbConn, err := w.connFactory.Open()
	if err == nil && dbConn == nil {
		err = ErrNoConnection
	}
	if err != nil {
		w.metrics.LogSQLError(w.id, "Open", "connect", err)
		// Retire this worker: cancel its derived context so it is not left
		// registered on the parent, and make sure Start cannot be retried
		// into a half-initialized state. The ramp scheduler retries with a
		// fresh worker instead.
		w.cancel()
		w.mu.Lock()
		w.closed = true
		w.mu.Unlock()
		return fmt.Errorf("worker %d failed to open database connection: %w", w.id, err)
	}

	w.mu.Lock()
	w.dbConn = dbConn
	w.running = true
	w.mu.Unlock()

	w.wg.Add(1)
	go w.run()
	return nil
}

// Stop stops the worker and closes the database connection.
// In-flight Firebird calls may ignore context cancel, so the connection is
// closed asynchronously and Wait is bounded to avoid hanging Ctrl-C / Stop.
//
// The handle is closed but never set to nil while the goroutine may still be
// running: a closed *sql.DB returns an error from every call, whereas a nil
// one panics. On timeout the goroutine is left to unwind on its own — its
// context is cancelled and its handle closed, so its next loop iteration exits.
func (w *Worker) Stop() error {
	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return nil
	}
	w.running = false
	w.closed = true
	conn := w.dbConn
	timeout := w.stopTimeout
	w.mu.Unlock()

	w.cancel()

	if conn != nil {
		go func() { _ = w.connFactory.Close(conn) }()
	}

	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	if timeout <= 0 {
		timeout = defaultStopTimeout
	}

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("worker %d stop timed out", w.id)
	}
}

// run is the main worker loop
func (w *Worker) run() {
	defer w.wg.Done()
	defer w.cleanup()
	// Last-resort guard. A load generator drives many databases from one
	// process, alongside the web UI session, so a single worker must fail on
	// its own rather than take all of them down. The panic is recorded, this
	// worker exits, and the ramp scheduler starts a replacement.
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("worker %d panic: %v", w.id, r)
			fmt.Printf("[Worker-%d] recovered from panic: %v\n%s\n", w.id, r, debug.Stack())
			w.metrics.LogSQLError(w.id, "run", "panic", err)
			w.metrics.RecordError(err)
		}
	}()

	for {
		select {
		case <-w.ctx.Done():
			return
		default:
			if !w.pauseGate.WaitIfPaused(w.ctx.Done()) {
				return
			}

			if err := w.executeOperation(); err != nil {
				if isCancelErr(err) || w.ctx.Err() != nil {
					return
				}
				// No usable handle: Stop closed it, or the pool is gone.
				// Exit rather than spin on a dead connection.
				if isDeadConnErr(err) {
					return
				}
				w.metrics.RecordError(err)
			}

			if w.thinkDuration > 0 {
				select {
				case <-time.After(w.thinkDuration):
				case <-w.ctx.Done():
					return
				}
			}
		}
	}
}

// executeOperation executes a single database operation
func (w *Worker) executeOperation() error {
	startTime := time.Now()

	if DebugEnabled.Load() {
		fmt.Printf("[Worker-%d] Starting operation...\n", w.id)
	}

	// Take the handle once, under the lock. Stop can close it concurrently;
	// a closed handle returns an error, but dereferencing a nil one panics.
	conn := w.conn()
	if conn == nil {
		return fmt.Errorf("worker %d: %w", w.id, ErrNoConnection)
	}

	// Begin transaction with timeout
	ctx, cancel := context.WithTimeout(w.ctx, w.txTimeout)
	defer cancel()

	// oltp-emul units require NOWAIT transactions (SP_CHECK_NOWAIT_OR_TIMEOUT
	// rejects WAIT); other profiles keep the driver default.
	var txOpts *sql.TxOptions
	if w.config != nil && w.config.Profile == "oltp-emul" {
		txOpts = emul.TxOptions()
	}
	tx, err := conn.BeginTx(ctx, txOpts)
	if err != nil {
		if isCancelErr(err) || w.ctx.Err() != nil || isDeadConnErr(err) {
			return err
		}
		if DebugEnabled.Load() {
			fmt.Printf("[Worker-%d] FAILED to begin transaction: %v\n", w.id, err)
		}
		w.metrics.LogSQLError(w.id, "BeginTx", "begin", err)
		return fmt.Errorf("worker %d failed to begin transaction: %w", w.id, err)
	}
	defer tx.Rollback()

	// Get next operation from profile
	op, opName := w.profile.NextOpWithName()
	if op == nil {
		err := fmt.Errorf("worker %d got nil operation from profile", w.id)
		w.metrics.LogSQLError(w.id, "NextOp", "command", err)
		return err
	}

	if DebugEnabled.Load() {
		fmt.Printf("[Worker-%d] Executing operation %s...\n", w.id, opName)
	}

	// Execute the operation
	if err := op(ctx, tx, w.cache); err != nil {
		if isCancelErr(err) || w.ctx.Err() != nil {
			return err
		}
		// oltp-emul: route the outcome into per-unit aggregation. The op
		// closure always wraps failures in emul.UnitError with the outcome.
		if w.config != nil && w.config.Profile == "oltp-emul" {
			dur := time.Since(startTime)
			var uo interface{ UnitOutcome() emul.Outcome }
			if errors.As(err, &uo) {
				w.metrics.RecordUnit(opName, dur, uo.UnitOutcome())
			} else {
				w.metrics.RecordUnit(opName, dur, emul.OutcomeFailure)
			}
		}
		isExpected, classifiedErr := ops.ClassifyError(err)
		w.metrics.RecordTransactionNamed(false, time.Since(startTime), opName)
		kind := "unexpected"
		if isExpected {
			kind = "expected"
		}
		w.metrics.LogSQLError(w.id, opName, kind, classifiedErr)
		if isExpected {
			if DebugEnabled.Load() {
				fmt.Printf("[Worker-%d] Operation %s FAILED (expected): %v\n", w.id, opName, classifiedErr)
			}
			return classifiedErr
		}
		if DebugEnabled.Load() {
			fmt.Printf("[Worker-%d] Operation %s FAILED (unexpected): %v\n", w.id, opName, err)
		}
		return fmt.Errorf("worker %d unexpected error: %w", w.id, classifiedErr)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		if isCancelErr(err) || w.ctx.Err() != nil {
			return err
		}
		w.metrics.RecordTransactionNamed(false, time.Since(startTime), opName)
		w.metrics.LogSQLError(w.id, opName, "commit", err)
		if DebugEnabled.Load() {
			fmt.Printf("[Worker-%d] Operation %s FAILED to commit: %v\n", w.id, opName, err)
		}
		return fmt.Errorf("worker %d failed to commit transaction: %w", w.id, err)
	}

	// Record successful transaction
	w.metrics.RecordTransactionNamed(true, time.Since(startTime), opName)
	if w.config != nil && w.config.Profile == "oltp-emul" {
		w.metrics.RecordUnit(opName, time.Since(startTime), emul.OutcomeOK)
	}
	if DebugEnabled.Load() {
		fmt.Printf("[Worker-%d] Operation %s SUCCESS (%.2fms)\n", w.id, opName, float64(time.Since(startTime).Microseconds())/1000.0)
	}
	return nil
}

// isCancelErr reports shutdown/cancel noise that should not be logged as SQL failures.
func isCancelErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "operation was cancelled") ||
		strings.Contains(s, "context canceled") ||
		strings.Contains(s, "transaction has already been committed") ||
		strings.Contains(s, "transaction has already been rolled back")
}

// isDeadConnErr reports a handle this worker can no longer use, so the run
// loop stops instead of retrying against it.
func isDeadConnErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrNoConnection) || errors.Is(err, sql.ErrConnDone) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "sql: database is closed")
}

// cleanup performs cleanup when the worker stops. It runs on the worker
// goroutine, before wg.Done, so nothing else reads dbConn afterwards.
func (w *Worker) cleanup() {
	w.mu.Lock()
	conn := w.dbConn
	w.dbConn = nil
	w.running = false
	stopClosed := w.closed
	w.closed = true
	w.mu.Unlock()

	// Stop already closed the handle; skip the redundant second close.
	if conn != nil && !stopClosed {
		w.connFactory.Close(conn)
	}
}

// GetID returns the worker ID
func (w *Worker) GetID() int {
	return w.id
}

// IsRunning returns true if the worker is currently running
func (w *Worker) IsRunning() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.running
}

// GetErrorStats returns the shared per-session error statistics
func (w *Worker) GetErrorStats() *ops.ErrorStats {
	if w.metrics == nil {
		return nil
	}
	return w.metrics.ErrorStats()
}

// GetProfileName returns the name of the current profile
func (w *Worker) GetProfileName() string {
	return w.profile.Name()
}

// MetricsCollector collects metrics from all workers in a session.
type MetricsCollector struct {
	txSuccess atomic.Int64
	txError   atomic.Int64
	connCount atomic.Int64

	// Latency histogram buckets (in milliseconds). Buckets 0-7 are the legacy
	// ranges kept for report comparability; 8-13 cover the heavy SELECT /
	// bulk DML latencies (>=2 s range, F4).
	latBuckets [14]atomic.Int64

	opMu     sync.Mutex
	opCounts map[string]int64

	// Per-unit oltp-emul aggregation (nil maps until RecordUnit is used).
	unitAgg map[string]emul.OutcomeStats

	errorStats *ops.ErrorStats
	errorLog   *errlog.Logger
	opsLog     *opslog.Logger

	mu              sync.RWMutex
	startTime       time.Time
	lastReportTime  time.Time
	lastReportTotal int64
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector() *MetricsCollector {
	now := time.Now()
	return &MetricsCollector{
		opCounts:       make(map[string]int64),
		errorStats:     ops.NewErrorStats(),
		startTime:      now,
		lastReportTime: now,
	}
}

// RecordTransaction records the result of a transaction
func (mc *MetricsCollector) RecordTransaction(success bool, latency time.Duration) {
	mc.RecordTransactionNamed(success, latency, "")
}

// RecordTransactionNamed records a transaction with an optional operation name.
func (mc *MetricsCollector) RecordTransactionNamed(success bool, latency time.Duration, opName string) {
	if success {
		mc.txSuccess.Add(1)
	} else {
		mc.txError.Add(1)
	}

	latMs := int64(latency.Milliseconds())
	bucket := getLatencyBucket(latMs)
	mc.latBuckets[bucket].Add(1)

	if opName != "" {
		mc.opMu.Lock()
		mc.opCounts[opName]++
		mc.opMu.Unlock()
	}
}

// RecordError records an error against the shared session taxonomy.
// It does not increment txError — callers already count failures via RecordTransaction.
func (mc *MetricsCollector) RecordError(err error) {
	if mc.errorStats != nil {
		mc.errorStats.RecordError(err)
	}
}

// SetErrorLogger attaches a file logger for SQL/command failures.
func (mc *MetricsCollector) SetErrorLogger(l *errlog.Logger) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.errorLog = l
}

// ErrorLogger returns the attached error logger, if any.
func (mc *MetricsCollector) ErrorLogger() *errlog.Logger {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.errorLog
}

// LogSQLError writes a SQL/command failure to the error log file.
func (mc *MetricsCollector) LogSQLError(workerID int, op, kind string, err error) {
	if mc == nil || err == nil {
		return
	}
	mc.mu.RLock()
	l := mc.errorLog
	mc.mu.RUnlock()
	if l == nil {
		return
	}
	l.Log(errlog.Entry{
		WorkerID: workerID,
		Op:       op,
		Kind:     kind,
		Err:      err,
	})
}

// SetOpsLog attaches the operations/transactions log for this session.
func (mc *MetricsCollector) SetOpsLog(l *opslog.Logger) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.opsLog = l
}

// OpsLog returns the attached operations log, if any.
func (mc *MetricsCollector) OpsLog() *opslog.Logger {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.opsLog
}

// SetConnectionCount sets the current connection count (absolute, not a delta).
func (mc *MetricsCollector) SetConnectionCount(n int64) {
	mc.connCount.Store(n)
}

// RecordConnectionChange is kept for callers that still pass an absolute count;
// prefer SetConnectionCount.
func (mc *MetricsCollector) RecordConnectionChange(n int64) {
	mc.SetConnectionCount(n)
}

// LatencyBucketCount is the number of histogram buckets.
const LatencyBucketCount = 14

// LatencyBucketLabels are the display labels for buckets 0..13. Buckets 0-7
// are the legacy ranges ("<5ms" .. "<1000ms"); 8-13 extend into the seconds
// range for heavy SELECT / bulk DML operations. The legacy ">=1000ms" figure
// of old reports is the sum of buckets 8..13.
var LatencyBucketLabels = [LatencyBucketCount]string{
	"<5ms", "<10ms", "<25ms", "<50ms", "<100ms", "<250ms", "<500ms", "<1000ms",
	"<2000ms", "<5000ms", "<10000ms", "<30000ms", "<60000ms", ">=60000ms",
}

// GetLatencyBucketMs maps a latency in milliseconds onto its bucket index.
func GetLatencyBucketMs(latMs int64) int {
	switch {
	case latMs < 5:
		return 0
	case latMs < 10:
		return 1
	case latMs < 25:
		return 2
	case latMs < 50:
		return 3
	case latMs < 100:
		return 4
	case latMs < 250:
		return 5
	case latMs < 500:
		return 6
	case latMs < 1000:
		return 7
	case latMs < 2000:
		return 8
	case latMs < 5000:
		return 9
	case latMs < 10000:
		return 10
	case latMs < 30000:
		return 11
	case latMs < 60000:
		return 12
	default:
		return 13
	}
}

func getLatencyBucket(latMs int64) int {
	return GetLatencyBucketMs(latMs)
}

// GetLatencyBucketCounts returns a copy of the histogram.
func (mc *MetricsCollector) GetLatencyBucketCounts() [LatencyBucketCount]int64 {
	var out [LatencyBucketCount]int64
	for i := range mc.latBuckets {
		out[i] = mc.latBuckets[i].Load()
	}
	return out
}

// GetTotalTransactions returns the total number of transactions
func (mc *MetricsCollector) GetTotalTransactions() int64 {
	return mc.txSuccess.Load() + mc.txError.Load()
}

// GetSuccessRate returns the success rate as a percentage
func (mc *MetricsCollector) GetSuccessRate() float64 {
	total := mc.GetTotalTransactions()
	if total == 0 {
		return 0.0
	}
	return float64(mc.txSuccess.Load()) / float64(total) * 100.0
}

// GetTPS returns the transactions per second since start
func (mc *MetricsCollector) GetTPS() float64 {
	mc.mu.RLock()
	elapsed := time.Since(mc.startTime).Seconds()
	mc.mu.RUnlock()
	total := mc.GetTotalTransactions()
	if elapsed <= 0 {
		return 0.0
	}
	return float64(total) / elapsed
}

// GetTPSInterval returns TPS for the interval since the last UpdateLastReportTime call.
func (mc *MetricsCollector) GetTPSInterval() float64 {
	mc.mu.RLock()
	elapsed := time.Since(mc.lastReportTime).Seconds()
	lastTotal := mc.lastReportTotal
	mc.mu.RUnlock()
	delta := mc.GetTotalTransactions() - lastTotal
	if elapsed <= 0 {
		return 0.0
	}
	return float64(delta) / elapsed
}

// GetLatencyPercentiles returns latency percentiles
func (mc *MetricsCollector) GetLatencyPercentiles() (p50, p95, p99 int64) {
	total := mc.GetTotalTransactions()
	if total == 0 {
		return 0, 0, 0
	}

	cumulative := int64(0)
	var bucketCounts [LatencyBucketCount]int64
	for i := range mc.latBuckets {
		bucketCounts[i] = mc.latBuckets[i].Load()
	}

	p50Target := total * 50 / 100
	p95Target := total * 95 / 100
	p99Target := total * 99 / 100

	p50, p95, p99 = -1, -1, -1

	for i, count := range bucketCounts {
		cumulative += count
		bucketMs := getBucketUpperBound(i)

		if p50 == -1 && cumulative >= p50Target {
			p50 = bucketMs
		}
		if p95 == -1 && cumulative >= p95Target {
			p95 = bucketMs
		}
		if p99 == -1 && cumulative >= p99Target {
			p99 = bucketMs
		}

		if p50 != -1 && p95 != -1 && p99 != -1 {
			break
		}
	}

	if p50 == -1 {
		p50 = 1000
	}
	if p95 == -1 {
		p95 = 1000
	}
	if p99 == -1 {
		p99 = 1000
	}

	return p50, p95, p99
}

func getBucketUpperBound(bucket int) int64 {
	switch bucket {
	case 0:
		return 5
	case 1:
		return 10
	case 2:
		return 25
	case 3:
		return 50
	case 4:
		return 100
	case 5:
		return 250
	case 6:
		return 500
	case 7:
		return 1000
	case 8:
		return 2000
	case 9:
		return 5000
	case 10:
		return 10000
	case 11:
		return 30000
	case 12:
		return 60000
	default:
		return 60000
	}
}

// GetStats returns a summary of current statistics
func (mc *MetricsCollector) GetStats() string {
	total := mc.GetTotalTransactions()
	successRate := mc.GetSuccessRate()
	tps := mc.GetTPS()
	p50, p95, p99 := mc.GetLatencyPercentiles()

	return fmt.Sprintf("Total: %d, Success: %.1f%%, TPS: %.1f, Lat: p50=%dms p95=%dms p99=%dms",
		total, successRate, tps, p50, p95, p99)
}

// Reset resets all metrics
func (mc *MetricsCollector) Reset() {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.txSuccess.Store(0)
	mc.txError.Store(0)
	mc.connCount.Store(0)
	for i := range mc.latBuckets {
		mc.latBuckets[i].Store(0)
	}
	mc.opMu.Lock()
	mc.opCounts = make(map[string]int64)
	mc.opMu.Unlock()
	if mc.errorStats != nil {
		mc.errorStats.Reset()
	}
	now := time.Now()
	mc.startTime = now
	mc.lastReportTime = now
	mc.lastReportTotal = 0
}

// UpdateLastReportTime advances the interval window used by GetTPSInterval.
// Callers (reporter/collector) should invoke this after sampling interval TPS.
func (mc *MetricsCollector) UpdateLastReportTime() {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.lastReportTime = time.Now()
	mc.lastReportTotal = mc.txSuccess.Load() + mc.txError.Load()
}

// GetTxSuccess returns successful transaction count
func (mc *MetricsCollector) GetTxSuccess() int64 {
	return mc.txSuccess.Load()
}

// GetTxError returns error transaction count
func (mc *MetricsCollector) GetTxError() int64 {
	return mc.txError.Load()
}

// GetConnectionCount returns the current connection count
func (mc *MetricsCollector) GetConnectionCount() int64 {
	return mc.connCount.Load()
}

// ErrorStats returns the shared error taxonomy collector.
func (mc *MetricsCollector) ErrorStats() *ops.ErrorStats {
	return mc.errorStats
}

// GetOpCounts returns a copy of per-operation transaction counts.
func (mc *MetricsCollector) GetOpCounts() map[string]int64 {
	mc.opMu.Lock()
	defer mc.opMu.Unlock()
	out := make(map[string]int64, len(mc.opCounts))
	for k, v := range mc.opCounts {
		out[k] = v
	}
	return out
}

// RecordUnit records one oltp-emul business-unit execution with its outcome
// and latency for the per-unit table. No-op for empty unit names.
func (mc *MetricsCollector) RecordUnit(unit string, latency time.Duration, outcome emul.Outcome) {
	if unit == "" {
		return
	}
	ms := latency.Milliseconds()
	mc.opMu.Lock()
	defer mc.opMu.Unlock()
	if mc.unitAgg == nil {
		mc.unitAgg = make(map[string]emul.OutcomeStats)
	}
	agg := mc.unitAgg[unit]
	switch outcome {
	case emul.OutcomeOK:
		agg.OK++
	case emul.OutcomeConflict:
		agg.Conflict++
	case emul.OutcomeRejected:
		agg.Rejected++
	default:
		agg.Failure++
	}
	agg.LatSumMs += ms
	if ms > agg.MaxMs {
		agg.MaxMs = ms
	}
	agg.N++
	mc.unitAgg[unit] = agg
}

// GetUnitStats returns a copy of the per-unit oltp-emul aggregation.
func (mc *MetricsCollector) GetUnitStats() map[string]emul.OutcomeStats {
	mc.opMu.Lock()
	defer mc.opMu.Unlock()
	out := make(map[string]emul.OutcomeStats, len(mc.unitAgg))
	for k, v := range mc.unitAgg {
		out[k] = v
	}
	return out
}

// Counters returns the cumulative transaction totals (success, error).
func (mc *MetricsCollector) Counters() (success, failed int64) {
	return mc.txSuccess.Load(), mc.txError.Load()
}

// GetStartTime returns when metrics collection started.
func (mc *MetricsCollector) GetStartTime() time.Time {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.startTime
}
