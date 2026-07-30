package worker

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"fb-loadgen/config"
	"fb-loadgen/db"
	"fb-loadgen/errlog"
	"fb-loadgen/ops"
	"fb-loadgen/profile"
)

// DebugEnabled controls debug output
var DebugEnabled = false

// Worker represents a single worker goroutine that executes database operations
type Worker struct {
	id            int
	ctx           context.Context
	cancel        context.CancelFunc
	connFactory   *db.ConnectionFactory
	cache         *ops.Cache
	profile       profile.Profile
	config        *config.Config
	metrics       *MetricsCollector
	pauseGate     *PauseGate
	thinkDuration time.Duration
	txTimeout     time.Duration

	// Worker state
	dbConn  *sql.DB
	running bool
	wg      sync.WaitGroup
}

// NewWorker creates a new worker instance
func NewWorker(id int, ctx context.Context, connFactory *db.ConnectionFactory, cache *ops.Cache, profile profile.Profile, config *config.Config, metrics *MetricsCollector) *Worker {
	return NewWorkerWithPause(id, ctx, connFactory, cache, profile, config, metrics, nil)
}

// NewWorkerWithPause creates a worker that honors an optional pause gate.
func NewWorkerWithPause(id int, ctx context.Context, connFactory *db.ConnectionFactory, cache *ops.Cache, profile profile.Profile, config *config.Config, metrics *MetricsCollector, pause *PauseGate) *Worker {
	workerCtx, cancel := context.WithCancel(ctx)

	DebugEnabled = config.Debug

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
		running:       false,
	}
}

// Start starts the worker goroutine
func (w *Worker) Start() error {
	if w.running {
		return fmt.Errorf("worker %d is already running", w.id)
	}

	// Open database connection
	dbConn, err := w.connFactory.Open()
	if err != nil {
		w.metrics.LogSQLError(w.id, "Open", "connect", err)
		return fmt.Errorf("worker %d failed to open database connection: %w", w.id, err)
	}
	w.dbConn = dbConn

	w.running = true
	w.wg.Add(1)

	go w.run()
	return nil
}

// Stop stops the worker and closes the database connection
func (w *Worker) Stop() error {
	if !w.running {
		return nil
	}

	w.cancel()
	w.wg.Wait()

	if w.dbConn != nil {
		return w.connFactory.Close(w.dbConn)
	}
	return nil
}

// run is the main worker loop
func (w *Worker) run() {
	defer w.wg.Done()
	defer w.cleanup()

	for {
		select {
		case <-w.ctx.Done():
			return
		default:
			if !w.pauseGate.WaitIfPaused(w.ctx.Done()) {
				return
			}

			if err := w.executeOperation(); err != nil {
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

	if DebugEnabled {
		fmt.Printf("[Worker-%d] Starting operation...\n", w.id)
	}

	// Begin transaction with timeout
	ctx, cancel := context.WithTimeout(w.ctx, w.txTimeout)
	defer cancel()

	tx, err := w.dbConn.BeginTx(ctx, nil)
	if err != nil {
		if DebugEnabled {
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

	if DebugEnabled {
		fmt.Printf("[Worker-%d] Executing operation %s...\n", w.id, opName)
	}

	// Execute the operation
	if err := op(ctx, tx, w.cache); err != nil {
		isExpected, classifiedErr := ops.ClassifyError(err)
		w.metrics.RecordTransactionNamed(false, time.Since(startTime), opName)
		kind := "unexpected"
		if isExpected {
			kind = "expected"
		}
		w.metrics.LogSQLError(w.id, opName, kind, classifiedErr)
		if isExpected {
			if DebugEnabled {
				fmt.Printf("[Worker-%d] Operation %s FAILED (expected): %v\n", w.id, opName, classifiedErr)
			}
			return classifiedErr
		}
		if DebugEnabled {
			fmt.Printf("[Worker-%d] Operation %s FAILED (unexpected): %v\n", w.id, opName, err)
		}
		return fmt.Errorf("worker %d unexpected error: %w", w.id, classifiedErr)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		w.metrics.RecordTransactionNamed(false, time.Since(startTime), opName)
		w.metrics.LogSQLError(w.id, opName, "commit", err)
		if DebugEnabled {
			fmt.Printf("[Worker-%d] Operation %s FAILED to commit: %v\n", w.id, opName, err)
		}
		return fmt.Errorf("worker %d failed to commit transaction: %w", w.id, err)
	}

	// Record successful transaction
	w.metrics.RecordTransactionNamed(true, time.Since(startTime), opName)
	if DebugEnabled {
		fmt.Printf("[Worker-%d] Operation %s SUCCESS (%.2fms)\n", w.id, opName, float64(time.Since(startTime).Microseconds())/1000.0)
	}
	return nil
}

// cleanup performs cleanup when the worker stops
func (w *Worker) cleanup() {
	w.running = false
	if w.dbConn != nil {
		w.connFactory.Close(w.dbConn)
		w.dbConn = nil
	}
}

// GetID returns the worker ID
func (w *Worker) GetID() int {
	return w.id
}

// IsRunning returns true if the worker is currently running
func (w *Worker) IsRunning() bool {
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

	// Latency histogram buckets (in milliseconds)
	latBuckets [9]atomic.Int64 // <5, <10, <25, <50, <100, <250, <500, <1000, >=1000

	opMu     sync.Mutex
	opCounts map[string]int64

	errorStats *ops.ErrorStats
	errorLog   *errlog.Logger

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

// SetConnectionCount sets the current connection count (absolute, not a delta).
func (mc *MetricsCollector) SetConnectionCount(n int64) {
	mc.connCount.Store(n)
}

// RecordConnectionChange is kept for callers that still pass an absolute count;
// prefer SetConnectionCount.
func (mc *MetricsCollector) RecordConnectionChange(n int64) {
	mc.SetConnectionCount(n)
}

func getLatencyBucket(latMs int64) int {
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
	default:
		return 8
	}
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
	var bucketCounts [9]int64
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
		return 1000
	default:
		return 0
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

// GetStartTime returns when metrics collection started.
func (mc *MetricsCollector) GetStartTime() time.Time {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.startTime
}
