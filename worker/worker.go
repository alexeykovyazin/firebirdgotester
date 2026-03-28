package worker

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"fb-loadgen/config"
	"fb-loadgen/db"
	"fb-loadgen/ops"
	"fb-loadgen/profile"
)

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
	errorStats    *ops.ErrorStats
	thinkDuration time.Duration
	txTimeout     time.Duration

	// Worker state
	dbConn  *sql.DB
	running bool
	wg      sync.WaitGroup
}

// NewWorker creates a new worker instance
func NewWorker(id int, ctx context.Context, connFactory *db.ConnectionFactory, cache *ops.Cache, profile profile.Profile, config *config.Config, metrics *MetricsCollector) *Worker {
	workerCtx, cancel := context.WithCancel(ctx)

	return &Worker{
		id:            id,
		ctx:           workerCtx,
		cancel:        cancel,
		connFactory:   connFactory,
		cache:         cache,
		profile:       profile,
		config:        config,
		metrics:       metrics,
		errorStats:    ops.NewErrorStats(),
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
			// Execute one operation
			if err := w.executeOperation(); err != nil {
				// Log error but continue running
				w.metrics.RecordError(err)
				w.errorStats.RecordError(err)
			}

			// Think time between operations
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

	// Begin transaction with timeout
	ctx, cancel := context.WithTimeout(w.ctx, w.txTimeout)
	defer cancel()

	tx, err := w.dbConn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("worker %d failed to begin transaction: %w", w.id, err)
	}
	defer tx.Rollback()

	// Get next operation from profile
	op := w.profile.NextOp()
	if op == nil {
		return fmt.Errorf("worker %d got nil operation from profile", w.id)
	}

	// Execute the operation
	if err := op(ctx, tx, w.cache); err != nil {
		// Classify and handle the error
		isExpected, classifiedErr := ops.ClassifyError(err)
		if isExpected {
			// Expected error - just record it
			w.metrics.RecordTransaction(false, time.Since(startTime))
			return classifiedErr
		} else {
			// Unexpected error - log it and return
			w.metrics.RecordTransaction(false, time.Since(startTime))
			return fmt.Errorf("worker %d unexpected error: %w", w.id, classifiedErr)
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		w.metrics.RecordTransaction(false, time.Since(startTime))
		return fmt.Errorf("worker %d failed to commit transaction: %w", w.id, err)
	}

	// Record successful transaction
	w.metrics.RecordTransaction(true, time.Since(startTime))
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

// GetErrorStats returns the error statistics for this worker
func (w *Worker) GetErrorStats() *ops.ErrorStats {
	return w.errorStats
}

// GetProfileName returns the name of the current profile
func (w *Worker) GetProfileName() string {
	return w.profile.Name()
}

// MetricsCollector collects metrics from all workers
type MetricsCollector struct {
	// Atomic counters
	txSuccess int64
	txError   int64
	connCount int64

	// Latency histogram buckets (in milliseconds)
	latBuckets [9]int64 // <5, <10, <25, <50, <100, <250, <500, <1000, >=1000

	// Mutex for thread-safe access to non-atomic fields
	mu sync.RWMutex

	// Additional metrics
	startTime      time.Time
	lastReportTime time.Time
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		startTime:      time.Now(),
		lastReportTime: time.Now(),
	}
}

// RecordTransaction records the result of a transaction
func (mc *MetricsCollector) RecordTransaction(success bool, latency time.Duration) {
	if success {
		mc.txSuccess++
	} else {
		mc.txError++
	}

	// Record latency in appropriate bucket
	latMs := int64(latency.Milliseconds())
	bucket := mc.getLatencyBucket(latMs)
	mc.latBuckets[bucket]++
}

// RecordError records an error
func (mc *MetricsCollector) RecordError(err error) {
	mc.txError++
}

// RecordConnectionChange records a connection count change
func (mc *MetricsCollector) RecordConnectionChange(delta int64) {
	mc.connCount += delta
}

// getLatencyBucket determines which latency bucket to use
func (mc *MetricsCollector) getLatencyBucket(latMs int64) int {
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
	return mc.txSuccess + mc.txError
}

// GetSuccessRate returns the success rate as a percentage
func (mc *MetricsCollector) GetSuccessRate() float64 {
	total := mc.GetTotalTransactions()
	if total == 0 {
		return 0.0
	}
	return float64(mc.txSuccess) / float64(total) * 100.0
}

// GetTPS returns the transactions per second since start
func (mc *MetricsCollector) GetTPS() float64 {
	elapsed := time.Since(mc.startTime).Seconds()
	total := mc.GetTotalTransactions()
	if elapsed <= 0 {
		return 0.0
	}
	return float64(total) / elapsed
}

// GetTPSInterval returns the transactions per second for the last interval
func (mc *MetricsCollector) GetTPSInterval() float64 {
	elapsed := time.Since(mc.lastReportTime).Seconds()
	total := mc.GetTotalTransactions()
	if elapsed <= 0 {
		return 0.0
	}
	// This is a simplified calculation - in a real implementation,
	// you'd track transactions per interval
	return float64(total) / elapsed
}

// GetLatencyPercentiles returns latency percentiles
func (mc *MetricsCollector) GetLatencyPercentiles() (p50, p95, p99 int64) {
	total := mc.GetTotalTransactions()
	if total == 0 {
		return 0, 0, 0
	}

	// Calculate cumulative counts
	cumulative := int64(0)
	bucketCounts := [9]int64{}
	copy(bucketCounts[:], mc.latBuckets[:])

	// Find percentiles
	p50Target := total * 50 / 100
	p95Target := total * 95 / 100
	p99Target := total * 99 / 100

	p50, p95, p99 = -1, -1, -1

	for i, count := range bucketCounts {
		cumulative += count
		bucketMs := mc.getBucketUpperBound(i)

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

	// If percentiles weren't found, use the maximum bucket
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

// getBucketUpperBound returns the upper bound of a latency bucket in milliseconds
func (mc *MetricsCollector) getBucketUpperBound(bucket int) int64 {
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
		return 1000 // >= 1000ms
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

	mc.txSuccess = 0
	mc.txError = 0
	mc.connCount = 0
	for i := range mc.latBuckets {
		mc.latBuckets[i] = 0
	}
	mc.startTime = time.Now()
	mc.lastReportTime = time.Now()
}

// UpdateLastReportTime updates the last report time
func (mc *MetricsCollector) UpdateLastReportTime() {
	mc.lastReportTime = time.Now()
}

// GetConnectionCount returns the current connection count
func (mc *MetricsCollector) GetConnectionCount() int64 {
	return mc.connCount
}
