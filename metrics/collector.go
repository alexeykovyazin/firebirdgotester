package metrics

import (
	"context"
	"fmt"
	"sync"
	"time"

	"fb-loadgen/ops"
	"fb-loadgen/profile"
	"fb-loadgen/ramp"
	"fb-loadgen/worker"
)

// MetricsCollector collects and aggregates metrics from the entire system
type MetricsCollector struct {
	// Core metrics
	workerMetrics *worker.MetricsCollector

	// System state
	scheduler *ramp.Scheduler
	profile   profile.Profile
	cache     *ops.Cache

	// Aggregated statistics
	totalOps   int64
	successOps int64
	errorOps   int64
	avgLatency time.Duration
	minLatency time.Duration
	maxLatency time.Duration

	// Rate calculations
	startTime      time.Time
	lastReportTime time.Time
	lastTotalOps   int64

	// Concurrency tracking
	maxWorkers     int
	currentWorkers int

	// Error tracking
	errorCounts map[string]int64
	errorMutex  sync.RWMutex

	// Latency tracking
	latencyBuckets [9]int64 // <5, <10, <25, <50, <100, <250, <500, <1000, >=1000 ms
	latencyMutex   sync.RWMutex

	// Operation tracking
	opCounts map[string]int64
	opMutex  sync.RWMutex

	// Context for cancellation
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewMetricsCollector creates a new metrics collector
// workerMetrics must be the same instance passed to the scheduler/workers
func NewMetricsCollector(scheduler *ramp.Scheduler, profile profile.Profile, cache *ops.Cache, workerMetrics *worker.MetricsCollector) *MetricsCollector {
	ctx, cancel := context.WithCancel(context.Background())

	return &MetricsCollector{
		workerMetrics:  workerMetrics, // Use shared instance from scheduler
		scheduler:      scheduler,
		profile:        profile,
		cache:          cache,
		startTime:      time.Now(),
		lastReportTime: time.Now(),
		errorCounts:    make(map[string]int64),
		opCounts:       make(map[string]int64),
		ctx:            ctx,
		cancel:         cancel,
	}
}

// Start starts the metrics collection goroutine
func (mc *MetricsCollector) Start() {
	mc.wg.Add(1)
	go mc.collect()
}

// Stop stops the metrics collection
func (mc *MetricsCollector) Stop() {
	mc.cancel()
	mc.wg.Wait()
}

// collect runs the metrics collection loop
func (mc *MetricsCollector) collect() {
	defer mc.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-mc.ctx.Done():
			return
		case <-ticker.C:
			mc.updateMetrics()
		}
	}
}

// updateMetrics updates all metrics from the system components
func (mc *MetricsCollector) updateMetrics() {
	// Update from worker metrics
	mc.updateFromWorkerMetrics()

	// Update from scheduler
	mc.updateFromScheduler()

	// Update from cache
	mc.updateFromCache()

	// Update timestamp
	mc.lastReportTime = time.Now()
}

// updateFromWorkerMetrics updates metrics from the worker metrics collector
func (mc *MetricsCollector) updateFromWorkerMetrics() {
	// Get current totals from worker metrics
	total := mc.workerMetrics.GetTotalTransactions()
	successRate := mc.workerMetrics.GetSuccessRate()

	// Update our counters
	mc.totalOps = total
	mc.successOps = int64(float64(total) * successRate / 100.0)
	mc.errorOps = total - mc.successOps

	// Update latency metrics
	p50, p95, p99 := mc.workerMetrics.GetLatencyPercentiles()
	mc.avgLatency = time.Duration((p50+p95+p99)/3) * time.Millisecond
	mc.minLatency = time.Duration(p50) * time.Millisecond
	mc.maxLatency = time.Duration(p99) * time.Millisecond

	// Note: We can't directly access workerMetrics.latBuckets as it's unexported
	// The latency buckets will be updated through RecordTransaction calls
}

// updateFromScheduler updates metrics from the scheduler
func (mc *MetricsCollector) updateFromScheduler() {
	mc.currentWorkers = mc.scheduler.GetCurrentWorkerCount()
	if mc.currentWorkers > mc.maxWorkers {
		mc.maxWorkers = mc.currentWorkers
	}
}

// updateFromCache updates metrics from the cache
func (mc *MetricsCollector) updateFromCache() {
	// Cache hit/miss ratios could be tracked here if implemented
	// For now, we'll just note that cache is being used
}

// RecordTransaction records a transaction result
func (mc *MetricsCollector) RecordTransaction(success bool, latency time.Duration, opName string) {
	// Record in worker metrics
	if success {
		mc.workerMetrics.RecordTransaction(true, latency)
	} else {
		mc.workerMetrics.RecordTransaction(false, latency)
	}

	// Update operation counts
	mc.opMutex.Lock()
	mc.opCounts[opName]++
	mc.opMutex.Unlock()

	// Update latency buckets
	bucket := mc.getLatencyBucket(latency)
	mc.latencyMutex.Lock()
	mc.latencyBuckets[bucket]++
	mc.latencyMutex.Unlock()
}

// RecordError records an error
func (mc *MetricsCollector) RecordError(err error, opName string) {
	// Record in worker metrics
	mc.workerMetrics.RecordError(err)

	// Update error counts
	mc.errorMutex.Lock()
	errorKey := err.Error()
	mc.errorCounts[errorKey]++
	mc.errorMutex.Unlock()

	// Update operation counts (failed operations)
	mc.opMutex.Lock()
	mc.opCounts[opName]++
	mc.opMutex.Unlock()
}

// getLatencyBucket determines which latency bucket to use
func (mc *MetricsCollector) getLatencyBucket(latency time.Duration) int {
	latMs := int64(latency.Milliseconds())
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

// GetReport generates a comprehensive metrics report
func (mc *MetricsCollector) GetReport() *Report {
	mc.opMutex.RLock()
	defer mc.opMutex.RUnlock()

	mc.errorMutex.RLock()
	defer mc.errorMutex.RUnlock()

	mc.latencyMutex.RLock()
	defer mc.latencyMutex.RUnlock()

	elapsed := time.Since(mc.startTime)
	interval := time.Since(mc.lastReportTime)

	// Calculate rates
	totalRate := float64(mc.totalOps) / elapsed.Seconds()
	intervalRate := float64(mc.totalOps-mc.lastTotalOps) / interval.Seconds()

	// Calculate success rate
	successRate := 0.0
	if mc.totalOps > 0 {
		successRate = float64(mc.successOps) / float64(mc.totalOps) * 100.0
	}

	// Get latency percentiles
	p50, p95, p99 := mc.getLatencyPercentiles()

	// Copy operation counts
	opCounts := make(map[string]int64)
	for k, v := range mc.opCounts {
		opCounts[k] = v
	}

	// Copy error counts
	errorCounts := make(map[string]int64)
	for k, v := range mc.errorCounts {
		errorCounts[k] = v
	}

	// Get scheduler stats
	schedulerStats := mc.scheduler.GetStats()

	// Get cache stats
	cacheStats := mc.cache.GetStats()

	// Get profile stats
	profileStats := mc.profile.Name()

	report := &Report{
		Timestamp:         time.Now(),
		Elapsed:           elapsed,
		Interval:          interval,
		TotalOperations:   mc.totalOps,
		SuccessOperations: mc.successOps,
		ErrorOperations:   mc.errorOps,
		SuccessRate:       successRate,
		TotalTPS:          totalRate,
		IntervalTPS:       intervalRate,
		AverageLatency:    mc.avgLatency,
		MinLatency:        mc.minLatency,
		MaxLatency:        mc.maxLatency,
		P50Latency:        p50,
		P95Latency:        p95,
		P99Latency:        p99,
		MaxWorkers:        mc.maxWorkers,
		CurrentWorkers:    mc.currentWorkers,
		SchedulerStats:    schedulerStats,
		CacheStats:        cacheStats,
		ProfileName:       profileStats,
		OperationCounts:   opCounts,
		ErrorCounts:       errorCounts,
		LatencyBuckets:    mc.latencyBuckets,
	}

	mc.lastTotalOps = mc.totalOps
	return report
}

// getLatencyPercentiles calculates latency percentiles from buckets
func (mc *MetricsCollector) getLatencyPercentiles() (p50, p95, p99 time.Duration) {
	total := int64(0)
	for _, count := range mc.latencyBuckets {
		total += count
	}

	if total == 0 {
		return 0, 0, 0
	}

	// Calculate cumulative counts
	cumulative := int64(0)
	p50Target := total * 50 / 100
	p95Target := total * 95 / 100
	p99Target := total * 99 / 100

	p50, p95, p99 = 0, 0, 0

	for i, count := range mc.latencyBuckets {
		cumulative += count
		bucketMs := mc.getBucketUpperBound(i)

		if p50 == 0 && cumulative >= p50Target {
			p50 = time.Duration(bucketMs) * time.Millisecond
		}
		if p95 == 0 && cumulative >= p95Target {
			p95 = time.Duration(bucketMs) * time.Millisecond
		}
		if p99 == 0 && cumulative >= p99Target {
			p99 = time.Duration(bucketMs) * time.Millisecond
		}

		if p50 != 0 && p95 != 0 && p99 != 0 {
			break
		}
	}

	// If percentiles weren't found, use the maximum bucket
	if p50 == 0 {
		p50 = 1000 * time.Millisecond
	}
	if p95 == 0 {
		p95 = 1000 * time.Millisecond
	}
	if p99 == 0 {
		p99 = 1000 * time.Millisecond
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

// Reset resets all metrics
func (mc *MetricsCollector) Reset() {
	mc.workerMetrics.Reset()
	mc.totalOps = 0
	mc.successOps = 0
	mc.errorOps = 0
	mc.avgLatency = 0
	mc.minLatency = 0
	mc.maxLatency = 0
	mc.maxWorkers = 0
	mc.currentWorkers = 0
	mc.startTime = time.Now()
	mc.lastReportTime = time.Now()
	mc.lastTotalOps = 0

	mc.errorMutex.Lock()
	for k := range mc.errorCounts {
		delete(mc.errorCounts, k)
	}
	mc.errorMutex.Unlock()

	mc.opMutex.Lock()
	for k := range mc.opCounts {
		delete(mc.opCounts, k)
	}
	mc.opMutex.Unlock()

	mc.latencyMutex.Lock()
	for i := range mc.latencyBuckets {
		mc.latencyBuckets[i] = 0
	}
	mc.latencyMutex.Unlock()
}

// GetStats returns a summary of current statistics
func (mc *MetricsCollector) GetStats() string {
	report := mc.GetReport()
	return report.GetSummary()
}

// Report represents a comprehensive metrics report
type Report struct {
	Timestamp         time.Time
	Elapsed           time.Duration
	Interval          time.Duration
	TotalOperations   int64
	SuccessOperations int64
	ErrorOperations   int64
	SuccessRate       float64
	TotalTPS          float64
	IntervalTPS       float64
	AverageLatency    time.Duration
	MinLatency        time.Duration
	MaxLatency        time.Duration
	P50Latency        time.Duration
	P95Latency        time.Duration
	P99Latency        time.Duration
	MaxWorkers        int
	CurrentWorkers    int
	SchedulerStats    string
	CacheStats        string
	ProfileName       string
	OperationCounts   map[string]int64
	ErrorCounts       map[string]int64
	LatencyBuckets    [9]int64
}

// GetSummary returns a summary string of the report
func (r *Report) GetSummary() string {
	return fmt.Sprintf(
		"Total: %d, Success: %.1f%%, TPS: %.1f, Lat: avg=%v min=%v max=%v p50=%v p95=%v p99=%v, Workers: %d/%d, Profile: %s",
		r.TotalOperations, r.SuccessRate, r.TotalTPS,
		r.AverageLatency, r.MinLatency, r.MaxLatency,
		r.P50Latency, r.P95Latency, r.P99Latency,
		r.CurrentWorkers, r.MaxWorkers, r.ProfileName,
	)
}

// GetDetailedReport returns a detailed string representation of the report
func (r *Report) GetDetailedReport() string {
	return fmt.Sprintf(`=== Load Test Report ===
Timestamp: %s
Elapsed: %v
Interval: %v

Operations:
  Total: %d
  Success: %d (%.1f%%)
  Errors: %d

Performance:
  Total TPS: %.1f
  Interval TPS: %.1f
  Average Latency: %v
  Min Latency: %v
  Max Latency: %v
  P50 Latency: %v
  P95 Latency: %v
  P99 Latency: %v

Concurrency:
  Current Workers: %d
  Max Workers: %d

System:
  Profile: %s
  Scheduler: %s
  Cache: %s

Operation Distribution:
%s

Error Distribution:
%s

Latency Distribution:
%s
`,
		r.Timestamp.Format(time.RFC3339),
		r.Elapsed,
		r.Interval,
		r.TotalOperations,
		r.SuccessOperations,
		r.SuccessRate,
		r.ErrorOperations,
		r.TotalTPS,
		r.IntervalTPS,
		r.AverageLatency,
		r.MinLatency,
		r.MaxLatency,
		r.P50Latency,
		r.P95Latency,
		r.P99Latency,
		r.CurrentWorkers,
		r.MaxWorkers,
		r.ProfileName,
		r.SchedulerStats,
		r.CacheStats,
		r.formatOperationCounts(),
		r.formatErrorCounts(),
		r.formatLatencyBuckets(),
	)
}

// formatOperationCounts formats the operation counts for display
func (r *Report) formatOperationCounts() string {
	if len(r.OperationCounts) == 0 {
		return "  (none)"
	}

	result := ""
	for op, count := range r.OperationCounts {
		result += fmt.Sprintf("  %s: %d\n", op, count)
	}
	return result
}

// formatErrorCounts formats the error counts for display
func (r *Report) formatErrorCounts() string {
	if len(r.ErrorCounts) == 0 {
		return "  (none)"
	}

	result := ""
	for err, count := range r.ErrorCounts {
		result += fmt.Sprintf("  %s: %d\n", err, count)
	}
	return result
}

// formatLatencyBuckets formats the latency buckets for display
func (r *Report) formatLatencyBuckets() string {
	buckets := []string{"<5ms", "<10ms", "<25ms", "<50ms", "<100ms", "<250ms", "<500ms", "<1000ms", ">=1000ms"}
	total := int64(0)
	for _, count := range r.LatencyBuckets {
		total += count
	}

	if total == 0 {
		return "  (no data)"
	}

	result := ""
	for i, count := range r.LatencyBuckets {
		percentage := float64(count) / float64(total) * 100.0
		result += fmt.Sprintf("  %s: %d (%.1f%%)\n", buckets[i], count, percentage)
	}
	return result
}
