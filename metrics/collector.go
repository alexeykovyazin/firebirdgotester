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

	// Aggregated statistics. Written by the collect() goroutine, read by
	// GetReport/GetStats/Reset from reporter and session goroutines — all
	// access goes through aggMu.
	aggMu          sync.RWMutex
	totalOps       int64
	successOps     int64
	errorOps       int64
	avgLatency     time.Duration
	minLatency     time.Duration
	maxLatency     time.Duration
	lastReport     time.Time
	lastTotal      int64
	maxWorkers     int
	currentWorkers int

	// openConnsFn, when set, reports the actual number of pool connections
	// open across workers (Scheduler.OpenConnectionCount) — read under aggMu
	// alongside currentWorkers so the status line can show both the
	// bookkeeping count and the real socket count.
	openConnsFn     func() int
	openConnections int
	minOpenConns    int
	maxOpenConns    int

	// Per-minute load timeline for the final summary (whole run, capped).
	minuteMu   sync.Mutex
	minutes    []MinutePoint
	minuteIdx  int // elapsed-minute index the open bucket belongs to
	minOK      int64
	minErr     int64
	minLatMs   int64
	minSamples int64

	startTime time.Time // set at construction/Reset, read-only afterwards

	// Error tracking
	errorCounts map[string]int64
	errorMutex  sync.RWMutex

	// Latency tracking
	latencyBuckets [14]int64 // worker.LatencyBucketLabels ranges (legacy 0-7, extended 8-13)
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
		workerMetrics: workerMetrics, // Use shared instance from scheduler
		scheduler:     scheduler,
		profile:       profile,
		cache:         cache,
		startTime:     time.Now(),
		lastReport:    time.Now(),
		errorCounts:   make(map[string]int64),
		opCounts:      make(map[string]int64),
		ctx:           ctx,
		cancel:        cancel,
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

	// Fold this tick into the per-minute timeline
	mc.observeMinute()

	// Update timestamp
	mc.aggMu.Lock()
	mc.lastReport = time.Now()
	mc.aggMu.Unlock()
}

// MinutePoint is one per-minute load bucket of the final-summary timeline.
type MinutePoint struct {
	Minute   int   `json:"minute"`   // 1-based minute of the run
	OK       int64 `json:"ok"`       // successful transactions in this minute
	Err      int64 `json:"err"`      // failed transactions in this minute
	LatSumMs int64 `json:"latSumMs"` // estimated latency sum (avg*count per tick)
	N        int64 `json:"n"`        // total transactions in this minute
}

const minuteCap = 1440 // a day-long run is the sane upper bound

// observeMinute rolls 1-second collector ticks into per-minute buckets. The
// latency sum is estimated from the tick's average (bucket-midpoint based) —
// fine for a sparkline, not a substitute for the histogram.
func (mc *MetricsCollector) observeMinute() {
	mc.aggMu.RLock()
	total := mc.totalOps
	success := mc.successOps
	avgMs := mc.avgLatency.Milliseconds()
	start := mc.startTime
	mc.aggMu.RUnlock()

	idx := int(time.Since(start).Minutes())
	dOK := success - mc.minOK
	dErr := (total - success) - mc.minErr
	dN := total - mc.minSamples
	dLatMs := avgMs*total - mc.minLatMs

	mc.minuteMu.Lock()
	defer mc.minuteMu.Unlock()
	if idx != mc.minuteIdx {
		if idx > 0 {
			mc.minutes = append(mc.minutes, MinutePoint{
				Minute:   mc.minuteIdx + 1,
				OK:       mc.minOK,
				Err:      mc.minErr,
				LatSumMs: mc.minLatMs,
				N:        mc.minSamples,
			})
		}
		if len(mc.minutes) > minuteCap {
			mc.minutes = mc.minutes[len(mc.minutes)-minuteCap:]
		}
		mc.minuteIdx = idx
	}
	mc.minOK += dOK
	mc.minErr += dErr
	mc.minLatMs += dLatMs
	mc.minSamples += dN
}

// MinuteTimeline returns the closed per-minute buckets plus the open one.
func (mc *MetricsCollector) MinuteTimeline() []MinutePoint {
	mc.minuteMu.Lock()
	defer mc.minuteMu.Unlock()
	out := make([]MinutePoint, len(mc.minutes), len(mc.minutes)+1)
	copy(out, mc.minutes)
	out = append(out, MinutePoint{
		Minute:   mc.minuteIdx + 1,
		OK:       mc.minOK,
		Err:      mc.minErr,
		LatSumMs: mc.minLatMs,
		N:        mc.minSamples,
	})
	return out
}

// MinMaxOpenConns returns the observed range of open pool connections
// (nonzero ticks only: the 0 before the first connect is not a real minimum).
func (mc *MetricsCollector) MinMaxOpenConns() (min, max int) {
	mc.aggMu.RLock()
	defer mc.aggMu.RUnlock()
	return mc.minOpenConns, mc.maxOpenConns
}

// updateFromWorkerMetrics updates metrics from the worker metrics collector
func (mc *MetricsCollector) updateFromWorkerMetrics() {
	// Get current totals from worker metrics
	total := mc.workerMetrics.GetTotalTransactions()
	successRate := mc.workerMetrics.GetSuccessRate()

	// Pull the latency histogram the workers record (RecordTransactionNamed
	// — both EMPLOYEE-profile ops and emul units). Without this the report's
	// Latency Distribution section stays empty.
	buckets := mc.workerMetrics.GetLatencyBucketCounts()

	// Derive the aggregate latency figures from the histogram itself:
	// min/max are the bounds of the outermost non-empty buckets, the average
	// is a bucket-midpoint estimate. The previous figures mislabeled p50 and
	// the p50/p95/p99 mean as min and average.
	totalCount := int64(0)
	var weightedSum int64
	var minLower, maxBound int64
	any := false
	for i, count := range buckets {
		if count == 0 {
			continue
		}
		upper := worker.LatencyBucketUpperBoundMs(i)
		lower := int64(0)
		if i > 0 {
			lower = worker.LatencyBucketUpperBoundMs(i - 1)
		}
		if !any {
			minLower = lower
			any = true
		}
		maxBound = upper
		mid := (lower + upper) / 2
		weightedSum += mid * count
		totalCount += count
	}

	// Update our counters
	mc.aggMu.Lock()
	mc.totalOps = total
	mc.successOps = int64(float64(total) * successRate / 100.0)
	mc.errorOps = total - mc.successOps
	if any {
		mc.minLatency = time.Duration(minLower) * time.Millisecond
		mc.maxLatency = time.Duration(maxBound) * time.Millisecond
		mc.avgLatency = time.Duration(weightedSum/totalCount) * time.Millisecond
	} else {
		mc.minLatency = 0
		mc.maxLatency = 0
		mc.avgLatency = 0
	}
	mc.aggMu.Unlock()

	// Pull per-op counts recorded by workers via NextOpWithName
	if ops := mc.workerMetrics.GetOpCounts(); len(ops) > 0 {
		mc.opMutex.Lock()
		for k, v := range ops {
			mc.opCounts[k] = v
		}
		mc.opMutex.Unlock()
	}

	// Pull error taxonomy into report-friendly map
	if es := mc.workerMetrics.ErrorStats(); es != nil {
		_, _, _, _, counts := es.Snapshot()
		mc.errorMutex.Lock()
		for k, v := range counts {
			mc.errorCounts[k] = int64(v)
		}
		mc.errorMutex.Unlock()
	}

	mc.latencyMutex.Lock()
	mc.latencyBuckets = buckets
	mc.latencyMutex.Unlock()
}

// updateFromScheduler updates metrics from the scheduler
func (mc *MetricsCollector) updateFromScheduler() {
	current := mc.scheduler.GetCurrentWorkerCount()
	var open int
	if mc.openConnsFn != nil {
		open = mc.openConnsFn()
	}
	mc.aggMu.Lock()
	mc.currentWorkers = current
	mc.openConnections = open
	if mc.currentWorkers > mc.maxWorkers {
		mc.maxWorkers = mc.currentWorkers
	}
	if open > 0 {
		if mc.minOpenConns == 0 || open < mc.minOpenConns {
			mc.minOpenConns = open
		}
		if open > mc.maxOpenConns {
			mc.maxOpenConns = open
		}
	}
	mc.aggMu.Unlock()
}

// SetOpenConnectionsFunc wires the live pool-connection counter (usually
// Scheduler.OpenConnectionCount). Optional: without it the status line shows
// only the bookkeeping worker count.
func (mc *MetricsCollector) SetOpenConnectionsFunc(fn func() int) {
	mc.openConnsFn = fn
}

// updateFromCache updates metrics from the cache
func (mc *MetricsCollector) updateFromCache() {
	// Cache hit/miss ratios could be tracked here if implemented
	// For now, we'll just note that cache is being used
}

// RecordTransaction records a transaction result. Delegates to the worker
// metrics collector; the aggregate views (op counts, latency buckets) are
// pulled from there by the sync loop, so no local bookkeeping happens here —
// it used to double-count between overwrites.
func (mc *MetricsCollector) RecordTransaction(success bool, latency time.Duration, opName string) {
	mc.workerMetrics.RecordTransactionNamed(success, latency, opName)
}

// RecordError records an error. Delegates to the worker metrics collector;
// the sync loop pulls the taxonomy snapshot from there.
func (mc *MetricsCollector) RecordError(err error, opName string) {
	mc.workerMetrics.RecordError(err)
	_ = opName
}

// GetReport generates a comprehensive metrics report
func (mc *MetricsCollector) GetReport() *Report {
	mc.updateFromWorkerMetrics()
	mc.updateFromScheduler()

	mc.opMutex.RLock()
	defer mc.opMutex.RUnlock()

	mc.errorMutex.RLock()
	defer mc.errorMutex.RUnlock()

	mc.latencyMutex.RLock()
	defer mc.latencyMutex.RUnlock()

	mc.aggMu.RLock()
	defer mc.aggMu.RUnlock()

	elapsed := time.Since(mc.startTime)
	interval := time.Since(mc.lastReport)

	// Calculate rates
	totalRate := float64(mc.totalOps) / elapsed.Seconds()
	intervalRate := float64(mc.totalOps-mc.lastTotal) / interval.Seconds()

	// Calculate success rate
	successRate := 0.0
	if mc.totalOps > 0 {
		successRate = float64(mc.successOps) / float64(mc.totalOps) * 100.0
	}

	// Get latency percentiles
	p50, p95, p99 := mc.getLatencyPercentiles()
	if p50 == 0 && mc.workerMetrics != nil {
		wp50, wp95, wp99 := mc.workerMetrics.GetLatencyPercentiles()
		p50 = time.Duration(wp50) * time.Millisecond
		p95 = time.Duration(wp95) * time.Millisecond
		p99 = time.Duration(wp99) * time.Millisecond
	}

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

	// Get cache stats (nil for profiles that need no key cache, e.g. oltp-emul)
	cacheStats := ""
	if mc.cache != nil {
		cacheStats = mc.cache.GetStats()
	}

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
		OpenConnections:   mc.openConnections,
		SchedulerStats:    schedulerStats,
		CacheStats:        cacheStats,
		ProfileName:       profileStats,
		OperationCounts:   opCounts,
		ErrorCounts:       errorCounts,
		LatencyBuckets:    mc.latencyBuckets,
	}

	mc.lastTotal = mc.totalOps
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

	// Calculate cumulative counts. Percentile targets are rounded up so
	// small samples hit their own (non-empty) bucket instead of the first
	// one: for total=1 the single transaction is p50, p95 and p99.
	cumulative := int64(0)
	p50Target := (total*50 + 99) / 100
	p95Target := (total*95 + 99) / 100
	p99Target := (total*99 + 99) / 100

	p50, p95, p99 = 0, 0, 0

	for i, count := range mc.latencyBuckets {
		cumulative += count
		bucketMs := worker.LatencyBucketUpperBoundMs(i)

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

// Reset resets all metrics
func (mc *MetricsCollector) Reset() {
	mc.workerMetrics.Reset()
	mc.aggMu.Lock()
	mc.totalOps = 0
	mc.successOps = 0
	mc.errorOps = 0
	mc.avgLatency = 0
	mc.minLatency = 0
	mc.maxLatency = 0
	mc.maxWorkers = 0
	mc.currentWorkers = 0
	mc.openConnections = 0
	mc.minOpenConns = 0
	mc.maxOpenConns = 0
	mc.startTime = time.Now()
	mc.lastReport = time.Now()
	mc.lastTotal = 0
	mc.aggMu.Unlock()

	mc.minuteMu.Lock()
	mc.minutes = nil
	mc.minuteIdx = 0
	mc.minOK = 0
	mc.minErr = 0
	mc.minLatMs = 0
	mc.minSamples = 0
	mc.minuteMu.Unlock()

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
	OpenConnections   int
	SchedulerStats    string
	CacheStats        string
	ProfileName       string
	OperationCounts   map[string]int64
	ErrorCounts       map[string]int64
	LatencyBuckets    [14]int64
}

// GetSummary returns a summary string of the report
func (r *Report) GetSummary() string {
	return fmt.Sprintf(
		"Total: %d, Success: %.1f%%, TPS: %.1f, Lat: avg=%v min=%v max=%v p50=%v p95=%v p99=%v, Workers: %d/%d, Conns: %d, Profile: %s",
		r.TotalOperations, r.SuccessRate, r.TotalTPS,
		r.AverageLatency, r.MinLatency, r.MaxLatency,
		r.P50Latency, r.P95Latency, r.P99Latency,
		r.CurrentWorkers, r.MaxWorkers, r.OpenConnections, r.ProfileName,
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
	buckets := worker.LatencyBucketLabels[:]
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
