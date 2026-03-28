package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// Reporter handles output formatting and reporting
type Reporter struct {
	metricsCollector *MetricsCollector
	outputFile       *os.File
	outputMutex      sync.Mutex
	reportInterval   time.Duration
	format           string // "text", "json", "csv"

	// Context for cancellation
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewReporter creates a new reporter
func NewReporter(metricsCollector *MetricsCollector, outputFile *os.File, format string, reportInterval time.Duration) *Reporter {
	ctx, cancel := context.WithCancel(context.Background())

	return &Reporter{
		metricsCollector: metricsCollector,
		outputFile:       outputFile,
		format:           format,
		reportInterval:   reportInterval,
		ctx:              ctx,
		cancel:           cancel,
	}
}

// Start starts the reporting goroutine
func (r *Reporter) Start() {
	r.wg.Add(1)
	go r.report()
}

// Stop stops the reporting
func (r *Reporter) Stop() {
	r.cancel()
	r.wg.Wait()
}

// report runs the reporting loop
func (r *Reporter) report() {
	defer r.wg.Done()

	ticker := time.NewTicker(r.reportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			r.generateReport()
		}
	}
}

// generateReport generates and outputs a report
func (r *Reporter) generateReport() {
	report := r.metricsCollector.GetReport()

	r.outputMutex.Lock()
	defer r.outputMutex.Unlock()

	switch r.format {
	case "json":
		r.outputJSON(report)
	case "csv":
		r.outputCSV(report)
	default:
		r.outputText(report)
	}
}

// outputText outputs the report in text format
func (r *Reporter) outputText(report *Report) {
	output := report.GetDetailedReport()

	if r.outputFile != nil {
		r.outputFile.WriteString(output + "\n")
	} else {
		fmt.Print(output)
	}
}

// outputJSON outputs the report in JSON format
func (r *Reporter) outputJSON(report *Report) {
	jsonData, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		fmt.Printf("Error marshaling JSON: %v\n", err)
		return
	}

	output := string(jsonData) + "\n"

	if r.outputFile != nil {
		r.outputFile.WriteString(output)
	} else {
		fmt.Print(output)
	}
}

// outputCSV outputs the report in CSV format
func (r *Reporter) outputCSV(report *Report) {
	// CSV header (only on first call)
	if r.outputFile != nil {
		// Check if file is empty (first write)
		stat, err := r.outputFile.Stat()
		if err == nil && stat.Size() == 0 {
			header := "timestamp,elapsed,interval,total_operations,success_operations,error_operations,success_rate,total_tps,interval_tps,avg_latency,min_latency,max_latency,p50_latency,p95_latency,p99_latency,max_workers,current_workers,profile\n"
			r.outputFile.WriteString(header)
		}
	}

	// CSV data
	data := fmt.Sprintf("%s,%v,%v,%d,%d,%d,%.2f,%.2f,%.2f,%v,%v,%v,%v,%v,%v,%d,%d,%s\n",
		report.Timestamp.Format(time.RFC3339),
		report.Elapsed.Seconds(),
		report.Interval.Seconds(),
		report.TotalOperations,
		report.SuccessOperations,
		report.ErrorOperations,
		report.SuccessRate,
		report.TotalTPS,
		report.IntervalTPS,
		report.AverageLatency.Milliseconds(),
		report.MinLatency.Milliseconds(),
		report.MaxLatency.Milliseconds(),
		report.P50Latency.Milliseconds(),
		report.P95Latency.Milliseconds(),
		report.P99Latency.Milliseconds(),
		report.MaxWorkers,
		report.CurrentWorkers,
		report.ProfileName,
	)

	if r.outputFile != nil {
		r.outputFile.WriteString(data)
	} else {
		fmt.Print(data)
	}
}

// ReportSummary generates a summary report
func (r *Reporter) ReportSummary() string {
	report := r.metricsCollector.GetReport()
	return report.GetSummary()
}

// ReportFinal generates a final comprehensive report
func (r *Reporter) ReportFinal() string {
	report := r.metricsCollector.GetReport()

	summary := fmt.Sprintf(`
=== FINAL LOAD TEST REPORT ===

Summary:
%s

Operation Distribution:
%s

Error Distribution:
%s

Latency Distribution:
%s

System Configuration:
  Profile: %s
  Scheduler: %s
  Cache: %s

Test Duration: %v
`,
		report.GetSummary(),
		report.formatOperationCounts(),
		report.formatErrorCounts(),
		report.formatLatencyBuckets(),
		report.ProfileName,
		report.SchedulerStats,
		report.CacheStats,
		report.Elapsed,
	)

	return summary
}

// ReportLatencyHistogram outputs a detailed latency histogram
func (r *Reporter) ReportLatencyHistogram() string {
	report := r.metricsCollector.GetReport()

	buckets := []string{"<5ms", "<10ms", "<25ms", "<50ms", "<100ms", "<250ms", "<500ms", "<1000ms", ">=1000ms"}
	total := int64(0)
	for _, count := range report.LatencyBuckets {
		total += count
	}

	if total == 0 {
		return "No latency data available"
	}

	histogram := "Latency Histogram:\n"
	for i, count := range report.LatencyBuckets {
		percentage := float64(count) / float64(total) * 100.0
		bar := ""
		if percentage > 0 {
			barLength := int(percentage / 5) // Scale for display
			for j := 0; j < barLength; j++ {
				bar += "█"
			}
		}
		histogram += fmt.Sprintf("  %8s: %8d (%6.1f%%) %s\n", buckets[i], count, percentage, bar)
	}

	return histogram
}

// ReportOperationBreakdown outputs detailed operation statistics
func (r *Reporter) ReportOperationBreakdown() string {
	report := r.metricsCollector.GetReport()

	if len(report.OperationCounts) == 0 {
		return "No operation data available"
	}

	total := int64(0)
	for _, count := range report.OperationCounts {
		total += count
	}

	breakdown := "Operation Breakdown:\n"
	for op, count := range report.OperationCounts {
		percentage := float64(count) / float64(total) * 100.0
		breakdown += fmt.Sprintf("  %-25s: %8d (%6.1f%%)\n", op, count, percentage)
	}

	return breakdown
}

// ReportErrorBreakdown outputs detailed error statistics
func (r *Reporter) ReportErrorBreakdown() string {
	report := r.metricsCollector.GetReport()

	if len(report.ErrorCounts) == 0 {
		return "No errors occurred"
	}

	total := int64(0)
	for _, count := range report.ErrorCounts {
		total += count
	}

	breakdown := "Error Breakdown:\n"
	for err, count := range report.ErrorCounts {
		percentage := float64(count) / float64(total) * 100.0
		breakdown += fmt.Sprintf("  %-50s: %8d (%6.1f%%)\n", err, count, percentage)
	}

	return breakdown
}

// ReportPerformanceSummary outputs key performance metrics
func (r *Reporter) ReportPerformanceSummary() string {
	report := r.metricsCollector.GetReport()

	summary := fmt.Sprintf(`Performance Summary:
  Total Operations:    %d
  Successful:          %d (%.1f%%)
  Failed:              %d
  Total TPS:           %.1f
  Interval TPS:        %.1f
  Average Latency:     %v
  50th Percentile:     %v
  95th Percentile:     %v
  99th Percentile:     %v
  Min Latency:         %v
  Max Latency:         %v
  Max Workers:         %d
  Current Workers:     %d
`,
		report.TotalOperations,
		report.SuccessOperations,
		report.SuccessRate,
		report.ErrorOperations,
		report.TotalTPS,
		report.IntervalTPS,
		report.AverageLatency,
		report.P50Latency,
		report.P95Latency,
		report.P99Latency,
		report.MinLatency,
		report.MaxLatency,
		report.MaxWorkers,
		report.CurrentWorkers,
	)

	return summary
}

// ReportSystemStatus outputs current system status
func (r *Reporter) ReportSystemStatus() string {
	report := r.metricsCollector.GetReport()

	status := fmt.Sprintf(`System Status:
  Profile:             %s
  Scheduler:           %s
  Cache:               %s
  Test Duration:       %v
  Last Report:         %v ago
`,
		report.ProfileName,
		report.SchedulerStats,
		report.CacheStats,
		report.Elapsed,
		time.Since(report.Timestamp),
	)

	return status
}

// ReportToFile writes a specific report type to a file
func (r *Reporter) ReportToFile(filename, reportType string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", filename, err)
	}
	defer file.Close()

	var content string
	switch reportType {
	case "summary":
		content = r.ReportSummary()
	case "final":
		content = r.ReportFinal()
	case "latency":
		content = r.ReportLatencyHistogram()
	case "operations":
		content = r.ReportOperationBreakdown()
	case "errors":
		content = r.ReportErrorBreakdown()
	case "performance":
		content = r.ReportPerformanceSummary()
	case "status":
		content = r.ReportSystemStatus()
	default:
		return fmt.Errorf("unknown report type: %s", reportType)
	}

	_, err = file.WriteString(content)
	if err != nil {
		return fmt.Errorf("failed to write to file %s: %w", filename, err)
	}

	return nil
}

// ReportAllToFile writes all report types to separate files
func (r *Reporter) ReportAllToFile(baseFilename string) error {
	reportTypes := []string{"summary", "final", "latency", "operations", "errors", "performance", "status"}

	for _, reportType := range reportTypes {
		filename := fmt.Sprintf("%s_%s.txt", baseFilename, reportType)
		err := r.ReportToFile(filename, reportType)
		if err != nil {
			return err
		}
	}

	return nil
}

// SetOutputFile sets the output file for reporting
func (r *Reporter) SetOutputFile(file *os.File) {
	r.outputMutex.Lock()
	defer r.outputMutex.Unlock()
	r.outputFile = file
}

// SetFormat sets the output format
func (r *Reporter) SetFormat(format string) {
	r.outputMutex.Lock()
	defer r.outputMutex.Unlock()
	r.format = format
}

// SetReportInterval sets the report interval
func (r *Reporter) SetReportInterval(interval time.Duration) {
	r.outputMutex.Lock()
	defer r.outputMutex.Unlock()
	r.reportInterval = interval
}

// GetMetricsCollector returns the metrics collector
func (r *Reporter) GetMetricsCollector() *MetricsCollector {
	return r.metricsCollector
}

// ReportConfig outputs the current configuration
func (r *Reporter) ReportConfig() string {
	// This would need to be enhanced to include actual config
	return "Configuration: (not implemented in this version)"
}

// ReportHealth outputs system health information
func (r *Reporter) ReportHealth() string {
	report := r.metricsCollector.GetReport()

	health := fmt.Sprintf(`System Health:
  Status:              %s
  Workers:             %d/%d
  Success Rate:        %.1f%%
  Error Rate:          %.1f%%
  Average TPS:         %.1f
  Current Latency:     %v
  Peak Latency:        %v
  Memory Usage:        (not implemented)
  CPU Usage:           (not implemented)
`,
		"Healthy", // This would be calculated based on actual metrics
		report.CurrentWorkers,
		report.MaxWorkers,
		report.SuccessRate,
		100.0-report.SuccessRate,
		report.TotalTPS,
		report.AverageLatency,
		report.MaxLatency,
	)

	return health
}
