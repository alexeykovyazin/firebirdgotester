package worker

import (
	"testing"
	"time"
)

// TestLatencyBucketUpperBoundMs pins the single-source bucket table: the
// reporting package derives percentiles from the same ranges, and a local
// stale copy once reported p95=p99=0ms for every >=2s transaction.
func TestLatencyBucketUpperBoundMs(t *testing.T) {
	cases := []struct {
		bucket int
		want   int64
	}{
		{0, 5}, {1, 10}, {2, 25}, {3, 50}, {4, 100}, {5, 250}, {6, 500}, {7, 1000},
		{8, 2000}, {9, 5000}, {10, 10000}, {11, 30000}, {12, 60000}, {13, 60000},
		{99, 60000},
	}
	for _, c := range cases {
		if got := LatencyBucketUpperBoundMs(c.bucket); got != c.want {
			t.Errorf("LatencyBucketUpperBoundMs(%d) = %d, want %d", c.bucket, got, c.want)
		}
	}
}

// TestLatencyPercentilesSmallSamples guards the percentile targets: with
// rounded-down targets a single slow transaction was reported as p50=5ms.
func TestLatencyPercentilesSmallSamples(t *testing.T) {
	mc := NewMetricsCollector()
	mc.RecordTransactionNamed(true, 30*time.Second, "op")
	p50, p95, p99 := mc.GetLatencyPercentiles()
	// 30s lands in bucket 11 (<30000ms) — no wait: 30s == 30000ms is not
	// < 30000ms, so it is bucket 12 (<60000ms).
	if p50 != 60000 || p95 != 60000 || p99 != 60000 {
		t.Fatalf("single 30s tx percentiles = %d/%d/%d ms, want 60000/60000/60000", p50, p95, p99)
	}

	mc = NewMetricsCollector()
	mc.RecordTransactionNamed(true, 30*time.Millisecond, "op")
	p50, p95, p99 = mc.GetLatencyPercentiles()
	if p50 != 50 || p95 != 50 || p99 != 50 {
		t.Fatalf("single 30ms tx percentiles = %d/%d/%d ms, want 50/50/50", p50, p95, p99)
	}

	// Two samples: nearest-rank p50 of {10ms, 30s} must be the smaller one.
	// 10ms is bucket 2 (<25ms), so its upper bound is 25.
	mc = NewMetricsCollector()
	mc.RecordTransactionNamed(true, 10*time.Millisecond, "op")
	mc.RecordTransactionNamed(true, 30*time.Second, "op")
	p50, _, _ = mc.GetLatencyPercentiles()
	if p50 != 25 {
		t.Fatalf("p50 of {10ms, 30s} = %d ms, want 25", p50)
	}
}

// TestMetricsReset zeroes every counter: a fresh session must not inherit
// the previous session's histogram.
func TestMetricsReset(t *testing.T) {
	mc := NewMetricsCollector()
	mc.RecordTransactionNamed(true, 20*time.Millisecond, "op")
	mc.RecordTransactionNamed(false, 5*time.Second, "op")
	if mc.GetTotalTransactions() != 2 {
		t.Fatalf("total = %d, want 2", mc.GetTotalTransactions())
	}
	mc.Reset()
	if mc.GetTotalTransactions() != 0 || mc.GetTxSuccess() != 0 || mc.GetTxError() != 0 {
		t.Fatalf("counters not reset: total=%d", mc.GetTotalTransactions())
	}
	if got := mc.GetLatencyBucketCounts(); got != ([LatencyBucketCount]int64{}) {
		t.Fatalf("buckets not reset: %v", got)
	}
	if len(mc.GetOpCounts()) != 0 {
		t.Fatalf("op counts not reset: %v", mc.GetOpCounts())
	}
}
