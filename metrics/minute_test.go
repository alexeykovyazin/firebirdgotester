package metrics

import (
	"testing"
	"time"

	"fb-loadgen/worker"
)

func TestCollectorMinuteTimelineAndConns(t *testing.T) {
	mc := &MetricsCollector{startTime: time.Now().Add(-2 * time.Minute)}
	mc.aggMu.Lock()
	mc.totalOps = 100
	mc.successOps = 90
	mc.avgLatency = 5 * time.Millisecond
	mc.minOpenConns = 2
	mc.maxOpenConns = 20
	mc.aggMu.Unlock()

	mc.observeMinute()

	tl := mc.MinuteTimeline()
	if len(tl) != 2 {
		t.Fatalf("got %d bucket(s), want 2 (closed minute 1 + open minute 3)", len(tl))
	}
	if tl[0].Minute != 1 || tl[0].OK != 0 || tl[0].N != 0 {
		t.Fatalf("closed bucket wrong: %+v", tl[0])
	}
	if tl[1].Minute != 3 || tl[1].OK != 90 || tl[1].Err != 10 || tl[1].N != 100 {
		t.Fatalf("open bucket wrong: %+v", tl[1])
	}
	if tl[1].LatSumMs != 500 { // avg 5ms x 100 ops
		t.Fatalf("LatSumMs = %d, want 500", tl[1].LatSumMs)
	}

	if min, max := mc.MinMaxOpenConns(); min != 2 || max != 20 {
		t.Fatalf("MinMaxOpenConns = %d/%d, want 2/20", min, max)
	}
}

func TestCollectorMinuteTimelineReset(t *testing.T) {
	mc := &MetricsCollector{startTime: time.Now(), workerMetrics: worker.NewMetricsCollector()}
	mc.observeMinute()
	mc.Reset()
	if tl := mc.MinuteTimeline(); len(tl) != 1 || tl[0].N != 0 {
		t.Fatalf("Reset did not clear the timeline: %+v", tl)
	}
	if min, max := mc.MinMaxOpenConns(); min != 0 || max != 0 {
		t.Fatalf("Reset did not clear conn range: %d/%d", min, max)
	}
}
