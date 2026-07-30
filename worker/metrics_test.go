package worker

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSetConnectionCountAbsolute(t *testing.T) {
	mc := NewMetricsCollector()
	mc.SetConnectionCount(5)
	mc.SetConnectionCount(3)
	if got := mc.GetConnectionCount(); got != 3 {
		t.Fatalf("expected absolute count 3, got %d", got)
	}
}

func TestRecordConnectionChangeAbsolute(t *testing.T) {
	mc := NewMetricsCollector()
	mc.RecordConnectionChange(10)
	mc.RecordConnectionChange(4)
	if got := mc.GetConnectionCount(); got != 4 {
		t.Fatalf("expected absolute count 4, got %d", got)
	}
}

func TestTPSIntervalUsesDelta(t *testing.T) {
	mc := NewMetricsCollector()
	for i := 0; i < 10; i++ {
		mc.RecordTransaction(true, time.Millisecond)
	}
	mc.UpdateLastReportTime()
	for i := 0; i < 20; i++ {
		mc.RecordTransaction(true, time.Millisecond)
	}
	// Force a non-zero window
	mc.mu.Lock()
	mc.lastReportTime = time.Now().Add(-2 * time.Second)
	mc.mu.Unlock()

	tps := mc.GetTPSInterval()
	if tps < 9 || tps > 11 {
		t.Fatalf("expected ~10 TPS interval, got %.2f", tps)
	}
}

func TestConcurrentCountersNoRace(t *testing.T) {
	mc := NewMetricsCollector()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				mc.RecordTransactionNamed(j%2 == 0, time.Millisecond*time.Duration(j%20), "OP")
				if j%7 == 0 {
					mc.RecordError(fmt.Errorf("boom"))
				}
				mc.SetConnectionCount(int64(j % 10))
			}
		}()
	}
	wg.Wait()
	if mc.GetTotalTransactions() != 32*200 {
		t.Fatalf("lost transactions: %d", mc.GetTotalTransactions())
	}
	total, _, _, _, _ := mc.ErrorStats().Snapshot()
	if total == 0 {
		t.Fatal("expected shared error stats to retain records")
	}
}
