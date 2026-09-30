package worker

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestTopErrorTextsCountsAndSort(t *testing.T) {
	mc := NewMetricsCollector()

	lockErr := errors.New("lock conflict on no wait transaction")
	for i := 0; i < 5; i++ {
		mc.RecordError(lockErr)
	}
	mc.RecordError(errors.New("deadlock"))
	mc.RecordError(nil) // must be ignored

	top := mc.TopErrorTexts(10)
	if len(top) != 2 {
		t.Fatalf("got %d distinct texts, want 2", len(top))
	}
	if top[0].Count != 5 || top[0].Message != "lock conflict on no wait transaction" {
		t.Fatalf("top[0] = %+v, want lock conflict x5", top[0])
	}
	if top[1].Count != 1 {
		t.Fatalf("top[1].Count = %d, want 1", top[1].Count)
	}
	if top[0].FirstSeen.IsZero() || top[0].LastSeen.Before(top[0].FirstSeen) {
		t.Fatalf("first/last seen not tracked: %+v", top[0])
	}
}

func TestTopErrorTextsLimitAndReset(t *testing.T) {
	mc := NewMetricsCollector()
	for i := 0; i < maxErrTexts+10; i++ {
		mc.RecordError(fmt.Errorf("err kind %d", i))
	}
	if got := len(mc.TopErrorTexts(maxErrTexts + 50)); got != maxErrTexts {
		t.Fatalf("map grew beyond the cap: %d distinct texts", got)
	}

	mc.Reset()
	if got := len(mc.TopErrorTexts(10)); got != 0 {
		t.Fatalf("Reset left %d distinct texts", got)
	}
}

func TestRecordErrorTextTruncatesLongMessages(t *testing.T) {
	mc := NewMetricsCollector()
	long := make([]byte, 500)
	for i := range long {
		long[i] = 'x'
	}
	mc.RecordError(errors.New(string(long)))
	top := mc.TopErrorTexts(1)
	if len(top) != 1 || len(top[0].Message) != 300 {
		t.Fatalf("message not truncated: len=%d", len(top[0].Message))
	}
	if time.Since(top[0].FirstSeen) > time.Minute {
		t.Fatalf("FirstSeen not set: %v", top[0].FirstSeen)
	}
}
