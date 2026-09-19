package session

import (
	"path/filepath"
	"strings"
	"testing"
)

func newTestHistory(t *testing.T) *RunHistory {
	t.Helper()
	h := NewRunHistory(filepath.Join(t.TempDir(), "runs.json"))
	return h
}

func TestRunCreateUpdateFinishOutcome(t *testing.T) {
	h := newTestHistory(t)
	run := h.Create(OriginSchedule, "sch_1", []SessionRun{
		{SessionID: "a", RelPath: "db1/EMPLOYEE.FDB"},
		{SessionID: "b", RelPath: "db2/EMPLOYEE.FDB"},
	})
	if run.Outcome != RunRunning {
		t.Fatalf("new run outcome = %q, want running", run.Outcome)
	}

	finished := h.UpdateSession(run.ID, "a", SessionRun{Status: string(StatusCompleted), Success: 10, TPS: 2.5})
	if finished != nil {
		t.Fatalf("run finished while session b still pending")
	}

	finished = h.UpdateSession(run.ID, "b", SessionRun{Status: string(StatusCompleted), Success: 5})
	if finished == nil || finished.Outcome != RunOK {
		t.Fatalf("outcome = %+v, want ok", finished)
	}
	if finished.FinishedAt == "" {
		t.Fatal("finished run must have FinishedAt")
	}

	got, ok := h.Get(run.ID)
	if !ok || got.Outcome != RunOK || len(got.Sessions) != 2 {
		t.Fatalf("Get mismatch: %+v ok=%v", got, ok)
	}
	if got.Sessions[0].Success != 10 || got.Sessions[0].Pending {
		t.Fatalf("entry a not updated: %+v", got.Sessions[0])
	}
}

func TestRunUpdateIdempotent(t *testing.T) {
	h := newTestHistory(t)
	run := h.Create(OriginManual, "", []SessionRun{{SessionID: "a"}})
	if h.UpdateSession(run.ID, "a", SessionRun{Status: string(StatusCompleted)}) == nil {
		t.Fatal("first update should finalize the run")
	}
	if h.UpdateSession(run.ID, "a", SessionRun{Status: "Idle", Success: 99}) != nil {
		t.Fatal("second update must be ignored")
	}
	got, _ := h.Get(run.ID)
	if got.Sessions[0].Success != 0 || got.Sessions[0].Status != string(StatusCompleted) {
		t.Fatalf("terminal entry was overwritten: %+v", got.Sessions[0])
	}
}

func TestRunOutcomes(t *testing.T) {
	cases := []struct {
		name string
		stat []string
		want RunOutcome
	}{
		{"all completed", []string{string(StatusCompleted), string(StatusCompleted)}, RunOK},
		{"mixed", []string{string(StatusCompleted), string(StatusFailed)}, RunPartial},
		{"all failed", []string{string(StatusFailed), string(StatusFailed)}, RunFailed},
		{"all skipped", []string{"Skipped", "Skipped"}, RunSkipped},
		{"completed+skipped", []string{string(StatusCompleted), "Skipped"}, RunPartial},
	}
	for _, tc := range cases {
		h := newTestHistory(t)
		var entries []SessionRun
		for i, st := range tc.stat {
			entries = append(entries, SessionRun{SessionID: string(rune('a' + i)), Status: st})
		}
		// Entries arrive pre-terminal (e.g. all targets skipped at fire time).
		run := h.Create(OriginManual, "", entries)
		h.MaybeFinish(run.ID)
		got, _ := h.Get(run.ID)
		if got.Outcome != tc.want {
			t.Errorf("%s: outcome = %q, want %q", tc.name, got.Outcome, tc.want)
		}
	}
}

func TestRunCancel(t *testing.T) {
	h := newTestHistory(t)
	run := h.Create(OriginSchedule, "sch_1", []SessionRun{
		{SessionID: "a"},
		{SessionID: "b"},
	})
	h.UpdateSession(run.ID, "b", SessionRun{Status: string(StatusCompleted)})

	if !h.Cancel(run.ID, "test") {
		t.Fatal("cancel should succeed on a running run")
	}
	got, _ := h.Get(run.ID)
	if got.Outcome != RunCancelled || got.FinishedAt == "" {
		t.Fatalf("cancel not recorded: %+v", got)
	}
	if got.Sessions[0].Status != "Cancelled" || got.Sessions[0].Reason != "test" {
		t.Fatalf("pending entry not cancelled: %+v", got.Sessions[0])
	}
	if h.Cancel(run.ID, "again") {
		t.Fatal("second cancel must fail")
	}
}

func TestRunRetention(t *testing.T) {
	h := newTestHistory(t)
	h.SetLimits(2, 30)
	var kept []string
	h.SetKeepRun(func(id string) bool {
		for _, k := range kept {
			if k == id {
				return true
			}
		}
		return false
	})

	r1 := h.Create(OriginManual, "", []SessionRun{{SessionID: "a"}})
	h.UpdateSession(r1.ID, "a", SessionRun{Status: string(StatusCompleted)})
	r2 := h.Create(OriginManual, "", []SessionRun{{SessionID: "a"}})
	h.UpdateSession(r2.ID, "a", SessionRun{Status: string(StatusCompleted)})
	kept = []string{r1.ID} // protect the oldest run from pruning
	r3 := h.Create(OriginManual, "", []SessionRun{{SessionID: "a"}})
	h.UpdateSession(r3.ID, "a", SessionRun{Status: string(StatusCompleted)})

	runs := h.List(ListFilter{})
	if len(runs) != 2 {
		t.Fatalf("expected referenced run + 1 recent, got %d", len(runs))
	}
	ids := map[string]bool{}
	for _, r := range runs {
		ids[r.ID] = true
	}
	if !ids[r1.ID] {
		t.Fatal("referenced run was pruned")
	}
	if ids[r2.ID] {
		t.Fatal("unprotected oldest run should have been pruned")
	}
	if !ids[r3.ID] {
		t.Fatal("newest run missing")
	}
}

func TestRunLoadMarksInterruptedCancelled(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "runs.json")

	h := NewRunHistory(path)
	run := h.Create(OriginSchedule, "sch_1", []SessionRun{{SessionID: "a"}})
	// simulate process death: run persisted, never finished
	h2 := NewRunHistory(path)
	if err := h2.Load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	got, ok := h2.Get(run.ID)
	if !ok {
		t.Fatal("run not found after load")
	}
	if got.Outcome != RunCancelled || got.FinishedAt == "" {
		t.Fatalf("interrupted run not cancelled: %+v", got)
	}
	if got.Sessions[0].Status != "Cancelled" || got.Sessions[0].Reason != "process restarted" {
		t.Fatalf("session entry not marked: %+v", got.Sessions[0])
	}
}

func TestRunPersistRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "runs.json")
	h := NewRunHistory(path)
	run := h.Create(OriginSchedule, "sch_x", []SessionRun{{SessionID: "a", RelPath: "db1/x.FDB"}})
	h.UpdateSession(run.ID, "a", SessionRun{Status: string(StatusCompleted), Success: 7})

	h2 := NewRunHistory(path)
	if err := h2.Load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	got, ok := h2.Get(run.ID)
	if !ok || got.Outcome != RunOK || got.ScheduleID != "sch_x" || got.Sessions[0].Success != 7 {
		t.Fatalf("round trip mismatch: %+v ok=%v", got, ok)
	}
	if !strings.Contains(got.ID, "run_") {
		t.Fatalf("bad run id: %s", got.ID)
	}
}
