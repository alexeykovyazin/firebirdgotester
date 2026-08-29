package schedule

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"fb-loadgen/session"
)

// fakeRunner records engine calls without touching real sessions.
type fakeRunner struct {
	mu          sync.Mutex
	refs        []session.TargetRef
	unknown     []string
	busy        []string
	freeBudget  int
	runs        []session.Run
	stoppedIDs  []string
	staggerUsed time.Duration
	stopBusyArg bool
}

func (f *fakeRunner) ResolveTargets(all bool, ids []string) ([]session.TargetRef, []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.refs, f.unknown
}

func (f *fakeRunner) StartBulk(origin session.RunOrigin, scheduleID string, refs []session.TargetRef, skips []session.SkipTarget, spec session.RunSpec, stagger time.Duration, stopBusy bool) session.Run {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.staggerUsed = stagger
	f.stopBusyArg = stopBusy
	sessions := make([]session.SessionRun, 0, len(refs)+len(skips))
	for _, r := range refs {
		sessions = append(sessions, session.SessionRun{SessionID: r.ID, RelPath: r.RelPath, Status: "Starting", Pending: true})
	}
	for _, sk := range skips {
		sessions = append(sessions, session.SessionRun{SessionID: sk.ID, RelPath: sk.RelPath, Status: "Skipped", Reason: sk.Reason})
	}
	run := session.Run{
		ID:         fmt.Sprintf("run_%d", len(f.runs)+1),
		Origin:     string(origin),
		ScheduleID: scheduleID,
		Sessions:   sessions,
	}
	f.runs = append(f.runs, run)
	return run
}

func (f *fakeRunner) AnyBusy(ids []string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.busy
}

func (f *fakeRunner) BudgetFree() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.freeBudget
}

func (f *fakeRunner) StopSessions(ids []string) []error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stoppedIDs = append(f.stoppedIDs, ids...)
	return nil
}

func (f *fakeRunner) runCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.runs)
}

func baseSchedule(now time.Time) Schedule {
	return Schedule{
		Name:    "test",
		Enabled: true,
		Trigger: Trigger{Type: TriggerOnce, At: now.Add(-time.Second).Format(time.RFC3339)},
		Targets: Targets{SessionIDs: []string{"a"}},
		Run:     session.RunSpec{TimeLimitMin: 15},
	}
}

// waitFor polls cond until true or the deadline passes.
func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

func TestEngineOnceFiresOnceThenDisables(t *testing.T) {
	dir := t.TempDir()
	fr := &fakeRunner{refs: []session.TargetRef{{ID: "a", RelPath: "db1/x.FDB", ConnMax: 2}}, freeBudget: 10}
	e := NewEngine(filepath.Join(dir, "schedules.json"), fr)
	defer e.Stop()

	now := time.Now()
	e.SetNow(func() time.Time { return now })
	s, err := e.Create(baseSchedule(now))
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	e.tick(e.now())
	waitFor(t, 3*time.Second, func() bool { return fr.runCount() == 1 })

	got, _ := e.Get(s.ID)
	if got.LastFire == nil || got.LastFire.Outcome != FireFired || got.LastFire.RunID == "" {
		t.Fatalf("lastFire = %+v, want fired with runId", got.LastFire)
	}
	if got.Enabled {
		t.Fatal("once schedule must be disabled after firing")
	}
	if got.NextRunAt != "" {
		t.Fatalf("once schedule must have no next run, got %s", got.NextRunAt)
	}

	// a second tick must not fire again
	e.tick(e.now())
	time.Sleep(200 * time.Millisecond)
	if fr.runCount() != 1 {
		t.Fatalf("once schedule fired %d times", fr.runCount())
	}
}

func TestEngineIntervalRecomputesNext(t *testing.T) {
	dir := t.TempDir()
	fr := &fakeRunner{refs: []session.TargetRef{{ID: "a", RelPath: "db1/x.FDB", ConnMax: 2}}, freeBudget: 10}
	e := NewEngine(filepath.Join(dir, "schedules.json"), fr)
	defer e.Stop()

	now := time.Now()
	e.SetNow(func() time.Time { return now })
	s, err := e.Create(Schedule{
		Name:    "every-5",
		Enabled: true,
		Trigger: Trigger{Type: TriggerInterval, EveryMin: 5},
		Targets: Targets{SessionIDs: []string{"a"}},
		Run:     session.RunSpec{TimeLimitMin: 10},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	got, _ := e.Get(s.ID)
	if got.NextRunAt != now.Add(5*time.Minute).Format(time.RFC3339) {
		t.Fatalf("next = %s, want %s", got.NextRunAt, now.Add(5*time.Minute).Format(time.RFC3339))
	}

	later := now.Add(6 * time.Minute) // past the first due time
	e.SetNow(func() time.Time { return later })
	e.tick(later)
	waitFor(t, 3*time.Second, func() bool { return fr.runCount() == 1 })
	got, _ = e.Get(s.ID)
	want := later.Add(5 * time.Minute).Format(time.RFC3339)
	if got.NextRunAt != want {
		t.Fatalf("next after fire = %s, want %s (anchored at fire time)", got.NextRunAt, want)
	}
	if got.Enabled != true {
		t.Fatal("recurring schedule must stay enabled")
	}
}

func TestEngineCronNext(t *testing.T) {
	dir := t.TempDir()
	fr := &fakeRunner{}
	e := NewEngine(filepath.Join(dir, "schedules.json"), fr)
	defer e.Stop()

	// 2026-08-29 is a Saturday; 0 2 * * * → next is 2026-08-30 02:00 local
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.Local)
	e.SetNow(func() time.Time { return now })
	s, err := e.Create(Schedule{
		Name:    "nightly",
		Enabled: true,
		Trigger: Trigger{Type: TriggerCron, Cron: "0 2 * * *"},
		Targets: Targets{SessionIDs: []string{"a"}},
		Run:     session.RunSpec{TimeLimitMin: 30},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	got, _ := e.Get(s.ID)
	next, err := time.Parse(time.RFC3339, got.NextRunAt)
	if err != nil {
		t.Fatalf("parse next %q: %v", got.NextRunAt, err)
	}
	want := time.Date(2026, 8, 30, 2, 0, 0, 0, time.Local)
	if !next.Equal(want) {
		t.Fatalf("next = %s, want %s", next, want)
	}
}

func TestEngineCronTimezone(t *testing.T) {
	dir := t.TempDir()
	fr := &fakeRunner{}
	e := NewEngine(filepath.Join(dir, "schedules.json"), fr)
	defer e.Stop()

	// 2026-08-29 12:00 UTC = 15:00 Moscow; next 02:00 MSK = 2026-08-29 23:00 UTC
	loc := time.FixedZone("MSK", 3*3600)
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	e.SetNow(func() time.Time { return now })
	s, err := e.Create(Schedule{
		Name:    "nightly-msk",
		Enabled: true,
		Trigger: Trigger{Type: TriggerCron, Cron: "0 2 * * *", TZ: "Europe/Moscow"},
		Targets: Targets{SessionIDs: []string{"a"}},
		Run:     session.RunSpec{TimeLimitMin: 30},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	got, _ := e.Get(s.ID)
	next, err := time.Parse(time.RFC3339, got.NextRunAt)
	if err != nil {
		t.Fatalf("parse next: %v", err)
	}
	want := time.Date(2026, 8, 29, 23, 0, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Fatalf("next = %s UTC, want %s UTC (02:00 MSK)", next.UTC(), want)
	}
	_ = loc
}

func TestEngineSkipsWhenBusy(t *testing.T) {
	dir := t.TempDir()
	fr := &fakeRunner{
		refs:       []session.TargetRef{{ID: "a", RelPath: "db1/x.FDB", ConnMax: 2}},
		busy:       []string{"a"},
		freeBudget: 10,
	}
	e := NewEngine(filepath.Join(dir, "schedules.json"), fr)
	defer e.Stop()

	s, err := e.Create(Schedule{
		Name:    "busy-guard",
		Enabled: true,
		Trigger: Trigger{Type: TriggerOnce, At: time.Now().Add(-time.Second).Format(time.RFC3339)},
		Targets: Targets{SessionIDs: []string{"a"}},
		Run:     session.RunSpec{TimeLimitMin: 15},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	e.tick(e.now())
	waitFor(t, 3*time.Second, func() bool {
		got, _ := e.Get(s.ID)
		return got.LastFire != nil
	})
	got, _ := e.Get(s.ID)
	if got.LastFire.Outcome != FireSkipped || got.LastFire.RunID != "" {
		t.Fatalf("lastFire = %+v, want skipped without runId", got.LastFire)
	}
	if !strings.Contains(got.LastFire.Detail, "busy") {
		t.Fatalf("detail = %q, want busy mention", got.LastFire.Detail)
	}
	if fr.runCount() != 0 {
		t.Fatal("no run should start when targets are busy")
	}
}

func TestEngineStopAndRunStopsBusy(t *testing.T) {
	dir := t.TempDir()
	fr := &fakeRunner{
		refs:       []session.TargetRef{{ID: "a", RelPath: "db1/x.FDB", ConnMax: 2, Status: session.StatusRunning}},
		busy:       []string{"a"},
		freeBudget: 10,
	}
	e := NewEngine(filepath.Join(dir, "schedules.json"), fr)
	defer e.Stop()

	_, err := e.Create(Schedule{
		Name:    "takeover",
		Enabled: true,
		Trigger: Trigger{Type: TriggerOnce, At: time.Now().Add(-time.Second).Format(time.RFC3339)},
		Targets: Targets{SessionIDs: []string{"a"}},
		Run:     session.RunSpec{TimeLimitMin: 15},
		Policy:  Policy{IfRunning: "stopAndRun"},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	e.tick(e.now())
	waitFor(t, 3*time.Second, func() bool { return fr.runCount() == 1 })
	if len(fr.stoppedIDs) != 1 || fr.stoppedIDs[0] != "a" {
		t.Fatalf("stopped = %v, want [a]", fr.stoppedIDs)
	}
}

func TestEngineBudgetSkipImmediate(t *testing.T) {
	dir := t.TempDir()
	fr := &fakeRunner{
		refs:       []session.TargetRef{{ID: "a", RelPath: "db1/x.FDB", ConnMax: 8}},
		freeBudget: 2,
	}
	e := NewEngine(filepath.Join(dir, "schedules.json"), fr)
	defer e.Stop()

	s, err := e.Create(Schedule{
		Name:    "budget-skip",
		Enabled: true,
		Trigger: Trigger{Type: TriggerOnce, At: time.Now().Add(-time.Second).Format(time.RFC3339)},
		Targets: Targets{SessionIDs: []string{"a"}},
		Run:     session.RunSpec{TimeLimitMin: 15},
		Policy:  Policy{IfBudgetFull: "skip"},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	e.tick(e.now())
	waitFor(t, 3*time.Second, func() bool {
		got, _ := e.Get(s.ID)
		return got.LastFire != nil
	})
	got, _ := e.Get(s.ID)
	if got.LastFire.Outcome != FireSkipped || !strings.Contains(got.LastFire.Detail, "budget") {
		t.Fatalf("lastFire = %+v, want budget skip", got.LastFire)
	}
	if fr.runCount() != 0 {
		t.Fatal("no run should start without budget")
	}
}

func TestEngineBudgetWaitTimesOut(t *testing.T) {
	dir := t.TempDir()
	fr := &fakeRunner{
		refs:       []session.TargetRef{{ID: "a", RelPath: "db1/x.FDB", ConnMax: 8}},
		freeBudget: 2,
	}
	e := NewEngine(filepath.Join(dir, "schedules.json"), fr)
	defer e.Stop()

	s, err := e.Create(Schedule{
		Name:    "budget-wait",
		Enabled: true,
		Trigger: Trigger{Type: TriggerOnce, At: time.Now().Add(-time.Second).Format(time.RFC3339)},
		Targets: Targets{SessionIDs: []string{"a"}},
		Run:     session.RunSpec{TimeLimitMin: 15},
		Policy:  Policy{IfBudgetFull: "wait", WaitBudgetSec: 1},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	e.tick(e.now())
	waitFor(t, 6*time.Second, func() bool {
		got, _ := e.Get(s.ID)
		return got.LastFire != nil
	})
	got, _ := e.Get(s.ID)
	if got.LastFire.Outcome != FireSkipped || !strings.Contains(got.LastFire.Detail, "waited") {
		t.Fatalf("lastFire = %+v, want wait timeout", got.LastFire)
	}
}

func TestEngineUnknownAndMissingTargetsRecorded(t *testing.T) {
	dir := t.TempDir()
	fr := &fakeRunner{
		refs:       []session.TargetRef{{ID: "a", RelPath: "db1/x.FDB", ConnMax: 2, Missing: true}},
		unknown:    []string{"ghost"},
		freeBudget: 10,
	}
	e := NewEngine(filepath.Join(dir, "schedules.json"), fr)
	defer e.Stop()

	_, err := e.Create(Schedule{
		Name:    "dangling",
		Enabled: true,
		Trigger: Trigger{Type: TriggerOnce, At: time.Now().Add(-time.Second).Format(time.RFC3339)},
		Targets: Targets{SessionIDs: []string{"a", "ghost"}},
		Run:     session.RunSpec{TimeLimitMin: 15},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	e.tick(e.now())
	waitFor(t, 3*time.Second, func() bool { return fr.runCount() == 1 })
	fr.mu.Lock()
	run := fr.runs[0]
	fr.mu.Unlock()
	if len(run.Sessions) != 2 {
		t.Fatalf("run entries = %+v, want ghost + missing recorded", run.Sessions)
	}
	found := map[string]string{}
	for _, se := range run.Sessions {
		found[se.SessionID] = se.Reason
	}
	if !strings.Contains(found["ghost"], "unknown") {
		t.Fatalf("ghost reason = %q", found["ghost"])
	}
	if !strings.Contains(found["a"], "missing") {
		t.Fatalf("missing reason = %q", found["a"])
	}
}

func TestEngineRecurringRequiresTimeLimit(t *testing.T) {
	dir := t.TempDir()
	fr := &fakeRunner{}
	e := NewEngine(filepath.Join(dir, "schedules.json"), fr)
	defer e.Stop()

	_, err := e.Create(Schedule{
		Name:    "bad",
		Enabled: true,
		Trigger: Trigger{Type: TriggerInterval, EveryMin: 60},
		Targets: Targets{All: true},
		Run:     session.RunSpec{TimeLimitMin: 0},
	})
	if err == nil || !strings.Contains(err.Error(), "timeLimitMin") {
		t.Fatalf("err = %v, want timeLimitMin guard", err)
	}
}

func TestEngineStoreRoundTripAndMissedOnce(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "schedules.json")
	fr := &fakeRunner{refs: []session.TargetRef{{ID: "a", RelPath: "db1/x.FDB", ConnMax: 2}}, freeBudget: 10}

	e := NewEngine(path, fr)
	past := time.Now().Add(-time.Hour)
	e.SetNow(func() time.Time { return past })
	s, err := e.Create(Schedule{
		Name:    "old-once",
		Enabled: true,
		Trigger: Trigger{Type: TriggerOnce, At: past.Add(-time.Minute).Format(time.RFC3339)},
		Targets: Targets{SessionIDs: []string{"a"}},
		Run:     session.RunSpec{TimeLimitMin: 15},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	// Simulate restart later: fresh engine, clock moved forward.
	e2 := NewEngine(path, fr)
	nowLater := time.Now()
	e2.SetNow(func() time.Time { return nowLater })
	if err := e2.Load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	defer e2.Stop()
	got, ok := e2.Get(s.ID)
	if !ok {
		t.Fatal("schedule lost across restart")
	}
	if got.Enabled {
		t.Fatal("missed once schedule must be disabled")
	}
	if got.LastFire == nil || got.LastFire.Outcome != FireMissed {
		t.Fatalf("lastFire = %+v, want missed", got.LastFire)
	}
	if fr.runCount() != 0 {
		t.Fatal("missed once must not fire at boot")
	}
}

func TestEngineLoadCatchUpFires(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "schedules.json")
	fr := &fakeRunner{refs: []session.TargetRef{{ID: "a", RelPath: "db1/x.FDB", ConnMax: 2}}, freeBudget: 10}

	e := NewEngine(path, fr)
	past := time.Now().Add(-time.Hour)
	e.SetNow(func() time.Time { return past })
	e.Create(Schedule{
		Name:    "catchup",
		Enabled: true,
		Trigger: Trigger{Type: TriggerInterval, EveryMin: 60},
		Targets: Targets{SessionIDs: []string{"a"}},
		Run:     session.RunSpec{TimeLimitMin: 10},
		Policy:  Policy{CatchUp: true},
	})

	e2 := NewEngine(path, fr)
	if err := e2.Load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	defer e2.Stop()
	e2.tick(e2.now())
	waitFor(t, 3*time.Second, func() bool { return fr.runCount() == 1 })
}

func TestEngineTriggerNowIgnoresNextRun(t *testing.T) {
	dir := t.TempDir()
	fr := &fakeRunner{refs: []session.TargetRef{{ID: "a", RelPath: "db1/x.FDB", ConnMax: 2}}, freeBudget: 10}
	e := NewEngine(filepath.Join(dir, "schedules.json"), fr)
	defer e.Stop()

	s, err := e.Create(baseSchedule(time.Now())) // due now
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := e.TriggerNow(s.ID); err != nil {
		t.Fatalf("trigger: %v", err)
	}
	waitFor(t, 3*time.Second, func() bool { return fr.runCount() == 1 })
}

func TestEngineUpdateRecomputesNext(t *testing.T) {
	dir := t.TempDir()
	fr := &fakeRunner{}
	e := NewEngine(filepath.Join(dir, "schedules.json"), fr)
	defer e.Stop()

	now := time.Now()
	e.SetNow(func() time.Time { return now })
	s, err := e.Create(Schedule{
		Name:    "recompute",
		Enabled: true,
		Trigger: Trigger{Type: TriggerInterval, EveryMin: 60},
		Targets: Targets{All: true},
		Run:     session.RunSpec{TimeLimitMin: 30},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	updated, err := e.Update(s.ID, func(sc *Schedule) { sc.Trigger.EveryMin = 10 })
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if updated.NextRunAt != now.Add(10*time.Minute).Format(time.RFC3339) {
		t.Fatalf("next = %s, want +10m", updated.NextRunAt)
	}
}

func TestModelNormalizeDefaults(t *testing.T) {
	s := baseSchedule(time.Now())
	s.Normalize()
	if s.Policy.IfRunning != "skip" || s.Policy.IfBudgetFull != "wait" || s.Policy.WaitBudgetSec != 300 || s.Policy.StaggerSec != 2 {
		t.Fatalf("defaults not applied: %+v", s.Policy)
	}
}
