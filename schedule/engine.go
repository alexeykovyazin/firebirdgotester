package schedule

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"fb-loadgen/session"
)

// Runner is the engine's view of the session manager.
type Runner interface {
	ResolveTargets(all bool, ids []string) (refs []session.TargetRef, unknown []string)
	StartBulk(origin session.RunOrigin, scheduleID string, refs []session.TargetRef, skips []session.SkipTarget, spec session.RunSpec, stagger time.Duration, stopBusy bool) session.Run
	AnyBusy(ids []string) []string
	BudgetFree() int
	StopSessions(ids []string) []error
}

type fileStore struct {
	Version   int         `json:"version"`
	Schedules []*Schedule `json:"schedules"`
}

// Engine owns schedules, computes next fires, and starts runs through Runner.
type Engine struct {
	mu     sync.Mutex
	path   string
	scheds map[string]*Schedule

	runner   Runner
	now      func() time.Time
	notifier *Notifier
	stopCh   chan struct{}
	stopped  bool
	pending  map[string]bool
	fires    map[string]map[FireOutcome]int64
}

// NewEngine creates an engine persisting to path. Call Load, then Run in a
// goroutine.
func NewEngine(path string, runner Runner) *Engine {
	return &Engine{
		path:    path,
		scheds:  map[string]*Schedule{},
		runner:  runner,
		now:     time.Now,
		stopCh:  make(chan struct{}),
		pending: map[string]bool{},
		fires:   map[string]map[FireOutcome]int64{},
	}
}

// SetNotifier attaches the webhook dispatcher (run-finished events carry it).
func (e *Engine) SetNotifier(n *Notifier) {
	e.notifier = n
}

// SetNow overrides the clock (tests).
func (e *Engine) SetNow(f func() time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.now = f
}

// Stop terminates the tick loop and the notifier worker.
func (e *Engine) Stop() {
	e.mu.Lock()
	if !e.stopped {
		e.stopped = true
		close(e.stopCh)
	}
	e.mu.Unlock()
	if e.notifier != nil {
		e.notifier.Stop()
	}
}

// Load reads persisted schedules and resolves missed fires:
// catchUp=false records a missed fire; a missed "once" is spent.
func (e *Engine) Load() error {
	data, err := os.ReadFile(e.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var f fileStore
	if err := json.Unmarshal(data, &f); err != nil {
		return fmt.Errorf("parse %s: %w", e.path, err)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.now()
	for _, s := range f.Schedules {
		s.Normalize()
		if err := s.Validate(); err != nil {
			log.Printf("schedule %s (%s) invalid, skipping: %v", s.ID, s.Name, err)
			continue
		}
		if s.Enabled {
			next, _ := time.Parse(time.RFC3339, s.NextRunAt)
			if s.NextRunAt == "" || next.IsZero() {
				if n, err := nextRun(s, now); err == nil {
					s.NextRunAt = n.Format(time.RFC3339)
				}
			} else if next.Before(now) {
				if s.Policy.CatchUp {
					// keep the past NextRunAt: the engine fires once at boot
					log.Printf("schedule %s (%s): catch-up fire pending", s.ID, s.Name)
				} else {
					ref := RunRef{At: s.NextRunAt, Outcome: FireMissed, Detail: "missed while process was down"}
					s.LastFire = &ref
					if s.Trigger.Type == TriggerOnce {
						s.Enabled = false
						s.NextRunAt = ""
					} else if n, err := nextRun(s, now); err == nil {
						s.NextRunAt = n.Format(time.RFC3339)
					}
				}
			}
		} else {
			s.NextRunAt = ""
		}
		e.scheds[s.ID] = s
	}
	e.persistLocked()
	return nil
}

func (e *Engine) persistLocked() {
	f := fileStore{Version: storeVersion}
	for _, s := range e.scheds {
		cp := *s
		f.Schedules = append(f.Schedules, &cp)
	}
	sort.Slice(f.Schedules, func(i, j int) bool { return f.Schedules[i].ID < f.Schedules[j].ID })
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return
	}
	dir := filepath.Dir(e.path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return
		}
	}
	tmp := e.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return
	}
	_ = os.Rename(tmp, e.path)
}

// Run is the blocking tick loop; call from a goroutine.
func (e *Engine) Run() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.tick(e.now())
		}
	}
}

func (e *Engine) tick(now time.Time) {
	e.mu.Lock()
	var due []*Schedule
	for _, s := range e.scheds {
		if !s.Enabled || s.NextRunAt == "" || e.pending[s.ID] {
			continue
		}
		nt, err := time.Parse(time.RFC3339, s.NextRunAt)
		if err != nil || nt.After(now) {
			continue
		}
		e.pending[s.ID] = true
		cp := *s
		due = append(due, &cp)
	}
	e.mu.Unlock()
	for _, s := range due {
		go e.fire(s, false)
	}
}

// fire executes one fire attempt and records the result. forced=true skips
// the NextRunAt gate (manual trigger).
func (e *Engine) fire(s *Schedule, forced bool) {
	ref := e.executeFire(s, forced)

	e.mu.Lock()
	defer e.mu.Unlock()
	st := e.scheds[s.ID]
	if st == nil {
		return // deleted mid-fire
	}
	refCopy := ref
	st.LastFire = &refCopy
	if e.fires[s.ID] == nil {
		e.fires[s.ID] = map[FireOutcome]int64{}
	}
	e.fires[s.ID][ref.Outcome]++

	if st.Trigger.Type == TriggerOnce {
		// A once schedule is spent after its fire attempt, fired or not.
		st.Enabled = false
		st.NextRunAt = ""
	} else {
		if n, err := nextRun(st, e.now()); err == nil {
			st.NextRunAt = n.Format(time.RFC3339)
		}
	}
	e.persistLocked()
}

// executeFire resolves targets and starts the run per policy. It may block
// (budget wait, stagger) and always returns a fire reference.
func (e *Engine) executeFire(s *Schedule, forced bool) RunRef {
	now := e.now()
	at := now.Format(time.RFC3339)

	if s.Policy.IfRunning != "stopAndRun" {
		if busy := e.runner.AnyBusy(s.Targets.SessionIDs); len(busy) > 0 {
			return RunRef{At: at, Outcome: FireSkipped, Detail: fmt.Sprintf("targets busy: %v", busy)}
		}
	}

	refs, unknown := e.runner.ResolveTargets(s.Targets.All, s.Targets.SessionIDs)
	var skips []session.SkipTarget
	for _, id := range unknown {
		skips = append(skips, session.SkipTarget{ID: id, Reason: "unknown session id"})
	}
	if s.Policy.IfRunning == "stopAndRun" {
		var busy []string
		for _, r := range refs {
			switch r.Status {
			case session.StatusRunning, session.StatusStarting, session.StatusPaused, session.StatusStopping:
				busy = append(busy, r.ID)
			}
		}
		if len(busy) > 0 {
			if errs := e.runner.StopSessions(busy); len(errs) > 0 {
				log.Printf("schedule %s: stop-before-run errors: %v", s.ID, errs)
			}
		}
	}

	// Runnable = non-missing targets. Budget wait only counts what would run.
	var runnable []session.TargetRef
	for _, r := range refs {
		if r.Missing {
			skips = append(skips, session.SkipTarget{ID: r.ID, RelPath: r.RelPath, Reason: "database file is missing"})
			continue
		}
		runnable = append(runnable, r)
	}
	if len(runnable) == 0 && len(skips) == 0 {
		return RunRef{At: at, Outcome: FireSkipped, Detail: "no targets resolved"}
	}

	need := 0
	for _, r := range runnable {
		need += r.ConnMax
	}
	if need > 0 {
		waitSec := 0
		if s.Policy.IfBudgetFull == "wait" {
			waitSec = s.Policy.WaitBudgetSec
		}
		deadline := now.Add(time.Duration(waitSec) * time.Second)
		for e.runner.BudgetFree() < need {
			if e.now().After(deadline) {
				return RunRef{At: at, Outcome: FireSkipped,
					Detail: fmt.Sprintf("connection budget: need %d free conns, %d available (waited %ds)", need, e.runner.BudgetFree(), waitSec)}
			}
			select {
			case <-e.stopCh:
				return RunRef{At: at, Outcome: FireSkipped, Detail: "engine stopped while waiting for budget"}
			case <-time.After(time.Second):
			}
		}
	}

	run := e.runner.StartBulk(session.OriginSchedule, s.ID, runnable, skips, s.Run, time.Duration(s.Policy.StaggerSec)*time.Second, false)
	return RunRef{RunID: run.ID, At: at, Outcome: FireFired}
}

// List returns copies of all schedules sorted by name.
func (e *Engine) List() []Schedule {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]Schedule, 0, len(e.scheds))
	for _, s := range e.scheds {
		out = append(out, *s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Get returns a copy of one schedule.
func (e *Engine) Get(id string) (Schedule, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s, ok := e.scheds[id]
	if !ok {
		return Schedule{}, false
	}
	return *s, true
}

// Create validates and stores a new schedule.
func (e *Engine) Create(s Schedule) (Schedule, error) {
	s.Normalize()
	if err := s.Validate(); err != nil {
		return Schedule{}, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	s.ID = "sch_" + randomHex(4)
	s.CreatedAt = e.now().Format(time.RFC3339)
	if s.Enabled {
		n, err := nextRun(&s, e.now())
		if err != nil {
			return Schedule{}, err
		}
		s.NextRunAt = n.Format(time.RFC3339)
	}
	e.scheds[s.ID] = &s
	e.persistLocked()
	return s, nil
}

// Update applies a mutation function to a copy of the schedule, then
// validates and persists. Use for partial patches.
func (e *Engine) Update(id string, fn func(*Schedule)) (Schedule, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	cur, ok := e.scheds[id]
	if !ok {
		return Schedule{}, fmt.Errorf("schedule not found: %s", id)
	}
	s := *cur
	fn(&s)
	s.Normalize()
	if err := s.Validate(); err != nil {
		return Schedule{}, err
	}
	if s.Enabled {
		n, err := nextRun(&s, e.now())
		if err != nil {
			return Schedule{}, err
		}
		s.NextRunAt = n.Format(time.RFC3339)
	} else {
		s.NextRunAt = ""
	}
	e.scheds[id] = &s
	e.persistLocked()
	return s, nil
}

// Delete removes a schedule; an in-flight fire completes unaffected.
func (e *Engine) Delete(id string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.scheds[id]; !ok {
		return false
	}
	delete(e.scheds, id)
	e.persistLocked()
	return true
}

// TriggerNow fires the schedule immediately, ignoring NextRunAt. It runs in
// the background and returns as soon as the fire is queued.
func (e *Engine) TriggerNow(id string) error {
	e.mu.Lock()
	s, ok := e.scheds[id]
	if !ok {
		e.mu.Unlock()
		return fmt.Errorf("schedule not found: %s", id)
	}
	if e.pending[id] {
		e.mu.Unlock()
		return fmt.Errorf("schedule fire already in progress")
	}
	e.pending[id] = true
	cp := *s
	e.mu.Unlock()
	go func() {
		e.fire(&cp, true)
		e.mu.Lock()
		delete(e.pending, id)
		e.mu.Unlock()
	}()
	return nil
}

// ReferencesRun reports whether any schedule's last fire points at runID
// (retention hook for the run history).
func (e *Engine) ReferencesRun(runID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, s := range e.scheds {
		if s.LastFire != nil && s.LastFire.RunID == runID {
			return true
		}
	}
	return false
}

// FireCounters returns per-schedule fire outcome counters (for /metrics).
func (e *Engine) FireCounters() map[string]map[FireOutcome]int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := map[string]map[FireOutcome]int64{}
	for id, m := range e.fires {
		for k, v := range m {
			if out[id] == nil {
				out[id] = map[FireOutcome]int64{}
			}
			out[id][k] = v
		}
	}
	return out
}

// NotifyURL returns the schedule's webhook URL, if any.
func (e *Engine) NotifyURL(id string) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	if s, ok := e.scheds[id]; ok {
		return s.NotifyURL
	}
	return ""
}

func nextRun(s *Schedule, after time.Time) (time.Time, error) {
	switch s.Trigger.Type {
	case TriggerOnce:
		t, err := time.Parse(time.RFC3339, s.Trigger.At)
		if err != nil {
			return time.Time{}, fmt.Errorf("trigger.at: %w", err)
		}
		return t, nil
	case TriggerInterval:
		if s.Trigger.EveryMin < 1 {
			return time.Time{}, fmt.Errorf("trigger.everyMin must be >= 1")
		}
		return after.Add(time.Duration(s.Trigger.EveryMin) * time.Minute), nil
	case TriggerCron:
		sch, err := cronSpec(s.Trigger.Cron, s.Trigger.TZ)
		if err != nil {
			return time.Time{}, err
		}
		return sch.Next(after), nil
	default:
		return time.Time{}, fmt.Errorf("unknown trigger type %q", s.Trigger.Type)
	}
}
