package session

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// RunOrigin says what triggered a run.
type RunOrigin string

const (
	OriginManual   RunOrigin = "manual"
	OriginSchedule RunOrigin = "schedule"
)

// RunOutcome is the aggregated result of one run.
type RunOutcome string

const (
	RunRunning   RunOutcome = "running"
	RunOK        RunOutcome = "ok"
	RunPartial   RunOutcome = "partial"
	RunFailed    RunOutcome = "failed"
	RunCancelled RunOutcome = "cancelled"
	RunSkipped   RunOutcome = "skipped"
)

const (
	DefaultRunsFile  = "fb-loadgen.runs.json"
	runsStoreVersion = 1
)

// RunSpec is what to run: a time limit plus optional per-fire overrides.
// Overrides deliberately exclude connection counts — ConnMax is owned by the
// fleet budget and ConnMin follows it (see applyEvenConnBudgetLocked).
type RunSpec struct {
	TimeLimitMin int          `json:"timeLimitMin"`
	Overrides    RunOverrides `json:"overrides,omitempty"`
}

// RunOverrides are typed per-fire config overlays applied on top of the
// session config. Precedence: override > time-limit scaling > session config.
type RunOverrides struct {
	Profile     *string `json:"profile,omitempty"`
	ThinkMs     *int    `json:"thinkMs,omitempty"`
	TxTimeout   *int    `json:"txTimeout,omitempty"`
	SpikeCycles *int    `json:"spikeCycles,omitempty"`
	SpikeHold   *int    `json:"spikeHold,omitempty"`
}

// TargetRef is a resolved schedule target: the session's identity plus the
// facts the schedule engine needs for policy decisions.
type TargetRef struct {
	ID      string `json:"id"`
	RelPath string `json:"relPath"`
	Status  Status `json:"status"`
	Missing bool   `json:"missing"`
	ConnMax int    `json:"connMax"`
}

// SessionRun is one session's entry inside a run record.
type SessionRun struct {
	SessionID string  `json:"id"`
	RelPath   string  `json:"relPath"`
	Status    string  `json:"status"` // terminal Status value, or Skipped/Cancelled
	LastError string  `json:"lastError,omitempty"`
	ReportDir string  `json:"reportDir,omitempty"`
	TPS       float64 `json:"tps"`
	Success   int64   `json:"success"`
	Errors    int64   `json:"errors"`
	Pending   bool    `json:"pending,omitempty"` // started, awaiting terminal transition
	Reason    string  `json:"reason,omitempty"`  // why skipped/cancelled
}

// Run is one recorded execution: a schedule fire or a manual start.
type Run struct {
	ID         string       `json:"id"`
	Origin     string       `json:"origin"`
	ScheduleID string       `json:"scheduleId,omitempty"`
	StartedAt  string       `json:"startedAt"`
	FinishedAt string       `json:"finishedAt,omitempty"`
	Outcome    RunOutcome   `json:"outcome"`
	Sessions   []SessionRun `json:"sessions"`
}

type runStore struct {
	Version int    `json:"version"`
	Runs    []*Run `json:"runs"`
}

// RunHistory records runs, persists them atomically, and enforces retention.
// All methods are goroutine-safe. Terminal updates are idempotent: once an
// entry is no longer Pending, later updates for the same session are ignored.
type RunHistory struct {
	mu   sync.Mutex
	path string

	runs       []*Run
	maxRuns    int
	maxAgeDays int

	onFinished func(*Run)              // webhook hook, called after persist
	keepRun    func(runID string) bool // schedules protect referenced runs from retention
}

// NewRunHistory creates an empty history bound to a persistence path.
func NewRunHistory(path string) *RunHistory {
	return &RunHistory{
		path:       path,
		maxRuns:    500,
		maxAgeDays: 30,
	}
}

// SetLimits overrides retention defaults.
func (h *RunHistory) SetLimits(maxRuns, maxAgeDays int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if maxRuns > 0 {
		h.maxRuns = maxRuns
	}
	if maxAgeDays > 0 {
		h.maxAgeDays = maxAgeDays
	}
}

// SetOnFinished registers the callback fired when a run reaches a terminal outcome.
func (h *RunHistory) SetOnFinished(f func(*Run)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.onFinished = f
}

// SetKeepRun registers a predicate protecting runs from retention pruning.
func (h *RunHistory) SetKeepRun(f func(runID string) bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.keepRun = f
}

// Path returns the persistence file path.
func (h *RunHistory) Path() string {
	return h.path
}

type runFile struct {
	Version int    `json:"version"`
	Runs    []*Run `json:"runs"`
}

// Load reads persisted runs. Runs left without FinishedAt (process died
// mid-run) are marked cancelled with an honest reason.
func (h *RunHistory) Load() error {
	data, err := os.ReadFile(h.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var f runFile
	if err := json.Unmarshal(data, &f); err != nil {
		return fmt.Errorf("parse %s: %w", h.path, err)
	}
	now := time.Now()
	for _, r := range f.Runs {
		if r.FinishedAt == "" {
			r.Outcome = RunCancelled
			r.FinishedAt = now.Format(time.RFC3339)
			for i := range r.Sessions {
				if r.Sessions[i].Pending {
					r.Sessions[i].Pending = false
					r.Sessions[i].Status = "Cancelled"
					r.Sessions[i].Reason = "process restarted"
				}
			}
		}
	}
	h.mu.Lock()
	h.runs = f.Runs
	h.mu.Unlock()
	return nil
}

func (h *RunHistory) persistLocked() {
	f := runFile{Version: runsStoreVersion, Runs: h.pruneLocked()}
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return
	}
	dir := filepath.Dir(h.path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return
		}
	}
	tmp := h.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return
	}
	_ = os.Rename(tmp, h.path)
}

// pruneLocked drops runs beyond retention limits. Runs referenced by
// schedules (keepRun) are retained unless nothing else can go.
func (h *RunHistory) pruneLocked() []*Run {
	cutoff := time.Now().AddDate(0, 0, -h.maxAgeDays)
	keep := make([]*Run, 0, len(h.runs))
	for _, r := range h.runs {
		if h.keepRun != nil && h.keepRun(r.ID) {
			keep = append(keep, r)
			continue
		}
		t, err := time.Parse(time.RFC3339, r.StartedAt)
		if err == nil && t.Before(cutoff) {
			continue
		}
		keep = append(keep, r)
	}
	h.runs = keep
	for len(h.runs) > h.maxRuns {
		drop := -1
		for i, r := range h.runs {
			if h.keepRun == nil || !h.keepRun(r.ID) {
				drop = i
				break
			}
		}
		if drop < 0 {
			break // everything is referenced
		}
		h.runs = append(h.runs[:drop], h.runs[drop+1:]...)
	}
	return h.runs
}

func runIDNow() string {
	return "run_" + time.Now().UTC().Format("20060102T150405") + "_" + randomHex(4)
}

// Create registers a new run. Entries with empty/"Starting" status become
// pending; entries with a terminal status (e.g. Skipped) are stored as-is.
func (h *RunHistory) Create(origin RunOrigin, scheduleID string, entries []SessionRun) *Run {
	for i := range entries {
		switch entries[i].Status {
		case "", "Starting":
			entries[i].Pending = true
			if entries[i].Status == "" {
				entries[i].Status = "Starting"
			}
		}
	}
	r := &Run{
		ID:         runIDNow(),
		Origin:     string(origin),
		ScheduleID: scheduleID,
		StartedAt:  time.Now().Format(time.RFC3339),
		Outcome:    RunRunning,
		Sessions:   entries,
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.runs = append(h.runs, r)
	h.persistLocked()
	return r
}

// Get returns a copy of one run.
func (h *RunHistory) Get(id string) (Run, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.runs {
		if r.ID == id {
			return *r, true
		}
	}
	return Run{}, false
}

// ListFilter narrows List results.
type ListFilter struct {
	Limit      int
	SessionID  string
	ScheduleID string
	Since      string // RFC3339
}

// List returns runs newest-first.
func (h *RunHistory) List(f ListFilter) []Run {
	h.mu.Lock()
	defer h.mu.Unlock()
	var sinceT time.Time
	if f.Since != "" {
		sinceT, _ = time.Parse(time.RFC3339, f.Since)
	}
	out := make([]Run, 0, len(h.runs))
	for i := len(h.runs) - 1; i >= 0; i-- {
		r := h.runs[i]
		if f.SessionID != "" && !runHasSession(r, f.SessionID) {
			continue
		}
		if f.ScheduleID != "" && r.ScheduleID != f.ScheduleID {
			continue
		}
		if !sinceT.IsZero() {
			t, err := time.Parse(time.RFC3339, r.StartedAt)
			if err != nil || t.Before(sinceT) {
				continue
			}
		}
		out = append(out, *r)
		if f.Limit > 0 && len(out) >= f.Limit {
			break
		}
	}
	return out
}

func runHasSession(r *Run, id string) bool {
	for _, s := range r.Sessions {
		if s.SessionID == id {
			return true
		}
	}
	return false
}

// UpdateSession applies a terminal result for one session entry and
// finalizes the run when nothing is pending anymore. Returns the run when
// this call finalized it.
func (h *RunHistory) UpdateSession(runID, sessionID string, up SessionRun) *Run {
	h.mu.Lock()
	defer h.mu.Unlock()
	r := h.findLocked(runID)
	if r == nil {
		return nil
	}
	for i := range r.Sessions {
		e := &r.Sessions[i]
		if e.SessionID != sessionID {
			continue
		}
		if !e.Pending {
			return nil // already terminal; first writer wins
		}
		e.Pending = false
		e.Status = up.Status
		e.LastError = up.LastError
		e.ReportDir = up.ReportDir
		e.TPS = up.TPS
		e.Success = up.Success
		e.Errors = up.Errors
		e.Reason = up.Reason
		break
	}
	return h.maybeFinishLocked(r)
}

// AddSession appends a pending session entry to a run after creation.
func (h *RunHistory) AddSession(runID string, entry SessionRun) {
	h.mu.Lock()
	defer h.mu.Unlock()
	r := h.findLocked(runID)
	if r == nil {
		return
	}
	entry.Pending = true
	if entry.Status == "" {
		entry.Status = "Starting"
	}
	r.Sessions = append(r.Sessions, entry)
	h.persistLocked()
}

// SkipSession marks a target that never started (busy/missing/unknown).
// Updates the pending entry when present, otherwise appends one.
func (h *RunHistory) SkipSession(runID, sessionID, relPath, reason string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	r := h.findLocked(runID)
	if r == nil {
		return
	}
	for i := range r.Sessions {
		if r.Sessions[i].SessionID != sessionID || !r.Sessions[i].Pending {
			continue
		}
		e := &r.Sessions[i]
		e.Pending = false
		e.Status = "Skipped"
		e.Reason = reason
		if relPath != "" {
			e.RelPath = relPath
		}
		h.maybeFinishLocked(r)
		return
	}
	r.Sessions = append(r.Sessions, SessionRun{
		SessionID: sessionID,
		RelPath:   relPath,
		Status:    "Skipped",
		Reason:    reason,
	})
	h.maybeFinishLocked(r)
}

func (h *RunHistory) findLocked(id string) *Run {
	for _, r := range h.runs {
		if r.ID == id {
			return r
		}
	}
	return nil
}

// maybeFinishLocked finalizes the run when no session entry is pending.
func (h *RunHistory) maybeFinishLocked(r *Run) *Run {
	if r.FinishedAt != "" {
		return nil
	}
	for _, e := range r.Sessions {
		if e.Pending {
			return nil
		}
	}
	r.FinishedAt = time.Now().Format(time.RFC3339)
	r.Outcome = aggregateOutcome(r.Sessions)
	h.persistLocked()
	if h.onFinished != nil {
		cp := *r
		h.onFinished(&cp)
	}
	return r
}

// MaybeFinish finalizes the run if nothing is pending (used after a bulk
// start loop where every attempt may have resolved synchronously).
func (h *RunHistory) MaybeFinish(runID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if r := h.findLocked(runID); r != nil {
		h.maybeFinishLocked(r)
	}
}

func aggregateOutcome(entries []SessionRun) RunOutcome {
	var completed, failed, skipped, cancelled int
	for _, e := range entries {
		switch {
		case e.Status == "Skipped":
			skipped++
		case e.Status == "Cancelled":
			cancelled++
		case e.Status == string(StatusCompleted):
			completed++
		case e.Status == string(StatusFailed):
			failed++
		default: // Idle after manual stop counts as not-completed but not broken
			failed++
		}
	}
	switch {
	case cancelled > 0 && completed == 0 && failed == 0:
		return RunCancelled
	case completed == 0 && skipped > 0 && failed == 0:
		return RunSkipped
	case failed == 0 && skipped == 0:
		return RunOK
	case completed == 0:
		return RunFailed
	default:
		return RunPartial
	}
}

// Cancel marks a run cancelled; pending entries become Cancelled with a reason.
func (h *RunHistory) Cancel(runID, reason string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	r := h.findLocked(runID)
	if r == nil || r.FinishedAt != "" {
		return false
	}
	for i := range r.Sessions {
		if r.Sessions[i].Pending {
			r.Sessions[i].Pending = false
			r.Sessions[i].Status = "Cancelled"
			r.Sessions[i].Reason = reason
		}
	}
	r.FinishedAt = time.Now().Format(time.RFC3339)
	r.Outcome = RunCancelled
	h.persistLocked()
	if h.onFinished != nil {
		cp := *r
		h.onFinished(&cp)
	}
	return true
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	const hexDigits = "0123456789abcdef"
	for i := range b {
		b[i] = hexDigits[b[i]&0x0f]
	}
	return string(b)
}
