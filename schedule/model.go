// Package schedule implements persisted run schedules (once / interval /
// cron) that fire load runs through a Runner (the session manager) and a
// small tick engine. Schedules survive restarts; missed fires are recorded.
package schedule

import (
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"

	"fb-loadgen/session"
)

// TriggerType selects when a schedule fires.
type TriggerType string

const (
	TriggerOnce     TriggerType = "once"
	TriggerInterval TriggerType = "interval"
	TriggerCron     TriggerType = "cron"
)

// Trigger is the "when" part of a schedule.
type Trigger struct {
	Type     TriggerType `json:"type"`
	At       string      `json:"at,omitempty"`       // RFC3339, for once
	EveryMin int         `json:"everyMin,omitempty"` // >= 1, for interval
	Cron     string      `json:"cron,omitempty"`     // 5-field spec, for cron
	TZ       string      `json:"tz,omitempty"`       // IANA zone; "" = local
}

// Targets selects which sessions a schedule runs.
type Targets struct {
	All        bool     `json:"all,omitempty"`        // all discovered, non-missing
	SessionIDs []string `json:"sessionIds,omitempty"` // explicit subset
}

// Policy controls firing behavior at fire time.
type Policy struct {
	IfRunning     string `json:"ifRunning,omitempty"`     // "skip" (default) | "stopAndRun"
	IfBudgetFull  string `json:"ifBudgetFull,omitempty"`  // "wait" (default) | "skip"
	WaitBudgetSec int    `json:"waitBudgetSec,omitempty"` // default 300, for wait
	StaggerSec    int    `json:"staggerSec,omitempty"`    // default 2, capped at 60
	CatchUp       bool   `json:"catchUp,omitempty"`       // fire a run missed while the process was down
}

// FireOutcome describes what happened at a fire attempt.
type FireOutcome string

const (
	FireFired   FireOutcome = "fired"
	FireSkipped FireOutcome = "skipped"
	FireMissed  FireOutcome = "missed"
	FireFailed  FireOutcome = "failed"
)

// RunRef records one fire attempt. Skipped/missed fires carry a Detail and
// no RunID.
type RunRef struct {
	RunID   string      `json:"runId,omitempty"`
	At      string      `json:"at,omitempty"`
	Outcome FireOutcome `json:"outcome"`
	Detail  string      `json:"detail,omitempty"`
}

// Schedule is a persisted rule: when to fire and what to run.
type Schedule struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Enabled   bool            `json:"enabled"`
	Trigger   Trigger         `json:"trigger"`
	Targets   Targets         `json:"targets"`
	Run       session.RunSpec `json:"run"`
	Policy    Policy          `json:"policy"`
	NotifyURL string          `json:"notifyUrl,omitempty"`
	CreatedAt string          `json:"createdAt"`
	NextRunAt string          `json:"nextRunAt,omitempty"`
	LastFire  *RunRef         `json:"lastFire,omitempty"`
}

const storeVersion = 1

// Normalize fills policy defaults.
func (s *Schedule) Normalize() {
	if s.Policy.IfRunning == "" {
		s.Policy.IfRunning = "skip"
	}
	if s.Policy.IfBudgetFull == "" {
		s.Policy.IfBudgetFull = "wait"
	}
	if s.Policy.WaitBudgetSec <= 0 {
		s.Policy.WaitBudgetSec = 300
	}
	if s.Policy.WaitBudgetSec > 3600 {
		s.Policy.WaitBudgetSec = 3600
	}
	if s.Policy.StaggerSec < 0 {
		s.Policy.StaggerSec = 0
	}
	if s.Policy.StaggerSec > 60 {
		s.Policy.StaggerSec = 60
	}
	if s.Policy.StaggerSec == 0 {
		s.Policy.StaggerSec = 2
	}
}

// Recurring reports whether the trigger fires more than once.
func (s *Schedule) Recurring() bool {
	return s.Trigger.Type == TriggerInterval || s.Trigger.Type == TriggerCron
}

// Validate checks the schedule; it enforces the recurring + time-limit guard
// (an unlimited run on a recurring trigger would starve every later fire).
func (s *Schedule) Validate() error {
	if strings.TrimSpace(s.Name) == "" {
		return fmt.Errorf("name is required")
	}
	recurring := s.Recurring()
	switch s.Trigger.Type {
	case TriggerOnce:
		if _, err := time.Parse(time.RFC3339, s.Trigger.At); err != nil {
			return fmt.Errorf("trigger.at must be RFC3339: %w", err)
		}
	case TriggerInterval:
		if s.Trigger.EveryMin < 1 {
			return fmt.Errorf("trigger.everyMin must be >= 1")
		}
	case TriggerCron:
		if _, err := cronSpec(s.Trigger.Cron, s.Trigger.TZ); err != nil {
			return err
		}
	default:
		return fmt.Errorf("trigger.type must be once, interval, or cron")
	}
	if recurring && s.Run.TimeLimitMin < 1 {
		return fmt.Errorf("run.timeLimitMin must be >= 1 for recurring schedules (an unlimited run would block every later fire); use a once schedule for unlimited runs")
	}
	if s.Run.TimeLimitMin < 0 {
		return fmt.Errorf("run.timeLimitMin must be >= 0")
	}
	if !s.Targets.All && len(s.Targets.SessionIDs) == 0 {
		return fmt.Errorf("targets: set all=true or provide sessionIds")
	}
	switch s.Policy.IfRunning {
	case "skip", "stopAndRun":
	default:
		return fmt.Errorf("policy.ifRunning must be skip or stopAndRun")
	}
	switch s.Policy.IfBudgetFull {
	case "wait", "skip":
	default:
		return fmt.Errorf("policy.ifBudgetFull must be wait or skip")
	}
	return nil
}

// cronSpec parses a 5-field cron expression with an optional IANA timezone.
func cronSpec(expr, tz string) (cron.Schedule, error) {
	if strings.TrimSpace(expr) == "" {
		return nil, fmt.Errorf("trigger.cron is required")
	}
	if tz != "" && !strings.HasPrefix(strings.TrimSpace(expr), "CRON_TZ=") {
		if _, err := time.LoadLocation(tz); err != nil {
			return nil, fmt.Errorf("trigger.tz: %w", err)
		}
		expr = "CRON_TZ=" + tz + " " + strings.TrimSpace(expr)
	}
	sch, err := cron.ParseStandard(expr)
	if err != nil {
		return nil, fmt.Errorf("trigger.cron: %w", err)
	}
	return sch, nil
}
