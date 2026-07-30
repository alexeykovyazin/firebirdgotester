package worker

import (
	"sync/atomic"
	"time"
)

// PauseGate coordinates pause/resume across workers and the scheduler.
type PauseGate struct {
	paused atomic.Bool
}

// NewPauseGate creates a new pause gate (initially not paused).
func NewPauseGate() *PauseGate {
	return &PauseGate{}
}

// Pause marks the gate as paused.
func (g *PauseGate) Pause() {
	if g == nil {
		return
	}
	g.paused.Store(true)
}

// Resume clears pause.
func (g *PauseGate) Resume() {
	if g == nil {
		return
	}
	g.paused.Store(false)
}

// IsPaused reports whether the gate is paused.
func (g *PauseGate) IsPaused() bool {
	if g == nil {
		return false
	}
	return g.paused.Load()
}

// WaitIfPaused blocks while paused, returning false if ctxDone is closed.
func (g *PauseGate) WaitIfPaused(ctxDone <-chan struct{}) bool {
	if g == nil {
		return true
	}
	for g.paused.Load() {
		select {
		case <-ctxDone:
			return false
		case <-time.After(50 * time.Millisecond):
		}
	}
	return true
}
