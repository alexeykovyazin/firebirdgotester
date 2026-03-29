package ramp

import (
	"context"
	"fmt"
	"sync"
	"time"

	"fb-loadgen/config"
	"fb-loadgen/db"
	"fb-loadgen/ops"
	"fb-loadgen/profile"
	"fb-loadgen/worker"
)

// Scheduler manages the connection ramp-up and ramp-down phases
type Scheduler struct {
	config      *config.Config
	connFactory *db.ConnectionFactory
	cache       *ops.Cache
	profile     profile.Profile
	metrics     *worker.MetricsCollector

	// Worker management
	workers     []*worker.Worker
	workerMutex sync.RWMutex
	ctx         context.Context
	cancel      context.CancelFunc

	// Phase management
	currentPhase Phase
	startTime    time.Time
	elapsedTime  time.Duration

	// Ramp parameters
	warmupRate   float64
	cooldownRate float64

	// Spike-specific
	spikeManager *SpikeManager
}

// Phase represents the current ramp phase
type Phase int

const (
	PhaseWarmup Phase = iota
	PhaseMain
	PhaseCooldown
)

// String returns the string representation of a phase
func (p Phase) String() string {
	switch p {
	case PhaseWarmup:
		return "warmup"
	case PhaseMain:
		return "main"
	case PhaseCooldown:
		return "cooldown"
	default:
		return "unknown"
	}
}

// NewScheduler creates a new ramp scheduler
func NewScheduler(config *config.Config, connFactory *db.ConnectionFactory, cache *ops.Cache, profile profile.Profile, metrics *worker.MetricsCollector) *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())

	var spikeManager *SpikeManager
	if config.Profile == "spike" {
		spikeManager = NewSpikeManager(config)
	}

	return &Scheduler{
		config:       config,
		connFactory:  connFactory,
		cache:        cache,
		profile:      profile,
		metrics:      metrics,
		workers:      make([]*worker.Worker, 0),
		ctx:          ctx,
		cancel:       cancel,
		currentPhase: PhaseWarmup,
		startTime:    time.Now(),
		warmupRate:   config.GetRampRate(),
		cooldownRate: config.GetCooldownRate(),
		spikeManager: spikeManager,
	}
}

// Start begins the ramp schedule
func (s *Scheduler) Start() error {
	s.startTime = time.Now()

	// Start with initial connections
	if err := s.ensureWorkerCount(s.config.ConnInit); err != nil {
		return fmt.Errorf("failed to ramp to initial connections: %w", err)
	}

	// Start the main scheduler loop
	go s.run()
	return nil
}

// Stop stops the scheduler and drains all workers
func (s *Scheduler) Stop() error {
	s.cancel()
	return s.drainWorkers()
}

// run is the main scheduler loop
func (s *Scheduler) run() {
	defer s.cancel()

	ticker := time.NewTicker(500 * time.Millisecond) // 500ms tick for smooth ramping
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.update()
			// Check if test is complete
			if s.isComplete() {
				return
			}
		}
	}
}

// isComplete checks if the test run has completed
func (s *Scheduler) isComplete() bool {
	// Check if cooldown phase is complete
	if s.currentPhase == PhaseCooldown {
		// Check if cooldown duration has elapsed
		warmup := time.Duration(s.config.Warmup) * time.Second
		main := time.Duration(s.config.Main) * time.Second
		cooldown := time.Duration(s.config.Cooldown) * time.Second
		totalDuration := warmup + main + cooldown

		if s.elapsedTime >= totalDuration {
			// Cooldown complete - check if all workers are stopped
			s.workerMutex.RLock()
			allStopped := len(s.workers) == 0
			s.workerMutex.RUnlock()
			return allStopped
		}
	}
	return false
}

// update updates the scheduler state based on elapsed time
func (s *Scheduler) update() {
	s.elapsedTime = time.Since(s.startTime)

	// Determine current phase
	s.updatePhase()

	// Handle phase-specific logic
	switch s.currentPhase {
	case PhaseWarmup:
		s.handleWarmup()
	case PhaseMain:
		s.handleMain()
	case PhaseCooldown:
		s.handleCooldown()
	}

	// Update metrics
	s.metrics.RecordConnectionChange(int64(len(s.workers)))
	s.metrics.UpdateLastReportTime()
}

// updatePhase determines the current phase based on elapsed time
func (s *Scheduler) updatePhase() {
	warmupDuration := time.Duration(s.config.Warmup) * time.Second
	mainDuration := time.Duration(s.config.Main) * time.Second

	if s.elapsedTime < warmupDuration {
		s.currentPhase = PhaseWarmup
	} else if s.elapsedTime < warmupDuration+mainDuration {
		s.currentPhase = PhaseMain
	} else {
		s.currentPhase = PhaseCooldown
	}
}

// handleWarmup handles the warmup phase
func (s *Scheduler) handleWarmup() {
	target := s.calculateTargetConnections(s.elapsedTime, PhaseWarmup)
	s.ensureWorkerCount(target)
}

// handleMain handles the main phase
func (s *Scheduler) handleMain() {
	if s.config.Profile == "spike" && s.spikeManager != nil {
		s.spikeManager.Update(s.elapsedTime, time.Duration(s.config.Main)*time.Second)
		if s.spikeManager.IsInSpike() {
			// During spike: ramp up to peak
			s.ensureWorkerCount(s.config.ConnPeak)
		} else {
			// Between spikes: maintain mid-level
			midLevel := (s.config.ConnInit + s.config.ConnPeak) / 2
			s.ensureWorkerCount(midLevel)
		}
	} else {
		// Regular main phase: maintain peak
		s.ensureWorkerCount(s.config.ConnPeak)
	}
}

// handleCooldown handles the cooldown phase
func (s *Scheduler) handleCooldown() {
	// Calculate how much time has passed in cooldown
	warmupDuration := time.Duration(s.config.Warmup) * time.Second
	mainDuration := time.Duration(s.config.Main) * time.Second
	cooldownStart := warmupDuration + mainDuration
	cooldownElapsed := s.elapsedTime - cooldownStart

	target := s.calculateTargetConnections(cooldownElapsed, PhaseCooldown)
	s.ensureWorkerCount(target)
}

// calculateTargetConnections calculates the target number of connections for a given phase
func (s *Scheduler) calculateTargetConnections(elapsed time.Duration, phase Phase) int {
	switch phase {
	case PhaseWarmup:
		// Linear ramp from conn-init to conn-peak
		current := s.config.ConnInit + int(s.warmupRate*elapsed.Seconds())
		if current > s.config.ConnPeak {
			current = s.config.ConnPeak
		}
		return current

	case PhaseCooldown:
		// Linear ramp down from conn-peak to 0
		current := s.config.ConnPeak - int(s.cooldownRate*elapsed.Seconds())
		if current < 0 {
			current = 0
		}
		return current

	default:
		return s.config.ConnPeak
	}
}

// ensureWorkerCount ensures the specified number of workers are running
func (s *Scheduler) ensureWorkerCount(target int) error {
	s.workerMutex.Lock()
	defer s.workerMutex.Unlock()

	current := len(s.workers)

	if current < target {
		// Need to add workers
		for i := current; i < target; i++ {
			if err := s.addWorker(); err != nil {
				// Log error but continue
				fmt.Printf("Failed to add worker %d: %v\n", i, err)
			}
		}
	} else if current > target {
		// Need to remove workers
		for i := current - 1; i >= target; i-- {
			if err := s.removeWorker(i); err != nil {
				// Log error but continue
				fmt.Printf("Failed to remove worker %d: %v\n", i, err)
			}
		}
	}
	return nil
}

// addWorker adds a new worker
func (s *Scheduler) addWorker() error {
	workerID := len(s.workers)
	w := worker.NewWorker(workerID, s.ctx, s.connFactory, s.cache, s.profile, s.config, s.metrics)

	if err := w.Start(); err != nil {
		return err
	}

	s.workers = append(s.workers, w)
	return nil
}

// removeWorker removes a worker by index
func (s *Scheduler) removeWorker(index int) error {
	if index < 0 || index >= len(s.workers) {
		return fmt.Errorf("invalid worker index: %d", index)
	}

	worker := s.workers[index]

	// Stop the worker
	if err := worker.Stop(); err != nil {
		return err
	}

	// Remove from slice
	s.workers = append(s.workers[:index], s.workers[index+1:]...)

	// Re-index remaining workers
	for i, w := range s.workers {
		// Note: Worker ID is set at creation and doesn't change
		// This is mainly for consistency in logging/debugging
		_ = i // Suppress unused variable warning
		_ = w
	}

	return nil
}

// drainWorkers gracefully stops all workers
func (s *Scheduler) drainWorkers() error {
	s.workerMutex.Lock()
	defer s.workerMutex.Unlock()

	// Stop all workers
	for i := len(s.workers) - 1; i >= 0; i-- {
		if err := s.workers[i].Stop(); err != nil {
			// Log error but continue draining
			fmt.Printf("Error stopping worker %d: %v\n", i, err)
		}
	}

	s.workers = nil
	return nil
}

// GetCurrentWorkerCount returns the current number of active workers
func (s *Scheduler) GetCurrentWorkerCount() int {
	s.workerMutex.RLock()
	defer s.workerMutex.RUnlock()
	return len(s.workers)
}

// GetCurrentPhase returns the current ramp phase
func (s *Scheduler) GetCurrentPhase() Phase {
	return s.currentPhase
}

// GetElapsedTime returns the elapsed time since start
func (s *Scheduler) GetElapsedTime() time.Duration {
	return s.elapsedTime
}

// Done returns a channel that's closed when the scheduler has completed all phases
func (s *Scheduler) Done() <-chan struct{} {
	return s.ctx.Done()
}

// GetPhaseProgress returns the progress of the current phase (0.0 to 1.0)
func (s *Scheduler) GetPhaseProgress() float64 {
	switch s.currentPhase {
	case PhaseWarmup:
		total := time.Duration(s.config.Warmup) * time.Second
		if total <= 0 {
			return 1.0
		}
		return float64(s.elapsedTime) / float64(total)

	case PhaseMain:
		warmup := time.Duration(s.config.Warmup) * time.Second
		main := time.Duration(s.config.Main) * time.Second
		elapsedInMain := s.elapsedTime - warmup
		if main <= 0 {
			return 1.0
		}
		return float64(elapsedInMain) / float64(main)

	case PhaseCooldown:
		warmup := time.Duration(s.config.Warmup) * time.Second
		main := time.Duration(s.config.Main) * time.Second
		cooldown := time.Duration(s.config.Cooldown) * time.Second
		elapsedInCooldown := s.elapsedTime - warmup - main
		if cooldown <= 0 {
			return 1.0
		}
		progress := float64(elapsedInCooldown) / float64(cooldown)
		if progress > 1.0 {
			progress = 1.0
		}
		return progress

	default:
		return 1.0
	}
}

// GetStats returns a summary of current scheduler statistics
func (s *Scheduler) GetStats() string {
	workerCount := s.GetCurrentWorkerCount()
	phase := s.GetCurrentPhase()
	progress := s.GetPhaseProgress() * 100

	return fmt.Sprintf("Phase: %s (%.1f%%), Workers: %d, Elapsed: %v",
		phase, progress, workerCount, s.GetElapsedTime())
}

// SpikeManager handles spike-specific logic
type SpikeManager struct {
	config         *config.Config
	currentCycle   int
	inSpikePhase   bool
	spikeStartTime time.Time
}

// NewSpikeManager creates a new spike manager
func NewSpikeManager(config *config.Config) *SpikeManager {
	return &SpikeManager{
		config:       config,
		currentCycle: 0,
		inSpikePhase: false,
	}
}

// Update updates the spike state based on elapsed time
func (sm *SpikeManager) Update(elapsed, mainDuration time.Duration) {
	if sm.config.SpikeCycles <= 0 {
		sm.inSpikePhase = false
		return
	}

	// Calculate spike interval
	spikeHold := time.Duration(sm.config.SpikeHold) * time.Second
	betweenSpike := sm.calculateBetweenSpikeDuration()
	cycleDuration := spikeHold + betweenSpike
	totalSpikeDuration := time.Duration(sm.config.SpikeCycles) * cycleDuration

	if elapsed >= totalSpikeDuration {
		// After all spikes
		sm.inSpikePhase = false
		return
	}

	// Determine current cycle and phase
	currentCycle := int(elapsed / cycleDuration)
	phaseElapsed := elapsed % cycleDuration

	if phaseElapsed < spikeHold {
		// In spike phase
		sm.inSpikePhase = true
		sm.currentCycle = currentCycle
		if sm.spikeStartTime.IsZero() {
			sm.spikeStartTime = time.Now()
		}
	} else {
		// In between-spike phase
		sm.inSpikePhase = false
		sm.spikeStartTime = time.Time{}
	}
}

// IsInSpike returns true if currently in a spike phase
func (sm *SpikeManager) IsInSpike() bool {
	return sm.inSpikePhase
}

// GetCurrentCycle returns the current spike cycle
func (sm *SpikeManager) GetCurrentCycle() int {
	return sm.currentCycle
}

// calculateBetweenSpikeDuration calculates the duration between spike cycles
func (sm *SpikeManager) calculateBetweenSpikeDuration() time.Duration {
	if sm.config.SpikeCycles <= 0 {
		return 0
	}

	mainDuration := time.Duration(sm.config.Main) * time.Second
	spikeHoldTotal := time.Duration(sm.config.SpikeCycles*sm.config.SpikeHold) * time.Second
	return (mainDuration - spikeHoldTotal) / time.Duration(sm.config.SpikeCycles)
}

// GetSpikeStats returns spike-specific statistics
func (sm *SpikeManager) GetSpikeStats() string {
	phase := "between-spike"
	if sm.inSpikePhase {
		phase = "spike"
	}
	return fmt.Sprintf("Spike: cycle %d, phase %s", sm.currentCycle+1, phase)
}
