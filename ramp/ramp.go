package ramp

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
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
	pauseGate   *worker.PauseGate

	workers     []*worker.Worker
	workerMutex sync.RWMutex
	ctx         context.Context
	cancel      context.CancelFunc

	currentPhase Phase
	startTime    time.Time
	elapsedTime  time.Duration

	// Pause clock: wall time spent paused is excluded from phase elapsed
	pausedAccum  time.Duration
	pauseStarted time.Time
	wasPaused    bool

	warmupRate   float64
	cooldownRate float64

	spikeManager *SpikeManager

	walkTarget     int
	lastWalkAdjust time.Time
	rng            *rand.Rand

	stopping atomic.Bool
	runDone  chan struct{} // closed when run() exits
}

// Phase represents the current ramp phase
type Phase int

const (
	PhaseWarmup Phase = iota
	PhaseMain
	PhaseCooldown
)

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

const walkInterval = time.Second

// NewScheduler creates a new ramp scheduler
func NewScheduler(cfg *config.Config, connFactory *db.ConnectionFactory, cache *ops.Cache, profile profile.Profile, metrics *worker.MetricsCollector) *Scheduler {
	return NewSchedulerWithPause(cfg, connFactory, cache, profile, metrics, nil)
}

// NewSchedulerWithPause creates a scheduler that honors an optional pause gate.
func NewSchedulerWithPause(cfg *config.Config, connFactory *db.ConnectionFactory, cache *ops.Cache, profile profile.Profile, metrics *worker.MetricsCollector, pause *worker.PauseGate) *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())

	var spikeManager *SpikeManager
	if cfg.Profile == "spike" {
		spikeManager = NewSpikeManager(cfg)
	}

	min, _ := effectiveMinMax(cfg)

	return &Scheduler{
		config:       cfg,
		connFactory:  connFactory,
		cache:        cache,
		profile:      profile,
		metrics:      metrics,
		pauseGate:    pause,
		workers:      make([]*worker.Worker, 0),
		ctx:          ctx,
		cancel:       cancel,
		currentPhase: PhaseWarmup,
		startTime:    time.Now(),
		warmupRate:   cfg.GetRampRate(),
		cooldownRate: cfg.GetCooldownRate(),
		spikeManager: spikeManager,
		walkTarget:   min,
		rng:          rand.New(rand.NewSource(time.Now().UnixNano())),
		runDone:      make(chan struct{}),
	}
}

func effectiveMinMax(cfg *config.Config) (int, int) {
	min := cfg.ConnMin
	max := cfg.ConnMax
	if min <= 0 {
		min = cfg.ConnInit
	}
	if max <= 0 {
		max = cfg.ConnPeak
	}
	if min < 1 {
		min = 1
	}
	if max < min {
		max = min
	}
	return min, max
}

// Start begins the ramp schedule
func (s *Scheduler) Start() error {
	s.startTime = time.Now()
	go s.run()
	return nil
}

// Stop cancels the run loop, waits for it to exit, then drains all workers.
// Waiting for the loop first prevents orphaned workers being spawned after drain.
func (s *Scheduler) Stop() error {
	s.stopping.Store(true)
	if s.pauseGate != nil {
		s.pauseGate.Resume()
	}
	s.cancel()
	<-s.runDone
	return s.drainWorkers()
}

func (s *Scheduler) run() {
	defer close(s.runDone)

	min, _ := effectiveMinMax(s.config)
	if err := s.ensureWorkerCount(min); err != nil {
		fmt.Printf("Failed to ramp to initial connections: %v\n", err)
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if s.stopping.Load() {
				return
			}
			if s.pauseGate != nil && s.pauseGate.IsPaused() {
				if !s.wasPaused {
					s.pauseStarted = time.Now()
					s.wasPaused = true
				}
				continue
			}
			if s.wasPaused {
				s.pausedAccum += time.Since(s.pauseStarted)
				s.wasPaused = false
				s.pauseStarted = time.Time{}
			}

			s.update()
			if s.isComplete() {
				return
			}
		}
	}
}

func (s *Scheduler) isComplete() bool {
	if s.currentPhase != PhaseCooldown {
		return false
	}
	warmup := time.Duration(s.config.Warmup) * time.Second
	main := time.Duration(s.config.Main) * time.Second
	cooldown := time.Duration(s.config.Cooldown) * time.Second
	totalDuration := warmup + main + cooldown

	if s.elapsedTime >= totalDuration {
		s.workerMutex.RLock()
		allStopped := len(s.workers) == 0
		s.workerMutex.RUnlock()
		return allStopped
	}
	return false
}

func (s *Scheduler) update() {
	s.elapsedTime = time.Since(s.startTime) - s.pausedAccum
	s.updatePhase()

	switch s.currentPhase {
	case PhaseWarmup:
		s.handleWarmup()
	case PhaseMain:
		s.handleMain()
	case PhaseCooldown:
		s.handleCooldown()
	}

	s.metrics.SetConnectionCount(int64(len(s.workers)))
}

func (s *Scheduler) updatePhase() {
	warmupDuration := time.Duration(s.config.Warmup) * time.Second
	mainDuration := time.Duration(s.config.Main) * time.Second

	prev := s.currentPhase

	if s.elapsedTime < warmupDuration {
		s.currentPhase = PhaseWarmup
	} else if s.elapsedTime < warmupDuration+mainDuration {
		s.currentPhase = PhaseMain
		if prev != PhaseMain {
			s.workerMutex.RLock()
			s.walkTarget = len(s.workers)
			s.workerMutex.RUnlock()
			min, max := effectiveMinMax(s.config)
			if s.walkTarget < min {
				s.walkTarget = min
			}
			if s.walkTarget > max {
				s.walkTarget = max
			}
			s.lastWalkAdjust = time.Now()
		}
	} else {
		s.currentPhase = PhaseCooldown
	}
}

func (s *Scheduler) handleWarmup() {
	target := s.calculateTargetConnections(s.elapsedTime, PhaseWarmup)
	s.ensureWorkerCount(target)
}

func (s *Scheduler) handleMain() {
	min, max := effectiveMinMax(s.config)

	if s.config.Profile == "spike" && s.spikeManager != nil {
		warmup := time.Duration(s.config.Warmup) * time.Second
		mainElapsed := s.elapsedTime - warmup
		mainDur := time.Duration(s.config.Main) * time.Second
		s.spikeManager.Update(mainElapsed, mainDur)
		if sp, ok := s.profile.(*profile.SpikeProfile); ok {
			sp.UpdateSpikePhase(mainElapsed, mainDur)
		}
		if s.spikeManager.IsInSpike() {
			s.ensureWorkerCount(max)
		} else {
			midLevel := (min + max) / 2
			if midLevel < min {
				midLevel = min
			}
			s.ensureWorkerCount(midLevel)
		}
		return
	}

	// Random walk between min and max (hold if equal)
	if min == max {
		s.walkTarget = min
		s.ensureWorkerCount(min)
		return
	}

	now := time.Now()
	if now.Sub(s.lastWalkAdjust) >= walkInterval {
		step := 1
		if s.rng.Intn(2) == 0 {
			step = -1
		}
		s.walkTarget += step
		if s.walkTarget < min {
			s.walkTarget = min
		}
		if s.walkTarget > max {
			s.walkTarget = max
		}
		s.lastWalkAdjust = now
	}
	s.ensureWorkerCount(s.walkTarget)
}

func (s *Scheduler) handleCooldown() {
	warmupDuration := time.Duration(s.config.Warmup) * time.Second
	mainDuration := time.Duration(s.config.Main) * time.Second
	cooldownStart := warmupDuration + mainDuration
	cooldownElapsed := s.elapsedTime - cooldownStart

	target := s.calculateTargetConnections(cooldownElapsed, PhaseCooldown)
	s.ensureWorkerCount(target)
}

func (s *Scheduler) calculateTargetConnections(elapsed time.Duration, phase Phase) int {
	min, max := effectiveMinMax(s.config)
	switch phase {
	case PhaseWarmup:
		current := min + int(s.warmupRate*elapsed.Seconds())
		if current > max {
			current = max
		}
		if current < min {
			current = min
		}
		return current

	case PhaseCooldown:
		current := max - int(s.cooldownRate*elapsed.Seconds())
		if current < 0 {
			current = 0
		}
		return current

	default:
		return max
	}
}

func (s *Scheduler) ensureWorkerCount(target int) error {
	if s.ctx.Err() != nil || s.stopping.Load() {
		return nil
	}

	s.workerMutex.Lock()
	defer s.workerMutex.Unlock()

	if s.ctx.Err() != nil || s.stopping.Load() {
		return nil
	}

	current := len(s.workers)

	if current < target {
		for i := current; i < target; i++ {
			if s.ctx.Err() != nil || s.stopping.Load() {
				return nil
			}
			if err := s.addWorker(); err != nil {
				fmt.Printf("Failed to add worker %d: %v\n", i, err)
			}
		}
	} else if current > target {
		for i := current - 1; i >= target; i-- {
			if err := s.removeWorker(i); err != nil {
				fmt.Printf("Failed to remove worker %d: %v\n", i, err)
			}
		}
	}
	return nil
}

func (s *Scheduler) addWorker() error {
	workerID := len(s.workers)
	w := worker.NewWorkerWithPause(workerID, s.ctx, s.connFactory, s.cache, s.profile, s.config, s.metrics, s.pauseGate)

	if err := w.Start(); err != nil {
		return err
	}

	s.workers = append(s.workers, w)
	return nil
}

func (s *Scheduler) removeWorker(index int) error {
	if index < 0 || index >= len(s.workers) {
		return fmt.Errorf("invalid worker index: %d", index)
	}

	w := s.workers[index]
	if err := w.Stop(); err != nil {
		return err
	}

	s.workers = append(s.workers[:index], s.workers[index+1:]...)
	return nil
}

func (s *Scheduler) drainWorkers() error {
	s.workerMutex.Lock()
	defer s.workerMutex.Unlock()

	for i := len(s.workers) - 1; i >= 0; i-- {
		if err := s.workers[i].Stop(); err != nil {
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

// GetTargetWorkerCount returns the current walk/ramp target
func (s *Scheduler) GetTargetWorkerCount() int {
	return s.walkTarget
}

// GetCurrentPhase returns the current ramp phase
func (s *Scheduler) GetCurrentPhase() Phase {
	return s.currentPhase
}

// GetElapsedTime returns the elapsed time since start (excluding paused time)
func (s *Scheduler) GetElapsedTime() time.Duration {
	return s.elapsedTime
}

// Done returns a channel that's closed when the scheduler run loop has exited
// (natural completion or Stop).
func (s *Scheduler) Done() <-chan struct{} {
	return s.runDone
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
func NewSpikeManager(cfg *config.Config) *SpikeManager {
	return &SpikeManager{
		config:       cfg,
		currentCycle: 0,
		inSpikePhase: false,
	}
}

// Update updates the spike state based on elapsed time within the main phase
func (sm *SpikeManager) Update(elapsed, mainDuration time.Duration) {
	if sm.config.SpikeCycles <= 0 {
		sm.inSpikePhase = false
		return
	}

	spikeHold := time.Duration(sm.config.SpikeHold) * time.Second
	betweenSpike := sm.calculateBetweenSpikeDuration()
	cycleDuration := spikeHold + betweenSpike
	totalSpikeDuration := time.Duration(sm.config.SpikeCycles) * cycleDuration

	if elapsed >= totalSpikeDuration {
		sm.inSpikePhase = false
		return
	}

	currentCycle := int(elapsed / cycleDuration)
	phaseElapsed := elapsed % cycleDuration

	if phaseElapsed < spikeHold {
		sm.inSpikePhase = true
		sm.currentCycle = currentCycle
		if sm.spikeStartTime.IsZero() {
			sm.spikeStartTime = time.Now()
		}
	} else {
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

func (sm *SpikeManager) calculateBetweenSpikeDuration() time.Duration {
	if sm.config.SpikeCycles <= 0 {
		return 0
	}

	mainDuration := time.Duration(sm.config.Main) * time.Second
	spikeHoldTotal := time.Duration(sm.config.SpikeCycles*sm.config.SpikeHold) * time.Second
	remaining := mainDuration - spikeHoldTotal
	if remaining < 0 {
		remaining = 0
	}
	return remaining / time.Duration(sm.config.SpikeCycles)
}

// GetSpikeStats returns spike-specific statistics
func (sm *SpikeManager) GetSpikeStats() string {
	phase := "between-spike"
	if sm.inSpikePhase {
		phase = "spike"
	}
	return fmt.Sprintf("Spike: cycle %d, phase %s", sm.currentCycle+1, phase)
}
