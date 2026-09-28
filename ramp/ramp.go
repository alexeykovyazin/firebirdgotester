package ramp

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"fb-loadgen/config"
	"fb-loadgen/ops"
	"fb-loadgen/profile"
	"fb-loadgen/worker"
)

// Scheduler manages the connection ramp-up and ramp-down phases
type Scheduler struct {
	config      *config.Config
	connFactory worker.Connector
	cache       *ops.Cache
	profile     profile.Profile
	metrics     *worker.MetricsCollector
	pauseGate   *worker.PauseGate

	workers     []*worker.Worker
	nextID      int // monotonic, so a failed add/remove cannot reuse a live ID
	workerMutex sync.RWMutex
	ctx         context.Context
	cancel      context.CancelFunc

	// stateMu guards the run-loop state read by status getters from other
	// goroutines (web UI, reporter): currentPhase, elapsedTime, walkTarget,
	// cooldownFrom and the pause clock. The run loop writes them; the getters
	// may run concurrently with the loop.
	stateMu sync.RWMutex

	// workerStopTimeout overrides the per-worker Stop timeout; 0 keeps the
	// worker package default. Set by tests.
	workerStopTimeout time.Duration

	currentPhase Phase
	startTime    time.Time
	elapsedTime  time.Duration

	// Pause clock: wall time spent paused is excluded from phase elapsed
	pausedAccum  time.Duration
	pauseStarted time.Time
	wasPaused    bool

	warmupRate   float64
	cooldownRate float64

	// cooldownFrom is the live worker count when the cooldown phase begins;
	// the ramp-down descends from there instead of from max (which would
	// first spawn a burst up to max in a main phase that ended near min).
	cooldownFrom int

	spikeManager *SpikeManager

	walkTarget     int
	lastWalkAdjust time.Time
	rng            *rand.Rand

	liveConnMax atomic.Int64 // >0 overrides config ConnMax (live budget resize)

	stopping     atomic.Bool
	started      atomic.Bool
	runDone      chan struct{} // closed exactly once when run() exits (or Stop on a never-started scheduler)
	runDoneClose sync.Once
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
func NewScheduler(cfg *config.Config, connFactory worker.Connector, cache *ops.Cache, profile profile.Profile, metrics *worker.MetricsCollector) *Scheduler {
	return NewSchedulerWithPause(cfg, connFactory, cache, profile, metrics, nil)
}

// NewSchedulerWithPause creates a scheduler that honors an optional pause gate.
func NewSchedulerWithPause(cfg *config.Config, connFactory worker.Connector, cache *ops.Cache, profile profile.Profile, metrics *worker.MetricsCollector, pause *worker.PauseGate) *Scheduler {
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

// SetConnectionMax applies a live cap on the worker/connection count,
// overriding the config value from the next tick on. Used when the fleet
// budget changes while the session is running.
func (s *Scheduler) SetConnectionMax(max int) {
	if max < 0 {
		max = 0
	}
	s.liveConnMax.Store(int64(max))
}

// currentMinMax returns the effective min/max worker counts, honoring any
// live cap set via SetConnectionMax.
func (s *Scheduler) currentMinMax() (int, int) {
	min, max := effectiveMinMax(s.config)
	if cap := int(s.liveConnMax.Load()); cap > 0 {
		if max > cap {
			max = cap
		}
		if min > max {
			min = max
		}
	}
	return min, max
}

// Start begins the ramp schedule. Starting twice returns an error (two run
// loops would race on the walk target and double-close runDone).
func (s *Scheduler) Start() error {
	if !s.started.CompareAndSwap(false, true) {
		return fmt.Errorf("scheduler already started")
	}
	s.startTime = time.Now()
	go s.run()
	return nil
}

// closeRunDone closes runDone exactly once: run() on exit, or Stop on a
// scheduler that was never started (whose run() will never run).
func (s *Scheduler) closeRunDone() {
	s.runDoneClose.Do(func() { close(s.runDone) })
}

// Stop cancels the run loop, waits for it to exit, then drains all workers.
// Waiting for the loop first prevents orphaned workers being spawned after drain.
// Safe to call before Start (nothing to wait for) and after natural completion.
func (s *Scheduler) Stop() error {
	s.stopping.Store(true)
	if s.pauseGate != nil {
		s.pauseGate.Resume()
	}
	s.cancel()
	if s.started.Load() {
		<-s.runDone
	} else {
		s.closeRunDone()
	}
	return s.drainWorkers()
}

func (s *Scheduler) run() {
	defer s.closeRunDone()

	min, _ := s.currentMinMax()
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
	if s.config.UnboundedMain {
		return false
	}
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
	s.stateMu.Lock()
	s.elapsedTime = time.Since(s.startTime) - s.pausedAccum
	s.stateMu.Unlock()
	s.updatePhase()

	switch s.GetCurrentPhase() {
	case PhaseWarmup:
		s.handleWarmup()
	case PhaseMain:
		s.handleMain()
	case PhaseCooldown:
		s.handleCooldown()
	}

	s.metrics.SetConnectionCount(int64(s.GetCurrentWorkerCount()))
}

func (s *Scheduler) updatePhase() {
	warmupDuration := time.Duration(s.config.Warmup) * time.Second
	mainDuration := time.Duration(s.config.Main) * time.Second

	s.stateMu.Lock()
	elapsedTime := s.elapsedTime
	prev := s.currentPhase
	s.stateMu.Unlock()

	var next Phase
	switch {
	case elapsedTime < warmupDuration:
		next = PhaseWarmup
	case s.config.UnboundedMain || elapsedTime < warmupDuration+mainDuration:
		next = PhaseMain
	default:
		next = PhaseCooldown
	}

	if next == PhaseCooldown && prev != PhaseCooldown {
		// Anchor the ramp-down at the live worker count: descending from max
		// would first burst up to max in a main phase that ended near min.
		s.cooldownFrom = s.GetCurrentWorkerCount()
	}

	if next == PhaseMain && prev != PhaseMain {
		s.stateMu.Lock()
		s.walkTarget = s.GetCurrentWorkerCount()
		s.stateMu.Unlock()
		min, max := s.currentMinMax()
		if s.walkTarget < min {
			s.walkTarget = min
		}
		if s.walkTarget > max {
			s.walkTarget = max
		}
		s.lastWalkAdjust = time.Now()
	}

	s.stateMu.Lock()
	s.currentPhase = next
	s.stateMu.Unlock()
}

func (s *Scheduler) handleWarmup() {
	target := s.calculateTargetConnections(s.elapsedTime, PhaseWarmup)
	s.ensureWorkerCount(target)
}

func (s *Scheduler) handleMain() {
	min, max := s.currentMinMax()

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
	// Snap into range first so a live budget shrink applies on the next
	// tick instead of walking down one step at a time.
	s.stateMu.Lock()
	if s.walkTarget > max {
		s.walkTarget = max
	}
	if s.walkTarget < min {
		s.walkTarget = min
	}
	if min == max {
		s.walkTarget = min
		s.stateMu.Unlock()
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
	target := s.walkTarget
	s.stateMu.Unlock()
	s.ensureWorkerCount(target)
}

func (s *Scheduler) handleCooldown() {
	warmupDuration := time.Duration(s.config.Warmup) * time.Second
	mainDuration := time.Duration(s.config.Main) * time.Second
	cooldownStart := warmupDuration + mainDuration

	s.stateMu.Lock()
	cooldownElapsed := s.elapsedTime - cooldownStart
	from := s.cooldownFrom
	s.stateMu.Unlock()

	// Descend from the live worker count at cooldown start; without the
	// anchor the formula below targets max at elapsed 0 and ensureWorkerCount
	// would instantly spawn workers during the "ramp-down".
	if from <= 0 {
		_, from = s.currentMinMax()
	}
	target := from - int(s.cooldownRate*cooldownElapsed.Seconds())
	if target < 0 {
		target = 0
	}
	s.ensureWorkerCount(target)
}

func (s *Scheduler) calculateTargetConnections(elapsed time.Duration, phase Phase) int {
	min, max := s.currentMinMax()
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

	default:
		// Cooldown no longer routes through here: handleCooldown descends
		// from the live worker count anchored at phase start.
		return max
	}
}

func (s *Scheduler) ensureWorkerCount(target int) error {
	if s.ctx.Err() != nil || s.stopping.Load() {
		return nil
	}
	if target < 0 {
		target = 0
	}

	s.workerMutex.Lock()
	if s.ctx.Err() != nil || s.stopping.Load() {
		s.workerMutex.Unlock()
		return nil
	}

	// Reap self-exited workers first (panic recovery, failed pool rebuild):
	// keeping them in the set overstates the live connection count and the
	// ramp would believe the target is met while the load silently decays
	// (V17). Their contexts are cancelled and handles closed by cleanup, so
	// there is nothing left to stop.
	var live, dead []*worker.Worker
	for _, w := range s.workers {
		select {
		case <-w.Done():
			dead = append(dead, w)
		default:
			live = append(live, w)
		}
	}
	s.workers = live

	current := len(live)
	var removed []*worker.Worker
	if current > target {
		// Detach first, then stop outside the lock. Stop can block for its
		// full timeout, and holding the write lock that long stalls the run
		// loop and every status reader (web UI, reporter).
		removed = append(removed, live[target:]...)
		s.workers = live[:target]
	}
	s.workerMutex.Unlock()

	if len(dead) > 0 {
		for _, w := range dead {
			_ = w.Stop() // no-op for an already-exited worker
		}
		fmt.Printf("Reaped %d self-exited worker(s)\n", len(dead))
	}

	if len(removed) > 0 {
		s.stopWorkers(removed)
		return nil
	}

	for i := current; i < target; i++ {
		if s.ctx.Err() != nil || s.stopping.Load() {
			return nil
		}
		if err := s.addWorker(); err != nil {
			// Fail this one worker and let the next tick retry. Retrying
			// in-line would serialise one connect timeout per missing worker
			// into a single tick, which is how a server at its connection
			// limit used to stall the whole scheduler.
			fmt.Printf("Failed to add worker: %v\n", err)
			break
		}
	}
	return nil
}

func (s *Scheduler) addWorker() error {
	s.workerMutex.Lock()
	workerID := s.nextID
	s.nextID++
	s.workerMutex.Unlock()

	w := worker.NewWorkerWithPause(workerID, s.ctx, s.connFactory, s.cache, s.profile, s.config, s.metrics, s.pauseGate)
	if s.workerStopTimeout > 0 {
		w.SetStopTimeout(s.workerStopTimeout)
	}

	// Start opens the connection. A worker that fails to connect never starts
	// a goroutine and must not enter the set, or the scheduler would count a
	// worker that has no database handle.
	if err := w.Start(); err != nil {
		return err
	}

	s.workerMutex.Lock()
	s.workers = append(s.workers, w)
	s.workerMutex.Unlock()
	return nil
}

// stopWorkers stops already-detached workers in parallel. A worker whose Stop
// times out is reported but not retried and not put back: its context is
// cancelled and its connection closed, so its goroutine exits on its own next
// loop iteration. Leaving it in the set would pin the worker count and stop
// the ramp from ever shrinking.
func (s *Scheduler) stopWorkers(workers []*worker.Worker) {
	var wg sync.WaitGroup
	for _, w := range workers {
		wg.Add(1)
		go func(w *worker.Worker) {
			defer wg.Done()
			if err := w.Stop(); err != nil {
				fmt.Printf("Failed to remove worker %d: %v\n", w.GetID(), err)
			}
		}(w)
	}
	wg.Wait()
}

func (s *Scheduler) drainWorkers() error {
	s.workerMutex.Lock()
	workers := s.workers
	s.workers = nil
	s.workerMutex.Unlock()

	s.stopWorkers(workers)
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
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.walkTarget
}

// GetCurrentPhase returns the current ramp phase
func (s *Scheduler) GetCurrentPhase() Phase {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.currentPhase
}

// GetElapsedTime returns the elapsed time since start (excluding paused time)
func (s *Scheduler) GetElapsedTime() time.Duration {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.elapsedTime
}

// Done returns a channel that's closed when the scheduler run loop has exited
// (natural completion or Stop).
func (s *Scheduler) Done() <-chan struct{} {
	return s.runDone
}

// GetPhaseProgress returns the progress of the current phase (0.0 to 1.0)
func (s *Scheduler) GetPhaseProgress() float64 {
	s.stateMu.RLock()
	phase := s.currentPhase
	elapsedTime := s.elapsedTime
	s.stateMu.RUnlock()

	switch phase {
	case PhaseWarmup:
		total := time.Duration(s.config.Warmup) * time.Second
		if total <= 0 {
			return 1.0
		}
		return float64(elapsedTime) / float64(total)

	case PhaseMain:
		warmup := time.Duration(s.config.Warmup) * time.Second
		main := time.Duration(s.config.Main) * time.Second
		elapsedInMain := elapsedTime - warmup
		if main <= 0 {
			return 1.0
		}
		return float64(elapsedInMain) / float64(main)

	case PhaseCooldown:
		warmup := time.Duration(s.config.Warmup) * time.Second
		main := time.Duration(s.config.Main) * time.Second
		cooldown := time.Duration(s.config.Cooldown) * time.Second
		elapsedInCooldown := elapsedTime - warmup - main
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
	if cycleDuration <= 0 {
		sm.inSpikePhase = false
		return
	}

	// mainDuration <= 0 means unbounded main: keep cycling forever.
	if mainDuration > 0 {
		totalSpikeDuration := time.Duration(sm.config.SpikeCycles) * cycleDuration
		if elapsed >= totalSpikeDuration {
			sm.inSpikePhase = false
			return
		}
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
	if sm.config.UnboundedMain || sm.config.Main <= 0 {
		// No finite main to spread spikes over; use the default gap.
		return 30 * time.Second
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
