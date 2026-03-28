package profile

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"time"

	"fb-loadgen/ops"
)

// Profile defines the interface for simulation profiles
type Profile interface {
	// Name returns the profile name
	Name() string

	// NextOp returns the next operation to execute
	NextOp() func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error

	// Weights returns the operation weights for this profile
	Weights() []OpWeight
}

// OpWeight represents an operation with its weight
type OpWeight struct {
	Weight int
	Op     func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error
	Name   string
}

// WeightedSelector provides weighted random selection
type WeightedSelector struct {
	ops         []OpWeight
	totalWeight int
	rng         *rand.Rand
}

// NewWeightedSelector creates a new weighted selector
func NewWeightedSelector(ops []OpWeight) *WeightedSelector {
	totalWeight := 0
	for _, op := range ops {
		totalWeight += op.Weight
	}

	return &WeightedSelector{
		ops:         ops,
		totalWeight: totalWeight,
		rng:         rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Select randomly selects an operation based on weights
func (ws *WeightedSelector) Select() (func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error, string) {
	if len(ws.ops) == 0 {
		return nil, ""
	}

	target := ws.rng.Intn(ws.totalWeight)
	current := 0

	for _, op := range ws.ops {
		current += op.Weight
		if target < current {
			return op.Op, op.Name
		}
	}

	// Fallback to first operation
	return ws.ops[0].Op, ws.ops[0].Name
}

// BaseProfile provides common functionality for all profiles
type BaseProfile struct {
	name     string
	selector *WeightedSelector
	readOps  *ops.ReadOperations
	writeOps *ops.WriteOperations
	cache    *ops.Cache
}

// NewBaseProfile creates a new base profile
func NewBaseProfile(name string, ops []OpWeight, readOps *ops.ReadOperations, writeOps *ops.WriteOperations, cache *ops.Cache) *BaseProfile {
	return &BaseProfile{
		name:     name,
		selector: NewWeightedSelector(ops),
		readOps:  readOps,
		writeOps: writeOps,
		cache:    cache,
	}
}

// Name returns the profile name
func (bp *BaseProfile) Name() string {
	return bp.name
}

// Weights returns the operation weights
func (bp *BaseProfile) Weights() []OpWeight {
	return bp.selector.ops
}

// NextOp returns the next operation to execute
func (bp *BaseProfile) NextOp() func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error {
	op, _ := bp.selector.Select()
	return op
}

// ProfileFactory creates profiles
type ProfileFactory struct {
	readOps  *ops.ReadOperations
	writeOps *ops.WriteOperations
	cache    *ops.Cache
}

// NewProfileFactory creates a new profile factory
func NewProfileFactory(readOps *ops.ReadOperations, writeOps *ops.WriteOperations, cache *ops.Cache) *ProfileFactory {
	return &ProfileFactory{
		readOps:  readOps,
		writeOps: writeOps,
		cache:    cache,
	}
}

// CreateProfile creates a profile based on the name
func (pf *ProfileFactory) CreateProfile(name string) (Profile, error) {
	switch name {
	case "write-heavy":
		return NewWriteHeavyProfile(pf.readOps, pf.writeOps, pf.cache), nil
	case "read-heavy":
		return NewReadHeavyProfile(pf.readOps, pf.writeOps, pf.cache), nil
	case "spike":
		return NewSpikeProfile(pf.readOps, pf.writeOps, pf.cache), nil
	default:
		return nil, fmt.Errorf("unknown profile: %s", name)
	}
}

// GetAvailableProfiles returns a list of available profile names
func (pf *ProfileFactory) GetAvailableProfiles() []string {
	return []string{"write-heavy", "read-heavy", "spike"}
}

// ValidateProfile checks if a profile name is valid
func (pf *ProfileFactory) ValidateProfile(name string) bool {
	for _, profile := range pf.GetAvailableProfiles() {
		if profile == name {
			return true
		}
	}
	return false
}

// ProfileStats tracks profile execution statistics
type ProfileStats struct {
	ProfileName    string
	TotalOps       int
	ReadOps        int
	WriteOps       int
	OpCounts       map[string]int
	StartTime      time.Time
	LastReportTime time.Time
}

// NewProfileStats creates a new profile stats instance
func NewProfileStats(profileName string) *ProfileStats {
	return &ProfileStats{
		ProfileName:    profileName,
		OpCounts:       make(map[string]int),
		StartTime:      time.Now(),
		LastReportTime: time.Now(),
	}
}

// RecordOp records an operation execution
func (ps *ProfileStats) RecordOp(opName string, isRead bool) {
	ps.TotalOps++
	if isRead {
		ps.ReadOps++
	} else {
		ps.WriteOps++
	}
	ps.OpCounts[opName]++
}

// GetStats returns a summary of profile statistics
func (ps *ProfileStats) GetStats() string {
	elapsed := time.Since(ps.StartTime)
	tps := float64(ps.TotalOps) / elapsed.Seconds()

	return fmt.Sprintf("Profile: %s, Total: %d, Reads: %d, Writes: %d, TPS: %.2f",
		ps.ProfileName, ps.TotalOps, ps.ReadOps, ps.WriteOps, tps)
}

// Reset resets all profile statistics
func (ps *ProfileStats) Reset() {
	ps.TotalOps = 0
	ps.ReadOps = 0
	ps.WriteOps = 0
	for k := range ps.OpCounts {
		delete(ps.OpCounts, k)
	}
	ps.StartTime = time.Now()
	ps.LastReportTime = time.Now()
}

// ProfileManager manages profile lifecycle
type ProfileManager struct {
	currentProfile Profile
	stats          *ProfileStats
}

// NewProfileManager creates a new profile manager
func NewProfileManager(profile Profile) *ProfileManager {
	return &ProfileManager{
		currentProfile: profile,
		stats:          NewProfileStats(profile.Name()),
	}
}

// SwitchProfile switches to a different profile
func (pm *ProfileManager) SwitchProfile(profile Profile) {
	pm.currentProfile = profile
	pm.stats = NewProfileStats(profile.Name())
}

// GetCurrentProfile returns the current profile
func (pm *ProfileManager) GetCurrentProfile() Profile {
	return pm.currentProfile
}

// GetStats returns the current profile statistics
func (pm *ProfileManager) GetStats() *ProfileStats {
	return pm.stats
}

// RecordOp records an operation execution
func (pm *ProfileManager) RecordOp(opName string, isRead bool) {
	pm.stats.RecordOp(opName, isRead)
}

// ProfilePhase represents a phase in profile execution
type ProfilePhase int

const (
	PhaseWarmup ProfilePhase = iota
	PhaseMain
	PhaseCooldown
	PhaseSpike
	PhaseBetweenSpike
)

// String returns the string representation of a profile phase
func (pp ProfilePhase) String() string {
	switch pp {
	case PhaseWarmup:
		return "warmup"
	case PhaseMain:
		return "main"
	case PhaseCooldown:
		return "cooldown"
	case PhaseSpike:
		return "spike"
	case PhaseBetweenSpike:
		return "between-spike"
	default:
		return "unknown"
	}
}

// PhaseManager manages profile phases
type PhaseManager struct {
	currentPhase   ProfilePhase
	startTime      time.Time
	phaseDurations map[ProfilePhase]time.Duration
}

// NewPhaseManager creates a new phase manager
func NewPhaseManager(warmup, main, cooldown time.Duration) *PhaseManager {
	return &PhaseManager{
		currentPhase: PhaseWarmup,
		startTime:    time.Now(),
		phaseDurations: map[ProfilePhase]time.Duration{
			PhaseWarmup:       warmup,
			PhaseMain:         main,
			PhaseCooldown:     cooldown,
			PhaseSpike:        0, // Set dynamically for spike profile
			PhaseBetweenSpike: 0, // Set dynamically for spike profile
		},
	}
}

// GetCurrentPhase returns the current phase
func (pm *PhaseManager) GetCurrentPhase() ProfilePhase {
	return pm.currentPhase
}

// SetPhase sets the current phase
func (pm *PhaseManager) SetPhase(phase ProfilePhase) {
	pm.currentPhase = phase
	pm.startTime = time.Now()
}

// GetPhaseDuration returns the duration of the current phase
func (pm *PhaseManager) GetPhaseDuration() time.Duration {
	return pm.phaseDurations[pm.currentPhase]
}

// GetElapsedTime returns the elapsed time in the current phase
func (pm *PhaseManager) GetElapsedTime() time.Duration {
	return time.Since(pm.startTime)
}

// IsPhaseComplete checks if the current phase is complete
func (pm *PhaseManager) IsPhaseComplete() bool {
	return pm.GetElapsedTime() >= pm.GetPhaseDuration()
}

// GetPhaseProgress returns the progress of the current phase (0.0 to 1.0)
func (pm *PhaseManager) GetPhaseProgress() float64 {
	duration := pm.GetPhaseDuration()
	if duration <= 0 {
		return 1.0
	}
	elapsed := pm.GetElapsedTime()
	if elapsed >= duration {
		return 1.0
	}
	return float64(elapsed) / float64(duration)
}
