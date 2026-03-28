package profile

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"fb-loadgen/ops"
)

// SpikeProfile implements the spike simulation profile
type SpikeProfile struct {
	*BaseProfile
	// Spike-specific configuration
	spikeCycles    int
	spikeHold      time.Duration
	betweenSpike   time.Duration
	currentCycle   int
	inSpikePhase   bool
	spikeStartTime time.Time
}

// NewSpikeProfile creates a new spike profile
func NewSpikeProfile(readOps *ops.ReadOperations, writeOps *ops.WriteOperations, cache *ops.Cache) *SpikeProfile {
	// Spike profile starts with read-heavy operations as base
	// During spike cycles, it switches to write-heavy operations
	opsWeights := []OpWeight{
		{
			Weight: 30,
			Op:     func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error { return readOps.CallOrgChart(ctx, tx) },
			Name:   "CallOrgChart",
		},
		{
			Weight: 25,
			Op:     func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error { return readOps.CallDeptBudget(ctx, tx) },
			Name:   "CallDeptBudget",
		},
		{
			Weight: 15,
			Op:     func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error { return readOps.CallMailLabel(ctx, tx) },
			Name:   "CallMailLabel",
		},
		{
			Weight: 10,
			Op:     func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error { return readOps.CallGetEmpProj(ctx, tx) },
			Name:   "CallGetEmpProj",
		},
		{
			Weight: 10,
			Op: func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error {
				return readOps.CallSubTotBudget(ctx, tx)
			},
			Name: "CallSubTotBudget",
		},
		{
			Weight: 5,
			Op: func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error {
				return readOps.SelectEmployeeDeptJob(ctx, tx)
			},
			Name: "SelectEmployeeDeptJob",
		},
		{
			Weight: 5,
			Op: func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error {
				return writeOps.UpdateDeptBudget(ctx, tx)
			},
			Name: "UpdateDeptBudget",
		},
	}

	baseProfile := NewBaseProfile("spike", opsWeights, readOps, writeOps, cache)
	return &SpikeProfile{
		BaseProfile:    baseProfile,
		spikeCycles:    3, // Default values, will be set by config
		spikeHold:      10 * time.Second,
		betweenSpike:   30 * time.Second,
		currentCycle:   0,
		inSpikePhase:   false,
		spikeStartTime: time.Time{},
	}
}

// SetSpikeConfiguration sets the spike-specific configuration
func (sp *SpikeProfile) SetSpikeConfiguration(cycles int, hold, between time.Duration) {
	sp.spikeCycles = cycles
	sp.spikeHold = hold
	sp.betweenSpike = between
}

// Name returns the profile name
func (sp *SpikeProfile) Name() string {
	return "spike"
}

// NextOp returns the next operation to execute
// During spike phases, it returns write-heavy operations
// During between-spike phases, it returns read-heavy operations
func (sp *SpikeProfile) NextOp() func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error {
	if sp.inSpikePhase {
		// During spike: use write-heavy operations
		return sp.getWriteHeavyOperation()
	} else {
		// Between spikes: use read-heavy operations
		op, _ := sp.selector.Select()
		return op
	}
}

// getWriteHeavyOperation returns a write-heavy operation
func (sp *SpikeProfile) getWriteHeavyOperation() func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error {
	// Create a temporary weighted selector for write-heavy operations
	writeOpsWeights := []OpWeight{
		{
			Weight: 25,
			Op: func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error {
				return sp.writeOps.InsertCustomer(ctx, tx)
			},
			Name: "InsertCustomer",
		},
		{
			Weight: 20,
			Op:     func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error { return sp.writeOps.InsertSales(ctx, tx) },
			Name:   "InsertSales",
		},
		{
			Weight: 15,
			Op: func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error {
				return sp.writeOps.UpdateSalesStatus(ctx, tx)
			},
			Name: "UpdateSalesStatus",
		},
		{
			Weight: 15,
			Op: func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error {
				return sp.writeOps.CallShipOrder(ctx, tx)
			},
			Name: "CallShipOrder",
		},
		{
			Weight: 10,
			Op: func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error {
				return sp.writeOps.UpdateEmployeeSalary(ctx, tx)
			},
			Name: "UpdateEmployeeSalary",
		},
		{
			Weight: 10,
			Op: func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error {
				return sp.writeOps.CallAddEmpProj(ctx, tx)
			},
			Name: "CallAddEmpProj",
		},
		{
			Weight: 5,
			Op: func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error {
				return sp.writeOps.DeleteEmpProj(ctx, tx)
			},
			Name: "DeleteEmpProj",
		},
	}

	selector := NewWeightedSelector(writeOpsWeights)
	op, _ := selector.Select()
	return op
}

// UpdateSpikePhase updates the current spike phase based on elapsed time
func (sp *SpikeProfile) UpdateSpikePhase(elapsed time.Duration, mainDuration time.Duration) {
	if sp.spikeCycles <= 0 {
		// No spikes configured, stay in between-spike phase
		sp.inSpikePhase = false
		return
	}

	// Calculate spike interval
	totalSpikeDuration := time.Duration(sp.spikeCycles) * (sp.spikeHold + sp.betweenSpike)
	if elapsed >= totalSpikeDuration {
		// After all spikes, stay in between-spike phase
		sp.inSpikePhase = false
		return
	}

	// Determine current cycle and phase
	cycleDuration := sp.spikeHold + sp.betweenSpike
	currentCycle := int(elapsed / cycleDuration)
	phaseElapsed := elapsed % cycleDuration

	if phaseElapsed < sp.spikeHold {
		// In spike phase
		sp.inSpikePhase = true
		sp.currentCycle = currentCycle
		if sp.spikeStartTime.IsZero() {
			sp.spikeStartTime = time.Now()
		}
	} else {
		// In between-spike phase
		sp.inSpikePhase = false
		sp.spikeStartTime = time.Time{}
	}
}

// IsInSpikePhase returns true if currently in a spike phase
func (sp *SpikeProfile) IsInSpikePhase() bool {
	return sp.inSpikePhase
}

// GetCurrentCycle returns the current spike cycle
func (sp *SpikeProfile) GetCurrentCycle() int {
	return sp.currentCycle
}

// GetSpikeProgress returns the progress of the current spike (0.0 to 1.0)
func (sp *SpikeProfile) GetSpikeProgress() float64 {
	if !sp.inSpikePhase || sp.spikeStartTime.IsZero() {
		return 0.0
	}

	elapsed := time.Since(sp.spikeStartTime)
	if elapsed >= sp.spikeHold {
		return 1.0
	}
	return float64(elapsed) / float64(sp.spikeHold)
}

// GetSpikeStats returns spike-specific statistics
func (sp *SpikeProfile) GetSpikeStats() string {
	phase := "between-spike"
	if sp.inSpikePhase {
		phase = "spike"
	}
	progress := sp.GetSpikeProgress()
	return fmt.Sprintf("Spike: cycle %d, phase %s, progress %.1f%%", sp.currentCycle+1, phase, progress*100)
}

// GetProfileDescription returns a description of the spike profile
func (sp *SpikeProfile) GetProfileDescription() string {
	return `Spike Profile:
- Starts with read-heavy operations (base load)
- At each spike cycle: rapidly increases connections to peak
- During spike: switches to write-heavy operations (full OLTP load)
- After spike: drops connections back to mid-level, returns to read-heavy
- Repeats for configured number of cycles

Designed to simulate sudden load spikes and measure system response.
Connection count follows a sawtooth wave during main period.`
}

// ValidateProfile validates that the spike profile is properly configured
func (sp *SpikeProfile) ValidateProfile() error {
	if sp.spikeCycles < 1 {
		return fmt.Errorf("spike cycles must be >= 1, got %d", sp.spikeCycles)
	}
	if sp.spikeHold <= 0 {
		return fmt.Errorf("spike hold duration must be > 0, got %v", sp.spikeHold)
	}
	if sp.betweenSpike < 0 {
		return fmt.Errorf("between spike duration must be >= 0, got %v", sp.betweenSpike)
	}
	return nil
}

// GetSpikeConfiguration returns the current spike configuration
func (sp *SpikeProfile) GetSpikeConfiguration() (int, time.Duration, time.Duration) {
	return sp.spikeCycles, sp.spikeHold, sp.betweenSpike
}

// GetExpectedTPS returns the expected transactions per second for this profile
func (sp *SpikeProfile) GetExpectedTPS() float64 {
	// Spike profile varies between read-heavy and write-heavy
	// Average of both profiles
	return 40.0 // transactions per second (average estimate)
}

// GetProfileComplexity returns the complexity level of this profile
func (sp *SpikeProfile) GetProfileComplexity() string {
	return "high" // Dynamic behavior and phase management
}

// GetRecommendedConnectionCount returns the recommended number of connections for this profile
func (sp *SpikeProfile) GetRecommendedConnectionCount() int {
	// Spike profile needs enough connections for both base and peak load
	return 20
}

// GetProfileCharacteristics returns key characteristics of this profile
func (sp *SpikeProfile) GetProfileCharacteristics() map[string]interface{} {
	return map[string]interface{}{
		"profile_type":            "spike",
		"primary_operations":      []string{"read_heavy_base", "write_heavy_spike"},
		"transaction_pattern":     "dynamic",
		"lock_contention":         "variable",
		"database_impact":         "burst_load",
		"recommended_connections": 20,
		"expected_tps":            40.0,
		"complexity":              "high",
		"spike_cycles":            sp.spikeCycles,
		"spike_hold_duration":     sp.spikeHold.Seconds(),
		"between_spike_duration":  sp.betweenSpike.Seconds(),
	}
}

// ResetSpikeState resets the spike state
func (sp *SpikeProfile) ResetSpikeState() {
	sp.currentCycle = 0
	sp.inSpikePhase = false
	sp.spikeStartTime = time.Time{}
}

// GetPhaseType returns the current phase type
func (sp *SpikeProfile) GetPhaseType() string {
	if sp.inSpikePhase {
		return "spike"
	}
	return "between-spike"
}

// GetSpikeCycleDuration returns the duration of one complete spike cycle
func (sp *SpikeProfile) GetSpikeCycleDuration() time.Duration {
	return sp.spikeHold + sp.betweenSpike
}

// GetTotalSpikeDuration returns the total duration of all spike cycles
func (sp *SpikeProfile) GetTotalSpikeDuration() time.Duration {
	return time.Duration(sp.spikeCycles) * sp.GetSpikeCycleDuration()
}
