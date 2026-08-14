package session

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"fb-loadgen/config"
	"fb-loadgen/discover"
)

// Status represents session lifecycle state.
type Status string

const (
	StatusIdle      Status = "Idle"
	StatusStarting  Status = "Starting"
	StatusRunning   Status = "Running"
	StatusPaused    Status = "Paused"
	StatusStopping  Status = "Stopping"
	StatusCompleted Status = "Completed"
	StatusFailed    Status = "Failed"
)

// SessionConfig holds per-database run settings.
type SessionConfig struct {
	Name    string `json:"name"`
	RelPath string `json:"relPath"`
	AbsPath string `json:"absPath"`
	DSN     string `json:"dsn"`
	User    string `json:"user"`
	Pass    string `json:"pass"`

	Profile string `json:"profile"`

	ConnMin int `json:"connMin"`
	ConnMax int `json:"connMax"`

	Warmup   int `json:"warmup"`
	Main     int `json:"main"`
	Cooldown int `json:"cooldown"`

	SpikeCycles int `json:"spikeCycles"`
	SpikeHold   int `json:"spikeHold"`

	ThinkMs   int `json:"thinkMs"`
	TxTimeout int `json:"txTimeout"`
	Debug     bool `json:"debug"`
}

// Defaults returns a SessionConfig seeded from shared CLI defaults.
func DefaultsFromCLI(cfg *config.Config, info discover.DatabaseInfo, host string, port int) SessionConfig {
	connMin := cfg.ConnMin
	connMax := cfg.ConnMax
	if connMin <= 0 {
		connMin = cfg.ConnInit
	}
	if connMax <= 0 {
		connMax = cfg.ConnPeak
	}
	if connMin <= 0 {
		connMin = 2
	}
	if connMax < connMin {
		connMax = connMin
	}

	profile := cfg.Profile
	if profile == "" {
		profile = "write-heavy"
	}

	return SessionConfig{
		Name:        info.Name,
		RelPath:     info.RelPath,
		AbsPath:     info.AbsPath,
		DSN:         discover.BuildDSN(host, port, info.AbsPath),
		User:        cfg.User,
		Pass:        cfg.Pass,
		Profile:     profile,
		ConnMin:     connMin,
		ConnMax:     connMax,
		Warmup:      nonzero(cfg.Warmup, 30),
		Main:        nonzero(cfg.Main, 120),
		Cooldown:    nonzero(cfg.Cooldown, 20),
		SpikeCycles: nonzero(cfg.SpikeCycles, 3),
		SpikeHold:   nonzero(cfg.SpikeHold, 10),
		ThinkMs:     cfg.ThinkMs,
		TxTimeout:   nonzero(cfg.TxTimeout, 10),
		Debug:       cfg.Debug,
	}
}

func nonzero(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
}

// IDFromAbsPath returns a stable session ID from absolute path.
func IDFromAbsPath(absPath string) string {
	sum := sha256.Sum256([]byte(absPath))
	return hex.EncodeToString(sum[:])[:16]
}

// Validate checks session config fields.
func (c *SessionConfig) Validate() error {
	switch c.Profile {
	case "write-heavy", "read-heavy", "spike":
	default:
		return fmt.Errorf("invalid profile: %s", c.Profile)
	}
	if c.ConnMin < 1 {
		return fmt.Errorf("connMin must be >= 1")
	}
	if c.ConnMax < c.ConnMin {
		return fmt.Errorf("connMax must be >= connMin")
	}
	if c.Warmup < 0 || c.Main < 0 || c.Cooldown < 0 {
		return fmt.Errorf("timing values must be >= 0")
	}
	if c.Profile == "spike" {
		if c.SpikeCycles < 1 || c.SpikeHold < 1 {
			return fmt.Errorf("spike requires spikeCycles>=1 and spikeHold>=1")
		}
	}
	if c.TxTimeout < 1 {
		return fmt.Errorf("txTimeout must be >= 1")
	}
	if c.AbsPath == "" || c.DSN == "" {
		return fmt.Errorf("absPath and dsn are required")
	}
	return nil
}

// ToRunConfig converts to the runtime config used by ramp/worker.
func (c *SessionConfig) ToRunConfig() *config.Config {
	return c.ToRunConfigWithReportEvery(5)
}

// ToRunConfigWithReportEvery converts to runtime config with a report interval.
func (c *SessionConfig) ToRunConfigWithReportEvery(reportEvery int) *config.Config {
	if reportEvery < 1 {
		reportEvery = 5
	}
	return &config.Config{
		DSN:         c.DSN,
		User:        c.User,
		Pass:        c.Pass,
		Profile:     c.Profile,
		ConnInit:    c.ConnMin,
		ConnPeak:    c.ConnMax,
		ConnMin:     c.ConnMin,
		ConnMax:     c.ConnMax,
		Warmup:      c.Warmup,
		Main:        c.Main,
		Cooldown:    c.Cooldown,
		SpikeCycles: c.SpikeCycles,
		SpikeHold:   c.SpikeHold,
		ThinkMs:     c.ThinkMs,
		TxTimeout:   c.TxTimeout,
		Debug:       c.Debug,
		ReportEvery: reportEvery,
	}
}

// OpCount is a named operation counter for snapshots.
type OpCount struct {
	Name  string `json:"name"`
	Count int64  `json:"count"`
}

// Snapshot is a JSON-serializable view of a session for the UI.
type Snapshot struct {
	ID               string    `json:"id"`
	Name             string    `json:"name"`
	RelPath          string    `json:"relPath"`
	AbsPath          string    `json:"absPath"`
	DSN              string    `json:"dsn"`
	Status           Status    `json:"status"`
	Profile          string    `json:"profile"`
	ConnMin          int       `json:"connMin"`
	ConnMax          int       `json:"connMax"`
	Warmup           int       `json:"warmup"`
	Main             int       `json:"main"`
	Cooldown         int       `json:"cooldown"`
	SpikeCycles      int       `json:"spikeCycles"`
	SpikeHold        int       `json:"spikeHold"`
	ThinkMs          int       `json:"thinkMs"`
	TxTimeout        int       `json:"txTimeout"`
	Phase            string    `json:"phase"`
	PhaseProgress    float64   `json:"phaseProgress"`
	TargetConns      int       `json:"targetConns"`
	CurrentConns     int       `json:"currentConns"`
	TPS              float64   `json:"tps"`
	Errors           int64     `json:"errors"`
	ExpectedErrors   int64     `json:"expectedErrors"`
	UnexpectedErrors int64     `json:"unexpectedErrors"`
	Success          int64     `json:"success"`
	LastError        string    `json:"lastError"`
	ElapsedSec       float64   `json:"elapsedSec"`
	TimeLimitMin     int       `json:"timeLimitMin"`
	RemainingSec     float64   `json:"remainingSec"`
	LatencyP50       int64     `json:"latencyP50"`
	LatencyP95       int64     `json:"latencyP95"`
	LatencyP99       int64     `json:"latencyP99"`
	TopOps           []OpCount `json:"topOps,omitempty"`
	ReportDir        string    `json:"reportDir,omitempty"`
	Missing          bool      `json:"missing"`
	UpdatedAt        string    `json:"updatedAt"`
}

// FleetSummary aggregates live metrics across sessions.
type FleetSummary struct {
	Running      int            `json:"running"`
	Paused       int            `json:"paused"`
	Idle         int            `json:"idle"`
	Failed       int            `json:"failed"`
	Completed    int            `json:"completed"`
	Missing      int            `json:"missing"`
	Databases    int            `json:"databases"`
	PerDBMax     int            `json:"perDbMax"`
	TotalConns   int            `json:"totalConns"`
	TotalTPS     float64        `json:"totalTps"`
	BudgetUsed   int            `json:"budgetUsed"`
	BudgetLimit  int            `json:"budgetLimit"`
	TopErrors    map[string]int `json:"topErrors,omitempty"`
}

// EvenPerDBMax returns floor(total/n), at least 1. If n < 1, returns total (or 1).
func EvenPerDBMax(total, n int) int {
	if total < 1 {
		total = 1
	}
	if n < 1 {
		return total
	}
	per := total / n
	if per < 1 {
		return 1
	}
	return per
}

func nowStamp() string {
	return time.Now().Format(time.RFC3339)
}

// PrefsFromConfig extracts persistable prefs from a session config.
func PrefsFromConfig(c SessionConfig) config.SessionPrefs {
	return config.SessionPrefs{
		Profile:     c.Profile,
		ConnMin:     c.ConnMin,
		ConnMax:     c.ConnMax,
		Warmup:      c.Warmup,
		Main:        c.Main,
		Cooldown:    c.Cooldown,
		SpikeCycles: c.SpikeCycles,
		SpikeHold:   c.SpikeHold,
		ThinkMs:     c.ThinkMs,
		TxTimeout:   c.TxTimeout,
	}
}

// ApplyPrefs overlays persisted prefs onto a session config.
func ApplyPrefs(c *SessionConfig, p config.SessionPrefs) {
	if p.Profile != "" {
		c.Profile = p.Profile
	}
	if p.ConnMin > 0 {
		c.ConnMin = p.ConnMin
	}
	if p.ConnMax > 0 {
		c.ConnMax = p.ConnMax
	}
	c.Warmup = p.Warmup
	c.Main = p.Main
	c.Cooldown = p.Cooldown
	if p.SpikeCycles > 0 {
		c.SpikeCycles = p.SpikeCycles
	}
	if p.SpikeHold > 0 {
		c.SpikeHold = p.SpikeHold
	}
	c.ThinkMs = p.ThinkMs
	if p.TxTimeout > 0 {
		c.TxTimeout = p.TxTimeout
	}
}
