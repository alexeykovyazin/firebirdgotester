package config

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

// Config holds all CLI configuration
type Config struct {
	// Connection
	DSN  string
	User string
	Pass string

	// Profile
	Profile string

	// Connection scaling
	ConnInit int
	ConnPeak int

	// Timing (all in seconds)
	Warmup   int
	Main     int
	Cooldown int

	// Spike profile extras
	SpikeCycles int
	SpikeHold   int

	// Output
	CSV         string
	ReportEvery int

	// Misc
	ThinkMs   int
	TxTimeout int
	DryRun    bool
}

// ParseFlags parses CLI flags and returns a validated Config
func ParseFlags() (*Config, error) {
	cfg := &Config{}

	// Connection flags
	flag.StringVar(&cfg.DSN, "dsn", "localhost/3055:./EMPLOYEE.FDB", "Firebird DSN")
	flag.StringVar(&cfg.User, "user", "SYSDBA", "DB user")
	flag.StringVar(&cfg.Pass, "pass", "masterkey", "DB password")

	// Profile flags
	flag.StringVar(&cfg.Profile, "profile", "", "Simulation profile: write-heavy | read-heavy | spike (required)")

	// Connection scaling flags
	flag.IntVar(&cfg.ConnInit, "conn-init", 2, "Initial number of connections")
	flag.IntVar(&cfg.ConnPeak, "conn-peak", 20, "Peak number of connections")

	// Timing flags
	flag.IntVar(&cfg.Warmup, "warmup", 30, "Ramp-up / heat period in seconds")
	flag.IntVar(&cfg.Main, "main", 120, "Main steady-state period in seconds")
	flag.IntVar(&cfg.Cooldown, "cooldown", 20, "Graceful disconnect period in seconds")

	// Spike profile extras
	flag.IntVar(&cfg.SpikeCycles, "spike-cycles", 3, "Number of spike cycles during main period")
	flag.IntVar(&cfg.SpikeHold, "spike-hold", 10, "Seconds to sustain peak before dropping")

	// Output flags
	flag.StringVar(&cfg.CSV, "csv", "results.csv", "Path to CSV output file")
	flag.IntVar(&cfg.ReportEvery, "report-every", 5, "Console report interval in seconds")

	// Misc flags
	flag.IntVar(&cfg.ThinkMs, "think-ms", 50, "Worker think time between ops in ms")
	flag.IntVar(&cfg.TxTimeout, "tx-timeout", 10, "Statement timeout in seconds")
	flag.BoolVar(&cfg.DryRun, "dry-run", false, "Connect, list what would run, exit")

	flag.Parse()

	// Validation
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// Validate performs all configuration validation
func (c *Config) Validate() error {
	// Profile validation
	if c.Profile == "" {
		return fmt.Errorf("profile is required (--profile write-heavy|read-heavy|spike)")
	}
	if c.Profile != "write-heavy" && c.Profile != "read-heavy" && c.Profile != "spike" {
		return fmt.Errorf("invalid profile: %s (must be write-heavy, read-heavy, or spike)", c.Profile)
	}

	// Connection validation
	if c.ConnInit < 1 {
		return fmt.Errorf("conn-init must be >= 1, got %d", c.ConnInit)
	}
	if c.ConnPeak < c.ConnInit {
		return fmt.Errorf("conn-peak must be >= conn-init (%d), got %d", c.ConnInit, c.ConnPeak)
	}

	// Timing validation
	if c.Warmup < 0 {
		return fmt.Errorf("warmup must be >= 0, got %d", c.Warmup)
	}
	if c.Main < 0 {
		return fmt.Errorf("main must be >= 0, got %d", c.Main)
	}
	if c.Cooldown < 0 {
		return fmt.Errorf("cooldown must be >= 0, got %d", c.Cooldown)
	}

	// Spike-specific validation
	if c.Profile == "spike" {
		if c.SpikeCycles < 1 {
			return fmt.Errorf("spike-cycles must be >= 1 for spike profile, got %d", c.SpikeCycles)
		}
		if c.SpikeHold < 1 {
			return fmt.Errorf("spike-hold must be >= 1 for spike profile, got %d", c.SpikeHold)
		}
	}

	// Output validation
	if c.ReportEvery < 1 {
		return fmt.Errorf("report-every must be >= 1, got %d", c.ReportEvery)
	}

	// Misc validation
	if c.ThinkMs < 0 {
		return fmt.Errorf("think-ms must be >= 0, got %d", c.ThinkMs)
	}
	if c.TxTimeout < 1 {
		return fmt.Errorf("tx-timeout must be >= 1, got %d", c.TxTimeout)
	}

	return nil
}

// DSNString builds the full DSN string for firebirdsql
func (c *Config) DSNString() string {
	// Format: user:pass@host/port:path
	return fmt.Sprintf("%s:%s@%s", c.User, c.Pass, c.DSN)
}

// String returns a human-readable summary of the configuration
func (c *Config) String() string {
	var sb strings.Builder
	sb.WriteString("Configuration:\n")
	sb.WriteString(fmt.Sprintf("  DSN: %s\n", c.DSN))
	sb.WriteString(fmt.Sprintf("  User: %s\n", c.User))
	sb.WriteString(fmt.Sprintf("  Profile: %s\n", c.Profile))
	sb.WriteString(fmt.Sprintf("  Connections: %d → %d\n", c.ConnInit, c.ConnPeak))
	sb.WriteString(fmt.Sprintf("  Timing: warmup=%ds main=%ds cooldown=%ds\n", c.Warmup, c.Main, c.Cooldown))
	if c.Profile == "spike" {
		sb.WriteString(fmt.Sprintf("  Spike: cycles=%d hold=%ds\n", c.SpikeCycles, c.SpikeHold))
	}
	sb.WriteString(fmt.Sprintf("  Output: csv=%s report-every=%ds\n", c.CSV, c.ReportEvery))
	sb.WriteString(fmt.Sprintf("  Misc: think=%dms tx-timeout=%ds dry-run=%v\n", c.ThinkMs, c.TxTimeout, c.DryRun))
	return sb.String()
}

// PrintUsage prints the help text
func PrintUsage() {
	fmt.Fprintf(os.Stderr, "Usage: fb-loadgen [flags]\n\n")
	fmt.Fprintf(os.Stderr, "Connection:\n")
	fmt.Fprintf(os.Stderr, "  --dsn           string   Firebird DSN (default: \"localhost/3055:./EMPLOYEE.FDB\")\n")
	fmt.Fprintf(os.Stderr, "  --user          string   DB user (default: \"SYSDBA\")\n")
	fmt.Fprintf(os.Stderr, "  --pass          string   DB password (default: \"masterkey\")\n\n")
	fmt.Fprintf(os.Stderr, "Profile:\n")
	fmt.Fprintf(os.Stderr, "  --profile       string   Simulation profile: write-heavy | read-heavy | spike (required)\n\n")
	fmt.Fprintf(os.Stderr, "Connection scaling:\n")
	fmt.Fprintf(os.Stderr, "  --conn-init     int      Initial number of connections (default: 2)\n")
	fmt.Fprintf(os.Stderr, "  --conn-peak     int      Peak number of connections (default: 20)\n\n")
	fmt.Fprintf(os.Stderr, "Timing (all in seconds):\n")
	fmt.Fprintf(os.Stderr, "  --warmup        int      Ramp-up / heat period in seconds (default: 30)\n")
	fmt.Fprintf(os.Stderr, "  --main          int      Main steady-state period in seconds (default: 120)\n")
	fmt.Fprintf(os.Stderr, "  --cooldown      int      Graceful disconnect period in seconds (default: 20)\n\n")
	fmt.Fprintf(os.Stderr, "Spike profile extras:\n")
	fmt.Fprintf(os.Stderr, "  --spike-cycles  int      Number of spike cycles during main period (default: 3)\n")
	fmt.Fprintf(os.Stderr, "  --spike-hold    int      Seconds to sustain peak before dropping (default: 10)\n\n")
	fmt.Fprintf(os.Stderr, "Output:\n")
	fmt.Fprintf(os.Stderr, "  --csv           string   Path to CSV output file (default: \"results.csv\")\n")
	fmt.Fprintf(os.Stderr, "  --report-every  int      Console report interval in seconds (default: 5)\n\n")
	fmt.Fprintf(os.Stderr, "Misc:\n")
	fmt.Fprintf(os.Stderr, "  --think-ms      int      Worker think time between ops in ms (default: 50)\n")
	fmt.Fprintf(os.Stderr, "  --tx-timeout    int      Statement timeout in seconds (default: 10)\n")
	fmt.Fprintf(os.Stderr, "  --dry-run       bool     Connect, list what would run, exit (default: false)\n")
}

// ValidateDryRun performs validation specific to dry-run mode
func (c *Config) ValidateDryRun() error {
	if c.DryRun {
		fmt.Println("Dry-run mode: will connect, load cache, and exit without running load")
		fmt.Println(c.String())
	}
	return nil
}

// GetSpikeInterval returns the interval between spike cycles in seconds
func (c *Config) GetSpikeInterval() int {
	if c.Profile != "spike" || c.SpikeCycles <= 0 {
		return 0
	}
	return c.Main / c.SpikeCycles
}

// GetSpikeDuration returns the total duration of spike phases in seconds
func (c *Config) GetSpikeDuration() int {
	if c.Profile != "spike" {
		return 0
	}
	return c.SpikeCycles * c.SpikeHold
}

// GetBetweenSpikeDuration returns the duration between spike cycles in seconds
func (c *Config) GetBetweenSpikeDuration() int {
	if c.Profile != "spike" {
		return c.Main
	}
	return c.Main - c.GetSpikeDuration()
}

// GetRampRate returns connections per second during warmup
func (c *Config) GetRampRate() float64 {
	if c.Warmup <= 0 {
		return float64(c.ConnPeak)
	}
	return float64(c.ConnPeak-c.ConnInit) / float64(c.Warmup)
}

// GetCooldownRate returns connections per second during cooldown
func (c *Config) GetCooldownRate() float64 {
	if c.Cooldown <= 0 {
		return float64(c.ConnPeak)
	}
	return float64(c.ConnPeak) / float64(c.Cooldown)
}

// GetThinkDuration returns think time as time.Duration
func (c *Config) GetThinkDuration() time.Duration {
	return time.Duration(c.ThinkMs) * time.Millisecond
}

// GetTxTimeout returns transaction timeout as time.Duration
func (c *Config) GetTxTimeout() time.Duration {
	return time.Duration(c.TxTimeout) * time.Second
}
