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
	Host string
	Port int

	// Profile
	Profile string

	// Connection scaling (ConnInit/ConnPeak kept for CLI compat; ConnMin/ConnMax preferred)
	ConnInit int
	ConnPeak int
	ConnMin  int
	ConnMax  int

	// Timing (all in seconds)
	Warmup   int
	Main     int
	Cooldown int

	// UnboundedMain keeps the run in the main phase forever (no cooldown,
	// no natural completion); used by the UI "No limit" mode.
	UnboundedMain bool

	// Spike profile extras
	SpikeCycles int
	SpikeHold   int

	// Output
	CSV         string
	ReportEvery int
	ErrorLog    string

	// Misc
	ThinkMs   int
	TxTimeout int
	DryRun    bool
	Debug     bool

	// Web UI / multi-DB
	UI                 bool
	UIAddr             string
	UIToken            string
	DiscoverDir        string
	DiscoverMask       string
	DiscoverRecursive  bool
	MaxTotalConns      int
}

// ParseFlags parses CLI flags and returns a validated Config
func ParseFlags() (*Config, error) {
	cfg := &Config{}

	flag.StringVar(&cfg.DSN, "dsn", "localhost/3050:./EMPLOYEE.FDB", "Firebird DSN")
	flag.StringVar(&cfg.User, "user", "SYSDBA", "DB user")
	flag.StringVar(&cfg.Pass, "pass", "masterkey", "DB password")
	flag.StringVar(&cfg.Host, "host", "localhost", "Firebird host for discovered databases")
	flag.IntVar(&cfg.Port, "port", 3050, "Firebird port for discovered databases")

	flag.StringVar(&cfg.Profile, "profile", "", "Simulation profile: write-heavy | read-heavy | spike (required unless --ui)")

	flag.IntVar(&cfg.ConnInit, "conn-init", 2, "Initial number of connections (alias for conn-min)")
	flag.IntVar(&cfg.ConnPeak, "conn-peak", 20, "Peak number of connections (alias for conn-max)")
	flag.IntVar(&cfg.ConnMin, "conn-min", 0, "Min connections (0 = use conn-init)")
	flag.IntVar(&cfg.ConnMax, "conn-max", 0, "Max connections (0 = use conn-peak)")

	flag.IntVar(&cfg.Warmup, "warmup", 30, "Ramp-up / heat period in seconds")
	flag.IntVar(&cfg.Main, "main", 120, "Main steady-state period in seconds")
	flag.IntVar(&cfg.Cooldown, "cooldown", 20, "Graceful disconnect period in seconds")

	flag.IntVar(&cfg.SpikeCycles, "spike-cycles", 3, "Number of spike cycles during main period")
	flag.IntVar(&cfg.SpikeHold, "spike-hold", 10, "Seconds to sustain peak before dropping")

	flag.StringVar(&cfg.CSV, "csv", "results.csv", "Path to CSV output file")
	flag.IntVar(&cfg.ReportEvery, "report-every", 5, "Console report interval in seconds")
	flag.StringVar(&cfg.ErrorLog, "error-log", "", "SQL/command error log path (default: <csv>_sql_errors.log or sql_errors.log)")

	flag.IntVar(&cfg.ThinkMs, "think-ms", 50, "Worker think time between ops in ms")
	flag.IntVar(&cfg.TxTimeout, "tx-timeout", 10, "Statement timeout in seconds")
	flag.BoolVar(&cfg.DryRun, "dry-run", false, "Connect, list what would run, exit")
	flag.BoolVar(&cfg.Debug, "debug", false, "Enable debug output for each operation")

	flag.BoolVar(&cfg.UI, "ui", false, "Start web UI control plane")
	flag.StringVar(&cfg.UIAddr, "ui-addr", "127.0.0.1:9000", "Web UI listen address (default localhost only)")
	flag.StringVar(&cfg.UIToken, "ui-token", "", "Optional bearer token required for mutating UI API calls")
	flag.StringVar(&cfg.DiscoverDir, "discover-dir", ".", "Root directory for database discovery")
	flag.StringVar(&cfg.DiscoverMask, "discover-mask", "*.fdb", "Glob mask for database files")
	flag.BoolVar(&cfg.DiscoverRecursive, "discover-recursive", true, "Recursively scan discover-dir")
	flag.IntVar(&cfg.MaxTotalConns, "max-total-conns", 200, "Hard budget for sum of running session conn-max")

	flag.Parse()

	cfg.normalizeConnAliases()

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) normalizeConnAliases() {
	if c.ConnMin <= 0 {
		c.ConnMin = c.ConnInit
	}
	if c.ConnMax <= 0 {
		c.ConnMax = c.ConnPeak
	}
	// Keep init/peak in sync for ramp code that still reads them
	c.ConnInit = c.ConnMin
	c.ConnPeak = c.ConnMax
}

// Validate performs all configuration validation
func (c *Config) Validate() error {
	if c.UI {
		if c.UIAddr == "" {
			return fmt.Errorf("ui-addr is required when --ui is set")
		}
		if c.DiscoverDir == "" {
			return fmt.Errorf("discover-dir is required")
		}
		if c.DiscoverMask == "" {
			return fmt.Errorf("discover-mask is required")
		}
		if c.MaxTotalConns < 1 {
			return fmt.Errorf("max-total-conns must be >= 1")
		}
		if c.Port < 1 {
			return fmt.Errorf("port must be >= 1")
		}
		// Profile optional in UI mode (per-row)
		if c.Profile != "" && c.Profile != "write-heavy" && c.Profile != "read-heavy" && c.Profile != "spike" {
			return fmt.Errorf("invalid profile: %s", c.Profile)
		}
		return c.validateCommon()
	}

	if c.Profile == "" {
		return fmt.Errorf("profile is required (--profile write-heavy|read-heavy|spike)")
	}
	if c.Profile != "write-heavy" && c.Profile != "read-heavy" && c.Profile != "spike" {
		return fmt.Errorf("invalid profile: %s (must be write-heavy, read-heavy, or spike)", c.Profile)
	}

	if c.ConnInit < 1 {
		return fmt.Errorf("conn-init/conn-min must be >= 1, got %d", c.ConnInit)
	}
	if c.ConnPeak < c.ConnInit {
		return fmt.Errorf("conn-peak/conn-max must be >= conn-init/conn-min (%d), got %d", c.ConnInit, c.ConnPeak)
	}

	if c.Profile == "spike" {
		if c.SpikeCycles < 1 {
			return fmt.Errorf("spike-cycles must be >= 1 for spike profile, got %d", c.SpikeCycles)
		}
		if c.SpikeHold < 1 {
			return fmt.Errorf("spike-hold must be >= 1 for spike profile, got %d", c.SpikeHold)
		}
	}

	return c.validateCommon()
}

func (c *Config) validateCommon() error {
	if c.Warmup < 0 {
		return fmt.Errorf("warmup must be >= 0, got %d", c.Warmup)
	}
	if c.Main < 0 {
		return fmt.Errorf("main must be >= 0, got %d", c.Main)
	}
	if c.Cooldown < 0 {
		return fmt.Errorf("cooldown must be >= 0, got %d", c.Cooldown)
	}
	if c.ReportEvery < 1 {
		return fmt.Errorf("report-every must be >= 1, got %d", c.ReportEvery)
	}
	if c.ThinkMs < 0 {
		return fmt.Errorf("think-ms must be >= 0, got %d", c.ThinkMs)
	}
	if c.TxTimeout < 1 {
		return fmt.Errorf("tx-timeout must be >= 1, got %d", c.TxTimeout)
	}
	return nil
}

// ConnectionString builds the complete connection string for the firebirdsql driver.
func (c *Config) ConnectionString() string {
	host, port, database := c.parseDSN(c.DSN)
	return fmt.Sprintf("%s:%s@%s:%d/%s", c.User, c.Pass, host, port, database)
}

func (c *Config) parseDSN(dsn string) (host string, port int, database string) {
	host = "localhost"
	port = 3050
	database = dsn

	if slashIdx := strings.Index(dsn, "/"); slashIdx != -1 {
		beforeSlash := dsn[:slashIdx]
		afterSlash := dsn[slashIdx+1:]

		if colonIdx := strings.Index(beforeSlash, ":"); colonIdx != -1 {
			host = beforeSlash[:colonIdx]
			portStr := beforeSlash[colonIdx+1:]
			database = afterSlash
			if p, err := parsePort(portStr); err == nil {
				port = p
			}
			return
		}

		if colonIdx := strings.Index(afterSlash, ":"); colonIdx != -1 {
			host = beforeSlash
			portStr := afterSlash[:colonIdx]
			database = afterSlash[colonIdx+1:]
			if p, err := parsePort(portStr); err == nil {
				port = p
			}
			return
		}

		host = beforeSlash
		database = afterSlash
		return
	}

	if colonIdx := strings.Index(dsn, ":"); colonIdx != -1 {
		host = dsn[:colonIdx]
		portStr := dsn[colonIdx+1:]
		if p, err := parsePort(portStr); err == nil {
			port = p
		}
		database = ""
		return
	}

	database = dsn
	return
}

func parsePort(s string) (int, error) {
	var port int
	_, err := fmt.Sscanf(s, "%d", &port)
	return port, err
}

// DSNString returns the raw DSN field value
func (c *Config) DSNString() string {
	return c.DSN
}

// String returns a human-readable summary of the configuration
func (c *Config) String() string {
	var sb strings.Builder
	sb.WriteString("Configuration:\n")
	if c.UI {
		sb.WriteString(fmt.Sprintf("  UI: %s\n", c.UIAddr))
		sb.WriteString(fmt.Sprintf("  Discover: dir=%s mask=%s recursive=%v\n", c.DiscoverDir, c.DiscoverMask, c.DiscoverRecursive))
		sb.WriteString(fmt.Sprintf("  Host: %s Port: %d User: %s\n", c.Host, c.Port, c.User))
		sb.WriteString(fmt.Sprintf("  MaxTotalConns: %d\n", c.MaxTotalConns))
		return sb.String()
	}
	sb.WriteString(fmt.Sprintf("  DSN: %s\n", c.DSN))
	sb.WriteString(fmt.Sprintf("  User: %s\n", c.User))
	sb.WriteString(fmt.Sprintf("  Profile: %s\n", c.Profile))
	sb.WriteString(fmt.Sprintf("  Connections: %d → %d\n", c.ConnMin, c.ConnMax))
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
	fmt.Fprintf(os.Stderr, "  --dsn           string   Firebird DSN (default: \"localhost/3050:./EMPLOYEE.FDB\")\n")
	fmt.Fprintf(os.Stderr, "  --user          string   DB user (default: \"SYSDBA\")\n")
	fmt.Fprintf(os.Stderr, "  --pass          string   DB password (default: \"masterkey\")\n")
	fmt.Fprintf(os.Stderr, "  --host          string   Host for discovered DBs (default: localhost)\n")
	fmt.Fprintf(os.Stderr, "  --port          int      Port for discovered DBs (default: 3050)\n\n")
	fmt.Fprintf(os.Stderr, "Profile:\n")
	fmt.Fprintf(os.Stderr, "  --profile       string   write-heavy | read-heavy | spike (required unless --ui)\n\n")
	fmt.Fprintf(os.Stderr, "Connection scaling:\n")
	fmt.Fprintf(os.Stderr, "  --conn-init / --conn-min   int   Min/initial connections (default: 2)\n")
	fmt.Fprintf(os.Stderr, "  --conn-peak / --conn-max   int   Max/peak connections (default: 20)\n\n")
	fmt.Fprintf(os.Stderr, "Timing (all in seconds):\n")
	fmt.Fprintf(os.Stderr, "  --warmup        int      Ramp-up period (default: 30)\n")
	fmt.Fprintf(os.Stderr, "  --main          int      Main period (default: 120)\n")
	fmt.Fprintf(os.Stderr, "  --cooldown      int      Cooldown period (default: 20)\n\n")
	fmt.Fprintf(os.Stderr, "Spike profile extras:\n")
	fmt.Fprintf(os.Stderr, "  --spike-cycles  int      Spike cycles (default: 3)\n")
	fmt.Fprintf(os.Stderr, "  --spike-hold    int      Spike hold seconds (default: 10)\n\n")
	fmt.Fprintf(os.Stderr, "Web UI / multi-DB:\n")
	fmt.Fprintf(os.Stderr, "  --ui                     Start embedded web control plane\n")
	fmt.Fprintf(os.Stderr, "  --ui-addr       string   Listen address (default: 127.0.0.1:9000)\n")
	fmt.Fprintf(os.Stderr, "  --ui-token      string   Optional bearer token for mutating UI APIs\n")
	fmt.Fprintf(os.Stderr, "  --discover-dir  string   Root folder to scan (default: .)\n")
	fmt.Fprintf(os.Stderr, "  --discover-mask string   File mask (default: *.fdb)\n")
	fmt.Fprintf(os.Stderr, "  --discover-recursive     Recurse into subfolders (default: true)\n")
	fmt.Fprintf(os.Stderr, "  --max-total-conns int    Hard budget across running sessions (default: 200)\n\n")
	fmt.Fprintf(os.Stderr, "Output:\n")
	fmt.Fprintf(os.Stderr, "  --csv           string   Path to CSV output file (default: \"results.csv\")\n")
	fmt.Fprintf(os.Stderr, "  --report-every  int      Console report interval in seconds (default: 5)\n")
	fmt.Fprintf(os.Stderr, "  --error-log     string   SQL/command error log file (default: <csv>_sql_errors.log)\n\n")
	fmt.Fprintf(os.Stderr, "Misc:\n")
	fmt.Fprintf(os.Stderr, "  --think-ms      int      Worker think time between ops in ms (default: 50)\n")
	fmt.Fprintf(os.Stderr, "  --tx-timeout    int      Statement timeout in seconds (default: 10)\n")
	fmt.Fprintf(os.Stderr, "  --dry-run       bool     Print config and exit (default: false)\n")
	fmt.Fprintf(os.Stderr, "  --debug         bool     Enable debug output (default: false)\n")
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
	min, max := c.effectiveMinMax()
	if c.Warmup <= 0 {
		return float64(max)
	}
	return float64(max-min) / float64(c.Warmup)
}

// GetCooldownRate returns connections per second during cooldown
func (c *Config) GetCooldownRate() float64 {
	_, max := c.effectiveMinMax()
	if c.Cooldown <= 0 {
		return float64(max)
	}
	return float64(max) / float64(c.Cooldown)
}

func (c *Config) effectiveMinMax() (int, int) {
	min := c.ConnMin
	max := c.ConnMax
	if min <= 0 {
		min = c.ConnInit
	}
	if max <= 0 {
		max = c.ConnPeak
	}
	return min, max
}

// GetThinkDuration returns think time as time.Duration
func (c *Config) GetThinkDuration() time.Duration {
	return time.Duration(c.ThinkMs) * time.Millisecond
}

// GetTxTimeout returns transaction timeout as time.Duration
func (c *Config) GetTxTimeout() time.Duration {
	return time.Duration(c.TxTimeout) * time.Second
}
