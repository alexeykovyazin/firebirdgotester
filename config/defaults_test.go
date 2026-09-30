package config

import (
	"os"
	"testing"
)

// The default run length is 60 minutes of main phase (2026-09-29 follow-up
// plan): a CLI run without --main must resolve to main=3600. Calls the real
// ParseFlags once with no --main on the command line.
func TestDefaultMainIsOneHour(t *testing.T) {
	oldArgs := os.Args
	os.Args = []string{"fb-loadgen", "--profile", "write-heavy"}
	defer func() { os.Args = oldArgs }()

	cfg, err := ParseFlags()
	if err != nil {
		t.Fatalf("ParseFlags: %v", err)
	}
	if cfg.Main != 3600 {
		t.Fatalf("default --main = %d, want 3600 (60 minutes)", cfg.Main)
	}
	if cfg.Warmup != 30 || cfg.Cooldown != 20 {
		t.Fatalf("warmup/cooldown defaults changed: %d/%d, want 30/20", cfg.Warmup, cfg.Cooldown)
	}
}
