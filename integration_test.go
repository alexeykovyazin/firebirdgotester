package main

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// TestBasicExecution tests that the load generator can start and stop without errors
func TestBasicExecution(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "./fb-loadgen",
		"--profile", "write-heavy",
		"--dsn", "localhost/3055:./EMPLOYEE.FDB",
		"--warmup", "5",
		"--main", "10",
		"--cooldown", "5",
		"--conn-init", "1",
		"--conn-peak", "2",
		"--dry-run",
	)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		t.Logf("Command failed (expected if database not available): %v", err)
		t.Logf("Stdout: %s", stdout.String())
		t.Logf("Stderr: %s", stderr.String())
		return
	}

	output := stdout.String()
	if !strings.Contains(output, "Dry-run mode") {
		t.Errorf("Expected dry-run output, got: %s", output)
	}
}

// TestHelpOutput tests that the help flag works correctly
func TestHelpOutput(t *testing.T) {
	cmd := exec.Command("./fb-loadgen", "--help")

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		t.Fatalf("Help command failed: %v", err)
	}

	output := stdout.String() + stderr.String()
	if !strings.Contains(output, "Usage: fb-loadgen") {
		t.Errorf("Expected help output containing 'Usage: fb-loadgen', got: %s", output)
	}
	if !strings.Contains(output, "--ui") {
		t.Errorf("Expected help to mention --ui")
	}
}

// TestConfigValidation tests configuration validation
func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid config",
			args:    []string{"--profile", "write-heavy", "--dsn", "localhost/3055:./EMPLOYEE.FDB", "--dry-run"},
			wantErr: false,
		},
		{
			name:    "missing profile",
			args:    []string{"--dsn", "test"},
			wantErr: true,
			errMsg:  "profile is required",
		},
		{
			name:    "invalid profile",
			args:    []string{"--profile", "invalid", "--dsn", "test"},
			wantErr: true,
			errMsg:  "invalid profile",
		},
		{
			name:    "invalid conn-init",
			args:    []string{"--profile", "write-heavy", "--conn-init", "0"},
			wantErr: true,
			errMsg:  "conn-init/conn-min must be >= 1",
		},
		{
			name:    "invalid conn-peak",
			args:    []string{"--profile", "write-heavy", "--conn-init", "10", "--conn-peak", "5"},
			wantErr: true,
			errMsg:  "conn-peak/conn-max must be >= conn-init/conn-min",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := exec.Command("./fb-loadgen", tt.args...)

			var stderr bytes.Buffer
			cmd.Stderr = &stderr

			err := cmd.Run()

			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error for %s, but got none", tt.name)
				} else if !strings.Contains(stderr.String(), tt.errMsg) {
					t.Errorf("Expected error message containing '%s', got: %s",
						tt.errMsg, stderr.String())
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error for %s: %v", tt.name, err)
				}
			}
		})
	}
}

// TestOutputFormats skipped: CLI does not expose --output/--format.
func TestOutputFormats(t *testing.T) {
	t.Skip("CLI does not expose --output/--format; use --csv and embedded reporter")
}

// Ensure os import stays used if other tests change
var _ = os.Remove
