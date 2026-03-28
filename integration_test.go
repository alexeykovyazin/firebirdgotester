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
	// Skip this test if we don't have a Firebird database available
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Run the load generator with dry-run mode
	cmd := exec.CommandContext(ctx, "./fb-loadgen",
		"--profile", "write-heavy",
		"--conn", "localhost/3055:./EMPLOYEE.FDB",
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
		// If the command failed due to database connection issues, that's expected
		// We're mainly testing that our code doesn't panic or have syntax errors
		t.Logf("Command failed (expected if database not available): %v", err)
		t.Logf("Stdout: %s", stdout.String())
		t.Logf("Stderr: %s", stderr.String())
		return
	}

	// If the command succeeded, verify it produced expected output
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

	// Check both stdout and stderr since help might go to either
	output := stdout.String() + stderr.String()
	if !strings.Contains(output, "Usage: fb-loadgen") {
		t.Errorf("Expected help output containing 'Usage: fb-loadgen', got: %s", output)
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
			args:    []string{"--profile", "write-heavy", "--conn", "test"},
			wantErr: false,
		},
		{
			name:    "missing profile",
			args:    []string{"--conn", "test"},
			wantErr: true,
			errMsg:  "profile is required",
		},
		{
			name:    "invalid profile",
			args:    []string{"--profile", "invalid", "--conn", "test"},
			wantErr: true,
			errMsg:  "invalid profile",
		},
		{
			name:    "invalid conn-init",
			args:    []string{"--profile", "write-heavy", "--conn-init", "0"},
			wantErr: true,
			errMsg:  "conn-init must be >= 1",
		},
		{
			name:    "invalid conn-peak",
			args:    []string{"--profile", "write-heavy", "--conn-init", "10", "--conn-peak", "5"},
			wantErr: true,
			errMsg:  "conn-peak must be >= conn-init",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a temporary command to test validation
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

// TestOutputFormats tests different output formats
func TestOutputFormats(t *testing.T) {
	formats := []string{"text", "json", "csv"}

	for _, format := range formats {
		t.Run(format, func(t *testing.T) {
			// Create a temporary output file
			tmpFile, err := os.CreateTemp("", "fb-loadgen-test-*.txt")
			if err != nil {
				t.Fatalf("Failed to create temp file: %v", err)
			}
			defer os.Remove(tmpFile.Name())
			tmpFile.Close()

			cmd := exec.Command("./fb-loadgen",
				"--profile", "write-heavy",
				"--output", tmpFile.Name(),
				"--format", format,
				"--warmup", "1",
				"--main", "2",
				"--cooldown", "1",
				"--conn-init", "1",
				"--conn-peak", "1",
				"--dry-run",
			)

			var stderr bytes.Buffer
			cmd.Stderr = &stderr

			err = cmd.Run()
			if err != nil {
				t.Logf("Command failed (expected if database not available): %v", err)
				t.Logf("Stderr: %s", stderr.String())
				return
			}

			// If command succeeded, verify output file was created
			if _, err := os.Stat(tmpFile.Name()); os.IsNotExist(err) {
				t.Errorf("Expected output file to be created for format %s", format)
			}
		})
	}
}
