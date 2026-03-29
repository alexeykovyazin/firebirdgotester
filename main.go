package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"fb-loadgen/config"
	"fb-loadgen/db"
	"fb-loadgen/metrics"
	"fb-loadgen/ops"
	"fb-loadgen/profile"
	"fb-loadgen/ramp"
	"fb-loadgen/worker"
)

func main() {
	// Handle help flag before parsing
	if len(os.Args) > 1 && (os.Args[1] == "--help" || os.Args[1] == "-h" || os.Args[1] == "-help") {
		config.PrintUsage()
		os.Exit(0)
	}

	// Load configuration (this handles all flag parsing)
	cfg, err := config.ParseFlags()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create connection factory
	connFactory := db.NewConnectionFactory(cfg)

	// Create cache (skip in dry-run mode)
	var cache *ops.Cache
	if !cfg.DryRun {
		cache, err = ops.NewCache(connFactory)
		if err != nil {
			log.Fatalf("Failed to create cache: %v", err)
		}
	} else {
		fmt.Println("Dry-run mode: will connect, load cache, and exit without running load")
		fmt.Println(cfg.String())
		fmt.Printf("Actual connection string: %s\n", cfg.ConnectionString())
		return
	}

	// Create operations
	readOps := ops.NewReadOperations(connFactory, cache)
	writeOps := ops.NewWriteOperations(connFactory, cache)

	// Create profile
	profileFactory := profile.NewProfileFactory(readOps, writeOps, cache)
	profile, err := profileFactory.CreateProfile(cfg.Profile)
	if err != nil {
		log.Fatalf("Failed to create profile: %v", err)
	}

	// Create worker metrics collector
	workerMetrics := worker.NewMetricsCollector()

	// Create scheduler
	scheduler := ramp.NewScheduler(cfg, connFactory, cache, profile, workerMetrics)

	// Create metrics collector with shared workerMetrics
	metricsCollector := metrics.NewMetricsCollector(scheduler, profile, cache, workerMetrics)
	metricsCollector.Start()
	defer metricsCollector.Stop()

	// Create reporter
	var outputFileHandle *os.File
	if cfg.CSV != "" {
		outputFileHandle, err = os.Create(cfg.CSV)
		if err != nil {
			log.Fatalf("Failed to create output file: %v", err)
		}
		defer outputFileHandle.Close()
	}

	reporter := metrics.NewReporter(metricsCollector, outputFileHandle, "text", 5*time.Second)
	reporter.Start()
	defer reporter.Stop()

	// Start scheduler
	if err := scheduler.Start(); err != nil {
		log.Fatalf("Failed to start scheduler: %v", err)
	}

	// Wait for interrupt signal to gracefully shutdown
	fmt.Println("Load tester started. Press Ctrl+C to stop.")
	fmt.Printf("Profile: %s\n", cfg.Profile)
	fmt.Printf("Connection: %s\n", cfg.DSN)
	fmt.Printf("Actual connection string: %s\n", cfg.ConnectionString())
	fmt.Printf("Warmup: %ds, Main: %ds, Cooldown: %ds\n", cfg.Warmup, cfg.Main, cfg.Cooldown)
	fmt.Printf("Initial connections: %d, Peak connections: %d\n", cfg.ConnInit, cfg.ConnPeak)

	// Signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Status reporting goroutine
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				fmt.Printf("Status: %s\n", metricsCollector.GetStats())
			}
		}
	}()

	// Wait for either scheduler completion or interrupt signal
	// Scheduler completes automatically when all phases (warmup + main + cooldown) finish
	select {
	case <-sigChan:
		fmt.Println("\nReceived interrupt signal, initiating graceful shutdown...")
		cancel()
	case <-scheduler.Done():
		fmt.Println("\nScheduler completed all phases, initiating shutdown...")
		cancel()
	}

	// Give operations a chance to complete
	fmt.Println("Waiting for active operations to complete...")
	time.Sleep(2 * time.Second)

	// Stop scheduler if not already stopped
	fmt.Println("Stopping scheduler...")
	if err := scheduler.Stop(); err != nil {
		log.Printf("Error during scheduler shutdown: %v", err)
	}

	// Generate final report
	fmt.Println("\nGenerating final report...")
	finalReport := reporter.ReportFinal()
	fmt.Print(finalReport)

	// Generate additional reports if requested
	if cfg.CSV != "" {
		baseFilename := cfg.CSV
		if len(baseFilename) > 4 && baseFilename[len(baseFilename)-4:] == ".txt" {
			baseFilename = baseFilename[:len(baseFilename)-4]
		}

		fmt.Printf("Generating detailed reports to %s_*.txt...\n", baseFilename)
		if err := reporter.ReportAllToFile(baseFilename); err != nil {
			log.Printf("Error generating detailed reports: %v", err)
		}
	}

	fmt.Println("Shutdown complete.")
}
