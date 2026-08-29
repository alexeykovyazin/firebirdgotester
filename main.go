package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"fb-loadgen/config"
	"fb-loadgen/db"
	"fb-loadgen/errlog"
	"fb-loadgen/metrics"
	"fb-loadgen/ops"
	"fb-loadgen/profile"
	"fb-loadgen/ramp"
	"fb-loadgen/schedule"
	"fb-loadgen/session"
	"fb-loadgen/ui"
	"fb-loadgen/worker"
)

func main() {
	if len(os.Args) > 1 && (os.Args[1] == "--help" || os.Args[1] == "-h" || os.Args[1] == "-help") {
		config.PrintUsage()
		os.Exit(0)
	}

	cfg, err := config.ParseFlags()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	if cfg.UI {
		runUI(cfg)
		return
	}

	runCLI(cfg)
}

func runUI(cfg *config.Config) {
	settingsPath := config.DefaultUISettingsFile
	saved, loaded, err := config.LoadUISettings(settingsPath)
	if err != nil {
		log.Printf("Warning: could not load %s: %v", settingsPath, err)
	} else if loaded {
		saved.ApplyTo(cfg)
		fmt.Printf("Loaded UI settings from %s\n", settingsPath)
	} else {
		// Seed file with current defaults so first Save has a target
		_ = config.SaveUISettings(settingsPath, config.UISettingsFromConfig(cfg))
		fmt.Printf("Created default UI settings at %s\n", settingsPath)
	}

	fmt.Println(cfg.String())
	manager := session.NewManager(cfg)
	manager.SetSettingsPath(settingsPath)

	if _, err := manager.Discover("", cfg.DiscoverMask, nil, ""); err != nil {
		log.Printf("Initial discover warning: %v", err)
	}

	// Run history and schedules (API-first control plane).
	if err := manager.SetHistoryPath(cfg.RunsFile); err != nil {
		log.Printf("Warning: could not load run history %s: %v", cfg.RunsFile, err)
	}
	notifier := schedule.NewNotifier(cfg.WebhookSecret)
	hist := manager.History()
	engine := schedule.NewEngine(cfg.SchedulesFile, manager)
	engine.SetNotifier(notifier)
	hist.SetKeepRun(engine.ReferencesRun)
	hist.SetOnFinished(func(run *session.Run) {
		url := cfg.WebhookURL
		if run.ScheduleID != "" {
			if u := engine.NotifyURL(run.ScheduleID); u != "" {
				url = u
			}
		}
		if url != "" {
			notifier.Dispatch(url, map[string]interface{}{"event": "run.finished", "run": run})
		}
	})
	if err := engine.Load(); err != nil {
		log.Printf("Warning: could not load schedules %s: %v", cfg.SchedulesFile, err)
	}
	go engine.Run()
	defer engine.Stop()

	uiSrv := ui.NewWithToken(manager, cfg.UIToken)
	uiSrv.SetScheduleEngine(engine)
	uiSrv.SetAPIOnly(cfg.APIOnly)
	uiSrv.SetAuthAll(cfg.UIAuthAll)
	uiSrv.SetCORSOrigin(cfg.CORSOrigin)
	httpSrv := &http.Server{
		Addr:    cfg.UIAddr,
		Handler: uiSrv.Handler(),
	}

	if cfg.APIOnly {
		fmt.Printf("API server listening on http://%s (UI disabled)\n", normalizeUIAddr(cfg.UIAddr))
	} else {
		go func() {
			fmt.Printf("Web UI listening on http://%s\n", normalizeUIAddr(cfg.UIAddr))
		}()
	}
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("UI server failed: %v", err)
		}
	}()

	sigChan := make(chan os.Signal, 2)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	fmt.Println("\nShutting down UI; stopping active sessions... (Ctrl+C again to force exit)")

	go func() {
		<-sigChan
		fmt.Println("\nForced exit.")
		os.Exit(1)
	}()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
	_ = httpSrv.Shutdown(shutdownCtx)
	shutdownCancel()

	done := make(chan struct{})
	go func() {
		_ = manager.StopAll()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		fmt.Println("Session stop timed out; exiting.")
	}
	fmt.Println("Shutdown complete.")
	os.Exit(0)
}

func normalizeUIAddr(addr string) string {
	if len(addr) > 0 && addr[0] == ':' {
		return "localhost" + addr
	}
	return addr
}

func runCLI(cfg *config.Config) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	connFactory := db.NewConnectionFactory(cfg)

	var cache *ops.Cache
	var err error
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

	readOps := ops.NewReadOperations(connFactory, cache)
	writeOps := ops.NewWriteOperations(connFactory, cache)

	profileFactory := profile.NewProfileFactory(readOps, writeOps, cache)
	prof, err := profileFactory.CreateProfile(cfg.Profile)
	if err != nil {
		log.Fatalf("Failed to create profile: %v", err)
	}

	workerMetrics := worker.NewMetricsCollector()

	errLogPath := cfg.ErrorLog
	if errLogPath == "" {
		errLogPath = errlog.PathFromCSV(cfg.CSV)
	}
	sqlErrLog, err := errlog.Open(errLogPath, cfg.DSN)
	if err != nil {
		log.Printf("Warning: could not open error log %s: %v", errLogPath, err)
	} else {
		workerMetrics.SetErrorLogger(sqlErrLog)
		defer sqlErrLog.Close()
		fmt.Printf("SQL/command errors → %s\n", sqlErrLog.Path())
	}

	scheduler := ramp.NewScheduler(cfg, connFactory, cache, prof, workerMetrics)

	metricsCollector := metrics.NewMetricsCollector(scheduler, prof, cache, workerMetrics)
	metricsCollector.Start()
	defer metricsCollector.Stop()

	var outputFileHandle *os.File
	if cfg.CSV != "" {
		outputFileHandle, err = os.Create(cfg.CSV)
		if err != nil {
			log.Fatalf("Failed to create output file: %v", err)
		}
		defer outputFileHandle.Close()
	}

	reporter := metrics.NewReporter(metricsCollector, outputFileHandle, "text", time.Duration(cfg.ReportEvery)*time.Second)
	reporter.Start()
	defer reporter.Stop()

	if err := scheduler.Start(); err != nil {
		log.Fatalf("Failed to start scheduler: %v", err)
	}

	fmt.Println("Load tester started. Press Ctrl+C to stop.")
	fmt.Printf("Profile: %s\n", cfg.Profile)
	fmt.Printf("Connection: %s\n", cfg.DSN)
	fmt.Printf("Actual connection string: %s\n", cfg.ConnectionString())
	fmt.Printf("Warmup: %ds, Main: %ds, Cooldown: %ds\n", cfg.Warmup, cfg.Main, cfg.Cooldown)
	fmt.Printf("Connections: %d → %d (random walk in main)\n", cfg.ConnMin, cfg.ConnMax)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

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

	select {
	case <-sigChan:
		fmt.Println("\nReceived interrupt signal, initiating graceful shutdown...")
		cancel()
	case <-scheduler.Done():
		fmt.Println("\nScheduler completed all phases, initiating shutdown...")
		cancel()
	}

	go func() {
		<-sigChan
		fmt.Println("\nForced exit.")
		os.Exit(1)
	}()

	fmt.Println("Waiting for active operations to complete...")
	time.Sleep(2 * time.Second)

	fmt.Println("Stopping scheduler...")
	stopDone := make(chan error, 1)
	go func() { stopDone <- scheduler.Stop() }()
	select {
	case err := <-stopDone:
		if err != nil {
			log.Printf("Error during scheduler shutdown: %v", err)
		}
	case <-time.After(30 * time.Second):
		fmt.Println("Scheduler stop timed out; continuing shutdown.")
	}

	fmt.Println("\nGenerating final report...")
	finalReport := reporter.ReportFinal()
	fmt.Print(finalReport)

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
