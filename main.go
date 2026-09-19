package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"fb-loadgen/config"
	"fb-loadgen/db"
	"fb-loadgen/emul"
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
	if len(os.Args) > 1 && os.Args[1] == "provision" {
		runProvision(os.Args[2:])
		return
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

// runProvision implements `fb-loadgen provision ...`: create and populate an
// oltpemul database (see OLTP_EMUL_PLAN.md). Flags mirror the run flags so
// the same DSN can be reused verbatim.
func runProvision(args []string) {
	fs := flag.NewFlagSet("provision", flag.ExitOnError)
	dsn := fs.String("dsn", "localhost/3050:C:\\data\\oltpemul.fdb", "DSN of the database to create (host[/port]:server-side-path)")
	user := fs.String("user", "SYSDBA", "DB user")
	pass := fs.String("pass", "masterkey", "DB password")
	pageSize := fs.Int("page-size", 8192, "Database page size (oltp-emul standard: 8192)")
	initDocs := fs.Int("init-docs", 3000, "Documents to create before the run (start small; 3000-5000 for a first test)")
	workingMode := fs.String("working-mode", "SMALL_01", "Workload profile: DEBUG_01..04, DEBUG_1A, SMALL_01..03, MEDIUM_01..03, LARGE_01..03, HEAVY_01 (sets wares count, doc sizes, agent count)")
	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	host, port, dbPath := config.ParseDSN(*dsn)
	cfg := emul.Config{
		Host:        host,
		Port:        fmt.Sprintf("%d", port),
		User:        *user,
		Password:    *pass,
		DBPath:      dbPath,
		PageSize:    *pageSize,
		WorkingMode: *workingMode,
	}
	fmt.Printf("Provisioning oltpemul database at %s:%s/%s (page size %d)\n", host, cfg.Port, dbPath, *pageSize)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT)
		<-sig
		fmt.Println("\nProvision cancelled.")
		cancel()
	}()

	if err := emul.Provision(ctx, cfg, func(f string, a ...any) { fmt.Println("  " + fmt.Sprintf(f, a...)) }); err != nil {
		log.Fatalf("Provision failed: %v", err)
	}
	if *initDocs > 0 {
		db, err := sql.Open("firebirdsql", cfg.DSN())
		if err != nil {
			log.Fatalf("Connect failed: %v", err)
		}
		defer db.Close()
		if err := emul.Fill(ctx, db, *initDocs, func(f string, a ...any) { fmt.Println("  " + fmt.Sprintf(f, a...)) }); err != nil {
			log.Fatalf("Fill failed: %v", err)
		}
	}
	fmt.Println("Provision complete. Run with: --profile oltp-emul --dsn " + *dsn)
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
	if len(saved.EmulSessions) > 0 {
		manager.RestoreEmulSessions(saved.EmulSessions)
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

// startEmulSidecars runs the shared emul.RunSidecars helper for CLI runs:
// memory monitor, invariant checks, and the score/series ticker, all bound
// to the run context. Returns the live state for the end-of-run summary.
func startEmulSidecars(ctx context.Context, cfg *config.Config, sched *ramp.Scheduler, wm *worker.MetricsCollector, emulDB *sql.DB) *emul.EmulState {
	counts := func() (int64, int64, string) {
		ok, failed := wm.Counters()
		return ok, ok + failed, sched.GetCurrentPhase().String()
	}
	state := emul.RunSidecars(ctx, emulDB,
		time.Duration(cfg.EmulMonitorEvery)*time.Second,
		time.Duration(cfg.EmulInvariantEvery)*time.Second,
		10*time.Second,
		counts,
		func(format string, args ...any) { fmt.Printf(format+"\n", args...) })

	go func() {
		<-ctx.Done()
		score, ok, total, peaks, invariant, wm2 := state.Final()
		fmt.Printf("[emul] final: score=%.0f ops/min ok=%d/%d invariants=%s workingMode=%s\n",
			score, ok, total, invariant, wm2)
		fmt.Printf("[emul] memory peaks: db=%dMB att=%dMB trn=%dMB stmt=%dMB\n",
			peaks.DBBytes/(1<<20), peaks.AttBytes/(1<<20), peaks.TrnBytes/(1<<20), peaks.StmtBytes/(1<<20))
	}()
	return state
}

func runCLI(cfg *config.Config) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	connFactory := db.NewConnectionFactory(cfg)

	var cache *ops.Cache
	var emulDB *sql.DB
	var err error
	if !cfg.DryRun {
		if cfg.Profile == "oltp-emul" {
			// The oltpemul schema replaces the EMPLOYEE one: no key cache.
			emulDB, err = connFactory.Open()
			if err != nil {
				log.Fatalf("Failed to connect to oltpemul database: %v", err)
			}
			defer emulDB.Close()
			if err := emul.SchemaGuard(ctx, emulDB); err != nil {
				log.Fatalf("%v", err)
			}
		} else {
			cache, err = ops.NewCache(connFactory)
			if err != nil {
				log.Fatalf("Failed to create cache: %v", err)
			}
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
	if emulDB != nil {
		units, err := emul.LoadUnits(ctx, emulDB)
		if err != nil {
			log.Fatalf("%v", err)
		}
		profileFactory.SetEmulUnits(units)
		fmt.Printf("oltp-emul: %d units loaded from business_ops\n", len(units))
	}
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

	if emulDB != nil {
		startEmulSidecars(ctx, cfg, scheduler, workerMetrics, emulDB)
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
