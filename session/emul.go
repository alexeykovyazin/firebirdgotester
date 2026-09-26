package session

// Emul-specific session wiring: sidecar launch/stop, the frozen final
// report, and small helpers. Extracted from manager.go so the lifecycle
// code lives in one file (see IMPROVEMENTS_PLAN.md R8).

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"fb-loadgen/config"
	"fb-loadgen/emul"
	"fb-loadgen/ramp"
	"fb-loadgen/worker"
)

// launchEmulSidecars starts the shared sidecars (memory monitor, invariant
// checks, score/series ticker) for a running oltp-emul session, on their own
// dedicated pool bound to a context cancelled when the run stops.
func launchEmulSidecars(s *Session, runCfg *config.Config, wMetrics *worker.MetricsCollector, sched phaseSource) {
	s.emulFrozen = nil // a new run supersedes the previous run's frozen report
	emulCtx, emulCancel := context.WithCancel(context.Background())
	pool := emulSidecarPool(runCfg)
	counts := func() (int64, int64, string) {
		ok, failed := wMetrics.Counters()
		return ok, ok + failed, sched.GetCurrentPhase().String()
	}
	s.emulState = emul.RunSidecars(emulCtx, pool,
		time.Duration(runCfg.EmulMonitorEvery)*time.Second,
		time.Duration(runCfg.EmulInvariantEvery)*time.Second,
		10*time.Second,
		counts,
		logf)
	s.emulState.SetWorkingMode(runCfg.EmulWorkingMode)
	s.emulCancel = emulCancel
	s.emulPool = pool // emulStopLocked closes it

	// Extended load mix (T9/H3/H4): limbo recovery plus the periodic heavy
	// SELECT and bulk DML sidecars. The aux schema bootstrap ran earlier,
	// before the workers started (see startInternal), because the scenario
	// picker reads its config once.
	if runCfg.ExtendedLoad.Enabled {
		if runCfg.ExtendedLoad.TxVariants.Completion.Limbo > 0 {
			emul.StartLimboRecovery(emulCtx, runCfg, wMetrics.OpsLog(), emul.Extended())
		}
		if runCfg.ExtendedLoad.HeavySelect.EverySec > 0 || runCfg.ExtendedLoad.BulkDml.EverySec > 0 {
			emul.RunExtendedSidecars(emulCtx, pool, runCfg, wMetrics.OpsLog(), emul.Extended(), s.pauseGate)
		}
		if runCfg.ExtendedLoad.PlusDDL.Enabled {
			emul.StartDDLSidecar(emulCtx, pool, runCfg, wMetrics.OpsLog(), emul.Extended(), s.pauseGate)
		}
	}
}

// bootstrapExtended prepares the extended load mix before the workers start:
// aux schema (EL_BULK_ITEMS, autonomous-tx assets, EL_2PC.FDB) and the
// twoPhase enablement. Runs on a throwaway pool; the pool pinning one
// connection per worker is irrelevant here.
func bootstrapExtended(cfgCopy *config.Config) {
	if !cfgCopy.ExtendedLoad.Enabled {
		return
	}
	el := cfgCopy.ExtendedLoad
	el.Normalize()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := emulSidecarPool(cfgCopy)
	defer pool.Close()
	aux, berr := emul.BootstrapExtendedSchema(ctx, pool, cfgCopy)
	if berr != nil {
		logf("extended load: aux schema bootstrap failed (heavy/bulk/two-phase stay off): %v", berr)
		return
	}
	cfgCopy.ExtendedLoad.TxVariants.TwoPhaseAuxDB = aux
}

// phaseSource is what the sidecars read the current ramp phase from
// (*ramp.Scheduler satisfies it).
type phaseSource interface {
	GetCurrentPhase() ramp.Phase
}

// freezeEmulLocked captures the final per-unit table into s.emulFrozen while
// the metrics collector is still alive; the first freeze wins (later calls
// are no-ops), so a natural completion and a following Stop share one table.
func (s *Session) freezeEmulLocked() {
	if s.emulState == nil || s.emulFrozen != nil {
		return
	}
	var agg map[string]emul.OutcomeStats
	if s.metrics != nil {
		agg = s.metrics.GetUnitStats()
	}
	st := s.emulState.JSON(agg, s.emulUnits)
	s.emulFrozen = &st
}

// metricsTablesLocked captures the transaction-variant and completion-method
// aggregation for the final report. Caller holds s.mu (reads s.metrics).
func (s *Session) metricsTablesLocked() (variants, completions map[string]worker.VariantAgg) {
	if s.metrics == nil {
		return nil, nil
	}
	return s.metrics.GetVariantCounts(), s.metrics.GetCompletionCounts()
}

// writeEmulReportFile writes the frozen final oltp-emul state into the run's
// report directory (results_emul.txt), next to the standard results*.txt
// files. Callers capture the frozen view under the session lock and invoke
// this unlocked — file I/O under s.mu invites deadlocks.
// variantCounts/completionCounts feed the "Transaction variants" and
// "Completion methods" tables (T6); both may be nil.
func writeEmulReportFile(reportDir string, st *emul.EmulStateJSON, sc SessionConfig, variantCounts, completionCounts map[string]worker.VariantAgg) {
	if st == nil || reportDir == "" {
		return
	}
	pct := 0.0
	if st.TotalUnits > 0 {
		pct = 100 * float64(st.OKUnits) / float64(st.TotalUnits)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "=== OLTP-EMUL Final Report ===\n")
	fmt.Fprintf(&b, "Database:            %s\n", sc.DSN)
	fmt.Fprintf(&b, "Working mode:        %s\n", st.WorkingMode)
	fmt.Fprintf(&b, "Performance score:   %.0f successful business actions per minute\n", st.ScorePerMin)
	fmt.Fprintf(&b, "Units OK:            %d/%d (%.1f%%)\n", st.OKUnits, st.TotalUnits, pct)
	fmt.Fprintf(&b, "Memory peaks (MB):   db=%d att=%d trn=%d stmt=%d\n",
		st.MemPeaks.DBBytes/(1<<20), st.MemPeaks.AttBytes/(1<<20),
		st.MemPeaks.TrnBytes/(1<<20), st.MemPeaks.StmtBytes/(1<<20))
	fmt.Fprintf(&b, "Invariants:          %s\n", st.Invariant)
	b.WriteString("\nPer-unit breakdown:\n")
	b.WriteString("  unit                             kind        ok   conflict  rejected  failure  avg ms\n")
	for _, u := range st.PerUnit {
		fmt.Fprintf(&b, "  %-32s %-11s %5d %9d %9d %8d %7d\n",
			u.Unit, u.Kind, u.OK, u.Conflict, u.Rejected, u.Failure, u.AvgMs)
	}
	_ = os.WriteFile(filepath.Join(reportDir, "results_emul.txt"), []byte(b.String()), 0o644)
}

// emulSidecarPool opens a dedicated database pool for the emul sidecars
// (the run pool is capped at one connection per factory and fully held by
// workers).
func emulSidecarPool(cfg *config.Config) *sql.DB {
	pool, err := sql.Open("firebirdsql", cfg.ConnectionString())
	if err != nil {
		return nil
	}
	return pool
}

// logf is the session package's sidecar logger.
func logf(format string, args ...any) {
	log.Printf("[emul] "+format, args...)
}
