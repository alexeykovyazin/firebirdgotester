// Package summary assembles the detailed final-run summary printed at
// shutdown and served by the UI summary endpoint. Build only reads the live
// in-memory structures (collector, scheduler, worker metrics, emul state) —
// no database access — so it is safe to call while workers are still
// stopping or from a UI request mid-run (values then simply reflect the
// current state).
package summary

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"fb-loadgen/config"
	"fb-loadgen/emul"
	"fb-loadgen/metrics"
	"fb-loadgen/ramp"
	"fb-loadgen/worker"
)

// maxTopErrors and maxTopUnits bound the text rendering; the JSON carries
// the same caps to keep the endpoint payload small.
const (
	maxTopErrors = 30
	maxTopUnits  = 20
)

// Builder holds the sources of a run's summary. All fields optional except
// Collector and WM: nil Emul just omits the emul block.
type Builder struct {
	Collector *metrics.MetricsCollector
	Sched     *ramp.Scheduler
	WM        *worker.MetricsCollector
	Emul      *emul.EmulState // nil for non-emul profiles
	Cfg       *config.Config
	Version   string
}

// Summary is the full JSON projection served by the UI and rendered to text
// for the console and <csv>_final_summary.txt.
type Summary struct {
	GeneratedAt time.Time `json:"generatedAt"`
	Version     string    `json:"version,omitempty"`

	Config      ConfigEcho                   `json:"config"`
	Totals      Totals                       `json:"totals"`
	Latency     LatencyBlock                 `json:"latency"`
	Concurrency Concurrency                  `json:"concurrency"`
	Pool        PoolStats                    `json:"pool"`
	Phases      []ramp.PhaseSpan             `json:"phases,omitempty"`
	Teardown    *ramp.TeardownStats          `json:"teardown,omitempty"`
	Errors      ErrorsBlock                  `json:"errors"`
	Units       []UnitRow                    `json:"units"`
	Variants    map[string]worker.VariantAgg `json:"variants,omitempty"`
	Completions map[string]worker.VariantAgg `json:"completions,omitempty"`
	Extended    *emul.ExtendedJSON           `json:"extended,omitempty"`
	Emul        *EmulBlock                   `json:"emul,omitempty"`
	Timeline    []metrics.MinutePoint        `json:"timeline"`
}

// ConfigEcho echoes the run configuration with the password redacted.
type ConfigEcho struct {
	DSN             string `json:"dsn"`
	Profile         string `json:"profile"`
	ConnMin         int    `json:"connMin"`
	ConnMax         int    `json:"connMax"`
	TxTimeoutSec    int    `json:"txTimeoutSec"`
	ExtendedLoad    bool   `json:"extendedLoad"`
	NoLimbo         bool   `json:"noLimbo"`
	CancelHardDrop  bool   `json:"cancelHardDrop"`
	HardDropGraceMs int    `json:"hardDropGraceMs"`
}

// Totals are the headline counters of the run.
type Totals struct {
	Elapsed     time.Duration `json:"elapsed"`
	Total       int64         `json:"total"`
	Success     int64         `json:"success"`
	Errors      int64         `json:"errors"`
	SuccessPct  float64       `json:"successPct"`
	TPS         float64       `json:"tps"`
	ScorePerMin float64       `json:"scorePerMin,omitempty"` // emul main-phase score only
}

// LatencyBlock is the aggregate latency view (histogram included).
type LatencyBlock struct {
	Avg     time.Duration `json:"avg"`
	Min     time.Duration `json:"min"`
	Max     time.Duration `json:"max"`
	P50     time.Duration `json:"p50"`
	P95     time.Duration `json:"p95"`
	P99     time.Duration `json:"p99"`
	Buckets [14]int64     `json:"buckets"`
	Labels  [14]string    `json:"labels"`
}

// Concurrency is the bookkeeping worker view (pool sockets live in Pool).
type Concurrency struct {
	WorkersCur int `json:"workersCur"`
	WorkersMax int `json:"workersMax"`
}

// PoolStats covers the real socket level and in-place rebuilds.
type PoolStats struct {
	ConnsCur       int   `json:"connsCur"`
	ConnsMin       int   `json:"connsMin"`
	ConnsMax       int   `json:"connsMax"`
	RebuildsOK     int64 `json:"rebuildsOk"`
	RebuildsFailed int64 `json:"rebuildsFailed"`
}

// ErrorsBlock is the taxonomy plus the top distinct messages.
type ErrorsBlock struct {
	Total      int64                 `json:"total"`
	Expected   int64                 `json:"expected"`
	Unexpected int64                 `json:"unexpected"`
	Retryable  int64                 `json:"retryable"`
	Kinds      map[string]int64      `json:"kinds"`
	Top        []worker.TopErrorText `json:"top"`
}

// UnitRow is one oltp-emul business unit's outcome line.
type UnitRow struct {
	Unit     string `json:"unit"`
	Attempts int64  `json:"attempts"`
	OK       int64  `json:"ok"`
	Conflict int64  `json:"conflict"`
	Rejected int64  `json:"rejected"`
	Failure  int64  `json:"failure"`
	AvgMs    int64  `json:"avgMs"`
	MaxMs    int64  `json:"maxMs"`
}

// EmulBlock carries the oltp-emul end-of-run state.
type EmulBlock struct {
	OKUnits        int64            `json:"okUnits"`
	TotalUnits     int64            `json:"totalUnits"`
	Invariant      string           `json:"invariant"`
	InvariantStats emul.InvCounters `json:"invariantStats"`
	WorkingMode    string           `json:"workingMode,omitempty"`
	MemPeaks       emul.Sample      `json:"memPeaks"`
}

// Build assembles the summary. Never fails: missing sources render as zero
// values / omitted sections.
func (b *Builder) Build() *Summary {
	s := &Summary{GeneratedAt: time.Now(), Version: b.Version}

	if b.Cfg != nil {
		s.Config = ConfigEcho{
			DSN:             b.Cfg.RedactedConnectionString(),
			Profile:         b.Cfg.Profile,
			ConnMin:         b.Cfg.ConnMin,
			ConnMax:         b.Cfg.ConnMax,
			TxTimeoutSec:    b.Cfg.TxTimeout,
			ExtendedLoad:    b.Cfg.ExtendedLoad.Enabled,
			NoLimbo:         b.Cfg.ExtendedLoad.TxVariants.NoLimbo,
			CancelHardDrop:  b.Cfg.CancelHardDrop,
			HardDropGraceMs: b.Cfg.CancelHardDropGraceMs,
		}
	}

	if b.Collector != nil {
		rep := b.Collector.GetReport()
		s.Totals = Totals{
			Elapsed:    rep.Elapsed,
			Total:      rep.TotalOperations,
			Success:    rep.SuccessOperations,
			Errors:     rep.ErrorOperations,
			SuccessPct: rep.SuccessRate,
			TPS:        sanitized(rep.TotalTPS),
		}
		s.Latency = LatencyBlock{
			Avg: rep.AverageLatency, Min: rep.MinLatency, Max: rep.MaxLatency,
			P50: rep.P50Latency, P95: rep.P95Latency, P99: rep.P99Latency,
			Buckets: rep.LatencyBuckets,
			Labels:  worker.LatencyBucketLabels,
		}
		s.Concurrency = Concurrency{WorkersCur: rep.CurrentWorkers, WorkersMax: rep.MaxWorkers}
		cMin, cMax := b.Collector.MinMaxOpenConns()
		s.Pool = PoolStats{ConnsCur: rep.OpenConnections, ConnsMin: cMin, ConnsMax: cMax}
		s.Timeline = b.Collector.MinuteTimeline()
	}

	if b.Sched != nil {
		s.Phases = b.Sched.PhaseSpans()
		td := b.Sched.Teardown()
		s.Teardown = &td
	}

	rbOK, rbFail := worker.RebuildCounts()
	s.Pool.RebuildsOK, s.Pool.RebuildsFailed = rbOK, rbFail

	if b.WM != nil {
		if es := b.WM.ErrorStats(); es != nil {
			total, expected, unexpected, retryable, kinds := es.Snapshot()
			kindMap := make(map[string]int64, len(kinds))
			for k, v := range kinds {
				kindMap[k] = int64(v)
			}
			s.Errors = ErrorsBlock{
				Total:      int64(total),
				Expected:   int64(expected),
				Unexpected: int64(unexpected),
				Retryable:  int64(retryable),
				Kinds:      kindMap,
				Top:        b.WM.TopErrorTexts(maxTopErrors),
			}
		}
		s.Units = unitRows(b.WM.GetUnitStats())
		s.Variants = b.WM.GetVariantCounts()
		s.Completions = b.WM.GetCompletionCounts()
	}

	s.Extended = emul.Extended().SnapshotJSON()

	if b.Emul != nil {
		score, ok, total, peaks, invariant, workingMode := b.Emul.Final()
		s.Totals.ScorePerMin = sanitized(score)
		s.Emul = &EmulBlock{
			OKUnits:        ok,
			TotalUnits:     total,
			Invariant:      invariant,
			InvariantStats: b.Emul.InvariantStats(),
			WorkingMode:    workingMode,
			MemPeaks:       peaks,
		}
	}

	return s
}

// unitRows converts the per-unit aggregation into rows sorted by failure
// count (worst first), then by attempts; capped at maxTopUnits.
func unitRows(agg map[string]emul.OutcomeStats) []UnitRow {
	rows := make([]UnitRow, 0, len(agg))
	for name, st := range agg {
		r := UnitRow{
			Unit: name, Attempts: st.N,
			OK: st.OK, Conflict: st.Conflict, Rejected: st.Rejected, Failure: st.Failure,
		}
		if st.N > 0 {
			r.AvgMs = st.LatSumMs / st.N
			r.MaxMs = st.MaxMs
		}
		rows = append(rows, r)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Failure != rows[j].Failure {
			return rows[i].Failure > rows[j].Failure
		}
		if rows[i].Attempts != rows[j].Attempts {
			return rows[i].Attempts > rows[j].Attempts
		}
		return rows[i].Unit < rows[j].Unit
	})
	if len(rows) > maxTopUnits {
		rows = rows[:maxTopUnits]
	}
	return rows
}

// Text renders the summary for the console log and <csv>_final_summary.txt.
func (s *Summary) Text() string {
	var sb strings.Builder
	w := func(format string, args ...any) { fmt.Fprintf(&sb, format+"\n", args...) }

	w("=== FINAL DETAILED SUMMARY ===")
	w("Generated: %s  Version: %s", s.GeneratedAt.Format(time.RFC3339), orDash(s.Version))
	w("Config: dsn=%s profile=%s conns=%d..%d tx-timeout=%ds extended=%v noLimbo=%v hardDrop=%v(%dms)",
		s.Config.DSN, s.Config.Profile, s.Config.ConnMin, s.Config.ConnMax, s.Config.TxTimeoutSec,
		s.Config.ExtendedLoad, s.Config.NoLimbo, s.Config.CancelHardDrop, s.Config.HardDropGraceMs)

	w("")
	w("Totals:")
	w("  Elapsed: %v  Units: %d total, %d OK (%.1f%%), %d err  TPS: %.1f",
		s.Totals.Elapsed, s.Totals.Total, s.Totals.Success, s.Totals.SuccessPct, s.Totals.Errors, s.Totals.TPS)
	if s.Totals.ScorePerMin > 0 || s.Emul != nil {
		w("  Score (main phase): %.0f ops/min", s.Totals.ScorePerMin)
	}

	w("")
	w("Latency:")
	w("  avg=%v min=%v max=%v p50=%v p95=%v p99=%v",
		s.Latency.Avg, s.Latency.Min, s.Latency.Max, s.Latency.P50, s.Latency.P95, s.Latency.P99)
	for i, n := range s.Latency.Buckets {
		if n > 0 {
			w("  %s: %d", s.Latency.Labels[i], n)
		}
	}

	w("")
	w("Concurrency:")
	w("  Workers cur/max: %d/%d  Conns cur/min/max: %d/%d/%d  Pool rebuilds ok/failed: %d/%d",
		s.Concurrency.WorkersCur, s.Concurrency.WorkersMax,
		s.Pool.ConnsCur, s.Pool.ConnsMin, s.Pool.ConnsMax,
		s.Pool.RebuildsOK, s.Pool.RebuildsFailed)

	if len(s.Phases) > 0 {
		w("")
		w("Phases (actual):")
		for _, p := range s.Phases {
			w("  %-8s %s -> %s (%v)", p.Phase, p.Start.Format("15:04:05"), p.End.Format("15:04:05"), p.End.Sub(p.Start))
		}
	}
	if s.Teardown != nil {
		w("")
		w("Teardown:")
		w("  stopFailures=%d reaped=%d drain=%v", s.Teardown.StopFailures, s.Teardown.Reaped, s.Teardown.DrainDuration)
	}

	w("")
	w("Errors:")
	w("  taxonomy: total=%d expected=%d UNEXPECTED=%d retryable=%d",
		s.Errors.Total, s.Errors.Expected, s.Errors.Unexpected, s.Errors.Retryable)
	if len(s.Errors.Kinds) > 0 {
		kinds := make([]string, 0, len(s.Errors.Kinds))
		for k := range s.Errors.Kinds {
			kinds = append(kinds, k)
		}
		sort.Strings(kinds)
		for _, k := range kinds {
			w("  kind %-24s %d", k+":", s.Errors.Kinds[k])
		}
	}
	if len(s.Errors.Top) > 0 {
		w("  top messages:")
		for _, e := range s.Errors.Top {
			w("  %6dx %s", e.Count, oneLine(e.Message))
			w("         first=%s last=%s", e.FirstSeen.Format("15:04:05"), e.LastSeen.Format("15:04:05"))
		}
	}

	if len(s.Units) > 0 {
		w("")
		w("Units (top %d by failure):", maxTopUnits)
		w("  %-32s %10s %8s %9s %9s %8s %8s %8s", "unit", "attempts", "ok", "conflict", "rejected", "failure", "avgMs", "maxMs")
		for _, u := range s.Units {
			w("  %-32s %10d %8d %9d %9d %8d %8d %8d",
				u.Unit, u.Attempts, u.OK, u.Conflict, u.Rejected, u.Failure, u.AvgMs, u.MaxMs)
		}
	}

	if len(s.Variants) > 0 || len(s.Completions) > 0 {
		w("")
		if len(s.Variants) > 0 {
			w("Transaction variants:")
			for _, v := range sortedAgg(s.Variants) {
				w("  %-46s attempts=%d ok=%d", v.Name, v.Agg.Attempts, v.Agg.OK)
			}
		}
		if len(s.Completions) > 0 {
			w("Completion methods:")
			for _, v := range sortedAgg(s.Completions) {
				w("  %-46s attempts=%d ok=%d", v.Name, v.Agg.Attempts, v.Agg.OK)
			}
		}
	}

	if s.Extended != nil {
		w("")
		w("Extended load:")
		w("  heavy rounds=%d (fail: %d), bulk ins/upd/del=%d/%d/%d rows=%d (fail: %d)",
			s.Extended.HeavyRounds, s.Extended.HeavyFailures,
			s.Extended.BulkInserts, s.Extended.BulkUpdates, s.Extended.BulkDeletes,
			s.Extended.BulkRows, s.Extended.BulkFailures)
		w("  ddl cols +/~/-=%d/%d/%d tables=%d/%d",
			s.Extended.ColumnsAdded, s.Extended.ColumnsAltered, s.Extended.ColumnsDropped,
			s.Extended.TablesCreated, s.Extended.TablesDropped)
		w("  limbo: resolved=%d (commit=%d rollback=%d two_phase=%d) peakUnresolved=%d maxAgeSec=%d",
			s.Extended.LimboResolved, s.Extended.LimboCommit, s.Extended.LimboRollback,
			s.Extended.LimboTwoPhase, s.Extended.LimboPeak, s.Extended.LimboMaxAgeSec)
		if s.Extended.TypeAlterOK > 0 || s.Extended.TypeAlterExpectedFail > 0 ||
			s.Extended.ProcAlterOK > 0 || s.Extended.ProcAlterExpectedFail > 0 ||
			s.Extended.ProcCallOK > 0 || s.Extended.ProcCallRaceErr > 0 {
			w("  ddl2: type alters=%d (expected fail %d), proc sig alters=%d (expected fail %d), victim calls=%d (race err %d)",
				s.Extended.TypeAlterOK, s.Extended.TypeAlterExpectedFail,
				s.Extended.ProcAlterOK, s.Extended.ProcAlterExpectedFail,
				s.Extended.ProcCallOK, s.Extended.ProcCallRaceErr)
		}
	}

	if s.Emul != nil {
		w("")
		w("Emul:")
		w("  units ok=%d/%d  score=%.0f/min  workingMode=%s",
			s.Emul.OKUnits, s.Emul.TotalUnits, s.Totals.ScorePerMin, orDash(s.Emul.WorkingMode))
		w("  invariants: verdict=%s disabled=%v checks=%d ok=%d transient=%d hard=%d",
			orDash(s.Emul.Invariant), s.Emul.InvariantStats.Disabled,
			s.Emul.InvariantStats.Checks, s.Emul.InvariantStats.OK,
			s.Emul.InvariantStats.Transient, s.Emul.InvariantStats.Hard)
		if s.Emul.InvariantStats.LastReason != "" {
			w("  invariants last reason: %s", oneLine(s.Emul.InvariantStats.LastReason))
		}
		w("  memory peaks: db=%dMB att=%dMB trn=%dMB stmt=%dMB",
			s.Emul.MemPeaks.DBBytes/(1<<20), s.Emul.MemPeaks.AttBytes/(1<<20),
			s.Emul.MemPeaks.TrnBytes/(1<<20), s.Emul.MemPeaks.StmtBytes/(1<<20))
	}

	if len(s.Timeline) > 0 {
		w("")
		w("Timeline (per-minute OK/err, avg ms):")
		for _, p := range s.Timeline {
			avg := int64(0)
			if p.N > 0 {
				avg = p.LatSumMs / p.N
			}
			w("  m%-3d ok=%-6d err=%-6d avg=%dms", p.Minute, p.OK, p.Err, avg)
		}
	}

	return sb.String()
}

// sanitized replaces NaN/Inf with 0: encoding/json refuses such floats and a
// zero-length run divides by zero elapsed.
func sanitized(f float64) float64 {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0
	}
	return f
}

func sortedAgg(m map[string]worker.VariantAgg) []struct {
	Name string
	Agg  worker.VariantAgg
} {
	out := make([]struct {
		Name string
		Agg  worker.VariantAgg
	}, 0, len(m))
	for k, v := range m {
		out = append(out, struct {
			Name string
			Agg  worker.VariantAgg
		}{k, v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// oneLine flattens an error message for the summary listing.
func oneLine(s string) string {
	s = strings.ReplaceAll(s, "\r\n", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) > 200 {
		s = s[:200] + "…"
	}
	return s
}
