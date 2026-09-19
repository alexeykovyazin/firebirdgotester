package emul

import (
	"context"
	"database/sql"
	"sort"
	"strings"
	"sync"
	"time"
)

// SeriesPoint is one live-chart sample: score and tps over the preceding
// interval plus the database-level memory at sampling time.
type SeriesPoint struct {
	TS          time.Time `json:"ts"`
	ScorePerMin float64   `json:"scorePerMin"`
	TPS         float64   `json:"tps"`
	DBBytes     int64     `json:"dbBytes"`
	Phase       string    `json:"phase"`
}

// UnitStat is the per-unit outcome/latency aggregation for one run.
type UnitStat struct {
	Unit     string `json:"unit"`
	Mode     string `json:"mode"`
	Kind     string `json:"kind"`
	OK       int64  `json:"ok"`
	Conflict int64  `json:"conflict"`
	Rejected int64  `json:"rejected"`
	Failure  int64  `json:"failure"`
	AvgMs    int64  `json:"avgMs"`
	MaxMs    int64  `json:"maxMs"`
}

// OutcomeStats is the raw per-unit aggregation recorded by the worker
// metrics collector (before the mode/kind registry merge).
type OutcomeStats struct {
	OK       int64 `json:"ok"`
	Conflict int64 `json:"conflict"`
	Rejected int64 `json:"rejected"`
	Failure  int64 `json:"failure"`
	LatSumMs int64 `json:"-"`
	MaxMs    int64 `json:"-"`
	N        int64 `json:"-"`
}

// EmulState is the live oltp-emul run state shared between the sidecars
// (monitor, invariants, series ticker) and the readers (Snapshot, state
// endpoint, final report). All methods are goroutine-safe.
type EmulState struct {
	mu sync.Mutex

	scorePerMin float64
	okUnits     int64
	totalUnits  int64
	phase       string

	invariant   string
	invariantAt time.Time

	memPeaks   Sample
	memSamples []Sample
	series     []SeriesPoint

	workingMode string

	startedAt time.Time
}

const seriesCap = 120 // 20 min at 10 s intervals; the final report has the full run

func (s *EmulState) observeMem(sm Sample) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.memPeaks.DBBytes < sm.DBBytes {
		s.memPeaks.DBBytes = sm.DBBytes
	}
	if s.memPeaks.AttBytes < sm.AttBytes {
		s.memPeaks.AttBytes = sm.AttBytes
	}
	if s.memPeaks.TrnBytes < sm.TrnBytes {
		s.memPeaks.TrnBytes = sm.TrnBytes
	}
	if s.memPeaks.StmtBytes < sm.StmtBytes {
		s.memPeaks.StmtBytes = sm.StmtBytes
	}
	s.memSamples = append(s.memSamples, sm)
	if len(s.memSamples) > seriesCap {
		s.memSamples = s.memSamples[len(s.memSamples)-seriesCap:]
	}
}

func (s *EmulState) observeSeries(p SeriesPoint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scorePerMin = p.ScorePerMin
	s.phase = p.Phase
	s.series = append(s.series, p)
	if len(s.series) > seriesCap {
		s.series = s.series[len(s.series)-seriesCap:]
	}
}

func (s *EmulState) setCounts(ok, total int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.okUnits, s.totalUnits = ok, total
}

// SetInvariant records the latest invariant check result.
func (s *EmulState) SetInvariant(status string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.invariant = status
	s.invariantAt = time.Now()
}

// SetWorkingMode records the working-mode echo (for reports).
func (s *EmulState) SetWorkingMode(wm string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workingMode = wm
}

// CountsSource returns the cumulative unit counters and the current ramp
// phase. Implemented over the worker metrics collector + scheduler.
type CountsSource func() (okUnits, totalUnits int64, phase string)

// RunSidecars launches the memory monitor, the invariant check loop, and
// the score/series ticker for the duration of ctx. It is the single
// implementation shared by the CLI (runCLI) and the session engine; both
// pass their own db pool (NOT the worker pool) and CountsSource.
// Returns the live state; read it after ctx is done for the final values.
func RunSidecars(ctx context.Context, db *sql.DB, monitorEvery, invariantEvery, seriesEvery time.Duration,
	counts CountsSource, logf func(format string, args ...any)) *EmulState {

	state := &EmulState{startedAt: time.Now()}
	if logf == nil {
		logf = func(string, ...any) {}
	}

	// Memory monitor (dedicated pool is the caller's responsibility).
	if monitorEvery > 0 && db != nil {
		mon := NewMonitor(monitorEvery)
		mon.SetErrorHandler(func(err error) {
			logf("[emul-mon] sampling stopped: %v", err)
		})
		go func() {
			_ = mon.Run(ctx, db, func(sm Sample) {
				state.observeMem(sm)
			})
		}()
	}

	// Invariant self-checks.
	if invariantEvery > 0 && db != nil {
		go func() {
			ticker := time.NewTicker(invariantEvery)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					tx, err := db.BeginTx(ctx, TxOptions())
					if err != nil {
						continue // transient (shutdown) — next tick retries
					}
					if err := CheckInvariants(ctx, tx); err != nil {
						_ = tx.Rollback()
						msg := "failed: " + err.Error()
						if containsAny(err.Error(),
							"EX_SNAPSHOT_ISOLATION_REQUIRED", "EX_NOWAIT_OR_TIMEOUT_REQUIRED") {
							msg = "disabled: server requires a snapshot+nowait transaction " +
								"(driver limitation on this engine)"
						}
						state.SetInvariant(msg)
						logf("[emul-inv] %s", msg)
						if msg != "ok" && containsAny(msg, "disabled:") {
							return
						}
						continue
					}
					if err := tx.Commit(); err != nil {
						continue
					}
					state.SetInvariant("ok")
					logf("[emul-inv] stock and money invariants OK")
				}
			}
		}()
	}

	// Score/series ticker.
	if seriesEvery > 0 && counts != nil {
		go func() {
			ticker := time.NewTicker(seriesEvery)
			defer ticker.Stop()
			var lastOK, lastTotal int64
			last := time.Now()
			for {
				select {
				case <-ctx.Done():
					return
				case now := <-ticker.C:
					ok, total, phase := counts()
					dt := now.Sub(last).Seconds()
					dOK, dTotal := ok-lastOK, total-lastTotal
					last, lastOK, lastTotal = now, ok, total
					if dt <= 0 {
						dt = float64(seriesEvery) / float64(time.Second)
					}
					var dbBytes int64
					state.mu.Lock()
					if n := len(state.memSamples); n > 0 {
						dbBytes = state.memSamples[n-1].DBBytes
					}
					state.mu.Unlock()
					state.observeSeries(SeriesPoint{
						TS:          now,
						ScorePerMin: float64(dOK) / dt * 60,
						TPS:         float64(dTotal) / dt,
						DBBytes:     dbBytes,
						Phase:       phase,
					})
					state.setCounts(ok, total)
				}
			}
		}()
	}

	return state
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if sub != "" && strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// EmulStateJSON is the JSON projection carried in the session Snapshot and
// served by the state endpoint.
type EmulStateJSON struct {
	ScorePerMin float64       `json:"scorePerMin"`
	OKUnits     int64         `json:"okUnits"`
	TotalUnits  int64         `json:"totalUnits"`
	Phase       string        `json:"phase"`
	Invariant   string        `json:"invariant"`
	InvariantAt time.Time     `json:"invariantAt"`
	MemPeaks    Sample        `json:"memPeaks"`
	Series      []SeriesPoint `json:"series"`
	PerUnit     []UnitStat    `json:"perUnit"`
	WorkingMode string        `json:"workingMode,omitempty"`
	StartedAt   time.Time     `json:"startedAt"`
}

// JSON renders a capped, thread-safe copy of the state. perUnitAgg comes
// from the worker metrics collector; registry (units with mode/kind, from
// business_ops) merges the raw outcome aggregates into full UnitStat rows.
// Units with recorded activity sort first by total count.
func (s *EmulState) JSON(perUnitAgg map[string]OutcomeStats, registry []Unit) EmulStateJSON {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := EmulStateJSON{
		ScorePerMin: s.scorePerMin,
		OKUnits:     s.okUnits,
		TotalUnits:  s.totalUnits,
		Phase:       s.phase,
		Invariant:   s.invariant,
		InvariantAt: s.invariantAt,
		MemPeaks:    s.memPeaks,
		WorkingMode: s.workingMode,
		StartedAt:   s.startedAt,
	}
	out.Series = make([]SeriesPoint, len(s.series))
	copy(out.Series, s.series)

	byName := make(map[string]Unit, len(registry))
	for _, u := range registry {
		byName[strings.ToLower(u.Name)] = u
	}
	out.PerUnit = make([]UnitStat, 0, len(perUnitAgg))
	for name, agg := range perUnitAgg {
		st := UnitStat{
			Unit:     name,
			OK:       agg.OK,
			Conflict: agg.Conflict,
			Rejected: agg.Rejected,
			Failure:  agg.Failure,
		}
		if u, ok := byName[strings.ToLower(name)]; ok {
			st.Mode, st.Kind = u.Mode, u.Kind
		}
		if agg.N > 0 {
			st.AvgMs = agg.LatSumMs / agg.N
			st.MaxMs = agg.MaxMs
		}
		out.PerUnit = append(out.PerUnit, st)
	}
	sort.Slice(out.PerUnit, func(i, j int) bool {
		a, b := out.PerUnit[i], out.PerUnit[j]
		at := a.OK + a.Conflict + a.Rejected + a.Failure
		bt := b.OK + b.Conflict + b.Rejected + b.Failure
		if at != bt {
			return at > bt
		}
		return a.Unit < b.Unit
	})
	return out
}

// Final returns the end-of-run values for the frozen report.
func (s *EmulState) Final() (scorePerMin float64, ok, total int64, peaks Sample, invariant, workingMode string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.scorePerMin, s.okUnits, s.totalUnits, s.memPeaks, s.invariant, s.workingMode
}
