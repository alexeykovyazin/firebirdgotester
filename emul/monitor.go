package emul

import (
	"context"
	"database/sql"
	"sync"
	"time"
)

// Sample is one monitoring snapshot of Firebird's memory counters,
// mirroring the four levels firebirdtest.com charts (all values in bytes).
// On servers with MON$MEMORY_USAGE (FB 4.0+) the values are the
// server-side running peaks (MON$MAX_MEMORY_USED); on FB 3.0 they are the
// live usage at sampling time and peaks are tracked client-side.
type Sample struct {
	TS        time.Time `json:"ts"`
	DBBytes   int64     `json:"dbBytes"`   // database level
	AttBytes  int64     `json:"attBytes"`  // attachment level
	TrnBytes  int64     `json:"trnBytes"`  // transaction level
	StmtBytes int64     `json:"stmtBytes"` // statement level
}

// Peaks accumulates the maximum of each level across all samples of a run -
// the per-run numbers published in the benchmark table.
type Peaks struct {
	mu    sync.Mutex
	peak  Sample
	count int
}

// Observe updates the peaks with s.
func (p *Peaks) Observe(s Sample) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.count++
	if s.DBBytes > p.peak.DBBytes {
		p.peak.DBBytes = s.DBBytes
	}
	if s.AttBytes > p.peak.AttBytes {
		p.peak.AttBytes = s.AttBytes
	}
	if s.TrnBytes > p.peak.TrnBytes {
		p.peak.TrnBytes = s.TrnBytes
	}
	if s.StmtBytes > p.peak.StmtBytes {
		p.peak.StmtBytes = s.StmtBytes
	}
	p.peak.TS = s.TS
}

// Result returns the accumulated peaks and the number of samples taken.
func (p *Peaks) Result() (Sample, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.peak, p.count
}

// Monitor periodically snapshots memory usage on a dedicated connection of
// db (excluded from the load pool sizing). Feed the channel samples into
// Peaks and/or a live UI stream.
type Monitor struct {
	interval time.Duration

	// onError receives the first sampling error (usually a version or
	// permission problem); subsequent errors are suppressed.
	onError func(error)
}

func NewMonitor(interval time.Duration) *Monitor {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	return &Monitor{interval: interval}
}

// SetErrorHandler installs a callback invoked once with the first sampling
// error. Without it, sampling errors are silent.
func (m *Monitor) SetErrorHandler(f func(error)) { m.onError = f }

// Run blocks until ctx is done, sampling every interval. One dedicated
// *sql.Conn is used so the monitoring traffic does not interleave with
// worker transactions.
func (m *Monitor) Run(ctx context.Context, db *sql.DB, sink func(Sample)) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	// FB 4.0+ has MON$MEMORY_USAGE with per-level running peaks; FB 3.0
	// only exposes live MON$MEMORY_USED on the per-object MON$ tables.
	newStyle := false
	var n int
	if err := conn.QueryRowContext(ctx,
		`select count(*) from rdb$relations where rdb$relation_name = 'MON$MEMORY_USAGE'`).Scan(&n); err == nil {
		newStyle = n > 0
	}

	reportErr := func(err error) {
		if m.onError != nil {
			m.onError(err)
			m.onError = nil
		}
	}

	sample := func() {
		var s Sample
		s.TS = time.Now()
		if newStyle {
			err := conn.QueryRowContext(ctx, `
				select
				  coalesce(max(case when mon$stat_group = 0 then mon$max_memory_used end), 0),
				  coalesce(max(case when mon$stat_group = 1 then mon$max_memory_used end), 0),
				  coalesce(max(case when mon$stat_group = 2 then mon$max_memory_used end), 0),
				  coalesce(max(case when mon$stat_group = 3 then mon$max_memory_used end), 0)
				from mon$memory_usage`).Scan(&s.DBBytes, &s.AttBytes, &s.TrnBytes, &s.StmtBytes)
			if err != nil {
				reportErr(err)
				return
			}
		} else {
			var attSum, trnMax, stmtMax sql.NullInt64
			err := conn.QueryRowContext(ctx, `
				select coalesce(sum(mon$memory_used),0), coalesce(max(mon$memory_used),0)
				from mon$attachments`).Scan(&s.DBBytes, &attSum)
			if err != nil {
				reportErr(err)
				return
			}
			_ = conn.QueryRowContext(ctx,
				`select coalesce(max(mon$memory_used),0) from mon$transactions`).Scan(&trnMax)
			_ = conn.QueryRowContext(ctx,
				`select coalesce(max(mon$memory_used),0) from mon$statements`).Scan(&stmtMax)
			s.AttBytes = attSum.Int64
			s.TrnBytes = trnMax.Int64
			s.StmtBytes = stmtMax.Int64
		}
		if sink != nil {
			sink(s)
		}
	}

	sample()
	tick := time.NewTicker(m.interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			sample()
		}
	}
}
