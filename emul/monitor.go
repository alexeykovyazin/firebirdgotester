package emul

import (
	"context"
	"database/sql"
	"sync"
	"time"
)

// Sample is one monitoring snapshot of Firebird's MON$ memory counters,
// mirroring the four levels firebirdtest.com charts (all values in bytes).
type Sample struct {
	TS        time.Time `json:"ts"`
	DBBytes   int64     `json:"dbBytes"`   // sum of MON$ATTACHMENTS.MON$MEMORY_USED
	AttBytes  int64     `json:"attBytes"`  // max single attachment
	TrnBytes  int64     `json:"trnBytes"`  // max single transaction
	StmtBytes int64     `json:"stmtBytes"` // max single statement
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

// Monitor periodically snapshots MON$ memory counters on a dedicated
// connection of db (excluded from the load pool sizing). Feed the channel
// samples into Peaks and/or a live UI stream.
type Monitor struct {
	interval time.Duration
}

func NewMonitor(interval time.Duration) *Monitor {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	return &Monitor{interval: interval}
}

// Run blocks until ctx is done, sampling every interval. One dedicated
// *sql.Conn is used so the monitoring traffic does not interleave with
// worker transactions.
func (m *Monitor) Run(ctx context.Context, db *sql.DB, sink func(Sample)) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	tick := time.NewTicker(m.interval)
	defer tick.Stop()

	sample := func() {
		var s Sample
		s.TS = time.Now()
		var attMax, trnMax, stmtMax sql.NullInt64
		err := conn.QueryRowContext(ctx, `
			select coalesce(sum(mon$memory_used),0), coalesce(max(mon$memory_used),0)
			from mon$attachments`).Scan(&s.DBBytes, &attMax)
		if err != nil {
			return // transient (sweep, shutdown) - skip this sample
		}
		_ = conn.QueryRowContext(ctx,
			`select coalesce(max(mon$memory_used),0) from mon$transactions`).Scan(&trnMax)
		_ = conn.QueryRowContext(ctx,
			`select coalesce(max(mon$memory_used),0) from mon$statements`).Scan(&stmtMax)
		s.AttBytes = attMax.Int64
		s.TrnBytes = trnMax.Int64
		s.StmtBytes = stmtMax.Int64
		if sink != nil {
			sink(s)
		}
	}

	sample()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			sample()
		}
	}
}
