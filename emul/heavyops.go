// Heavy SELECT and bulk DML sidecar operations of the extended load mix
// (EXTENDED_LOAD_PLAN.md Phase 3, H2/H4). They run on a dedicated sidecar
// connection with their own transaction scenarios and are recorded into a
// separate metrics series — never into the emul unit score.
package emul

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"time"

	"fb-loadgen/config"
	"fb-loadgen/opslog"
)

// PauseWaiter mirrors worker.PauseGate without importing it (worker imports
// emul; the dependency edge must not be reversed).
type PauseWaiter interface {
	WaitIfPaused(done <-chan struct{}) bool
}

// heavyJoinScan runs one multi-JOIN scan over the real business tables with a
// random id-range filter, an aggregate and a sorted cut. It runs inside the
// caller's transaction so the scenario picker governs its isolation.
func heavyJoinScan(ctx context.Context, tx *sql.Tx, maxID int64, rng *rand.Rand, minJoins int) (int64, error) {
	if maxID < 2 {
		maxID = 2
	}
	// narrow random window: heavy per round, bounded so a round finishes
	// well within the sidecar interval
	lo := rng.Int63n(maxID + 1)
	hi := lo + maxID/50 + 50
	q := `SELECT COUNT(*), CAST(COALESCE(SUM(dd.qty), 0) AS BIGINT),
	CAST(COALESCE(SUM(dd.cost_retail), 0) AS BIGINT)
FROM doc_list d
JOIN agents a ON a.id = d.agent_id
JOIN doc_data dd ON dd.doc_id = d.id
JOIN wares w ON w.id = dd.ware_id
WHERE d.id BETWEEN ? AND ?`
	if minJoins > 3 {
		// deeper variant: join the ware group dimension back onto wares
		q = `SELECT COUNT(*), CAST(COALESCE(SUM(dd.qty), 0) AS BIGINT),
	CAST(COALESCE(SUM(dd.cost_retail), 0) AS BIGINT)
FROM doc_list d
JOIN agents a ON a.id = d.agent_id
JOIN doc_data dd ON dd.doc_id = d.id
JOIN wares w ON w.id = dd.ware_id
JOIN wares wg ON wg.id = w.group_id
WHERE d.id BETWEEN ? AND ?`
	}
	args := []any{lo, hi}
	var cnt, sumQty, sumCost int64
	err := tx.QueryRowContext(ctx, q, args...).Scan(&cnt, &sumQty, &sumCost)
	return cnt, err
}

// bulkRound executes one mass INSERT/UPDATE/DELETE round on EL_BULK_ITEMS in
// a single transaction (RW). Rows survive only when the drawn completion is a
// commit; rollback completions log the full statement list anyway.
func bulkRound(ctx context.Context, tx *sql.Tx, rng *rand.Rand, cfg config.BulkDml, workerID int64) (rows int64, kind string, err error) {
	span := int64(cfg.MaxRows - cfg.MinRows + 1)
	n := int64(cfg.MinRows) + rng.Int63n(span)
	kind = []string{"insert", "update", "delete"}[rng.Intn(3)]
	switch kind {
	case "insert":
		var inserted int64
		for i := int64(0); i < n; i += 100 {
			batch := n - i
			if batch > 100 {
				batch = 100
			}
			res, e := bulkInsertValues(ctx, tx, workerID, batch)
			if e != nil {
				return 0, kind, e
			}
			aff, _ := res.RowsAffected()
			inserted += aff
		}
		return inserted, kind, nil
	case "update":
		res, e := tx.ExecContext(ctx,
			`UPDATE EL_BULK_ITEMS SET VAL = VAL + 0.01, PAYLOAD = PAYLOAD||'u'
			 WHERE ID IN (SELECT FIRST ? ID FROM EL_BULK_ITEMS ORDER BY ID)`, n)
		if e != nil {
			return 0, kind, e
		}
		aff, _ := res.RowsAffected()
		return aff, kind, nil
	default: // delete: oldest own rows first (GC pressure capped, R-d2)
		res, e := tx.ExecContext(ctx,
			`DELETE FROM EL_BULK_ITEMS WHERE ID IN (SELECT FIRST ? ID FROM EL_BULK_ITEMS ORDER BY ID)`, n)
		if e != nil {
			return 0, kind, e
		}
		aff, _ := res.RowsAffected()
		return aff, kind, nil
	}
}

// bulkInsertValues inserts `batch` rows with one EXECUTE BLOCK loop (Firebird
// has no multi-row VALUES INSERT). Arguments are integers generated locally.
func bulkInsertValues(ctx context.Context, tx *sql.Tx, roundID, batch int64) (sql.Result, error) {
	q := fmt.Sprintf(`EXECUTE BLOCK AS DECLARE VARIABLE I INTEGER; BEGIN
	 I = 0;
	 WHILE (I < %d) DO BEGIN
	   I = I + 1;
	   INSERT INTO EL_BULK_ITEMS (ID, ROUND_ID, PAYLOAD, VAL, CREATED_AT)
	   VALUES (NEXT VALUE FOR EL_BULK_SEQ, %d, 'payload', 1.5, CURRENT_TIMESTAMP);
	 END
	 END`, batch, roundID)
	return tx.ExecContext(ctx, q)
}

// RunExtendedSidecars launches the heavy SELECT and bulk DML tickers. Each
// sidecar pins its own connection out of the pool for its whole lifetime:
// rounds are long-running, and sharing the (often single-connection) sidecar
// pool would starve the bulk sidecar behind an in-flight heavy scan. The
// connection grab is asynchronous and bounded — a saturated pool disables the
// sidecar instead of blocking the run start. Both sidecars honor the pause
// gate and log every round to the ops log.
func RunExtendedSidecars(ctx context.Context, pool *sql.DB, cfg *config.Config, opsL *opslog.Logger, counters *ExtendedCounters, pause PauseWaiter) {
	el := cfg.ExtendedLoad
	el.Normalize()
	// heavy + bulk each pin one connection; factories cap pools at 1, which
	// would starve the second grab
	pool.SetMaxOpenConns(4)
	pool.SetMaxIdleConns(4)

	grab := func(run func(conn *sql.Conn)) {
		go func() {
			cctx, ccancel := context.WithTimeout(ctx, 10*time.Second)
			defer ccancel()
			conn, err := pool.Conn(cctx)
			if err != nil {
				return
			}
			defer func() { _ = conn.Close() }()
			run(conn)
		}()
	}

	if el.HeavySelect.EverySec > 0 {
		grab(func(conn *sql.Conn) { runHeavySidecar(ctx, conn, el.HeavySelect, opsL, counters, pause) })
	}
	if el.BulkDml.EverySec > 0 {
		grab(func(conn *sql.Conn) { runBulkSidecar(ctx, conn, el.BulkDml, opsL, counters, pause) })
	}
}

// runHeavySidecar ticks the heavy multi-JOIN SELECT round.
func runHeavySidecar(ctx context.Context, conn *sql.Conn, hs config.HeavySelect, opsL *opslog.Logger, counters *ExtendedCounters, pause PauseWaiter) {
	interval := time.Duration(hs.EverySec) * time.Second
	if interval <= 0 {
		return
	}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	var maxDocID int64
	_ = conn.QueryRowContext(ctx, `SELECT COALESCE(MAX(ID), 0) FROM DOC_LIST`).Scan(&maxDocID)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !pause.WaitIfPaused(ctx.Done()) {
				return
			}
			roundCtx, roundCancel := context.WithTimeout(ctx, interval*3)
			start := time.Now()
			txn := int64(0)
			if opsL != nil {
				txn = opsL.NextTx()
				opsL.TxStartRound(txn, "heavy-select", "RW/sidecar")
			}
			tx, err := conn.BeginTx(roundCtx, nil)
			if err != nil {
				roundCancel()
				if counters != nil {
					counters.HeavyFailures.Add(1)
				}
				continue
			}
			cnt, qerr := heavyJoinScan(roundCtx, tx, maxDocID, rng, hs.MinJoins)
			if qerr == nil {
				if terr := tx.Commit(); terr == nil {
					if counters != nil {
						counters.HeavyRounds.Add(1)
					}
					if opsL != nil {
						opsL.TableOp(txn, "SELECT", "DOC_LIST/DOC_DATA/AGENTS/WARES", cnt, time.Since(start), false)
						opsL.RoundCommit(txn, 0, 0, 0, time.Since(start), false)
					}
					roundCancel()
					continue
				}
			}
			_ = tx.Rollback()
			if counters != nil {
				counters.HeavyFailures.Add(1)
			}
			if opsL != nil {
				opsL.RoundRollback(txn, "failed", 0, 0, 0, time.Since(start), true)
			}
			roundCancel()
		}
	}
}

// runBulkSidecar ticks the bulk INSERT/UPDATE/DELETE round.
func runBulkSidecar(ctx context.Context, conn *sql.Conn, bd config.BulkDml, opsL *opslog.Logger, counters *ExtendedCounters, pause PauseWaiter) {
	interval := time.Duration(bd.EverySec) * time.Second
	if interval <= 0 {
		return
	}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !pause.WaitIfPaused(ctx.Done()) {
				return
			}
			start := time.Now()
			txn := int64(0)
			if opsL != nil {
				txn = opsL.NextTx()
				opsL.TxStartRound(txn, "bulk-dml", "RW/sidecar")
			}
			tx, err := conn.BeginTx(ctx, nil)
			if err != nil {
				if counters != nil {
					counters.BulkFailures.Add(1)
				}
				continue
			}
			rows, kind, berr := bulkRound(ctx, tx, rng, bd, time.Now().Unix()%1000000)
			if berr == nil {
				if terr := tx.Commit(); terr == nil {
					if counters != nil {
						countBulk(counters, kind, rows)
					}
					if opsL != nil {
						opsL.TableOp(txn, bulkVerb(kind), "EL_BULK_ITEMS", rows, time.Since(start), false)
						opsL.RoundCommit(txn, rows, 0, 0, time.Since(start), false)
					}
					continue
				}
			}
			_ = tx.Rollback()
			if counters != nil {
				counters.BulkFailures.Add(1)
			}
			if opsL != nil {
				opsL.RoundRollback(txn, "failed", 0, 0, 0, time.Since(start), true)
			}
		}
	}
}

func countBulk(c *ExtendedCounters, kind string, rows int64) {
	switch kind {
	case "insert":
		c.BulkInserts.Add(1)
	case "update":
		c.BulkUpdates.Add(1)
	case "delete":
		c.BulkDeletes.Add(1)
	}
	c.BulkRows.Add(rows)
}

func bulkVerb(kind string) string {
	switch kind {
	case "insert":
		return "INSERT"
	case "update":
		return "UPDATE"
	default:
		return "DELETE"
	}
}
