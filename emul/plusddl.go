// -plusddl DDL churn sidecar (EXTENDED_LOAD_PLAN.md Phase 4). Adds ALTER
// TABLE ADD/ALTER/DROP COLUMN rounds on working tables (own prefixed columns
// only), alternating CREATE TABLE / DROP TABLE rounds with all six triggers
// and grouped test DML, and deliberate DDL rollbacks (Firebird DDL is
// transactional). Expected metadata-collision errors are classified as
// expected-under-load; they are the point of the test.
package emul

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"fb-loadgen/config"
	"fb-loadgen/opslog"
)

// ddlColumnTypes is the safe type set for column churn (FB3-compatible).
var ddlColumnTypes = []string{
	"INTEGER", "BIGINT", "VARCHAR(50)", "NUMERIC(12,2)", "TIMESTAMP", "SMALLINT",
}

// ddlWorkTable is one owned working table for column churn.
type ddlWorkTable struct {
	name    string
	columns []string // own TST_ columns currently present
}

// ddlSidecar carries the state of the plusddl churn.
type ddlSidecar struct {
	cfg          config.PlusDDL
	rollbackFrac float64 // fraction of column DDLs deliberately rolled back (P8)
	tables       []ddlWorkTable
	tstN         int    // TST_<n> table counter
	owned        string // a created table awaiting its drop round
}

// StartDDLSidecar launches the plusddl churn ticker. Discovery re-owns
// leftover TST_ columns after a restart; every DDL runs in its own committed
// transaction (autocommit semantics), unlike the provision-time RunScript.
func StartDDLSidecar(ctx context.Context, pool *sql.DB, cfg *config.Config, opsL *opslog.Logger, counters *ExtendedCounters, pause PauseWaiter) {
	el := cfg.ExtendedLoad
	el.Normalize()
	s := &ddlSidecar{cfg: el.PlusDDL, rollbackFrac: el.TxVariants.DDLRollbackFrac}
	s.discover(ctx, pool)

	go func() {
		// first round immediately, then every EverySec
		if !pause.WaitIfPaused(ctx.Done()) {
			return
		}
		s.round(ctx, pool, opsL, counters)
		ticker := time.NewTicker(time.Duration(s.cfg.EverySec) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if !pause.WaitIfPaused(ctx.Done()) {
					return
				}
				s.round(ctx, pool, opsL, counters)
			}
		}
	}()
}

// discover re-owns leftover prefixed columns so restarts continue the churn.
func (s *ddlSidecar) discover(ctx context.Context, pool *sql.DB) {
	for _, t := range s.cfg.WorkTables {
		wt := ddlWorkTable{name: strings.ToUpper(t)}
		rows, err := pool.QueryContext(ctx,
			`SELECT rdb$field_name FROM rdb$relation_fields
			 WHERE rdb$relation_name = ? AND rdb$field_name STARTING WITH ?`, wt.name, s.cfg.ColPrefix)
		if err == nil {
			for rows.Next() {
				var col string
				if _ = rows.Scan(&col); col != "" {
					wt.columns = append(wt.columns, strings.TrimSpace(col))
				}
			}
			rows.Close()
		}
		s.tables = append(s.tables, wt)
	}
	// re-own leftover TST_<n> tables from previous runs
	rows, err := pool.QueryContext(ctx,
		`SELECT rdb$relation_name FROM rdb$relations
		 WHERE rdb$relation_name STARTING WITH ? AND rdb$system_flag = 0`, s.cfg.ColPrefix)
	if err == nil {
		for rows.Next() {
			var name string
			if _ = rows.Scan(&name); name != "" && s.owned == "" {
				s.owned = strings.TrimSpace(name)
			}
		}
		rows.Close()
	}
}

// round executes one churn round: column churn on a working table, or a
// table lifecycle round (create with triggers + grouped DML / drop).
func (s *ddlSidecar) round(ctx context.Context, pool *sql.DB, opsL *opslog.Logger, counters *ExtendedCounters) {
	if counters != nil {
		counters.DDLRounds.Add(1)
	}
	s.columnChurn(ctx, pool, opsL, counters)
	s.tableLifecycle(ctx, pool, opsL, counters)
}

// columnChurn performs one random ADD/ALTER/DROP of an own column.
func (s *ddlSidecar) columnChurn(ctx context.Context, pool *sql.DB, opsL *opslog.Logger, counters *ExtendedCounters) {
	if len(s.tables) == 0 {
		return
	}
	wt := &s.tables[rand.Intn(len(s.tables))]
	// deliberate rollback fraction (P8): DDL in a tx that is rolled back,
	// then verified absent via rdb$relation_fields.
	rollbackProbe := rand.Float64() < s.rollbackFrac

	if len(wt.columns) == 0 || rand.Intn(2) == 0 {
		// ADD
		col := fmt.Sprintf("%s%s%d", s.cfg.ColPrefix, strings.ToLower(strings.TrimPrefix(wt.name, "T")), time.Now().UnixNano()%100000)
		typ := ddlColumnTypes[rand.Intn(len(ddlColumnTypes))]
		ddl := fmt.Sprintf("ALTER TABLE %s ADD %s %s", wt.name, col, typ)
		s.execDDL(ctx, pool, opsL, counters, ddl, rollbackProbe, wt.name, col)
		if !rollbackProbe {
			wt.columns = append(wt.columns, col)
			if counters != nil {
				counters.ColumnsAdded.Add(1)
			}
		}
		return
	}
	// pick an own column
	idx := rand.Intn(len(wt.columns))
	col := wt.columns[idx]
	if rand.Intn(2) == 0 {
		// ALTER TYPE within the safe set
		typ := ddlColumnTypes[rand.Intn(len(ddlColumnTypes))]
		ddl := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s", wt.name, col, typ)
		s.execDDL(ctx, pool, opsL, counters, ddl, rollbackProbe, wt.name, col)
		if !rollbackProbe && counters != nil {
			counters.ColumnsAltered.Add(1)
		}
		return
	}
	// DROP (own columns only)
	ddl := fmt.Sprintf("ALTER TABLE %s DROP %s", wt.name, col)
	s.execDDL(ctx, pool, opsL, counters, ddl, rollbackProbe, wt.name, col)
	wt.columns = append(wt.columns[:idx], wt.columns[idx+1:]...)
	if !rollbackProbe && counters != nil {
		counters.ColumnsDropped.Add(1)
	}
}

// tableLifecycle alternates create/drop rounds. A create round makes
// TST_<n> with 2-5 random columns, all six triggers (BI/AI/BU/AU/BD/AD) with
// test bodies, then — in separate transactions — inserts 100, updates 50 and
// deletes 30 rows with a randomly drawn grouping.
func (s *ddlSidecar) tableLifecycle(ctx context.Context, pool *sql.DB, opsL *opslog.Logger, counters *ExtendedCounters) {
	if s.owned == "" {
		s.owned = fmt.Sprintf("%s%d", s.cfg.ColPrefix, time.Now().UnixNano()%1000000)
		s.tstN++
		table := s.owned
		cols := make([]string, 0, 5)
		for i := 0; i < 2+rand.Intn(4); i++ {
			cols = append(cols, fmt.Sprintf("C%d %s", i, ddlColumnTypes[rand.Intn(len(ddlColumnTypes))]))
		}
		s.lifecycleTx(ctx, pool, opsL, counters, func(tx *sql.Tx) error {
			return s.exec(tx, fmt.Sprintf(`RECREATE TABLE %s (
				ID INTEGER NOT NULL PRIMARY KEY,
				NOTE VARCHAR(60),
				%s)`, table, strings.Join(cols, ",\n")))
		})
		s.createTriggers(ctx, pool, opsL, counters, table)
		s.groupedDML(ctx, pool, opsL, counters, table)
		if counters != nil {
			counters.TablesCreated.Add(1)
		}
		return
	}
	// drop round: triggers go with the table
	s.lifecycleTx(ctx, pool, opsL, counters, func(tx *sql.Tx) error {
		return s.exec(tx, "DROP TABLE "+s.owned)
	})
	s.owned = ""
	if counters != nil {
		counters.TablesDropped.Add(1)
	}
}

// createTriggers adds the six required triggers with test bodies (P3).
func (s *ddlSidecar) createTriggers(ctx context.Context, pool *sql.DB, opsL *opslog.Logger, counters *ExtendedCounters, table string) {
	lower := strings.ToLower(table)
	triggers := []struct{ name, timing, event, body string }{
		{"BI", "BEFORE", "INSERT", "NEW.note = COALESCE(NEW.note, 'bi')"},
		{"AI", "AFTER", "INSERT", "UPDATE " + table + " SET note = note WHERE id = NEW.id"},
		{"BU", "BEFORE", "UPDATE", "IF (OLD.id IS NULL) THEN EXCEPTION EX_ELT_TEST"},
		{"AU", "AFTER", "UPDATE", "UPDATE " + table + " SET note = note WHERE id = OLD.id"},
		{"BD", "BEFORE", "DELETE", "IF (NOT EXISTS(SELECT 1 FROM " + table + " WHERE id = OLD.id)) THEN EXCEPTION EX_ELT_TEST"},
		{"AD", "AFTER", "DELETE", "BEGIN END"},
	}
	for _, tr := range triggers {
		s.lifecycleTx(ctx, pool, opsL, counters, func(tx *sql.Tx) error {
			return s.exec(tx, fmt.Sprintf(
				`CREATE TRIGGER TR_%s_%s FOR %s %s %s POSITION 0 AS BEGIN %s END`,
				lower, tr.name, table, tr.timing, tr.event, tr.body))
		})
	}
}

// groupedDML runs the test DML with a random grouping: all three in one tx /
// each in its own tx / [insert+update][delete] (P3). The grouping choice is
// logged.
func (s *ddlSidecar) groupedDML(ctx context.Context, pool *sql.DB, opsL *opslog.Logger, counters *ExtendedCounters, table string) {
	grouping := []string{"one-tx", "own-tx", "pair-and-single"}[rand.Intn(3)]
	txn := int64(0)
	if opsL != nil {
		txn = opsL.NextTx()
		opsL.TxStartRound(txn, "plusddl-dml", "grouping="+grouping)
	}
	// grouping realized as three txns / one txn / two txns
	type stepFn func(tx *sql.Tx) error
	ins := func(tx *sql.Tx) error {
		return s.exec(tx, fmt.Sprintf(
			`EXECUTE BLOCK AS DECLARE VARIABLE I INTEGER; BEGIN
			 I = 0;
			 WHILE (I < %d) DO BEGIN I = I + 1;
			   INSERT INTO %s (ID, NOTE) VALUES (NEXT VALUE FOR EL_BULK_SEQ, 'plusddl'); END
			 END`, s.cfg.TestInsertRows, table))
	}
	upd := func(tx *sql.Tx) error {
		return s.exec(tx, fmt.Sprintf("UPDATE %s SET note = note||'u' WHERE id IN (SELECT FIRST %d id FROM %s ORDER BY id)", table, s.cfg.TestUpdateRows, table))
	}
	del := func(tx *sql.Tx) error {
		return s.exec(tx, fmt.Sprintf("DELETE FROM %s WHERE id IN (SELECT FIRST %d id FROM %s ORDER BY id DESC)", table, s.cfg.TestDeleteRows, table))
	}

	var groups [][]stepFn
	switch grouping {
	case "one-tx":
		groups = [][]stepFn{{ins, upd, del}}
	case "pair-and-single":
		groups = [][]stepFn{{ins, upd}, {del}}
	default:
		groups = [][]stepFn{{ins}, {upd}, {del}}
	}
	for _, g := range groups {
		tx, err := pool.BeginTx(ctx, nil)
		if err != nil {
			continue
		}
		failed := false
		for _, step := range g {
			if err := step(tx); err != nil {
				failed = true
				break
			}
		}
		if failed {
			_ = tx.Rollback()
			if opsL != nil {
				opsL.RoundRollback(txn, "grouped_dml_failed", 0, 0, 0, 0, true)
			}
			continue
		}
		if err := tx.Commit(); err == nil && opsL != nil {
			opsL.RoundCommit(txn, 0, 0, 0, 0, false)
		}
	}
}

func (s *ddlSidecar) exec(tx *sql.Tx, q string) error {
	_, err := tx.ExecContext(context.Background(), q)
	return err
}

// execDDL runs one column DDL in its own transaction; with rollbackProbe the
// transaction is rolled back and the column absence verified via
// rdb$relation_fields (P8).
func (s *ddlSidecar) execDDL(ctx context.Context, pool *sql.DB, opsL *opslog.Logger, counters *ExtendedCounters, ddl string, rollbackProbe bool, table, column string) {
	start := time.Now()
	txn := int64(0)
	if opsL != nil {
		txn = opsL.NextTx()
		opsL.TxStartRound(txn, "plusddl-ddl", ddl)
	}
	tx, err := pool.BeginTx(ctx, nil)
	if err != nil {
		if opsL != nil {
			opsL.DDL(txn, ddl, time.Since(start), true)
		}
		return
	}
	_, err = tx.ExecContext(ctx, ddl)
	if err != nil {
		_ = tx.Rollback()
		// metadata collisions are expected under load (P4): classified in the
		// error taxonomy, they must not trip the invariant logic.
		if opsL != nil {
			opsL.DDL(txn, ddl, time.Since(start), true)
		}
		return
	}
	if rollbackProbe {
		_ = tx.Rollback()
		verified := s.columnAbsent(ctx, pool, table, column)
		if opsL != nil {
			opsL.DDL(txn, ddl, time.Since(start), false)
			opsL.RoundRollback(txn, fmt.Sprintf("rollback_ddl verified_absent=%v", verified), 0, 0, 0, time.Since(start), false)
		}
		return
	}
	if err := tx.Commit(); err == nil && opsL != nil {
		opsL.DDL(txn, ddl, time.Since(start), false)
		opsL.RoundCommit(txn, 0, 0, 0, time.Since(start), false)
	}
}

func (s *ddlSidecar) columnAbsent(ctx context.Context, pool *sql.DB, table, column string) bool {
	var n int
	err := pool.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM rdb$relation_fields WHERE rdb$relation_name = ? AND rdb$field_name = ?`,
		strings.ToUpper(table), column).Scan(&n)
	return err == nil && n == 0
}

// lifecycleTx runs a DDL step in its own committed transaction.
func (s *ddlSidecar) lifecycleTx(ctx context.Context, pool *sql.DB, opsL *opslog.Logger, counters *ExtendedCounters, fn func(tx *sql.Tx) error) {
	tx, err := pool.BeginTx(ctx, nil)
	if err != nil {
		return
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return
	}
	_ = tx.Commit()
}
