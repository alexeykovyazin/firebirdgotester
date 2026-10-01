// DDL phase-2 churn (DDL_EXTEND_PLAN_2026-10-01): ALTER COLUMN TYPE
// conversion cycles on EL_DDL_VICTIM and ALTER PROCEDURE signature flips on
// SP_ELT_VICTIM with a concurrent dynamic caller racing the flips. Objects
// live in the main emul database (same lock manager / statement caches as the
// business load — that is the point). Every non-OK DDL or call under this
// churn is expected by design: metadata races ("is in use", "lock conflict")
// or legitimate data-conversion refusals; they are counted, never swallowed.
package emul

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"fb-loadgen/config"
	"fb-loadgen/opslog"
)

const (
	ddlvTable   = "EL_DDL_VICTIM"
	ddlvProc    = "SP_ELT_VICTIM"
	ddlvLog     = "EL_DDL_LOG"
	ddlvSeq     = "EL_DDL_SEQ"
	victimEvery = 10 * time.Second // type + proc-sig step interval
)

// victimState is the in-memory model of the victim objects' metadata. Guarded
// by mu: the victim ticker mutates it, the caller goroutine reads procSig.
type victimState struct {
	mu           sync.Mutex
	tableExists  bool
	valLen       int // current VAL varchar length: 64 / 128 / 256
	valNullable  bool
	cntBigint    bool
	numPrecision int    // 12 or 15
	procSig      string // "A" / "B" / "" (unknown)
	stepIdx      int
}

// victimStep is one prepared ALTER with an optional normalization DML that
// must run before it (data conversion preparation).
type victimStep struct {
	name      string
	normalize string
	ddl       string
	revert    func()
}

// ddlExpectedError classifies the errors that a DDL churn (and its racing
// callers) legitimately produces under load: metadata collisions, NOWAIT
// lock conflicts, signature races, unknown-object windows, and data
// conversion refusals. Anything else is a real failure and stays visible.
func ddlExpectedError(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	for _, sub := range []string{
		"is in use",
		"lock conflict",
		"no wait",
		"deadlock",
		"unsuccessful metadata update",
		"does not match",
		"column unknown",
		"unknown column",
		"procedure unknown",
		"is not defined",
		"table unknown",
		"too short",
		"new length shorter",
		"attempt to store a value",
		"conversion error",
	} {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// --- object bootstrap -------------------------------------------------------

// bootstrapDDLVictim creates any missing victim objects. Best-effort: a
// failure here must not fail the run start — the sidecar retries lazily and
// reports through its counters.
func bootstrapDDLVictim(ctx context.Context, db *sql.DB) {
	stmts := []string{
		`CREATE SEQUENCE ` + ddlvSeq,
		`CREATE TABLE ` + ddlvLog + ` (
			ID BIGINT NOT NULL PRIMARY KEY,
			TS TIMESTAMP,
			KIND VARCHAR(16),
			NOTE VARCHAR(64)
		)`,
		`CREATE TABLE ` + ddlvTable + ` (
			ID INTEGER NOT NULL PRIMARY KEY,
			VAL VARCHAR(64),
			NUM NUMERIC(12,2),
			CNT INTEGER
		)`,
		`INSERT INTO ` + ddlvTable + ` (ID, VAL, NUM, CNT) VALUES (1, 'seed', 1.00, 1)`,
	}
	for _, q := range stmts {
		if _, err := db.ExecContext(ctx, q); err != nil && !isAlreadyExists(err) {
			fmt.Printf("[plusddl] victim bootstrap skipped step (%v); sidecar will retry\n", err)
			return
		}
	}
	if _, err := db.ExecContext(ctx, procVictimDDL("A")); err != nil && !isAlreadyExists(err) {
		fmt.Printf("[plusddl] victim bootstrap: procedure not created (%v); sidecar will retry\n", err)
	}
}

// victimEnsure creates missing objects from the sidecar side (same idempotent
// steps as the bootstrap; runs after discover on every sidecar start).
func (s *ddlSidecar) victimEnsure(ctx context.Context, pool *sql.DB) {
	if !objectExists(ctx, pool, "rdb$relations", "rdb$relation_name", ddlvSeq) {
		_ = s.lifecycleTx(ctx, pool, nil, func(tx *sql.Tx) error { return s.exec(ctx, tx, `CREATE SEQUENCE `+ddlvSeq) })
	}
	if !objectExists(ctx, pool, "rdb$relations", "rdb$relation_name", ddlvLog) {
		_ = s.lifecycleTx(ctx, pool, nil, func(tx *sql.Tx) error {
			return s.exec(ctx, tx, `CREATE TABLE `+ddlvLog+` (
				ID BIGINT NOT NULL PRIMARY KEY,
				TS TIMESTAMP,
				KIND VARCHAR(16),
				NOTE VARCHAR(64)
			)`)
		})
	}
	if !objectExists(ctx, pool, "rdb$relations", "rdb$relation_name", ddlvTable) {
		_ = s.lifecycleTx(ctx, pool, nil, func(tx *sql.Tx) error {
			return s.exec(ctx, tx, `CREATE TABLE `+ddlvTable+` (
				ID INTEGER NOT NULL PRIMARY KEY,
				VAL VARCHAR(64),
				NUM NUMERIC(12,2),
				CNT INTEGER
			)`)
		})
	}
	if !objectExists(ctx, pool, "rdb$procedures", "rdb$procedure_name", ddlvProc) {
		_ = s.lifecycleTx(ctx, pool, nil, func(tx *sql.Tx) error { return s.exec(ctx, tx, procVictimDDL("A")) })
	}
	var n int
	if err := pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+ddlvTable).Scan(&n); err == nil && n == 0 {
		_ = s.lifecycleTx(ctx, pool, nil, func(tx *sql.Tx) error {
			return s.exec(ctx, tx, `INSERT INTO `+ddlvTable+` (ID, VAL, NUM, CNT) VALUES (1, 'seed', 1.00, 1)`)
		})
	}
}

// --- discovery ---------------------------------------------------------------

// victimDiscover reads the current metadata of the victim objects into the
// model. Unknown pieces stay at their zero values and are re-discovered on
// demand (e.g. the caller re-reads the procedure signature after a mismatch).
func (s *ddlSidecar) victimDiscover(ctx context.Context, pool *sql.DB) {
	s.v.mu.Lock()
	defer s.v.mu.Unlock()
	s.v.tableExists = objectExists(ctx, pool, "rdb$relations", "rdb$relation_name", ddlvTable)
	s.v.procSig = ""

	if s.v.tableExists {
		rows, err := pool.QueryContext(ctx,
			`SELECT rf.rdb$field_name, f.rdb$field_type, f.rdb$field_length,
			        f.rdb$field_scale, rf.rdb$null_flag
			 FROM rdb$relation_fields rf
			 JOIN rdb$fields f ON f.rdb$field_name = rf.rdb$field_source
			 WHERE rf.rdb$relation_name = ? AND rf.rdb$field_name IN ('VAL','CNT','NUM')`,
			ddlvTable)
		if err == nil {
			for rows.Next() {
				var name string
				var ftype, flen int
				var scale, nullFlag sql.NullInt64
				if err := rows.Scan(&name, &ftype, &flen, &scale, &nullFlag); err != nil {
					continue
				}
				nullable := nullFlag.Int64 == 0
				name = strings.TrimSpace(name)
				switch strings.TrimSpace(strings.ToUpper(name)) {
				case "VAL":
					if ftype == 37 {
						s.v.valLen = flen
						s.v.valNullable = nullable
					}
				case "CNT":
					s.v.cntBigint = ftype == 16
				case "NUM":
					sc := -int(scale.Int64)
					if sc == 4 {
						s.v.numPrecision = 15
					} else {
						s.v.numPrecision = 12
					}
				}
			}
			rows.Close()
		}
	}
	if n := s.procedureInputParams(ctx, pool); n > 0 {
		s.v.procSig = parseProcSig(n)
	}
}

// procedureInputParams counts the input parameters of SP_ELT_VICTIM
// (0 = procedure missing, 1 = signature A, 2 = signature B).
func (s *ddlSidecar) procedureInputParams(ctx context.Context, pool *sql.DB) int {
	var n int
	err := pool.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM rdb$procedure_parameters
		 WHERE rdb$procedure_name = ? AND rdb$parameter_type = 0`, ddlvProc).Scan(&n)
	if err != nil {
		return 0
	}
	return n
}

// parseProcSig maps an input-parameter count to the signature id.
func parseProcSig(inputParams int) string {
	switch inputParams {
	case 2:
		return "B"
	case 1:
		return "A"
	}
	return ""
}

// --- signature DDL (pure functions: golden-tested) ---------------------------

// procVictimDDL returns the full ALTER PROCEDURE statement for a signature.
func procVictimDDL(sig string) string {
	if sig == "B" {
		return `ALTER PROCEDURE ` + ddlvProc + ` (IN1 INTEGER, IN2 VARCHAR(32))
RETURNS (OUT1 INTEGER, OUT2 VARCHAR(32))
AS BEGIN
  INSERT INTO ` + ddlvLog + ` (ID, TS, KIND, NOTE) VALUES (NEXT VALUE FOR ` + ddlvSeq + `, CURRENT_TIMESTAMP, 'B', IN2);
  OUT1 = IN1; OUT2 = IN2; END`
	}
	return `ALTER PROCEDURE ` + ddlvProc + ` (IN1 INTEGER)
RETURNS (OUT1 INTEGER)
AS BEGIN
  INSERT INTO ` + ddlvLog + ` (ID, TS, KIND, NOTE) VALUES (NEXT VALUE FOR ` + ddlvSeq + `, CURRENT_TIMESTAMP, 'A', 'sig-A');
  OUT1 = IN1; END`
}

// --- type steps (pure model functions: golden-tested) ------------------------

// victimStepName returns the column the next type step targets (round-robin).
func victimStepName(idx int) string {
	return []string{"VAL", "CNT", "NUM", "VAL_NULL"}[idx%4]
}

// nextTypeStep computes the next ALTER COLUMN TYPE step from the model and
// ADVANCES the model (call revert() if the DDL did not commit). normalize, if
// non-empty, must run before the ALTER.
func (v *victimState) nextTypeStep() (step victimStep) {
	idx := v.stepIdx
	v.stepIdx++
	switch victimStepName(idx) {
	case "VAL":
		if v.valLen < 256 {
			next := v.valLen * 2
			step = victimStep{
				name: "VAL",
				ddl:  fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN VAL TYPE VARCHAR(%d)`, ddlvTable, next),
			}
			old := v.valLen
			v.valLen = next
			step.revert = func() { v.valLen = old }
			return step
		}
		// at max length: normalize data, then step back down
		step = victimStep{
			name:      "VAL",
			normalize: `UPDATE ` + ddlvTable + ` SET VAL = NULL`,
			ddl:       fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN VAL TYPE VARCHAR(128)`, ddlvTable),
		}
		old := v.valLen
		v.valLen = 128
		step.revert = func() { v.valLen = old }
		return step
	case "CNT":
		if !v.cntBigint {
			step = victimStep{
				name: "CNT",
				ddl:  fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN CNT TYPE BIGINT`, ddlvTable),
			}
			v.cntBigint = true
			step.revert = func() { v.cntBigint = false }
			return step
		}
		step = victimStep{
			name:      "CNT",
			normalize: `UPDATE ` + ddlvTable + ` SET CNT = 0`,
			ddl:       fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN CNT TYPE INTEGER`, ddlvTable),
		}
		v.cntBigint = false
		step.revert = func() { v.cntBigint = true }
		return step
	case "NUM":
		if v.numPrecision < 15 {
			step = victimStep{
				name: "NUM",
				ddl:  fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN NUM TYPE NUMERIC(15,4)`, ddlvTable),
			}
			old := v.numPrecision
			v.numPrecision = 15
			step.revert = func() { v.numPrecision = old }
			return step
		}
		step = victimStep{
			name:      "NUM",
			normalize: `UPDATE ` + ddlvTable + ` SET NUM = ROUND(NUM, 2)`,
			ddl:       fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN NUM TYPE NUMERIC(12,2)`, ddlvTable),
		}
		old := v.numPrecision
		v.numPrecision = 12
		step.revert = func() { v.numPrecision = old }
		return step
	default: // VAL_NULL
		if v.valNullable {
			// fill NULLs first, then enforce NOT NULL (fails otherwise —
			// that expected refusal is part of the test matrix)
			step = victimStep{
				name:      "VAL_NULL",
				normalize: `UPDATE ` + ddlvTable + ` SET VAL = COALESCE(VAL, '')`,
				ddl:       fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN VAL SET NOT NULL`, ddlvTable),
			}
			v.valNullable = false
			step.revert = func() { v.valNullable = true }
			return step
		}
		step = victimStep{
			name: "VAL_NULL",
			ddl:  fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN VAL DROP NOT NULL`, ddlvTable),
		}
		v.valNullable = true
		step.revert = func() { v.valNullable = false }
		return step
	}
}

// --- sidecar integration ------------------------------------------------------

// victimTicker runs the phase-2 churn: one type step and one proc-signature
// flip per tick, with the caller goroutine racing the flips in between.
func (s *ddlSidecar) victimTicker(ctx context.Context, pool *sql.DB, opsL *opslog.Logger, counters *ExtendedCounters) {
	ticker := time.NewTicker(victimEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if s.cfg.AlterTypes {
				s.victimTypeStep(ctx, pool, opsL, counters)
			}
			if s.cfg.AlterProcs {
				s.victimProcStep(ctx, pool, opsL, counters)
			}
		}
	}
}

// victimTypeStep performs one ALTER COLUMN TYPE step (with its optional
// normalization DML). Counters: TypeAlterOK / TypeAlterExpectedFail.
func (s *ddlSidecar) victimTypeStep(ctx context.Context, pool *sql.DB, opsL *opslog.Logger, counters *ExtendedCounters) {
	s.v.mu.Lock()
	if !s.v.tableExists {
		s.v.mu.Unlock()
		s.victimEnsure(ctx, pool)
		s.victimDiscover(ctx, pool)
		s.v.mu.Lock()
		if !s.v.tableExists {
			s.v.mu.Unlock()
			return
		}
	}
	step := s.v.nextTypeStep()
	s.v.mu.Unlock()

	if step.normalize != "" {
		if err := s.dmlExec(ctx, pool, step.normalize); err != nil {
			// normalization is part of the step: without it the ALTER would
			// fail on data, which would teach us nothing new
			step.revert()
			return
		}
	}
	start := time.Now()
	txn := int64(0)
	if opsL != nil {
		txn = opsL.NextTx()
		opsL.TxStartRound(txn, "plusddl-ddl2", step.ddl)
	}
	committed := s.lifecycleTx(ctx, pool, opsL, func(tx *sql.Tx) error { return s.exec(ctx, tx, step.ddl) })
	if committed {
		if counters != nil {
			counters.TypeAlterOK.Add(1)
		}
	} else {
		step.revert()
		if counters != nil {
			counters.TypeAlterExpectedFail.Add(1)
		}
	}
	if opsL != nil {
		opsL.DDL(txn, step.ddl, time.Since(start), !committed)
		opsL.RoundCommit(txn, 0, 0, 0, time.Since(start), false)
	}
}

// victimProcStep flips SP_ELT_VICTIM to the other signature. Counters:
// ProcAlterOK / ProcAlterExpectedFail.
func (s *ddlSidecar) victimProcStep(ctx context.Context, pool *sql.DB, opsL *opslog.Logger, counters *ExtendedCounters) {
	s.v.mu.Lock()
	if s.v.procSig == "" {
		s.v.mu.Unlock()
		s.victimDiscover(ctx, pool)
		s.v.mu.Lock()
		if s.v.procSig == "" {
			s.v.mu.Unlock()
			return
		}
	}
	sig := "A"
	if s.v.procSig == "A" {
		sig = "B"
	}
	ddl := procVictimDDL(sig)
	s.v.mu.Unlock()

	start := time.Now()
	txn := int64(0)
	if opsL != nil {
		txn = opsL.NextTx()
		opsL.TxStartRound(txn, "plusddl-ddl2", ddl)
	}
	committed := s.lifecycleTx(ctx, pool, opsL, func(tx *sql.Tx) error { return s.exec(ctx, tx, ddl) })
	if committed {
		s.v.mu.Lock()
		s.v.procSig = sig
		s.v.mu.Unlock()
		if counters != nil {
			counters.ProcAlterOK.Add(1)
		}
	} else {
		if counters != nil {
			counters.ProcAlterExpectedFail.Add(1)
		}
	}
	if opsL != nil {
		opsL.DDL(txn, ddl, time.Since(start), !committed)
		opsL.RoundCommit(txn, 0, 0, 0, time.Since(start), false)
	}
}

// procCallerLoop calls SP_ELT_VICTIM dynamically on a short ticker, racing
// the signature flips. A signature mismatch re-arms discovery for the next
// tick. Counters: ProcCallOK / ProcCallRaceErr.
func (s *ddlSidecar) procCallerLoop(ctx context.Context, pool *sql.DB, opsL *opslog.Logger, counters *ExtendedCounters) {
	every := time.Duration(s.cfg.ProcCallEverySec) * time.Second
	if every <= 0 {
		every = 2 * time.Second
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.procCallerTick(ctx, pool, opsL, counters)
		}
	}
}

func (s *ddlSidecar) procCallerTick(ctx context.Context, pool *sql.DB, opsL *opslog.Logger, counters *ExtendedCounters) {
	s.v.mu.Lock()
	if !s.v.tableExists {
		s.v.mu.Unlock()
		return
	}
	sig := s.v.procSig
	s.v.mu.Unlock()
	if sig == "" {
		s.victimDiscover(ctx, pool)
		s.v.mu.Lock()
		sig = s.v.procSig
		s.v.mu.Unlock()
		if sig == "" {
			return
		}
	}

	start := time.Now()
	txn := int64(0)
	if opsL != nil {
		txn = opsL.NextTx()
	}
	var q string
	var args []any
	if sig == "B" {
		q = `SELECT OUT1, OUT2 FROM ` + ddlvProc + `(?, ?)`
		args = []any{42, "ddl"}
	} else {
		q = `SELECT OUT1 FROM ` + ddlvProc + `(?)`
		args = []any{42}
	}
	rows, err := pool.QueryContext(ctx, q, args...)
	if err != nil {
		if opsL != nil {
			opsL.DDL(txn, "caller "+q, time.Since(start), true)
		}
		if counters != nil && ddlExpectedError(err) {
			counters.ProcCallRaceErr.Add(1)
			if strings.Contains(strings.ToLower(err.Error()), "does not match") {
				// signature flipped under us: re-discover on the next tick
				s.v.mu.Lock()
				s.v.procSig = ""
				s.v.mu.Unlock()
			}
		}
		return
	}
	defer rows.Close()
	out1, out2 := 0, ""
	for rows.Next() {
		if sig == "B" {
			_ = rows.Scan(&out1, &out2)
		} else {
			_ = rows.Scan(&out1)
		}
	}
	if opsL != nil {
		opsL.DDL(txn, "caller "+q, time.Since(start), false)
	}
	if counters != nil {
		counters.ProcCallOK.Add(1)
	}
}

// dmlExec runs the normalization DML in its own committed transaction.
func (s *ddlSidecar) dmlExec(ctx context.Context, pool *sql.DB, q string) error {
	tx, err := pool.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, q); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// ddlVictimEnabled reports whether any phase-2 churn is on.
func ddlVictimEnabled(cfg config.PlusDDL) bool {
	return cfg.AlterTypes || cfg.AlterProcs
}
