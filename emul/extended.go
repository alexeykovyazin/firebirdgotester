// Extended load mix bootstrap and observability (EXTENDED_LOAD_PLAN.md
// Phase 3 H1 / T9). Creates the per-launch aux schema (EL_BULK_ITEMS,
// EL_AUTON_LOG + SP_ELT_AUTON_LOG, EL2PC_LOG in the aux DB) next to the
// running emul database and hosts the shared extended counters.
package emul

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"fb-loadgen/config"
)

// ExtendedCounters aggregates the extended-load sidecar activity for the
// emul state API and the final report.
type ExtendedCounters struct {
	HeavyRounds     atomic.Int64
	HeavyFailures   atomic.Int64
	BulkInserts     atomic.Int64 // rounds
	BulkUpdates     atomic.Int64
	BulkDeletes     atomic.Int64
	BulkFailures    atomic.Int64
	BulkRows        atomic.Int64
	DDLRounds       atomic.Int64
	ColumnsAdded    atomic.Int64
	ColumnsAltered  atomic.Int64
	ColumnsDropped  atomic.Int64
	TablesCreated   atomic.Int64
	TablesDropped   atomic.Int64
	LimboResolved   atomic.Int64
	AutonValidated  atomic.Int64
	AutonViolations atomic.Int64
}

// isAlreadyExists recognizes Firebird's "object already exists" errors.
func isAlreadyExists(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "already exists")
}

// auxObjectName guards the aux tables against concurrent bootstrap runs.
// EL2PC_LOG lives in the aux database, the rest in the main database.
const (
	elBulkTable  = "EL_BULK_ITEMS"
	elAutonTable = "EL_AUTON_LOG"
	elAutonSP    = "SP_ELT_AUTON_LOG"
	elAuxDBName  = "EL_2PC.FDB"
	el2pcLog     = "EL2PC_LOG"
)

// BootstrapExtendedSchema creates the per-launch aux schema. Drop/recreate
// mirrors the perf_estimated pattern (oltp_adjust_DDL.sql) but runs at every
// run start (provisioning skips already-provisioned databases, R7). All
// literals are ASCII-only (the emul database is CHARACTER SET NONE, V11).
func BootstrapExtendedSchema(ctx context.Context, mainDB *sql.DB, cfg *config.Config) (auxDBPath string, err error) {
	el := cfg.ExtendedLoad
	el.Normalize()

	if _, err = mainDB.ExecContext(ctx, `CREATE SEQUENCE EL_BULK_SEQ`); err != nil && !isAlreadyExists(err) {
		return "", fmt.Errorf("extended bootstrap (bulk seq): %w", err)
	}

	stmts := []string{
		`RECREATE TABLE ` + elBulkTable + ` (
			ID INTEGER NOT NULL PRIMARY KEY,
			ROUND_ID INTEGER,
			PAYLOAD VARCHAR(200),
			VAL NUMERIC(18,2),
			CREATED_AT TIMESTAMP
		)`,
		`CREATE INDEX EL_BULK_ROUND ON ` + elBulkTable + ` (ROUND_ID)`,
		`RECREATE GLOBAL TEMPORARY TABLE ` + elAutonTable + ` (
			ID INTEGER NOT NULL PRIMARY KEY,
			NOTE VARCHAR(100),
			STAMP TIMESTAMP
		) ON COMMIT PRESERVE ROWS`,
	}
	for _, q := range stmts {
		if _, err = mainDB.ExecContext(ctx, q); err != nil {
			return "", fmt.Errorf("extended bootstrap: %w", err)
		}
	}
	if err = createAutonomousSP(ctx, mainDB); err != nil {
		return "", err
	}

	// Aux database for the 2PC completion variant (engine-side two-phase via
	// EXECUTE STATEMENT ... WITH COMMON TRANSACTION). Created in the main
	// database's directory; only its existence enables twoPhase draws.
	if dsn := auxDriverDSN(cfg); dsn != "" {
		if err = createAuxDatabase(ctx, dsn); err != nil {
			return "", fmt.Errorf("extended bootstrap (aux db): %w", err)
		}
		auxDBPath = dsn
	}
	return auxDBPath, nil
}

// auxDriverDSN builds the driver DSN of the aux database next to the main one.
func auxDriverDSN(cfg *config.Config) string {
	dbPath := dsnDatabase(cfg.DSN)
	if dbPath == "" {
		return ""
	}
	at := strings.LastIndex(cfg.DSN, "@")
	if at < 0 {
		return ""
	}
	dir := dbPath[:strings.LastIndex(dbPath, "/")+1]
	auxPath := dir + strings.ToLower(elAuxDBName)
	return cfg.DSN[:at+1] + auxPath
}

// createAuxDatabase creates the aux database through the driver's createdb
// path and makes sure the 2PC log table exists (idempotent).
func createAuxDatabase(ctx context.Context, dsn string) error {
	db, err := sql.Open("firebirdsql_createdb", dsn)
	if err != nil {
		return err
	}
	_, execErr := db.ExecContext(ctx, "select * from rdb$database")
	db.Close()
	if execErr != nil && !strings.Contains(execErr.Error(), "exists") {
		return execErr
	}
	db, err = sql.Open("firebirdsql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	qctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err = db.ExecContext(qctx, `RECREATE TABLE `+el2pcLog+` (STAMP TIMESTAMP, NOTE VARCHAR(60))`); err != nil {
		return err
	}
	return nil
}

func createAutonomousSP(ctx context.Context, db *sql.DB) error {
	// IN AUTONOMOUS TRANSACTION: the row survives the outer rollback (T5).
	q := `RECREATE PROCEDURE ` + elAutonSP + ` (NOTE VARCHAR(100))
AS
BEGIN
	IN AUTONOMOUS TRANSACTION DO
		INSERT INTO ` + elAutonTable + ` (ID, NOTE, STAMP) VALUES (NEXT VALUE FOR EL_AUTON_SEQ, :NOTE, CURRENT_TIMESTAMP);
END`
	if _, err := db.ExecContext(ctx, `CREATE SEQUENCE EL_AUTON_SEQ`); err != nil && !isAlreadyExists(err) {
		return fmt.Errorf("extended bootstrap (sequence): %w", err)
	}
	if _, err := db.ExecContext(ctx, q); err != nil {
		return fmt.Errorf("extended bootstrap (autonomous sp): %w", err)
	}
	return nil
}

// auxDatabasePath returns the filesystem path of the aux database next to the
// main database file.
func auxDatabasePath(cfg *config.Config) string {
	if p := cfg.ExtendedLoad.TxVariants.TwoPhaseAuxDB; p != "" && p != "<mainDbDir>/EL_2PC.FDB" {
		return p
	}
	dbFile := dsnDatabase(cfg.DSN)
	if dbFile == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(dbFile), elAuxDBName)
}

func AuxDatabaseReady(ctx context.Context, dsn string) bool {
	db, err := sql.Open("firebirdsql", dsn)
	if err != nil {
		return false
	}
	defer db.Close()
	qctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err = db.ExecContext(qctx, "select 1 from rdb$database")
	return err == nil
}

// defaultCounters is the package-level extended counters instance used by
// the session sidecars.
var defaultCounters ExtendedCounters

// Extended returns the package-level extended counters singleton.
func Extended() *ExtendedCounters {
	return &defaultCounters
}

// SnapshotJSON renders the current extended counters for the emul state API
// and the final report (nil = nothing happened, section omitted).
func (c *ExtendedCounters) SnapshotJSON() *ExtendedJSON {
	if c == nil {
		return nil
	}
	if c.HeavyRounds.Load()+c.BulkInserts.Load()+c.BulkUpdates.Load()+c.BulkDeletes.Load()+
		c.ColumnsAdded.Load()+c.LimboResolved.Load() == 0 {
		return nil
	}
	return &ExtendedJSON{
		HeavyRounds:    c.HeavyRounds.Load(),
		HeavyFailures:  c.HeavyFailures.Load(),
		BulkInserts:    c.BulkInserts.Load(),
		BulkUpdates:    c.BulkUpdates.Load(),
		BulkDeletes:    c.BulkDeletes.Load(),
		BulkFailures:   c.BulkFailures.Load(),
		BulkRows:       c.BulkRows.Load(),
		ColumnsAdded:   c.ColumnsAdded.Load(),
		ColumnsAltered: c.ColumnsAltered.Load(),
		ColumnsDropped: c.ColumnsDropped.Load(),
		TablesCreated:  c.TablesCreated.Load(),
		TablesDropped:  c.TablesDropped.Load(),
		LimboResolved:  c.LimboResolved.Load(),
	}
}
