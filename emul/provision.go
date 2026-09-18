// Package emul embeds the FirebirdSQL oltp-emul workload (MIT, (C) Pavel
// Zotov - see assets/NOTICE) into fb-loadgen: it provisions the oltpemul
// benchmark database and runs its business-process units from Go instead of
// the upstream isql/bash orchestration.
package emul

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"strings"
	"time"

	_ "github.com/nakagami/firebirdsql" // registers "firebirdsql" and "firebirdsql_createdb"
)

//go:embed assets/*.sql
var assetsFS embed.FS

func asset(name string) (string, error) {
	b, err := assetsFS.ReadFile("assets/" + name)
	if err != nil {
		return "", fmt.Errorf("emul: embedded asset %s: %w", name, err)
	}
	return string(b), nil
}

// Script execution order, mirroring upstream 1run_oltp_emul.sh build phase.
// provisionScripts run in this order; oltp_adjust_DDL must come AFTER
// settings injection (it reads separate_workers and rebuilds perf_estimated
// with its before-insert trigger) and BEFORE any unit executes (units call
// sp_add_perf_log, which inserts into v_perf_estimated).
var provisionScripts = []string{
	"oltp30_DDL.sql",
	"oltp30_sp.sql",
	"oltp_common_sp.sql",
	"oltp_main_filling.sql",
	"oltp_adjust_DDL.sql",
	"oltp_data_filling.sql",
}

// Config locates the target database. DBPath is the SERVER-SIDE file path
// (e.g. `C:\data\oltpemul.fdb` or `/var/db/oltpemul.fdb`); provisioning and
// benchmark runs require a server-local path, exactly like upstream oltp-emul.
type Config struct {
	Host     string
	Port     string
	User     string
	Password string
	DBPath   string

	// PageSize of the created database. oltp-emul hardcodes 8192; the driver
	// cannot pass a page size on create, so a value other than the driver
	// default (4096) is applied via a services-manager backup/restore round
	// trip. Set 0 to keep the driver default.
	PageSize int

	// WorkingMode stored in the settings table. It is NOT 'common': it
	// selects one of the workload profiles seeded by oltp_main_filling.sql
	// (DEBUG_01..04, DEBUG_1A, SMALL_01..03, MEDIUM_01..03, LARGE_01..03,
	// HEAVY_01), which key settings like C_NUMBER_OF_AGENTS and doc-size
	// limits. Upstream sample configs default to DEBUG_01.
	WorkingMode string
}

func (c *Config) DSN() string {
	return fmt.Sprintf("%s:%s@%s:%s/%s", c.User, c.Password, c.Host, c.Port, c.DBPath)
}

func (c *Config) workingMode() string {
	if c.WorkingMode == "" {
		return "SMALL_01"
	}
	return c.WorkingMode
}

func (c *Config) pageSize() int {
	if c.PageSize == 0 {
		return 4096 // the driver's opCreate default
	}
	return c.PageSize
}

// Progress receives human-readable provisioning milestones (and fill counts).
// It must be safe to call from the provisioning goroutine.
type Progress func(format string, args ...any)

func nopProgress(string, ...any) {}

// Provision creates and populates the oltpemul database. An existing database
// at DBPath is reused as-is if it already contains the schema (idempotent);
// use a different DBPath to start over.
func Provision(ctx context.Context, cfg Config, progress Progress) error {
	if progress == nil {
		progress = nopProgress
	}
	if err := createDatabase(ctx, cfg, progress); err != nil {
		return fmt.Errorf("emul: create database: %w", err)
	}

	// charset NONE matches upstream isql sessions (the database is created
	// CHARACTER SET NONE; e.g. adjust_DDL declares varchar(32765), which
	// would exceed the statement limit under a UTF8 connection).
	db, err := sql.Open("firebirdsql", cfg.DSN()+"?charset=NONE")
	if err != nil {
		return fmt.Errorf("emul: open: %w", err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("emul: connect: %w", err)
	}

	if provisioned(ctx, db) {
		progress("schema already present, skipping scripts")
		return nil
	}

	for _, name := range provisionScripts {
		progress("running %s ...", name)
		start := time.Now()
		if err := RunScript(ctx, db, name, progress); err != nil {
			return fmt.Errorf("emul: run %s: %w", name, err)
		}
		progress("%s done in %s", name, time.Since(start).Round(time.Millisecond))
		if name == "oltp_main_filling.sql" {
			if err := injectSettings(ctx, db, cfg); err != nil {
				return fmt.Errorf("emul: inject settings: %w", err)
			}
			progress("settings injected (working_mode=%s)", cfg.workingMode())
		}
	}

	// Final build step upstream (1run_oltp_emul.sh "ACTIVATE DB-LEVEL
	// TRIGGERS"): re-enable the CONNECT trigger so every new connection
	// loads its USER_SESSION context via sp_init_ctx. Without this, every
	// unit fails with EX_CONTEXT_VAR_NOT_FOUND.
	if err := activateTriggers(ctx, db); err != nil {
		return fmt.Errorf("emul: activate triggers: %w", err)
	}
	progress("trg_connect activated")

	return verifySchema(ctx, db)
}

// activateTriggers re-enables the connect trigger and disables the
// disconnect one (we have no EDS connection pool to track).
func activateTriggers(ctx context.Context, db *sql.DB) error {
	stmts := []string{
		`alter trigger trg_connect active`,
		// Best effort: TRG_DISCONNECT only exists in some builds.
		`alter trigger trg_disconnect inactive`,
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			if strings.Contains(err.Error(), "TRG_DISCONNECT") {
				continue
			}
			return err
		}
	}
	return nil
}

// provisioned reports whether the oltpemul schema already exists.
func provisioned(ctx context.Context, db *sql.DB) bool {
	var n int
	if err := db.QueryRowContext(ctx,
		`select count(*) from rdb$relations where rdb$relation_name = 'BUSINESS_OPS'`,
	).Scan(&n); err != nil {
		return false
	}
	return n > 0
}

// verifySchema checks post-provision object counts - a misparsed script that
// silently dropped objects must fail loudly here.
func verifySchema(ctx context.Context, db *sql.DB) error {
	checks := []struct {
		sql  string
		min  int
		what string
	}{
		{`select count(*) from rdb$procedures`, 100, "procedures"},
		{`select count(*) from rdb$relations where coalesce(rdb$system_flag,0) = 0`, 20, "user tables"},
		{`select count(*) from business_ops`, 5, "business_ops registry rows"},
	}
	for _, c := range checks {
		var n int
		if err := db.QueryRowContext(ctx, c.sql).Scan(&n); err != nil {
			return fmt.Errorf("verify %s: %w", c.what, err)
		}
		if n < c.min {
			return fmt.Errorf("verify %s: got %d, want >= %d (a script was likely misparsed)", c.what, n, c.min)
		}
	}
	return nil
}

// injectSettings replicates the essential inject_actual_setting calls from
// upstream 1run_oltp_emul.sh. sp_init_ctx (a CONNECT trigger) reads these
// rows into USER_SESSION variables on every connection, so they drive unit
// behavior at runtime.
//
// The settings table has a computed unique index on mcode (COMMON and INIT
// scopes collapse to the same key), so each mcode is updated wherever it
// already lives and only inserted when absent - never inserted per-scope.
func injectSettings(ctx context.Context, db *sql.DB, cfg Config) error {
	settings := []struct{ mcode, svalue string }{
		{"working_mode", cfg.workingMode()},
		{"use_es", "0"},
		{"unit_selection_method", "random"},
		{"separate_workers", "0"},
		{"update_conflict_percent", "0"},
		{"enable_mon_query", "0"},
		// Remaining placeholders main_filling seeds as
		// '*** TAKE AT RUNTIME FROM CONFIG ***' - defaults mirror the
		// upstream sample configs (non-replicated standalone test).
		{"used_in_replication", "0"},
		{"mon_unit_list", ""},
		{"halt_test_on_errors", "/CK/"},
		{"qmism_verify_bitset", "1"},
		{"recalc_idx_min_interval", "30"},
	}
	for _, s := range settings {
		res, err := db.ExecContext(ctx,
			`update settings set svalue = ? where upper(mcode) = upper(?)`,
			s.svalue, s.mcode)
		if err != nil {
			return fmt.Errorf("setting %s: update: %w", s.mcode, err)
		}
		if affected, _ := res.RowsAffected(); affected > 0 {
			continue
		}
		if _, err := db.ExecContext(ctx,
			`insert into settings(working_mode, mcode, svalue) values (upper('init'), upper(?), ?)`,
			s.mcode, s.svalue); err != nil {
			return fmt.Errorf("setting %s: insert: %w", s.mcode, err)
		}
	}
	return nil
}

// RunScript parses an embedded isql script and executes it over db with
// isql semantics: the vendored scripts run with AUTODDL OFF, so all
// statements execute inside one open transaction and only explicit COMMIT /
// ROLLBACK statements (plus end-of-script) close it. Standalone isql-only
// commands are skipped; standalone EXIT commits and stops the script, QUIT
// rolls back and stops it.
func RunScript(ctx context.Context, db *sql.DB, assetName string, progress Progress) error {
	text, err := asset(assetName)
	if err != nil {
		return err
	}
	stmts, err := ParseScript(text)
	if err != nil {
		return fmt.Errorf("parse: %w", err)
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	var tx *sql.Tx
	closeTx := func(commit bool) error {
		if tx == nil {
			return nil
		}
		t := tx
		tx = nil
		if commit {
			return t.Commit()
		}
		return t.Rollback()
	}
	exec := func(query string) error {
		if tx != nil {
			_, err := tx.ExecContext(ctx, query)
			return err
		}
		_, err := conn.ExecContext(ctx, query)
		return err
	}

	for i, s := range stmts {
		if err := ctx.Err(); err != nil {
			_ = closeTx(false)
			return err
		}
		switch s.Kind {
		case StmtSkip:
			continue
		case StmtCommit:
			if err := closeTx(true); err != nil {
				return fmt.Errorf("line %d: commit: %w", s.Line, err)
			}
		case StmtRollback:
			if err := closeTx(false); err != nil {
				return fmt.Errorf("line %d: rollback: %w", s.Line, err)
			}
		case StmtExit:
			if err := closeTx(true); err != nil {
				return fmt.Errorf("line %d: exit: %w", s.Line, err)
			}
			progress("script %s: EXIT at line %d, %d/%d statements executed", assetName, s.Line, i+1, len(stmts))
			return nil
		case StmtQuit:
			if err := closeTx(false); err != nil {
				return fmt.Errorf("line %d: quit: %w", s.Line, err)
			}
			progress("script %s: QUIT at line %d, %d/%d statements executed", assetName, s.Line, i+1, len(stmts))
			return nil
		default:
			if tx == nil {
				if tx, err = conn.BeginTx(ctx, nil); err != nil {
					return fmt.Errorf("line %d: begin: %w", s.Line, err)
				}
			}
			if err := exec(s.SQL); err != nil {
				_ = closeTx(false)
				return scriptError(assetName, s, err)
			}
		}
	}
	return closeTx(true)
}

// scriptError renders an execution failure with enough context to locate the
// offending statement in the vendored script.
func scriptError(assetName string, s Statement, err error) error {
	head := s.SQL
	if i := strings.IndexByte(head, '\n'); i >= 0 {
		head = head[:i]
	}
	if len(head) > 120 {
		head = head[:120]
	}
	return fmt.Errorf("line %d (%s...): %w", s.Line, head, err)
}
