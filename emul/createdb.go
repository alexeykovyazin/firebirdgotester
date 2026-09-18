package emul

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// createDatabase makes an empty database at cfg.DBPath with cfg.PageSize.
//
// Strategy: isql carries a real CREATE DATABASE ... PAGE_SIZE clause, so use
// it when an isql executable can be found (exact page size, one step). If
// isql is unavailable, fall back to the fork's firebirdsql_createdb driver
// (which creates at 4096) and then attempt a services-manager
// backup/restore round trip to rewrite the file at the requested page size;
// if that also fails, provisioning continues at 4096 with a warning.
func createDatabase(ctx context.Context, cfg Config, progress Progress) error {
	if cfg.pageSize() == 4096 {
		return createDatabaseDriver(ctx, cfg)
	}
	if isql, err := findIsql(); err == nil {
		if err := createDatabaseIsql(ctx, isql, cfg); err != nil {
			return fmt.Errorf("isql create failed: %w", err)
		}
		progress("database created via isql (page size %d)", cfg.pageSize())
		return nil
	}
	progress("isql not found, creating via driver (default page size 4096)...")
	if err := createDatabaseDriver(ctx, cfg); err != nil {
		return err
	}
	if err := resizePageSize(ctx, cfg); err != nil {
		progress("WARNING: could not apply page size %d (%v); continuing at 4096", cfg.pageSize(), err)
	} else {
		progress("database created via driver + backup/restore (page size %d)", cfg.pageSize())
	}
	return nil
}

// createDatabaseDriver creates an empty database via the fork's
// firebirdsql_createdb driver variant (always page size 4096).
func createDatabaseDriver(ctx context.Context, cfg Config) error {
	db, err := sql.Open("firebirdsql_createdb", cfg.DSN())
	if err != nil {
		return err
	}
	defer db.Close()
	// Any statement on the connection materializes the create.
	if _, err := db.ExecContext(ctx, "select * from rdb$database"); err != nil {
		return err
	}
	return nil
}

// createDatabaseIsql runs CREATE DATABASE through an isql executable,
// which supports an explicit page size.
func createDatabaseIsql(ctx context.Context, isql string, cfg Config) error {
	script := fmt.Sprintf("CREATE DATABASE '%s/%s:%s' PAGE_SIZE %d DEFAULT CHARACTER SET NONE;\n", cfg.Host, cfg.Port, cfg.DBPath, cfg.pageSize())
	tmp, err := os.CreateTemp("", "fbloadgen_create_*.sql")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString(script); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	cmdCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(cmdCtx, isql, "-user", cfg.User, "-pass", cfg.Password, "-q", "-i", tmp.Name())
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// resizePageSize matches oltp-emul's page size when the database was created
// by the driver at 4096: a services-manager backup + restore round trip
// rewrites the file at the requested page size.
func resizePageSize(ctx context.Context, cfg Config) error {
	mgr, err := newBackupManager(cfg)
	if err != nil {
		return err
	}
	tmp := cfg.DBPath + ".tmp.fbk"
	defer func() { _ = removeLocalFile(tmp) }()
	if err := mgr.Backup(cfg.DBPath, tmp, backupOptions(), nil); err != nil {
		return fmt.Errorf("backup: %w", err)
	}
	if err := mgr.Restore(tmp, cfg.DBPath, restoreOptions(cfg.pageSize()), nil); err != nil {
		return fmt.Errorf("restore: %w", err)
	}
	return nil
}

// findIsql looks for an isql executable: FIREBIRD_ISQL env var, PATH, then
// common Firebird install locations.
func findIsql() (string, error) {
	if p := os.Getenv("FIREBIRD_ISQL"); p != "" {
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
	}
	if p, err := exec.LookPath("isql"); err == nil {
		return p, nil
	}
	if p, err := exec.LookPath("isql-fb"); err == nil {
		return p, nil
	}
	var candidates []string
	switch runtime.GOOS {
	case "windows":
		for _, base := range []string{`C:\Program Files\Firebird`, `C:\Program Files (x86)\Firebird`, `C:\HQbird`} {
			matches, _ := filepath.Glob(filepath.Join(base, "*", "bin", "isql.exe"))
			matches2, _ := filepath.Glob(filepath.Join(base, "Firebird*", "isql.exe"))
			candidates = append(candidates, matches...)
			candidates = append(candidates, matches2...)
		}
	default:
		for _, p := range []string{"/usr/bin/isql-fb", "/usr/local/firebird/bin/isql", "/opt/firebird/bin/isql"} {
			candidates = append(candidates, p)
		}
	}
	for _, p := range candidates {
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
	}
	return "", fmt.Errorf("isql executable not found")
}
