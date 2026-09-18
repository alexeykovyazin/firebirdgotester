package emul

import (
	"errors"
	"fmt"
	"net"
	"os"

	"github.com/nakagami/firebirdsql"
)

// This file isolates all direct firebirdsql driver specifics (DSN building,
// services manager, structured errors) behind small helpers.

// GDS codes relevant for unit classification (aliases keep run.go free of
// driver imports).
const (
	firebirdISCDeadlock       = firebirdsql.ISCDeadlock       // 335544336
	firebirdISCUpdateConflict = firebirdsql.ISCUpdateConflict // 335544451
	firebirdISCLockTimeout    = firebirdsql.ISCLockTimeout    // 335544510
)

func newBackupManager(cfg Config) (*firebirdsql.BackupManager, error) {
	addr := net.JoinHostPort(cfg.Host, cfg.Port)
	mgr, err := firebirdsql.NewBackupManager(addr, cfg.User, cfg.Password, firebirdsql.ServiceManagerOptions{
		AuthPlugin: "Srp256",
		WireCrypt:  false,
	})
	if err != nil {
		return nil, fmt.Errorf("services manager on %s: %w", addr, err)
	}
	return mgr, nil
}

func backupOptions() firebirdsql.BackupOptions {
	opts := firebirdsql.GetDefaultBackupOptions()
	opts.GarbageCollect = false // empty database, nothing to collect
	return opts
}

func restoreOptions(pageSize int) firebirdsql.RestoreOptions {
	opts := firebirdsql.GetDefaultRestoreOptions()
	opts.PageSize = int32(pageSize)
	opts.Replace = true
	return opts
}

// removeLocalFile removes the temporary backup file. The backup path is
// SERVER-side, so removal only succeeds when the client runs on the same
// host as the server (the normal provisioning case); otherwise the file is
// left behind and this is silently ignored.
func removeLocalFile(path string) error {
	if path == "" {
		return nil
	}
	return os.Remove(path)
}

// fbError unwraps a driver-structured Firebird error, if that is what err is.
func fbError(err error) (*firebirdsql.FbError, bool) {
	var fe *firebirdsql.FbError
	if errors.As(err, &fe) {
		return fe, true
	}
	return nil, false
}
