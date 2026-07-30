package db

import (
	"fmt"
	"time"

	"fb-loadgen/config"
)

// DialSettings holds connection parameters independent of a shared CLI config.
type DialSettings struct {
	// DriverDSN is the full firebirdsql connection string: user:pass@host:port/path
	DriverDSN string
	// DisplayDSN is the user-facing DSN (host/port:path) without credentials
	DisplayDSN string
	User       string
	TxTimeout  time.Duration
}

// DialFromConfig builds DialSettings from a CLI/session config.
func DialFromConfig(cfg *config.Config) DialSettings {
	return DialSettings{
		DriverDSN:  cfg.ConnectionString(),
		DisplayDSN: cfg.DSN,
		User:       cfg.User,
		TxTimeout:  cfg.GetTxTimeout(),
	}
}

// String returns a safe summary for logging (no password).
func (d DialSettings) String() string {
	return fmt.Sprintf("Firebird connection: %s (user: %s)", d.DisplayDSN, d.User)
}
