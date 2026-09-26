// Limbo recovery sidecar (T9): discovers transactions left in limbo by the
// limbo completion variant (prepare-then-die) and resolves them round-robin
// via the native Services API — no gfix subprocess needed. Every resolution
// lands in the ops log as an empty-brackets record:
//
//	[] RESOLVED commit trn=42 (gap_sec: 12.3)
package emul

import (
	"context"
	"math/rand"
	"strings"
	"time"

	firebirdsql "github.com/nakagami/firebirdsql"

	"fb-loadgen/config"
	"fb-loadgen/opslog"
)

// limboRecoveryMethod selects the resolution applied by the sidecar.
type limboRecoveryMethod string

const (
	limboCommit   limboRecoveryMethod = "commit"
	limboRollback limboRecoveryMethod = "rollback"
	limboTwoPhase limboRecoveryMethod = "two_phase"
)

// StartLimboRecovery launches the recovery ticker. It returns immediately;
// the sidecar stops when ctx is cancelled. DB path and credentials come from
// the session config (DSN host:port + db file path). A nil opsLog disables
// logging; every resolution still counts into limboResolved.
func StartLimboRecovery(ctx context.Context, cfg *config.Config, opsL *opslog.Logger, counters *ExtendedCounters) {
	el := cfg.ExtendedLoad
	el.Normalize()
	// Poll fast and independent of the picker's rare-completion gap: every
	// second a prepared 2PC transaction stays unresolved, emul units hitting
	// its rows fail with "record ... is stuck in limbo", so resolution
	// latency, not the draw rate, is what bounds the damage.
	const gap = 2 * time.Second
	addr := dsnAddr(cfg.DSN)
	if addr == "" || cfg.User == "" {
		return
	}
	dbPath := dsnDatabase(cfg.DSN)
	methods := []limboRecoveryMethod{limboCommit, limboRollback, limboTwoPhase}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	go func() {
		// MaintenanceManager opens a fresh service attachment per call and
		// holds no persistent connection — nothing to close here.
		mm, merr := firebirdsql.NewMaintenanceManager(addr, cfg.User, cfg.Pass, firebirdsql.GetDefaultServiceManagerOptions())
		if merr != nil {
			return
		}
		ticker := time.NewTicker(gap)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				tids, err := mm.GetLimboTransactions(dbPath)
				if err != nil || len(tids) == 0 {
					continue
				}
				start := time.Now()
				for _, tid := range tids {
					m := methods[rng.Intn(len(methods))]
					var rerr error
					switch m {
					case limboCommit:
						rerr = mm.CommitLimboTransaction(dbPath, tid)
					case limboRollback:
						rerr = mm.RollbackLimboTransaction(dbPath, tid)
					default:
						rerr = mm.TwoPhaseRecovery(dbPath, tid)
					}
					if rerr == nil {
						if counters != nil {
							counters.LimboResolved.Add(1)
						}
						if opsL != nil {
							opsL.Resolved(string(m), tid, time.Since(start))
						}
					}
				}
			}
		}
	}()
}

// dsnAddr extracts "host:port" from a DSN. Both shapes occur: the driver
// format "user:pass@host:port/path" (session mode) and the CLI format
// "host[/port]:path" (runCLI).
func dsnAddr(dsn string) string {
	if at := strings.LastIndex(dsn, "@"); at >= 0 {
		rest := dsn[at+1:]
		if slash := strings.Index(rest, "/"); slash >= 0 {
			return rest[:slash]
		}
		return rest
	}
	// CLI format: host[/port]:path
	head := dsn
	if colon := strings.Index(head, ":"); colon >= 0 {
		head = head[:colon]
	}
	return strings.Replace(head, "/", ":", 1)
}

// dsnDatabase extracts the database path from a DSN (driver or CLI format).
func dsnDatabase(dsn string) string {
	if at := strings.LastIndex(dsn, "@"); at >= 0 {
		rest := dsn[at+1:]
		if slash := strings.Index(rest, "/"); slash >= 0 {
			return rest[slash+1:]
		}
		return ""
	}
	if colon := strings.Index(dsn, ":"); colon >= 0 {
		return dsn[colon+1:]
	}
	return ""
}
