// Limbo recovery sidecar (T9): discovers transactions left in limbo by the
// limbo completion variant (prepare-then-die) and resolves them round-robin
// via the native Services API — no gfix subprocess needed. Every resolution
// lands in the ops log as an empty-brackets record:
//
//	[] RESOLVED commit trn=42 (gap_sec: 12.3)
package emul

import (
	"context"
	"log"
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
			// The limbo-recovery feature silently did not exist when this
			// failed — make it visible; prepare-then-die scenarios would
			// otherwise leave limbo transactions unresolved with no trace.
			log.Printf("[emul-limbo] recovery disabled: maintenance manager unavailable: %v", merr)
			return
		}
		ticker := time.NewTicker(gap)
		defer ticker.Stop()
		// firstSeen bounds the observed prepare→resolve latency: the sidecar
		// polls every 2 s, so an age measured from the first scan that saw
		// the tid upper-bounds the real limbo stay by one gap.
		firstSeen := make(map[int64]time.Time)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				tids, err := mm.GetLimboTransactions(dbPath)
				if err != nil || len(tids) == 0 {
					continue
				}
				now := time.Now()
				if counters != nil {
					bumpMax(&counters.LimboPeak, int64(len(tids)))
				}
				for _, tid := range tids {
					if _, seen := firstSeen[tid]; !seen {
						firstSeen[tid] = now
					}
				}
				start := now
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
					if rerr != nil {
						continue
					}
					// Action-level failures are silent: the services API
					// reports them only through output lines that
					// resolveLimbo never reads. Until the server sweeps the
					// dead prepare-then-die attachment, commit/rollback of
					// its prepared transaction fails without any error on
					// the wire (verified on FB4). Count a resolution only
					// once the tid has actually left the limbo list; the
					// next tick retries otherwise. If the re-list call
					// itself fails, the outcome is unknown — do not count
					// (the previous fall-through counted unverified
					// resolutions).
					still, verr := mm.GetLimboTransactions(dbPath)
					if verr != nil {
						continue
					}
					if limboListHas(still, tid) {
						continue
					}
					if counters != nil {
						counters.LimboResolved.Add(1)
						switch m {
						case limboCommit:
							counters.LimboCommit.Add(1)
						case limboRollback:
							counters.LimboRollback.Add(1)
						default:
							counters.LimboTwoPhase.Add(1)
						}
						if first, ok := firstSeen[tid]; ok {
							bumpMax(&counters.LimboMaxAgeSec, int64(time.Since(first).Seconds()))
						}
					}
					delete(firstSeen, tid)
					if opsL != nil {
						opsL.Resolved(string(m), tid, time.Since(start))
					}
				}
			}
		}
	}()
}

// limboListHas reports whether the limbo list still contains tid.
func limboListHas(tids []int64, tid int64) bool {
	for _, t := range tids {
		if t == tid {
			return true
		}
	}
	return false
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
