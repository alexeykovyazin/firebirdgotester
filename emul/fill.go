package emul

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// fillKinds is the unit filter for initial document population, mirroring
// the upstream fill phase (creation + forward state changes only).
var fillKinds = map[string]bool{
	"creation":   true,
	"state_next": true,
}

// Fill populates the database with initial documents by executing creation
// units until doc_list holds at least initDocs documents - the Go equivalent
// of upstream's generated document-filling script. Units that raise
// (business rejection, conflict) are counted and skipped.
func Fill(ctx context.Context, db *sql.DB, initDocs int, progress Progress) error {
	if progress == nil {
		progress = nopProgress
	}
	if initDocs <= 0 {
		return nil
	}
	units, err := LoadUnits(ctx, db)
	if err != nil {
		return err
	}
	sel := NewSelector(units, time.Now().UnixNano())

	var done, conflicts, rejected, failures int
	lastReport := time.Now()
	iterations := 0
	consecutiveFailures := 0
	var firstErr error
	const checkEvery = 25
	const maxConsecutiveFailures = 50 // real errors: systematic problem
	const maxStalledChecks = 40       // 40*checkEvery units without doc growth

	// Rejected (business) and conflicting units are normal during fill:
	// a state_next unit may run before its prerequisites exist and is
	// simply retried later (upstream swallows those the same way).
	stalledChecks := 0
	lastDocCount := -1

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		unit, ok := sel.Pick(fillKinds)
		if !ok {
			return fmt.Errorf("emul: no fillable units (kinds creation/state_next) in business_ops")
		}

		var tx *sql.Tx
		tx, err = db.BeginTx(ctx, TxOptions())
		if err != nil {
			return fmt.Errorf("emul: fill begin tx: %w", err)
		}
		_, outcome, execErr := ExecuteUnit(ctx, tx, unit)
		switch {
		case execErr == nil:
			if err := tx.Commit(); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("emul: fill commit %s: %w", unit.Name, err)
			}
			done++
			consecutiveFailures = 0
		default:
			_ = tx.Rollback()
			if firstErr == nil {
				firstErr = execErr
			}
			switch outcome {
			case OutcomeConflict:
				conflicts++
			case OutcomeRejected:
				rejected++
			default:
				failures++
				consecutiveFailures++
				if consecutiveFailures >= maxConsecutiveFailures {
					return fmt.Errorf("emul: fill aborted after %d consecutive failed units; last: %w (first: %v)",
						consecutiveFailures, execErr, firstErr)
				}
			}
		}

		if done >= initDocs {
			progress("fill complete: %d documents (%d conflicts, %d rejected, %d failed units)",
				done, conflicts, rejected, failures)
			return nil
		}
		iterations++
		if iterations%checkEvery == 0 || time.Since(lastReport) > 5*time.Second {
			var have int
			if err := db.QueryRowContext(ctx, `select count(*) from doc_list`).Scan(&have); err == nil {
				progress("fill: %d/%d documents (%d conflicts, %d rejected, %d failed units)",
					have, initDocs, conflicts, rejected, failures)
				if have >= initDocs {
					return nil
				}
				if have == lastDocCount {
					stalledChecks++
					if stalledChecks >= maxStalledChecks {
						return fmt.Errorf("emul: fill stalled: document count stuck at %d after %d units (%d conflicts, %d rejected, %d failed); last error: %v",
							have, stalledChecks*checkEvery, conflicts, rejected, failures, firstErr)
					}
				} else {
					stalledChecks = 0
					lastDocCount = have
				}
			}
			lastReport = time.Now()
		}
	}
}
