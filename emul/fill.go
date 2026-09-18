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
	const checkEvery = 25

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		unit, ok := sel.Pick(fillKinds)
		if !ok {
			return fmt.Errorf("emul: no fillable units (kinds creation/state_next) in business_ops")
		}

		var tx *sql.Tx
		tx, err = db.BeginTx(ctx, nil)
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
		default:
			_ = tx.Rollback()
			switch outcome {
			case OutcomeConflict:
				conflicts++
			case OutcomeRejected:
				rejected++
			default:
				failures++
			}
		}

		if done >= initDocs {
			progress("fill complete: %d documents (%d conflicts, %d rejected, %d failed units)",
				done, conflicts, rejected, failures)
			return nil
		}
		if done%checkEvery == 0 || time.Since(lastReport) > 5*time.Second {
			var have int
			if err := db.QueryRowContext(ctx, `select count(*) from doc_list`).Scan(&have); err == nil {
				progress("fill: %d/%d documents (%d conflicts, %d rejected, %d failed units)",
					have, initDocs, conflicts, rejected, failures)
				if have >= initDocs {
					return nil
				}
			}
			lastReport = time.Now()
		}
	}
}
