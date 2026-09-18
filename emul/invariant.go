package emul

import (
	"context"
	"database/sql"
	"fmt"
)

// invariantProcedures are the upstream self-checks verifying that stock and
// money remain conserved across all business operations. A raised exception
// from either means the workload model has been violated - the strongest
// correctness signal a load generator can produce.
var invariantProcedures = []string{
	"SRV_MAKE_INVNT_SALDO",
	"SRV_MAKE_MONEY_SALDO",
}

// CheckInvariants runs both self-check procedures inside the caller's
// transaction (these are selectable procedures; their row output is the
// recomputed saldo and is deliberately discarded - only success/failure
// matters here).
func CheckInvariants(ctx context.Context, tx *sql.Tx) error {
	for _, name := range invariantProcedures {
		rows, err := tx.QueryContext(ctx, "select * from "+name)
		if err != nil {
			return fmt.Errorf("emul: invariant %s failed: %w", name, err)
		}
		for rows.Next() {
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("emul: invariant %s failed: %w", name, err)
		}
		rows.Close()
	}
	return nil
}
