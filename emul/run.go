package emul

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"
)

// Unit is one row of the oltp-emul business_ops registry: an executable
// stored procedure implementing a business action (order, invoice, payment,
// cancellation, self-check).
type Unit struct {
	Name   string `json:"name"`
	Mode   string `json:"mode"`   // stock | payments | service
	Kind   string `json:"kind"`   // creation | removal | state_next | state_back | service
	Weight int    `json:"weight"` // random_selection_weight; 0-weight units are never picked
}

// Unit phases: during warmup the database must GROW, so cancellations
// (kind=removal) and backward state changes are excluded - mirroring the
// warmup filter inside upstream srv_random_unit_choice.
var warmupKinds = map[string]bool{
	"creation":   true,
	"state_next": true,
	"service":    true,
}

// Queryer covers *sql.DB and *sql.Tx.
type Queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// LoadUnits reads the business_ops registry. Calling this requires the
// oltpemul schema (see SchemaGuard).
func LoadUnits(ctx context.Context, q Queryer) ([]Unit, error) {
	rows, err := q.QueryContext(ctx,
		`select unit, coalesce(mode,''), coalesce(kind,''), random_selection_weight
		 from business_ops order by sort_prior`)
	if err != nil {
		return nil, fmt.Errorf("emul: load business_ops: %w", err)
	}
	defer rows.Close()

	var units []Unit
	for rows.Next() {
		var u Unit
		if err := rows.Scan(&u.Name, &u.Mode, &u.Kind, &u.Weight); err != nil {
			return nil, fmt.Errorf("emul: scan business_ops: %w", err)
		}
		units = append(units, u)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(units) == 0 {
		return nil, errors.New("emul: business_ops registry is empty (run provision first)")
	}
	return units, nil
}

// SchemaGuard verifies the target database has the oltpemul schema, failing
// with an actionable message instead of cryptic SQL errors later.
func SchemaGuard(ctx context.Context, db *sql.DB) error {
	var n int
	err := db.QueryRowContext(ctx,
		`select count(*) from rdb$relations where rdb$relation_name = 'BUSINESS_OPS'`).Scan(&n)
	if err != nil {
		return fmt.Errorf("emul: schema check failed: %w (is this an oltpemul database? run provision first)", err)
	}
	if n == 0 {
		return errors.New("emul: table BUSINESS_OPS not found - provision the oltpemul schema first (see OLTP_EMUL_PLAN.md)")
	}
	return nil
}

// Selector picks units with weights, mirroring upstream srv_random_unit_choice
// but on the Go side (one less round trip per unit).
type Selector struct {
	units  []Unit
	weight int
	rng    *rand.Rand
}

func NewSelector(units []Unit, seed int64) *Selector {
	s := &Selector{units: units, rng: rand.New(rand.NewSource(seed))}
	for _, u := range units {
		if u.Weight > 0 {
			s.weight += u.Weight
		}
	}
	return s
}

// Pick chooses a random unit among those whose kind is allowed. Returns
// false only if no unit qualifies (e.g. empty registry).
func (s *Selector) Pick(allowedKinds map[string]bool) (Unit, bool) {
	if s.weight == 0 {
		return Unit{}, false
	}
	for range 100 {
		target := s.rng.Intn(s.weight)
		acc := 0
		for _, u := range s.units {
			if u.Weight <= 0 {
				continue
			}
			acc += u.Weight
			if target < acc {
				if allowedKinds != nil && !allowedKinds[u.Kind] {
					break // rejected: resample
				}
				return u, true
			}
		}
	}
	return Unit{}, false
}

// Outcome classifies one unit execution, following oltp-emul semantics:
// conflicts (deadlock / update conflict / lock timeout) are normal events
// under load; ex_* user exceptions are business rejections; everything else
// is a real failure.
type Outcome int

const (
	OutcomeOK Outcome = iota
	OutcomeConflict
	OutcomeRejected
	OutcomeFailure
)

func (o Outcome) String() string {
	switch o {
	case OutcomeOK:
		return "ok"
	case OutcomeConflict:
		return "conflict"
	case OutcomeRejected:
		return "rejected"
	default:
		return "failure"
	}
}

// NoWaitIsolation is the driver's custom sql.TxOptions isolation value:
// READ COMMITTED with NOWAIT lock resolution. oltp-emul units REQUIRE
// transactions started NO WAIT (or with a lock timeout) - SP_CHECK_NOWAIT_
// OR_TIMEOUT rejects otherwise - so workers running the oltp-emul profile
// must begin transactions with these options.
const NoWaitIsolation = sql.IsolationLevel(1000) // firebirdsql.LevelReadCommittedNoWait

// TxOptions returns the transaction options required for unit execution.
func TxOptions() *sql.TxOptions {
	return &sql.TxOptions{Isolation: NoWaitIsolation}
}

// SnapshotTxOptions returns the transaction options required by the
// invariant self-checks: SRV_MAKE_INVNT_SALDO / SRV_MAKE_MONEY_SALDO demand
// TIL = SNAPSHOT (they total the turnover logs and need a stable view).
func SnapshotTxOptions() *sql.TxOptions {
	return &sql.TxOptions{Isolation: sql.LevelRepeatableRead}
}

// ExecuteUnit runs one business unit inside the caller's transaction. A unit
// is an executable stored procedure with no input parameters; randomness
// lives server-side. The returned error is always nil for OutcomeOK; for
// OutcomeConflict/OutcomeRejected it carries a wrapped classification that
// callers may inspect with UnitOutcome, and the transaction MUST be rolled
// back (the SP raised, so its work is undone).
func ExecuteUnit(ctx context.Context, tx *sql.Tx, unit Unit) (elapsed time.Duration, outcome Outcome, err error) {
	start := time.Now()
	rows, qErr := tx.QueryContext(ctx, "execute procedure "+unit.Name)
	if qErr == nil {
		// Drain outputs (selectable procedures return rows).
		for rows.Next() {
		}
		qErr = rows.Err()
		rows.Close()
	}
	elapsed = time.Since(start)
	if qErr == nil {
		return elapsed, OutcomeOK, nil
	}
	return elapsed, classifyUnitError(qErr), fmt.Errorf("unit %s: %w", unit.Name, qErr)
}

// UnitError carries a classified unit outcome. It implements
// ExpectedUnderLoad() so ops.ClassifyError treats conflicts and business
// rejections as expected: the worker logs and continues instead of failing.
type UnitError struct {
	Unit    string
	Outcome Outcome
	Err     error
}

func (e *UnitError) Error() string {
	return fmt.Sprintf("unit %s: %s: %v", e.Unit, e.Outcome, e.Err)
}

func (e *UnitError) Unwrap() error { return e.Err }

// ExpectedUnderLoad is true for every outcome except a real failure.
func (e *UnitError) ExpectedUnderLoad() bool { return e.Outcome != OutcomeFailure }

// UnitOutcome lets the worker route the outcome into per-unit aggregation
// without importing concrete error types.
func (e *UnitError) UnitOutcome() Outcome { return e.Outcome }

// classifyUnitError maps a driver error onto an Outcome using the structured
// GDS codes (message-independent, unlike the EMPLOYEE-profile classifier).
func classifyUnitError(err error) Outcome {
	if fe, ok := fbError(err); ok {
		for _, code := range fe.GDSCodes {
			switch code {
			case firebirdISCDeadlock, firebirdISCUpdateConflict, firebirdISCLockTimeout:
				return OutcomeConflict
			}
		}
		// oltp-emul user exceptions are named ex_*; the driver renders them
		// with the exception name substituted into the message.
		if strings.Contains(fe.Message, "ex_") {
			return OutcomeRejected
		}
	}
	low := strings.ToLower(err.Error())
	if strings.Contains(low, "deadlock") || strings.Contains(low, "update conflict") ||
		strings.Contains(low, "lock conflict") || strings.Contains(low, "lock time-out") ||
		strings.Contains(low, "stuck in limbo") {
		return OutcomeConflict
	}
	if strings.Contains(low, "ex_") {
		return OutcomeRejected
	}
	return OutcomeFailure
}
