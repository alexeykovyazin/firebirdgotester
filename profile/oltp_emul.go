package profile

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"fb-loadgen/emul"
	"fb-loadgen/ops"
)

// oltpEmulProfile runs the oltp-emul business-process units (see
// OLTP_EMUL_PLAN.md). Units are loaded from the target database's
// business_ops registry at construction time; one unit = one transaction.
//
// Deviation from upstream (documented in the plan): during the ramp warmup
// phase removals are NOT excluded - phase awareness is not plumbed through
// the Profile interface in v1, and the provision-time fill already grew the
// database creation-only. Conflicts and business rejections are expected
// outcomes (see emul.UnitError); only real failures are unexpected.
type oltpEmulProfile struct {
	selector *emul.Selector
	weights  []OpWeight
}

// NewOLTPEmulProfile builds the profile from a loaded unit registry.
func NewOLTPEmulProfile(units []emul.Unit, seed int64) *oltpEmulProfile {
	p := &oltpEmulProfile{
		selector: emul.NewSelector(units, seed),
	}
	for _, u := range units {
		if u.Weight <= 0 {
			continue
		}
		unit := u // capture
		p.weights = append(p.weights, OpWeight{
			Weight: unit.Weight,
			Name:   unit.Name,
			Op: func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error {
				_, outcome, err := emul.ExecuteUnit(ctx, tx, unit)
				if err == nil {
					return nil
				}
				return &emul.UnitError{Unit: unit.Name, Outcome: outcome, Err: err}
			},
		})
	}
	return p
}

func (p *oltpEmulProfile) Name() string { return "oltp-emul" }

func (p *oltpEmulProfile) NextOp() func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error {
	op, _ := p.NextOpWithName()
	return op
}

func (p *oltpEmulProfile) NextOpWithName() (func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error, string) {
	unit, ok := p.selector.Pick(nil)
	if !ok {
		return nil, ""
	}
	for _, w := range p.weights {
		if w.Name == unit.Name {
			return w.Op, unit.Name
		}
	}
	return nil, ""
}

func (p *oltpEmulProfile) Weights() []OpWeight { return p.weights }

var _ Profile = (*oltpEmulProfile)(nil)

// SetEmulUnits stores a loaded business_ops registry on the factory so
// CreateProfile("oltp-emul") can build the profile. The factory stays free
// of database access; the caller (main runCLI) loads the units itself.
func (pf *ProfileFactory) SetEmulUnits(units []emul.Unit) {
	pf.emulUnits = units
}

// buildEmulProfile constructs the oltp-emul profile from loaded units.
func (pf *ProfileFactory) buildEmulProfile() (Profile, error) {
	if len(pf.emulUnits) == 0 {
		return nil, fmt.Errorf("profile oltp-emul requires a provisioned oltpemul database (no units loaded; run provision, then point --dsn at the oltpemul database)")
	}
	return NewOLTPEmulProfile(pf.emulUnits, time.Now().UnixNano()), nil
}
