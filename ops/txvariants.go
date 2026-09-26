// Package ops: txvariants draws randomized transaction-parameter scenarios
// for the extended load mix (EXTENDED_LOAD_PLAN.md Phase 2). A scenario is
// decided before BeginTx and encoded into sql.TxOptions.Isolation using the
// driver fork's numeric intent encoding: database/sql's Tx interface takes
// no completion arguments, so the intent travels at begin time.
package ops

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	firebirdsql "github.com/nakagami/firebirdsql"

	"fb-loadgen/config"
)

// Completion is the transaction completion axis.
type Completion string

const (
	CompletionCommit            Completion = "commit"
	CompletionRollback          Completion = "rollback"
	CompletionCommitRetaining   Completion = "commitRetaining"
	CompletionRollbackRetaining Completion = "rollbackRetaining"
	CompletionTwoPhase          Completion = "twoPhase"
	CompletionLimbo             Completion = "limbo"
	CompletionConnDrop          Completion = "connDrop"
)

// OpKind constrains the access-mode axis: SELECT-kind operations may draw
// read-only transactions; data-modifying operations and oltp-emul units
// (execute-procedure) are write-only.
type OpKind int

const (
	KindWrite OpKind = iota
	KindRead
)

// Scenario is a drawn transaction configuration. Level is the full driver
// encoding (base isolation, lock timeout or completion intent).
type Scenario struct {
	Level          int
	ReadOnly       bool
	Completion     Completion
	LockTimeoutSec int    // 0 = none
	IsolationName  string // human-readable, ops log params field
	RetainedFrom   int64  // opslog tx number of the retained context this tx reuses
}

// Picker draws scenarios per worker. Not safe for concurrent use: every
// worker and sidecar owns its own instance (ops/cache.go keeps a shared
// unguarded rng — do not repeat that).
type Picker struct {
	mode     string // off | emul-safe | full
	cfg      config.TxVariants
	rng      *rand.Rand
	workerID int

	// retained-chain state (T4/R5): after a commit-retaining completion the
	// wire tx IS the next transaction on the connection, so the next scenario
	// is pinned to the retained TPB. Only the completion is redrawn.
	mu              sync.Mutex
	retained        bool
	retainedFrom    int64
	retainedLevel   int
	retainedRO      bool
	retainedISOName string
	retainChain     int

	lastRare time.Time // rate limiter for limbo/connDrop
}

// NewPicker creates a per-worker scenario picker.
func NewPicker(cfg config.ExtendedLoad, workerID int, seed int64) *Picker {
	p := &Picker{
		mode:     cfg.TxVariants.Mode,
		cfg:      cfg.TxVariants,
		workerID: workerID,
	}
	p.cfg.LockTimeoutChoicesSec = append([]int(nil), cfg.TxVariants.LockTimeoutChoicesSec...)
	p.rng = rand.New(rand.NewSource(seed))
	return p
}

// isoFamily is one isolation family of the matrix with its two wait-mode
// encodings (wait / nowait). RO access composes via TxOptions.ReadOnly in the
// fork's BeginTx; infinite wait is emul-illegal (V3) and fullOnly.
type isoFamily struct {
	name     string
	plain    int // wait encoding
	nowait   int // nowait encoding
	internal int // internal preset for the completion-intent composition
	fullOnly bool
}

func isoFamilies(full bool) []isoFamily {
	all := []isoFamily{
		{"RC/rec_version", firebirdsql.LevelReadCommittedRecVersion, firebirdsql.LevelReadCommittedNoWait, firebirdsql.IsoRC, false},
		{"RC/no_rec_version", firebirdsql.LevelReadCommittedLegacy, firebirdsql.LevelReadCommittedLegacyNoWait, firebirdsql.IsoRCLegacy, false},
		{"snapshot", firebirdsql.LevelSnapshot, firebirdsql.LevelSnapshotNoWait, firebirdsql.IsoSnapshot, false},
		{"consistency", firebirdsql.LevelConsistency, firebirdsql.LevelConsistency, firebirdsql.IsoConsistency, true},
	}
	if !full {
		out := all[:0]
		for _, f := range all {
			if !f.fullOnly {
				out = append(out, f)
			}
		}
		return out
	}
	return all
}

// waitChoice is a drawn wait-resolution variant.
type waitChoice struct {
	name        string
	nowait      bool
	lockTimeout int // seconds, 0 = none
	fullOnly    bool
}

func (p *Picker) waitChoices(full bool) []waitChoice {
	out := []waitChoice{{"nowait", true, 0, false}}
	for _, s := range p.cfg.LockTimeoutChoicesSec {
		out = append(out, waitChoice{fmt.Sprintf("wait; lock_timeout=%d", s), false, s, false})
	}
	if full {
		out = append(out, waitChoice{"wait", false, 0, true})
	}
	return out
}

// Pick draws a scenario. enabled=false reproduces today's behavior
// (mode=off or the extended load disabled).
func (p *Picker) Pick(kind OpKind) (Scenario, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pickLocked(kind)
}

func (p *Picker) pickLocked(kind OpKind) (Scenario, bool) {
	if p.mode == "" || p.mode == "off" {
		return Scenario{}, false
	}
	full := p.mode == "full"

	// A live retained context pins the next transaction on this connection
	// (R5): same TPB, freshly drawn completion.
	if p.retained {
		sc := Scenario{
			Level:         p.retainedLevel,
			ReadOnly:      p.retainedRO,
			IsolationName: p.retainedISOName,
			RetainedFrom:  p.retainedFrom,
			Completion:    p.drawCompletionLocked(full),
		}
		sc.applyCompletion()
		return sc, true
	}

	families := isoFamilies(full)
	fam := families[p.rng.Intn(len(families))]
	wait := p.waitChoices(full)[p.rng.Intn(len(p.waitChoices(full)))]
	// read-only access composes with any family in BeginTx
	ro := kind == KindRead && p.rng.Intn(2) == 0

	sc := Scenario{
		ReadOnly:   ro,
		Completion: p.drawCompletionLocked(full),
	}
	switch {
	case wait.lockTimeout > 0:
		// wait + isc_tpb_lock_timeout(n) uses the driver's 2000+n RC encoding
		// (snapshot/consistency lock-timeout combinations are not representable
		// through database/sql); ReadOnly composes in BeginTx.
		sc.Level = firebirdsql.LevelLockTimeoutBase + wait.lockTimeout
		name := "RC/rec_version/" + wait.name
		if ro {
			name += "/RO"
		}
		sc.IsolationName = name
	case wait.nowait:
		sc.Level = fam.nowait
		sc.IsolationName = fam.name + "/nowait"
	default:
		sc.Level = fam.plain
		sc.IsolationName = fam.name + "/wait"
	}
	if ro {
		sc.IsolationName += "/RO"
	}
	sc.applyCompletion()
	return sc, true
}

// applyCompletion rewrites the begin-time level for intent-based completions
// (F5). Plain commit/rollback keep the level untouched.
func (s *Scenario) applyCompletion() {
	switch s.Completion {
	case CompletionCommitRetaining:
		s.Level = firebirdsql.LevelCommitRetainingBase + baseIso(s.Level)
	case CompletionRollbackRetaining:
		s.Level = firebirdsql.LevelRollbackRetainingBase + baseIso(s.Level)
	case CompletionLimbo:
		s.Level = firebirdsql.LevelPrepareThenDieBase + baseIso(s.Level)
	case CompletionConnDrop:
		s.Level = firebirdsql.LevelHardDropBase + baseIso(s.Level)
	}
}

// baseIso extracts the base isolation component of a (possibly encoded) level.
func baseIso(level int) int {
	for _, base := range []int{
		firebirdsql.LevelCommitRetainingBase, firebirdsql.LevelRollbackRetainingBase,
		firebirdsql.LevelPrepareThenDieBase, firebirdsql.LevelHardDropBase,
	} {
		if level >= base && level < base+firebirdsql.NumInternalIsolationLevels {
			return level - base
		}
	}
	if level > firebirdsql.LevelLockTimeoutBase && level <= firebirdsql.LevelLockTimeoutBase+firebirdsql.MaxLockTimeoutEnc {
		return firebirdsql.IsoRC
	}
	return firebirdsql.IsoRC
}

// drawCompletionLocked weighted-draws the completion axis; limbo/connDrop are
// rate-limited by RareCompletionMinGapSec; twoPhase requires an aux DB path.
func (p *Picker) drawCompletionLocked(full bool) Completion {
	w := p.cfg.Completion
	total := w.Total()
	if total <= 0 {
		return CompletionCommit
	}
	rareGap := time.Duration(p.cfg.RareCompletionMinGapSec) * time.Second
	rareAllowed := time.Since(p.lastRare) >= rareGap
	twoPhaseAllowed := p.cfg.TwoPhaseAuxDB != ""

	for attempts := 0; attempts < 8; attempts++ {
		n := p.rng.Intn(total)
		var c Completion
		switch {
		case n < w.Commit:
			c = CompletionCommit
		case n < w.Commit+w.Rollback:
			c = CompletionRollback
		case n < w.Commit+w.Rollback+w.CommitRetaining:
			c = CompletionCommitRetaining
		case n < w.Commit+w.Rollback+w.CommitRetaining+w.RollbackRetaining:
			c = CompletionRollbackRetaining
		case n < w.Commit+w.Rollback+w.CommitRetaining+w.RollbackRetaining+w.TwoPhase:
			c = CompletionTwoPhase
		case n < w.Commit+w.Rollback+w.CommitRetaining+w.RollbackRetaining+w.TwoPhase+w.Limbo:
			c = CompletionLimbo
		default:
			c = CompletionConnDrop
		}
		switch c {
		case CompletionLimbo, CompletionConnDrop:
			if !rareAllowed {
				continue
			}
			p.lastRare = time.Now()
			return c
		case CompletionTwoPhase:
			if !twoPhaseAllowed {
				continue
			}
			return c
		default:
			return c
		}
	}
	return CompletionCommit
}

// NoteResult feeds the chain state back after a transaction completed.
// retained = the completion left a live retained context (commit/rollback
// retaining via the fork's reuse branch). opsTxn = the opslog transaction
// number of THIS tx for retained_from attribution.
func (p *Picker) NoteResult(sc Scenario, retained bool, opsTxn int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if retained {
		p.retainChain++
		if p.retainChain >= p.cfg.RetainingChainMax {
			// a chain must terminate: force a plain completion next
			p.retained = false
			p.retainChain = 0
			return
		}
		p.retained = true
		p.retainedFrom = opsTxn
		p.retainedLevel = sc.Level
		p.retainedRO = sc.ReadOnly
		p.retainedISOName = sc.IsolationName
		return
	}
	p.retained = false
	p.retainChain = 0
}

// Params renders the scenario for the ops log params field.
func (s Scenario) Params() string {
	p := s.IsolationName
	if s.RetainedFrom > 0 {
		p += fmt.Sprintf(", retained_from=%d", s.RetainedFrom)
	}
	switch s.Completion {
	case CompletionTwoPhase:
		p += "; completion=two-phase"
	case CompletionLimbo:
		p += "; completion=prepare-then-die"
	case CompletionConnDrop:
		p += "; completion=hard-drop"
	}
	return p
}

// EnlistTwoPhase enlists the aux database into the current transaction via
// EXECUTE STATEMENT ... WITH COMMON TRANSACTION, so the subsequent commit
// runs as an engine-side two-phase commit (T4/F6a). The aux DB and its
// EL2PC_LOG table are created by the extended-load bootstrap.
func EnlistTwoPhase(ctx context.Context, tx *sql.Tx, auxDB, user, pass string, workerID int) error {
	q := fmt.Sprintf(
		"EXECUTE STATEMENT ON EXTERNAL DATA SOURCE '%s' AS USER '%s' PASSWORD '%s' "+
			"WITH COMMON TRANSACTION INSERT INTO EL2PC_LOG (STAMP, NOTE) VALUES (CURRENT_TIMESTAMP, '%s')",
		sqlQuote(auxDB), sqlQuote(user), sqlQuote(pass), sqlQuote(fmt.Sprintf("worker-%d", workerID)))
	_, err := tx.ExecContext(ctx, q)
	return err
}

func sqlQuote(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
