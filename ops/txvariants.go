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
	"os"
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
// encoding (base isolation, lock timeout or completion intent); plainLevel is
// the same TPB without completion-intent bits (the fork's reuse validation
// compares TPBs, which do not carry intents).
type Scenario struct {
	Level          int
	plainLevel     int
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
	retainedPlain   int // TPB level of the retained context WITHOUT intent bits
	retainedRO      bool
	retainedISOName string
	retainChain     int
}

// Run-global rare gate: RareCompletionMinGapSec bounds limbo/connDrop
// completions across ALL workers, not per picker — a per-picker gate
// multiplied by the pool size (20 workers ≈ 2 rare completions/s), and each
// unprepared-then-abandoned 2PC transaction blocks every emul unit that
// reads its rows until the recovery sidecar resolves it.
var (
	rareGateMu   sync.Mutex
	rareGateLast time.Time
)

// claimRareGate atomically checks the run-global rare window and claims it.
func claimRareGate(gapSec int) bool {
	rareGateMu.Lock()
	defer rareGateMu.Unlock()
	if time.Since(rareGateLast) < time.Duration(gapSec)*time.Second {
		return false
	}
	rareGateLast = time.Now()
	return true
}

// RareGateReset clears the run-global rare gate (tests).
func RareGateReset() {
	rareGateMu.Lock()
	rareGateLast = time.Time{}
	rareGateMu.Unlock()
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
	if os.Getenv("FB_PICKER_DEBUG") != "" {
		fmt.Printf("[picker] worker=%d mode=%s weights=%+v total=%d\n",
			workerID, p.mode, p.cfg.Completion, p.cfg.Completion.Total())
	}
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
	// (R5): same TPB (the plain, intent-free level), freshly drawn completion
	// encoded on top. A plain-commit continuation ends the chain (the fork's
	// reuse hands back the live wire tx and its plain Commit finishes it).
	if p.retained {
		sc := Scenario{
			Level:         p.retainedPlain,
			plainLevel:    p.retainedPlain,
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
	completion := p.drawCompletionLocked(full)
	waits := p.waitChoices(full)
	// Intent encodings cannot carry the lock-timeout value: in emul-safe mode
	// an intent must not land on a lock-timeout TPB (dropping the timeout
	// would leave infinite WAIT, which emul units reject — V3).
	if !full && isIntentCompletion(completion) {
		nowait := make([]waitChoice, 0, len(waits))
		for _, wc := range waits {
			if wc.nowait {
				nowait = append(nowait, wc)
			}
		}
		waits = nowait
	}
	wait := waits[p.rng.Intn(len(waits))]
	// read-only access composes with any family in BeginTx
	ro := kind == KindRead && p.rng.Intn(2) == 0

	sc := Scenario{
		ReadOnly:   ro,
		Completion: completion,
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

// baseIso maps a (possibly encoded) level onto the internal isolation
// constant that preserves its wait semantics: the intent encodings ride on
// internal constants, and bare IsoRC means WAIT — an emul-unit rejection
// (V3) unless the wait mode is carried over explicitly.
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
		// lock-timeout encodings are RC/wait+timeout; the timeout value
		// cannot travel in the intent, so the base falls back to RC WAIT —
		// emul-safe mode therefore never combines intents with lock timeouts
		return firebirdsql.IsoRC
	}
	switch level {
	case firebirdsql.LevelReadCommittedRecVersion:
		return firebirdsql.IsoRC
	case firebirdsql.LevelReadCommittedNoWait:
		return firebirdsql.IsoRCNoWait
	case firebirdsql.LevelReadCommittedLegacy:
		return firebirdsql.IsoRCLegacy
	case firebirdsql.LevelReadCommittedLegacyNoWait:
		return firebirdsql.IsoRCLegacyNoWait
	case firebirdsql.LevelSnapshot:
		return firebirdsql.IsoSnapshot
	case firebirdsql.LevelSnapshotNoWait:
		return firebirdsql.IsoSnapshotNoWait
	case firebirdsql.LevelConsistency:
		return firebirdsql.IsoConsistency
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
	rareGap := p.cfg.RareCompletionMinGapSec
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
		if os.Getenv("FB_PICKER_DEBUG") != "" {
			fmt.Printf("[draw] worker=%d n=%d total=%d c=%s\n", p.workerID, n, total, c)
		}
		switch c {
		case CompletionLimbo, CompletionConnDrop:
			// Claim the run-global rare window atomically; a draw without a
			// claimable window falls through to plain commit (the loop
			// re-rolls, so an all-rare weight config cannot spin forever —
			// bounded by attempts).
			if !claimRareGate(rareGap) {
				continue
			}
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
		p.retainedPlain = sc.plainLevel
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

// isIntentCompletion reports whether the completion rewrites the begin-time
// level (retaining / limbo / hard drop).
func isIntentCompletion(c Completion) bool {
	switch c {
	case CompletionCommitRetaining, CompletionRollbackRetaining, CompletionLimbo, CompletionConnDrop:
		return true
	}
	return false
}
