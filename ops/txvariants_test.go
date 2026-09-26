package ops

import (
	"strings"
	"testing"

	firebirdsql "github.com/nakagami/firebirdsql"

	"fb-loadgen/config"
)

func testCfg(mode string) config.ExtendedLoad {
	el := config.DefaultExtendedLoad()
	el.Enabled = true
	el.TxVariants.Mode = mode
	el.TxVariants.TwoPhaseAuxDB = "" // no aux DB: twoPhase must not be drawn
	el.TxVariants.RareCompletionMinGapSec = 0
	return el
}

func TestModeOffReproducesDefault(t *testing.T) {
	p := NewPicker(testCfg("off"), 1, 42)
	for i := 0; i < 200; i++ {
		if _, ok := p.Pick(KindWrite); ok {
			t.Fatalf("mode=off must not draw scenarios")
		}
	}
}

func TestEmulSafeStaysV3Legal(t *testing.T) {
	// emul units reject infinite WAIT (V3): the lock resolution must be
	// nowait or a finite lock timeout. Infinite-wait encodings are the
	// plain family levels (1050..1300) and must never be drawn.
	p := NewPicker(testCfg("emul-safe"), 1, 7)
	sawTimeout := false
	sawNowait := false
	infiniteWait := map[int]string{
		firebirdsql.LevelReadCommittedRecVersion: "RC wait",
		firebirdsql.LevelReadCommittedLegacy:     "RC legacy wait",
		firebirdsql.LevelSnapshot:                "snapshot wait",
		firebirdsql.LevelConsistency:             "consistency wait",
	}
	for i := 0; i < 1000; i++ {
		sc, ok := p.Pick(KindWrite)
		if !ok {
			t.Fatal("emul-safe must draw")
		}
		if name, bad := infiniteWait[sc.Level]; bad {
			t.Fatalf("emul-safe drew an infinite-wait encoding %d (%s)", sc.Level, name)
		}
		switch {
		case sc.Level == firebirdsql.LevelReadCommittedNoWait,
			sc.Level == firebirdsql.LevelReadCommittedLegacyNoWait,
			sc.Level == firebirdsql.LevelSnapshotNoWait:
			sawNowait = true
		case sc.Level > firebirdsql.LevelLockTimeoutBase && sc.Level <= firebirdsql.LevelLockTimeoutBase+firebirdsql.MaxLockTimeoutEnc:
			sawTimeout = true
		}
	}
	if !sawTimeout || !sawNowait {
		t.Errorf("emul-safe coverage: nowait=%v lockTimeout=%v", sawNowait, sawTimeout)
	}
}

func TestFullMatrixCoversWaitAndConsistency(t *testing.T) {
	p := NewPicker(testCfg("full"), 1, 11)
	sawWait := false
	sawConsistency := false
	for i := 0; i < 3000 && !(sawWait && sawConsistency); i++ {
		sc, ok := p.Pick(KindWrite)
		if !ok {
			t.Fatal("full must draw")
		}
		if sc.Level == firebirdsql.LevelConsistency {
			sawConsistency = true
		}
		if sc.Level == firebirdsql.LevelReadCommittedRecVersion ||
			sc.Level == firebirdsql.LevelReadCommittedLegacy ||
			sc.Level == firebirdsql.LevelSnapshot {
			sawWait = true
		}
	}
	if !sawWait {
		t.Errorf("full mode never drew infinite wait")
	}
	if !sawConsistency {
		t.Errorf("full mode never drew consistency isolation")
	}
}

func TestReadOnlyOnlyForReadKind(t *testing.T) {
	p := NewPicker(testCfg("full"), 1, 13)
	for i := 0; i < 500; i++ {
		sc, ok := p.Pick(KindWrite)
		if !ok {
			t.Fatal("full must draw")
		}
		if sc.ReadOnly {
			t.Fatalf("write ops must never draw read-only transactions: %+v", sc)
		}
	}
	p2 := NewPicker(testCfg("full"), 2, 17)
	sawRO := false
	for i := 0; i < 500 && !sawRO; i++ {
		sc, ok := p2.Pick(KindRead)
		if !ok {
			t.Fatal("full must draw")
		}
		if sc.ReadOnly {
			sawRO = true
		}
	}
	if !sawRO {
		t.Errorf("read ops never drew a read-only transaction")
	}
}

func TestCompletionIntentsEncode(t *testing.T) {
	p := NewPicker(testCfg("emul-safe"), 3, 23)
	seen := map[Completion]bool{}
	for i := 0; i < 4000; i++ {
		sc, _ := p.Pick(KindWrite)
		seen[sc.Completion] = true
		switch sc.Completion {
		case CompletionCommitRetaining:
			if sc.Level < firebirdsql.LevelCommitRetainingBase || sc.Level >= firebirdsql.LevelCommitRetainingBase+firebirdsql.NumInternalIsolationLevels {
				t.Fatalf("commitRetaining level not encoded: %d", sc.Level)
			}
		case CompletionRollbackRetaining:
			if sc.Level < firebirdsql.LevelRollbackRetainingBase || sc.Level >= firebirdsql.LevelRollbackRetainingBase+firebirdsql.NumInternalIsolationLevels {
				t.Fatalf("rollbackRetaining level not encoded: %d", sc.Level)
			}
		case CompletionLimbo:
			if sc.Level < firebirdsql.LevelPrepareThenDieBase || sc.Level >= firebirdsql.LevelPrepareThenDieBase+firebirdsql.NumInternalIsolationLevels {
				t.Fatalf("limbo level not encoded: %d", sc.Level)
			}
		case CompletionConnDrop:
			if sc.Level < firebirdsql.LevelHardDropBase || sc.Level >= firebirdsql.LevelHardDropBase+firebirdsql.NumInternalIsolationLevels {
				t.Fatalf("connDrop level not encoded: %d", sc.Level)
			}
		case CompletionTwoPhase:
			t.Fatalf("twoPhase drawn without an aux DB")
		}
	}
	for _, want := range []Completion{CompletionCommit, CompletionRollback, CompletionCommitRetaining, CompletionRollbackRetaining, CompletionLimbo, CompletionConnDrop} {
		if !seen[want] {
			t.Errorf("completion %q never drawn in 4000 tries", want)
		}
	}
}

func TestTwoPhaseRequiresAuxDB(t *testing.T) {
	cfg := testCfg("emul-safe")
	cfg.TxVariants.TwoPhaseAuxDB = "/tmp/EL_2PC.FDB"
	p := NewPicker(cfg, 4, 29)
	saw2PC := false
	for i := 0; i < 4000 && !saw2PC; i++ {
		sc, _ := p.Pick(KindWrite)
		if sc.Completion == CompletionTwoPhase {
			saw2PC = true
			if !strings.Contains(sc.Params(), "two-phase") {
				t.Fatalf("twoPhase params not rendered: %q", sc.Params())
			}
		}
	}
	if !saw2PC {
		t.Errorf("twoPhase never drawn with aux DB configured")
	}
}

func TestRareCompletionsRateLimited(t *testing.T) {
	cfg := testCfg("emul-safe")
	cfg.TxVariants.RareCompletionMinGapSec = 3600 // effectively never again
	p := NewPicker(cfg, 5, 31)
	rare := 0
	for i := 0; i < 2000; i++ {
		sc, _ := p.Pick(KindWrite)
		if sc.Completion == CompletionLimbo || sc.Completion == CompletionConnDrop {
			rare++
		}
	}
	if rare > 1 {
		t.Errorf("rare completions not rate limited: %d in one window", rare)
	}
}

func TestRetainingChainCap(t *testing.T) {
	cfg := testCfg("emul-safe")
	cfg.TxVariants.RetainingChainMax = 3
	cfg.TxVariants.Completion = config.CompletionWeights{CommitRetaining: 1}
	p := NewPicker(cfg, 6, 37)
	// fill the chain exactly: 3 retained completions terminate it
	for i := 0; i < 3; i++ {
		sc, _ := p.Pick(KindWrite)
		p.NoteResult(sc, true, int64(i+1))
	}
	sc, _ := p.Pick(KindWrite)
	if sc.RetainedFrom != 0 {
		t.Fatalf("retaining chain must terminate at RetainingChainMax, got retained_from=%d", sc.RetainedFrom)
	}
}

func TestRetainedFromAttribution(t *testing.T) {
	cfg := testCfg("emul-safe")
	cfg.TxVariants.Completion = config.CompletionWeights{CommitRetaining: 1}
	p := NewPicker(cfg, 7, 41)
	sc1, _ := p.Pick(KindWrite)
	p.NoteResult(sc1, true, 100) // tx 100 retained
	sc2, _ := p.Pick(KindWrite)
	if sc2.RetainedFrom != 100 {
		t.Fatalf("next scenario must reference the retained tx, got %d", sc2.RetainedFrom)
	}
	p.NoteResult(sc2, false, 101)
	sc3, _ := p.Pick(KindWrite)
	if sc3.RetainedFrom != 0 {
		t.Fatalf("plain completion must end the chain, got %d", sc3.RetainedFrom)
	}
}
