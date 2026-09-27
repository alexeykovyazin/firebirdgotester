package config

import "testing"

// NoLimbo is the "no limbo transactions" switch (CLI --no-limbo, API
// extendedLoad.txVariants.noLimbo): Normalize must force the limbo completion
// weight to 0 so the existing Limbo>0 gates (recovery sidecar) follow the
// switch, while the rest of the completion mix stays at its defaults.
func TestNormalizeNoLimboZeroesCompletionWeight(t *testing.T) {
	el := ExtendedLoad{
		Enabled: true,
		TxVariants: TxVariants{
			Mode:    "emul-safe",
			NoLimbo: true,
		},
	}
	// the CLI chain: flags set leaves, then ApplyCLIDefaults, then Normalize
	el.ApplyCLIDefaults()
	if got := el.TxVariants.Completion.Limbo; got != 2 {
		t.Fatalf("CLI defaults should fill the limbo weight (2) before Normalize, got %d", got)
	}
	el.Normalize()
	if got := el.TxVariants.Completion.Limbo; got != 0 {
		t.Fatalf("Normalize with NoLimbo must zero the limbo weight, got %d", got)
	}
	if got := el.TxVariants.Completion.Commit; got != 60 {
		t.Fatalf("other completion weights must stay at defaults, got commit=%d", got)
	}
	if !el.TxVariants.NoLimbo {
		t.Fatal("NoLimbo must survive Normalize")
	}
}

func TestNormalizeKeepsLimboWeightWithoutSwitch(t *testing.T) {
	el := DefaultExtendedLoad()
	el.Normalize()
	if got := el.TxVariants.Completion.Limbo; got != 2 {
		t.Fatalf("default limbo weight must be preserved without NoLimbo, got %d", got)
	}
}

// The API shortcut PATCH {"extendedLoad":{"txVariants":{"noLimbo":true}}}
// unmarshals into a zero section: Normalize must fill the completion defaults
// (then zero the limbo weight), not leave an all-zero axis that would draw
// plain commits only.
func TestNormalizeApiPatchKeepsCompletionMix(t *testing.T) {
	el := ExtendedLoad{TxVariants: TxVariants{Mode: "emul-safe", NoLimbo: true}}
	el.Normalize()
	if got := el.TxVariants.Completion.Commit; got != 60 {
		t.Fatalf("completion defaults must be filled for a leaf-only patch, got commit=%d", got)
	}
	if got := el.TxVariants.Completion.Limbo; got != 0 {
		t.Fatalf("NoLimbo must still zero the limbo weight, got %d", got)
	}
}
