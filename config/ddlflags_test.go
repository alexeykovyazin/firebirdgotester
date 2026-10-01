package config

import "testing"

// --ddl-types / --ddl-procs ride on the plusddl sidecar: ApplyFlagImplications
// must enable PlusDDL.Enabled (and hence the extended umbrella); the caller
// interval falls back to its default when left unset.
func TestDDLVictimFlagImplications(t *testing.T) {
	e := ExtendedLoad{}
	e.PlusDDL.AlterTypes = true
	e.PlusDDL.AlterProcs = true
	e.ApplyFlagImplications()
	if !e.PlusDDL.Enabled {
		t.Fatal("phase-2 flags must enable PlusDDL.Enabled")
	}
	if !e.Enabled {
		t.Fatal("PlusDDL.Enabled must enable the extended umbrella")
	}

	// defaults fill around the leaves: ProcCallEverySec default, churn leaves
	// kept, and a plain --plusddl (no phase-2 leaves) stays off
	e2 := ExtendedLoad{}
	e2.PlusDDL.Enabled = true
	e2.ApplyCLIDefaults()
	if e2.PlusDDL.AlterTypes || e2.PlusDDL.AlterProcs {
		t.Fatal("plain plusddl must not enable the phase-2 churn")
	}
	if e2.PlusDDL.ProcCallEverySec != 2 {
		t.Fatalf("ProcCallEverySec default = %d, want 2", e2.PlusDDL.ProcCallEverySec)
	}

	e3 := ExtendedLoad{}
	e3.PlusDDL.AlterTypes = true
	e3.ApplyFlagImplications()
	e3.ApplyCLIDefaults()
	if !e3.PlusDDL.Enabled || !e3.PlusDDL.AlterTypes {
		t.Fatal("AlterTypes leaf lost during defaults fill")
	}
	if e3.PlusDDL.EverySec != 60 || len(e3.PlusDDL.WorkTables) == 0 {
		t.Fatalf("PlusDDL defaults not applied around the leaf: %+v", e3.PlusDDL)
	}
}
