package config

import (
	"fmt"
	"strings"
)

// ExtendedLoad is the configuration surface of the extended load mix
// (heavy JOIN SELECTs, bulk DML, the ops log, randomized transaction
// variants and the -plusddl DDL churn). Defaults follow the task
// specification; see EXTENDED_LOAD_PLAN.md §1.
type ExtendedLoad struct {
	Enabled     bool        `json:"enabled"` // umbrella; false = classic comparable score run
	HeavySelect HeavySelect `json:"heavySelect"`
	BulkDml     BulkDml     `json:"bulkDml"`
	OpsLog      OpsLog      `json:"opsLog"`
	TxVariants  TxVariants  `json:"txVariants"`
	PlusDDL     PlusDDL     `json:"plusDDL"`
}

// HeavySelect configures the periodic heavy multi-JOIN SELECT sidecar.
type HeavySelect struct {
	EverySec int `json:"everySec"`
	MinJoins int `json:"minJoins"`
}

// BulkDml configures the periodic mass INSERT/UPDATE/DELETE sidecar.
type BulkDml struct {
	EverySec int `json:"everySec"`
	MinRows  int `json:"minRows"`
	MaxRows  int `json:"maxRows"`
}

// OpsLog configures the operations/transactions log.
type OpsLog struct {
	Enabled       bool   `json:"enabled"`
	Level         string `json:"level"`         // all | periodic | errors
	MaxSizeMB     int    `json:"maxSizeMB"`     // rotate at this size (default 50)
	KeepArchives  int    `json:"keepArchives"`  // rotated files kept (default 3)
	RotateOnStart bool   `json:"rotateOnStart"` // rename the previous chain on restart
	Format        string `json:"format"`        // repl-print | tsv
	DumpRecords   bool   `json:"dumpRecords"`   // -R style JSON record dumps
}

// TxVariants configures the randomized transaction parameter scenarios.
type TxVariants struct {
	Mode                    string            `json:"mode"` // off | emul-safe | full
	LockTimeoutChoicesSec   []int             `json:"lockTimeoutChoicesSec"`
	Completion              CompletionWeights `json:"completion"`
	RareCompletionMinGapSec int               `json:"rareCompletionMinGapSec"`
	RetainingChainMax       int               `json:"retainingChainMax"`
	TwoPhaseAuxDB           string            `json:"twoPhaseAuxDB"`
	SavepointProb           float64           `json:"savepointProb"`
	AutonomousCallProb      float64           `json:"autonomousCallProb"`
	DDLRollbackFrac         float64           `json:"ddlRollbackFrac"`
}

// CompletionWeights are relative weights of the transaction completion axis
// (normalized at use time).
type CompletionWeights struct {
	Commit            int `json:"commit"`
	Rollback          int `json:"rollback"`
	CommitRetaining   int `json:"commitRetaining"`
	RollbackRetaining int `json:"rollbackRetaining"`
	TwoPhase          int `json:"twoPhase"`
	Limbo             int `json:"limbo"`
	ConnDrop          int `json:"connDrop"`
}

// PlusDDL configures the -plusddl DDL churn sidecar.
type PlusDDL struct {
	Enabled        bool     `json:"enabled"`
	EverySec       int      `json:"everySec"`
	ColPrefix      string   `json:"colPrefix"`
	TestInsertRows int      `json:"testInsertRows"`
	TestUpdateRows int      `json:"testUpdateRows"`
	TestDeleteRows int      `json:"testDeleteRows"`
	WorkTables     []string `json:"workTables"`
}

// DefaultExtendedLoad returns the task-specified defaults.
func DefaultExtendedLoad() ExtendedLoad {
	return ExtendedLoad{
		Enabled: true,
		HeavySelect: HeavySelect{
			EverySec: 5,
			MinJoins: 3,
		},
		BulkDml: BulkDml{
			EverySec: 30,
			MinRows:  100,
			MaxRows:  1000,
		},
		OpsLog: OpsLog{
			Enabled:       true,
			Level:         "all",
			MaxSizeMB:     50,
			KeepArchives:  3,
			RotateOnStart: true,
			Format:        "repl-print",
			DumpRecords:   false,
		},
		TxVariants: TxVariants{
			Mode:                    "emul-safe",
			LockTimeoutChoicesSec:   []int{1, 3, 5, 10},
			Completion:              CompletionWeights{Commit: 55, Rollback: 15, CommitRetaining: 10, RollbackRetaining: 5, TwoPhase: 5, Limbo: 2, ConnDrop: 2},
			RareCompletionMinGapSec: 10,
			RetainingChainMax:       50,
			SavepointProb:           0.15,
			AutonomousCallProb:      0.10,
			DDLRollbackFrac:         0.20,
		},
		PlusDDL: PlusDDL{
			Enabled:        false,
			EverySec:       60,
			ColPrefix:      "TST_",
			TestInsertRows: 100,
			TestUpdateRows: 50,
			TestDeleteRows: 30,
			WorkTables:     []string{"WARES", "AGENTS", "DOC_STATES"},
		},
	}
}

// ClassicPreset disables everything that changes the comparable oltp-emul
// score (decision D1): firebirdtest.com style score runs use it.
func (e *ExtendedLoad) ClassicPreset() {
	e.Enabled = false
	e.HeavySelect.EverySec = 0
	e.BulkDml.EverySec = 0
	e.TxVariants.Mode = "off"
	// the ops log and plusddl do not affect the score; keep user values
}

// Normalize fills defaults for zero values (following the UISettings pattern).
func (e *ExtendedLoad) Normalize() {
	def := DefaultExtendedLoad()
	if e.HeavySelect.EverySec < 0 {
		e.HeavySelect.EverySec = 0
	}
	if e.HeavySelect.MinJoins <= 0 {
		e.HeavySelect.MinJoins = def.HeavySelect.MinJoins
	}
	if e.BulkDml.EverySec < 0 {
		e.BulkDml.EverySec = 0
	}
	if e.BulkDml.MinRows <= 0 {
		e.BulkDml.MinRows = def.BulkDml.MinRows
	}
	if e.BulkDml.MaxRows < e.BulkDml.MinRows {
		e.BulkDml.MaxRows = e.BulkDml.MinRows
	}
	if e.OpsLog.Level == "" {
		e.OpsLog.Level = def.OpsLog.Level
	}
	if e.OpsLog.MaxSizeMB <= 0 {
		e.OpsLog.MaxSizeMB = def.OpsLog.MaxSizeMB
	}
	if e.OpsLog.KeepArchives <= 0 {
		e.OpsLog.KeepArchives = def.OpsLog.KeepArchives
	}
	if e.OpsLog.Format == "" {
		e.OpsLog.Format = def.OpsLog.Format
	}
	if e.TxVariants.Mode == "" {
		e.TxVariants.Mode = def.TxVariants.Mode
	}
	if len(e.TxVariants.LockTimeoutChoicesSec) == 0 {
		e.TxVariants.LockTimeoutChoicesSec = def.TxVariants.LockTimeoutChoicesSec
	}
	if e.TxVariants.RetainingChainMax <= 0 {
		e.TxVariants.RetainingChainMax = def.TxVariants.RetainingChainMax
	}
	if e.TxVariants.RareCompletionMinGapSec <= 0 {
		e.TxVariants.RareCompletionMinGapSec = def.TxVariants.RareCompletionMinGapSec
	}
	if e.TxVariants.TwoPhaseAuxDB == "" {
		e.TxVariants.TwoPhaseAuxDB = def.TxVariants.TwoPhaseAuxDB
	}
	if e.TxVariants.SavepointProb < 0 {
		e.TxVariants.SavepointProb = 0
	}
	if e.TxVariants.AutonomousCallProb < 0 {
		e.TxVariants.AutonomousCallProb = 0
	}
	if e.TxVariants.DDLRollbackFrac < 0 {
		e.TxVariants.DDLRollbackFrac = 0
	}
	if e.PlusDDL.EverySec <= 0 {
		e.PlusDDL.EverySec = def.PlusDDL.EverySec
	}
	if e.PlusDDL.ColPrefix == "" {
		e.PlusDDL.ColPrefix = def.PlusDDL.ColPrefix
	}
	if e.PlusDDL.TestInsertRows <= 0 {
		e.PlusDDL.TestInsertRows = def.PlusDDL.TestInsertRows
	}
	if e.PlusDDL.TestUpdateRows <= 0 {
		e.PlusDDL.TestUpdateRows = def.PlusDDL.TestUpdateRows
	}
	if e.PlusDDL.TestDeleteRows <= 0 {
		e.PlusDDL.TestDeleteRows = def.PlusDDL.TestDeleteRows
	}
	if len(e.PlusDDL.WorkTables) == 0 {
		e.PlusDDL.WorkTables = def.PlusDDL.WorkTables
	}
}

// Validate checks the extended load configuration.
func (e ExtendedLoad) Validate() error {
	switch e.OpsLog.Level {
	case "all", "periodic", "errors":
	default:
		return fmt.Errorf("extendedLoad.opsLog.level must be all|periodic|errors, got %q", e.OpsLog.Level)
	}
	switch strings.ToLower(e.OpsLog.Format) {
	case "repl-print", "tsv":
	default:
		return fmt.Errorf("extendedLoad.opsLog.format must be repl-print|tsv, got %q", e.OpsLog.Format)
	}
	switch e.TxVariants.Mode {
	case "off", "emul-safe", "full":
	default:
		return fmt.Errorf("extendedLoad.txVariants.mode must be off|emul-safe|full, got %q", e.TxVariants.Mode)
	}
	if e.BulkDml.MinRows > e.BulkDml.MaxRows {
		return fmt.Errorf("extendedLoad.bulkDml: minRows (%d) must be <= maxRows (%d)", e.BulkDml.MinRows, e.BulkDml.MaxRows)
	}
	if e.TxVariants.SavepointProb < 0 || e.TxVariants.SavepointProb > 1 ||
		e.TxVariants.AutonomousCallProb < 0 || e.TxVariants.AutonomousCallProb > 1 ||
		e.TxVariants.DDLRollbackFrac < 0 || e.TxVariants.DDLRollbackFrac > 1 {
		return fmt.Errorf("extendedLoad: probabilities must be within [0, 1]")
	}
	for _, t := range e.TxVariants.LockTimeoutChoicesSec {
		if t < 1 {
			return fmt.Errorf("extendedLoad.txVariants: lock timeout choices must be >= 1 second")
		}
	}
	return nil
}

// CompletionWeightsTotal sums the completion weights (0 when all zero).
func (c CompletionWeights) Total() int {
	return c.Commit + c.Rollback + c.CommitRetaining + c.RollbackRetaining + c.TwoPhase + c.Limbo + c.ConnDrop
}

// ApplyCLIDefaults fills the nested sections that individual flags cannot
// express (CLI flags set leaves; a zero section gets the full default).
func (e *ExtendedLoad) ApplyCLIDefaults() {
	def := DefaultExtendedLoad()
	if e.HeavySelect == (HeavySelect{}) {
		e.HeavySelect = def.HeavySelect
	}
	if e.BulkDml == (BulkDml{}) {
		e.BulkDml = def.BulkDml
	}
	if e.TxVariants.Mode != "" {
		// CLI flags set individual TxVariants leaves (--tx-variants, ...):
		// fill the remaining leaves field-wise, or a single flag like
		// --tx-variants would zero the completion weights and the picker
		// would draw plain commits only.
		if len(e.TxVariants.LockTimeoutChoicesSec) == 0 {
			e.TxVariants.LockTimeoutChoicesSec = def.TxVariants.LockTimeoutChoicesSec
		}
		if e.TxVariants.Completion.Total() == 0 {
			e.TxVariants.Completion = def.TxVariants.Completion
		}
		if e.TxVariants.RetainingChainMax == 0 {
			e.TxVariants.RetainingChainMax = def.TxVariants.RetainingChainMax
		}
		if e.TxVariants.RareCompletionMinGapSec == 0 {
			e.TxVariants.RareCompletionMinGapSec = def.TxVariants.RareCompletionMinGapSec
		}
	}
	if e.PlusDDL.EverySec == 0 && e.PlusDDL.ColPrefix == "" && len(e.PlusDDL.WorkTables) == 0 &&
		e.PlusDDL.TestInsertRows == 0 && e.PlusDDL.TestUpdateRows == 0 && e.PlusDDL.TestDeleteRows == 0 &&
		!e.PlusDDL.Enabled {
		e.PlusDDL = def.PlusDDL
	}
}
