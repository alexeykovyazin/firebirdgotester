package profile

import (
	"context"
	"database/sql"
	"fmt"

	"fb-loadgen/ops"
)

// ReadHeavyProfile implements the read-heavy simulation profile
type ReadHeavyProfile struct {
	*BaseProfile
}

// NewReadHeavyProfile creates a new read-heavy profile
func NewReadHeavyProfile(readOps *ops.ReadOperations, writeOps *ops.WriteOperations, cache *ops.Cache) *ReadHeavyProfile {
	// Define operation weights for read-heavy profile (from Technical Task)
	// 30% Call ORG_CHART SP
	// 25% Call DEPT_BUDGET SP
	// 15% Call MAIL_LABEL SP
	// 10% Call GET_EMP_PROJ SP
	// 10% Call SUB_TOT_BUDGET SP
	// 5% SELECT with JOIN: EMPLOYEE + DEPARTMENT + JOB
	// 5% UPDATE DEPARTMENT.BUDGET (rare write)
	opsWeights := []OpWeight{
		{
			Weight: 30,
			Op:     func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error { return readOps.CallOrgChart(ctx, tx) },
			Name:   "CallOrgChart",
		},
		{
			Weight: 25,
			Op:     func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error { return readOps.CallDeptBudget(ctx, tx) },
			Name:   "CallDeptBudget",
		},
		{
			Weight: 15,
			Op:     func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error { return readOps.CallMailLabel(ctx, tx) },
			Name:   "CallMailLabel",
		},
		{
			Weight: 10,
			Op:     func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error { return readOps.CallGetEmpProj(ctx, tx) },
			Name:   "CallGetEmpProj",
		},
		{
			Weight: 10,
			Op: func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error {
				return readOps.CallSubTotBudget(ctx, tx)
			},
			Name: "CallSubTotBudget",
		},
		{
			Weight: 5,
			Op: func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error {
				return readOps.SelectEmployeeDeptJob(ctx, tx)
			},
			Name: "SelectEmployeeDeptJob",
		},
		{
			Weight: 5,
			Op: func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error {
				return writeOps.UpdateDeptBudget(ctx, tx)
			},
			Name: "UpdateDeptBudget",
		},
	}

	baseProfile := NewBaseProfile("read-heavy", opsWeights, readOps, writeOps, cache)
	return &ReadHeavyProfile{
		BaseProfile: baseProfile,
	}
}

// Name returns the profile name
func (rp *ReadHeavyProfile) Name() string {
	return "read-heavy"
}

// NextOp returns the next operation to execute
func (rp *ReadHeavyProfile) NextOp() func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error {
	op, _ := rp.selector.Select()
	return op
}

// NextOpWithName returns the next operation and its name
func (rp *ReadHeavyProfile) NextOpWithName() (func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error, string) {
	return rp.selector.Select()
}

// Weights returns the operation weights for read-heavy profile
func (rp *ReadHeavyProfile) Weights() []OpWeight {
	return rp.BaseProfile.Weights()
}

// GetOperationName returns the name of the operation
func (rp *ReadHeavyProfile) GetOperationName(op func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error) string {
	// Find the operation name by comparing function pointers
	for _, weight := range rp.Weights() {
		if weight.Op != nil {
			return weight.Name
		}
	}
	return "unknown"
}

// GetProfileDescription returns a description of the read-heavy profile
func (rp *ReadHeavyProfile) GetProfileDescription() string {
	return `Read-Heavy Profile:
- 30% Call ORG_CHART stored procedure (full dept hierarchy)
- 25% Call DEPT_BUDGET stored procedure (recursive budget rollup)
- 15% Call MAIL_LABEL stored procedure (customer address fetch)
- 10% Call GET_EMP_PROJ stored procedure (employee projects)
- 10% Call SUB_TOT_BUDGET stored procedure (aggregate dept budgets)
- 5% SELECT with JOIN: EMPLOYEE + DEPARTMENT + JOB (multi-table read)
- 5% UPDATE DEPARTMENT.BUDGET (rare write to keep cache pressure up)

Designed for OLAP-style read stress with rare writes.
All reads use READ COMMITTED isolation.`
}

// ValidateProfile validates that the read-heavy profile is properly configured
func (rp *ReadHeavyProfile) ValidateProfile() error {
	weights := rp.Weights()

	// Check that we have the expected number of operations
	if len(weights) != 7 {
		return fmt.Errorf("expected 7 operations, got %d", len(weights))
	}

	// Check that total weight is reasonable
	totalWeight := 0
	for _, w := range weights {
		totalWeight += w.Weight
	}

	if totalWeight != 100 {
		return fmt.Errorf("expected total weight of 100, got %d", totalWeight)
	}

	// Check that we have the expected distribution
	expectedWeights := []int{30, 25, 15, 10, 10, 5, 5}
	for i, expectedWeight := range expectedWeights {
		if i >= len(weights) {
			break
		}
		if weights[i].Weight != expectedWeight {
			return fmt.Errorf("operation %d has weight %d, expected %d", i, weights[i].Weight, expectedWeight)
		}
	}

	return nil
}

// GetOperationDistribution returns the expected distribution of operations
func (rp *ReadHeavyProfile) GetOperationDistribution() map[string]int {
	return map[string]int{
		"CallOrgChart":          30,
		"CallDeptBudget":        25,
		"CallMailLabel":         15,
		"CallGetEmpProj":        10,
		"CallSubTotBudget":      10,
		"SelectEmployeeDeptJob": 5,
		"UpdateDeptBudget":      5,
	}
}

// IsWriteOperation checks if an operation is a write operation
func (rp *ReadHeavyProfile) IsWriteOperation(opName string) bool {
	return opName == "UpdateDeptBudget"
}

// GetReadOperationCount returns the number of read operations
func (rp *ReadHeavyProfile) GetReadOperationCount() int {
	count := 0
	for _, weight := range rp.Weights() {
		if !rp.IsWriteOperation(weight.Name) {
			count++
		}
	}
	return count
}

// GetWriteOperationCount returns the number of write operations
func (rp *ReadHeavyProfile) GetWriteOperationCount() int {
	count := 0
	for _, weight := range rp.Weights() {
		if rp.IsWriteOperation(weight.Name) {
			count++
		}
	}
	return count
}

// GetExpectedTPS returns the expected transactions per second for this profile
// Read operations are generally faster than writes, but complex queries can be slower
func (rp *ReadHeavyProfile) GetExpectedTPS() float64 {
	// Read-heavy operations vary widely in complexity
	// Complex stored procedures and joins can be slower
	return 30.0 // transactions per second (conservative estimate)
}

// GetProfileComplexity returns the complexity level of this profile
func (rp *ReadHeavyProfile) GetProfileComplexity() string {
	return "high" // Complex read queries and stored procedures
}

// GetRecommendedConnectionCount returns the recommended number of connections for this profile
func (rp *ReadHeavyProfile) GetRecommendedConnectionCount() int {
	// Read-heavy profiles can handle more connections since reads are less likely to cause lock contention
	return 20
}

// GetProfileCharacteristics returns key characteristics of this profile
func (rp *ReadHeavyProfile) GetProfileCharacteristics() map[string]interface{} {
	return map[string]interface{}{
		"profile_type":            "read-heavy",
		"primary_operations":      []string{"select", "stored_procedures"},
		"transaction_pattern":     "read_committed",
		"lock_contention":         "low",
		"database_impact":         "high_read_load",
		"recommended_connections": 20,
		"expected_tps":            30.0,
		"complexity":              "high",
		"read_operations":         rp.GetReadOperationCount(),
		"write_operations":        rp.GetWriteOperationCount(),
	}
}

// GetReadToWriteRatio returns the read-to-write ratio
func (rp *ReadHeavyProfile) GetReadToWriteRatio() float64 {
	readOps := rp.GetReadOperationCount()
	writeOps := rp.GetWriteOperationCount()
	if writeOps == 0 {
		return float64(readOps)
	}
	return float64(readOps) / float64(writeOps)
}

// GetComplexQueryOperations returns operations that involve complex queries
func (rp *ReadHeavyProfile) GetComplexQueryOperations() []string {
	return []string{
		"CallOrgChart",          // Recursive hierarchy
		"CallDeptBudget",        // Recursive budget rollup
		"CallSubTotBudget",      // Aggregate queries
		"SelectEmployeeDeptJob", // Multi-table JOIN
	}
}

// GetSimpleQueryOperations returns operations that involve simple queries
func (rp *ReadHeavyProfile) GetSimpleQueryOperations() []string {
	return []string{
		"CallMailLabel",  // Simple cursor select
		"CallGetEmpProj", // Simple cursor select
	}
}

// GetRareWriteOperations returns the rare write operations
func (rp *ReadHeavyProfile) GetRareWriteOperations() []string {
	return []string{
		"UpdateDeptBudget",
	}
}
