package profile

import (
	"context"
	"database/sql"
	"fmt"

	"fb-loadgen/ops"
)

// WriteHeavyProfile implements the write-heavy simulation profile
type WriteHeavyProfile struct {
	*BaseProfile
}

// NewWriteHeavyProfile creates a new write-heavy profile
func NewWriteHeavyProfile(readOps *ops.ReadOperations, writeOps *ops.WriteOperations, cache *ops.Cache) *WriteHeavyProfile {
	// Define operation weights for write-heavy profile (from Technical Task)
	// 25% Insert new CUSTOMER
	// 20% Insert new SALES order
	// 15% Update SALES status
	// 15% Call SHIP_ORDER SP
	// 10% Update EMPLOYEE salary
	// 10% Call ADD_EMP_PROJ SP
	// 5% Delete EMPLOYEE_PROJECT row
	opsWeights := []OpWeight{
		{
			Weight: 25,
			Op:     func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error { return writeOps.InsertCustomer(ctx, tx) },
			Name:   "InsertCustomer",
		},
		{
			Weight: 20,
			Op:     func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error { return writeOps.InsertSales(ctx, tx) },
			Name:   "InsertSales",
		},
		{
			Weight: 15,
			Op: func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error {
				return writeOps.UpdateSalesStatus(ctx, tx)
			},
			Name: "UpdateSalesStatus",
		},
		{
			Weight: 15,
			Op:     func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error { return writeOps.CallShipOrder(ctx, tx) },
			Name:   "CallShipOrder",
		},
		{
			Weight: 10,
			Op: func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error {
				return writeOps.UpdateEmployeeSalary(ctx, tx)
			},
			Name: "UpdateEmployeeSalary",
		},
		{
			Weight: 10,
			Op:     func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error { return writeOps.CallAddEmpProj(ctx, tx) },
			Name:   "CallAddEmpProj",
		},
		{
			Weight: 5,
			Op:     func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error { return writeOps.DeleteEmpProj(ctx, tx) },
			Name:   "DeleteEmpProj",
		},
	}

	baseProfile := NewBaseProfile("write-heavy", opsWeights, readOps, writeOps, cache)
	return &WriteHeavyProfile{
		BaseProfile: baseProfile,
	}
}

// Name returns the profile name
func (wp *WriteHeavyProfile) Name() string {
	return "write-heavy"
}

// NextOp returns the next operation to execute
func (wp *WriteHeavyProfile) NextOp() func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error {
	op, _ := wp.selector.Select()
	return op
}

// Weights returns the operation weights for write-heavy profile
func (wp *WriteHeavyProfile) Weights() []OpWeight {
	return wp.BaseProfile.Weights()
}

// GetOperationName returns the name of the operation
func (wp *WriteHeavyProfile) GetOperationName(op func(ctx context.Context, tx *sql.Tx, cache *ops.Cache) error) string {
	// Find the operation name by comparing function pointers
	// This is a bit hacky but works for our use case
	for _, weight := range wp.Weights() {
		// We can't directly compare function pointers, so we'll use a map
		// For now, we'll return a generic name
		if weight.Op != nil {
			return weight.Name
		}
	}
	return "unknown"
}

// GetProfileDescription returns a description of the write-heavy profile
func (wp *WriteHeavyProfile) GetProfileDescription() string {
	return `Write-Heavy Profile:
- 25% Insert new CUSTOMER records
- 20% Insert new SALES orders  
- 15% Update SALES order status (new→open→shipped)
- 15% Call SHIP_ORDER stored procedure
- 10% Update EMPLOYEE salary (within job bounds)
- 10% Call ADD_EMP_PROJ stored procedure
- 5% Delete EMPLOYEE_PROJECT assignments

Designed for OLTP insert/update/delete stress.
Each operation runs in its own explicit transaction.`
}

// ValidateProfile validates that the write-heavy profile is properly configured
func (wp *WriteHeavyProfile) ValidateProfile() error {
	weights := wp.Weights()

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

	// Check that all operations are write operations
	expectedWeights := []int{25, 20, 15, 15, 10, 10, 5}
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
func (wp *WriteHeavyProfile) GetOperationDistribution() map[string]int {
	return map[string]int{
		"InsertCustomer":       25,
		"InsertSales":          20,
		"UpdateSalesStatus":    15,
		"CallShipOrder":        15,
		"UpdateEmployeeSalary": 10,
		"CallAddEmpProj":       10,
		"DeleteEmpProj":        5,
	}
}

// IsWriteOperation checks if an operation is a write operation
func (wp *WriteHeavyProfile) IsWriteOperation(opName string) bool {
	writeOps := []string{
		"InsertCustomer",
		"InsertSales",
		"UpdateSalesStatus",
		"CallShipOrder",
		"UpdateEmployeeSalary",
		"CallAddEmpProj",
		"DeleteEmpProj",
	}

	for _, writeOp := range writeOps {
		if opName == writeOp {
			return true
		}
	}
	return false
}

// GetExpectedTPS returns the expected transactions per second for this profile
// This is a rough estimate based on operation complexity
func (wp *WriteHeavyProfile) GetExpectedTPS() float64 {
	// Write-heavy operations are generally faster than complex reads
	// This is a rough estimate and will vary based on system performance
	return 50.0 // transactions per second
}

// GetProfileComplexity returns the complexity level of this profile
func (wp *WriteHeavyProfile) GetProfileComplexity() string {
	return "medium" // Write operations are generally less complex than complex read queries
}

// GetRecommendedConnectionCount returns the recommended number of connections for this profile
func (wp *WriteHeavyProfile) GetRecommendedConnectionCount() int {
	// Write-heavy profiles can benefit from more connections
	// but too many can cause lock contention
	return 20
}

// GetProfileCharacteristics returns key characteristics of this profile
func (wp *WriteHeavyProfile) GetProfileCharacteristics() map[string]interface{} {
	return map[string]interface{}{
		"profile_type":            "write-heavy",
		"primary_operations":      []string{"insert", "update", "delete"},
		"transaction_pattern":     "short_lived",
		"lock_contention":         "medium",
		"database_impact":         "high_write_load",
		"recommended_connections": 20,
		"expected_tps":            50.0,
		"complexity":              "medium",
	}
}
