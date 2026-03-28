package ops

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"time"

	"fb-loadgen/db"
)

// WriteOperations provides all write operations for the simulation
type WriteOperations struct {
	connFactory *db.ConnectionFactory
	cache       *Cache
	rng         *rand.Rand
}

// NewWriteOperations creates a new WriteOperations instance
func NewWriteOperations(connFactory *db.ConnectionFactory, cache *Cache) *WriteOperations {
	return &WriteOperations{
		connFactory: connFactory,
		cache:       cache,
		rng:         rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// InsertCustomer inserts a new customer with constraint-aware data generation
func (wo *WriteOperations) InsertCustomer(ctx context.Context, tx *sql.Tx) error {
	// Generate customer data
	customerName := wo.cache.RandomName()
	address1, address2, city, state, country, postalCode := wo.generateCustomerAddress()
	onHold := wo.cache.RandomOnHold()

	// Insert customer using GEN_ID for CUST_NO
	result, err := tx.ExecContext(ctx, `
		INSERT INTO CUSTOMER (CUST_NO, CUSTOMER, ADDRESS_LINE1, ADDRESS_LINE2, CITY, STATE_PROVINCE, COUNTRY, POSTAL_CODE, ON_HOLD)
		VALUES (GEN_ID(CUST_NO_GEN, 1), ?, ?, ?, ?, ?, ?, ?, ?)
	`, customerName, address1, address2, city, state, country, postalCode, onHold)
	if err != nil {
		return fmt.Errorf("failed to insert customer: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected != 1 {
		return fmt.Errorf("expected 1 row affected, got %d", rowsAffected)
	}

	return nil
}

// InsertSales inserts a new sales order with constraint-aware data generation
func (wo *WriteOperations) InsertSales(ctx context.Context, tx *sql.Tx) error {
	// Generate sales data
	poNumber := wo.cache.RandomPONumber()
	custNo := wo.cache.RandomCustNo()
	empNo := wo.cache.RandomEmpNo()
	orderStatus := wo.cache.RandomOrderStatus()
	paid := wo.cache.RandomPaid()
	discount := wo.cache.RandomDiscount()

	// Insert sales order
	result, err := tx.ExecContext(ctx, `
		INSERT INTO SALES (PO_NUMBER, CUST_NO, EMP_NO, ORDER_STATUS, PAID, DISCOUNT)
		VALUES (?, ?, ?, ?, ?, ?)
	`, poNumber, custNo, empNo, orderStatus, paid, discount)
	if err != nil {
		return fmt.Errorf("failed to insert sales: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected != 1 {
		return fmt.Errorf("expected 1 row affected, got %d", rowsAffected)
	}

	return nil
}

// UpdateSalesStatus updates the status of a random sales order
func (wo *WriteOperations) UpdateSalesStatus(ctx context.Context, tx *sql.Tx) error {
	// Get a random sales order
	var poNumber string
	var currentStatus string

	err := tx.QueryRowContext(ctx, `
		SELECT PO_NUMBER, ORDER_STATUS 
		FROM SALES 
		ORDER BY RAND() 
		LIMIT 1
	`).Scan(&poNumber, &currentStatus)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("no sales orders found to update")
		}
		return fmt.Errorf("failed to get random sales order: %w", err)
	}

	// Determine next status based on current status
	nextStatus := wo.getNextOrderStatus(currentStatus)
	if nextStatus == currentStatus {
		// No transition available, skip this update
		return nil
	}

	// Update sales status
	result, err := tx.ExecContext(ctx, `
		UPDATE SALES 
		SET ORDER_STATUS = ? 
		WHERE PO_NUMBER = ?
	`, nextStatus, poNumber)
	if err != nil {
		return fmt.Errorf("failed to update sales status: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected != 1 {
		return fmt.Errorf("expected 1 row affected, got %d", rowsAffected)
	}

	return nil
}

// CallShipOrder calls the SHIP_ORDER stored procedure
func (wo *WriteOperations) CallShipOrder(ctx context.Context, tx *sql.Tx) error {
	// Get a random sales order that can be shipped
	var poNumber string

	err := tx.QueryRowContext(ctx, `
		SELECT PO_NUMBER 
		FROM SALES 
		WHERE ORDER_STATUS IN ('new', 'open', 'waiting')
		ORDER BY RAND() 
		LIMIT 1
	`).Scan(&poNumber)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("no shippable sales orders found")
		}
		return fmt.Errorf("failed to get random sales order: %w", err)
	}

	// Call SHIP_ORDER procedure
	_, err = tx.ExecContext(ctx, "EXECUTE PROCEDURE SHIP_ORDER(?)", poNumber)
	if err != nil {
		return fmt.Errorf("failed to call SHIP_ORDER(%s): %w", poNumber, err)
	}

	return nil
}

// UpdateEmployeeSalary updates the salary of a random employee
func (wo *WriteOperations) UpdateEmployeeSalary(ctx context.Context, tx *sql.Tx) error {
	// Get a random employee and their job salary range
	var empNo int
	var jobCode string
	var currentSalary float64

	err := tx.QueryRowContext(ctx, `
		SELECT E.EMP_NO, E.JOB_CODE, E.SALARY
		FROM EMPLOYEE E
		JOIN JOB J ON E.JOB_CODE = J.JOB_CODE
		ORDER BY RAND() 
		LIMIT 1
	`).Scan(&empNo, &jobCode, &currentSalary)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("no employees found to update salary")
		}
		return fmt.Errorf("failed to get random employee: %w", err)
	}

	// Get job salary range from cache
	jobRange, exists := wo.cache.JobSalaries[jobCode]
	if !exists {
		return fmt.Errorf("job salary range not found for job code %s", jobCode)
	}

	// Generate new salary within range
	newSalary := wo.cache.RandomSalaryInRange(jobRange.MinSalary, jobRange.MaxSalary)

	// Update employee salary
	result, err := tx.ExecContext(ctx, `
		UPDATE EMPLOYEE 
		SET SALARY = ? 
		WHERE EMP_NO = ?
	`, newSalary, empNo)
	if err != nil {
		return fmt.Errorf("failed to update employee salary: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected != 1 {
		return fmt.Errorf("expected 1 row affected, got %d", rowsAffected)
	}

	return nil
}

// CallAddEmpProj calls the ADD_EMP_PROJ stored procedure
func (wo *WriteOperations) CallAddEmpProj(ctx context.Context, tx *sql.Tx) error {
	// Get a random employee and project
	empNo := wo.cache.RandomEmpNo()
	projId := wo.cache.RandomProjId()

	// Call ADD_EMP_PROJ procedure
	_, err := tx.ExecContext(ctx, "EXECUTE PROCEDURE ADD_EMP_PROJ(?, ?)", empNo, projId)
	if err != nil {
		return fmt.Errorf("failed to call ADD_EMP_PROJ(%d, %s): %w", empNo, projId, err)
	}

	return nil
}

// DeleteEmpProj deletes a random employee-project assignment
func (wo *WriteOperations) DeleteEmpProj(ctx context.Context, tx *sql.Tx) error {
	// Get a random employee-project assignment
	var empNo int
	var projId string

	err := tx.QueryRowContext(ctx, `
		SELECT EMP_NO, PROJ_ID 
		FROM EMPLOYEE_PROJECT 
		ORDER BY RAND() 
		LIMIT 1
	`).Scan(&empNo, &projId)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("no employee-project assignments found to delete")
		}
		return fmt.Errorf("failed to get random employee-project assignment: %w", err)
	}

	// Delete employee-project assignment
	result, err := tx.ExecContext(ctx, `
		DELETE FROM EMPLOYEE_PROJECT 
		WHERE EMP_NO = ? AND PROJ_ID = ?
	`, empNo, projId)
	if err != nil {
		return fmt.Errorf("failed to delete employee-project assignment: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected != 1 {
		return fmt.Errorf("expected 1 row affected, got %d", rowsAffected)
	}

	return nil
}

// UpdateDeptBudget updates the budget of a random department
func (wo *WriteOperations) UpdateDeptBudget(ctx context.Context, tx *sql.Tx) error {
	// Get a random department
	deptNo := wo.cache.RandomDeptNo()

	// Generate new budget between 10001 and 2000000
	newBudget := wo.cache.RandomSalaryInRange(10001, 2000000)

	// Update department budget
	result, err := tx.ExecContext(ctx, `
		UPDATE DEPARTMENT 
		SET BUDGET = ? 
		WHERE DEPT_NO = ?
	`, newBudget, deptNo)
	if err != nil {
		return fmt.Errorf("failed to update department budget: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected != 1 {
		return fmt.Errorf("expected 1 row affected, got %d", rowsAffected)
	}

	return nil
}

// generateCustomerAddress generates random customer address data
func (wo *WriteOperations) generateCustomerAddress() (address1, address2, city, state, country, postalCode string) {
	address1 = wo.cache.RandomAddress()
	address2 = ""                    // Optional
	city = wo.cache.RandomAddress()  // Reuse address generation for city
	state = wo.cache.RandomCountry() // Simplified
	country = wo.cache.RandomCountry()
	postalCode = fmt.Sprintf("%05d", wo.rng.Intn(99999))
	return
}

// getNextOrderStatus determines the next status in the order lifecycle
func (wo *WriteOperations) getNextOrderStatus(currentStatus string) string {
	switch currentStatus {
	case "new":
		return "open"
	case "open":
		return "shipped"
	case "waiting":
		return "shipped"
	case "shipped":
		return "shipped" // Already shipped, no transition
	default:
		return currentStatus
	}
}

// WriteOperation represents a single write operation
type WriteOperation func(ctx context.Context, tx *sql.Tx) error

// WriteOperationInfo holds information about a write operation
type WriteOperationInfo struct {
	Name     string
	Weight   int
	Function WriteOperation
}

// GetWriteOperations returns all available write operations with their weights for write-heavy profile
func (wo *WriteOperations) GetWriteOperations() []WriteOperationInfo {
	return []WriteOperationInfo{
		{"InsertCustomer", 25, wo.InsertCustomer},
		{"InsertSales", 20, wo.InsertSales},
		{"UpdateSalesStatus", 15, wo.UpdateSalesStatus},
		{"CallShipOrder", 15, wo.CallShipOrder},
		{"UpdateEmployeeSalary", 10, wo.UpdateEmployeeSalary},
		{"CallAddEmpProj", 10, wo.CallAddEmpProj},
		{"DeleteEmpProj", 5, wo.DeleteEmpProj},
	}
}

// GetWriteOperationsForSpike returns write operations for spike profile (same as write-heavy)
func (wo *WriteOperations) GetWriteOperationsForSpike() []WriteOperationInfo {
	return wo.GetWriteOperations()
}

// GetRareWriteOperations returns rare write operations for read-heavy profile
func (wo *WriteOperations) GetRareWriteOperations() []WriteOperationInfo {
	return []WriteOperationInfo{
		{"UpdateDeptBudget", 100, wo.UpdateDeptBudget}, // High weight since it's the only rare write
	}
}

// GetWriteOperationByName returns a specific write operation by name
func (wo *WriteOperations) GetWriteOperationByName(name string) (WriteOperation, bool) {
	ops := wo.GetWriteOperations()
	for _, op := range ops {
		if op.Name == name {
			return op.Function, true
		}
	}
	return nil, false
}

// ExecuteRandomWriteOperation executes a random write operation based on weights
func (wo *WriteOperations) ExecuteRandomWriteOperation(ctx context.Context, tx *sql.Tx) error {
	ops := wo.GetWriteOperations()

	// Calculate total weight
	totalWeight := 0
	for _, op := range ops {
		totalWeight += op.Weight
	}

	// Pick a random operation based on weight
	target := wo.rng.Intn(totalWeight)
	current := 0

	for _, op := range ops {
		current += op.Weight
		if target < current {
			return op.Function(ctx, tx)
		}
	}

	// Fallback to first operation if something goes wrong
	return ops[0].Function(ctx, tx)
}

// ExecuteRandomRareWriteOperation executes a random rare write operation
func (wo *WriteOperations) ExecuteRandomRareWriteOperation(ctx context.Context, tx *sql.Tx) error {
	ops := wo.GetRareWriteOperations()

	// For rare writes, just pick one randomly since there's usually only one type
	if len(ops) == 0 {
		return nil
	}

	op := ops[wo.rng.Intn(len(ops))]
	return op.Function(ctx, tx)
}
