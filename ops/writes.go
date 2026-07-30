package ops

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
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
	// CUSTOMER column is VARCHAR(25), need to truncate
	if len(customerName) > 25 {
		customerName = customerName[:25]
	}
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
	totalValue := float64(wo.rng.Intn(10000) + 100) // Random value between 100 and 10100

	// Insert sales order - note SALES.SALES_REP is the FK to EMPLOYEE, not EMP_NO
	// SALES table requires: PO_NUMBER, CUST_NO, ORDER_STATUS, TOTAL_VALUE (others have defaults)
	result, err := tx.ExecContext(ctx, `
		INSERT INTO SALES (PO_NUMBER, CUST_NO, SALES_REP, ORDER_STATUS, PAID, DISCOUNT, TOTAL_VALUE)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, poNumber, custNo, empNo, orderStatus, paid, discount, totalValue)
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

// UpdateSalesStatus updates the status of a random sales order.
// Shipping is only applied when the customer is not ON_HOLD (SALES CHECK INTEG_80).
func (wo *WriteOperations) UpdateSalesStatus(ctx context.Context, tx *sql.Tx) error {
	var poNumber string
	var currentStatus string
	var onHold sql.NullString

	err := tx.QueryRowContext(ctx, `
		SELECT S.PO_NUMBER, S.ORDER_STATUS, C.ON_HOLD
		FROM SALES S
		JOIN CUSTOMER C ON C.CUST_NO = S.CUST_NO
		WHERE S.ORDER_STATUS IN ('new', 'open', 'waiting')
		ORDER BY RAND()
		ROWS 1
	`).Scan(&poNumber, &currentStatus, &onHold)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil // nothing progressable
		}
		return fmt.Errorf("failed to get random sales order: %w", err)
	}

	nextStatus := wo.getNextOrderStatus(currentStatus)
	if nextStatus == currentStatus {
		return nil
	}

	// Cannot mark shipped while customer is on hold (CHECK / CHECK_28 / INTEG_80).
	if nextStatus == "shipped" && onHold.Valid && strings.TrimSpace(onHold.String) == "*" {
		// Advance only as far as open when blocked from shipping.
		if currentStatus == "new" {
			nextStatus = "open"
		} else {
			return nil
		}
	}

	var result sql.Result
	var errExec error
	if nextStatus == "shipped" {
		result, errExec = tx.ExecContext(ctx, `
			UPDATE SALES 
			SET ORDER_STATUS = ?, SHIP_DATE = COALESCE(SHIP_DATE, CURRENT_TIMESTAMP)
			WHERE PO_NUMBER = ?
			  AND EXISTS (
				SELECT 1 FROM CUSTOMER C
				WHERE C.CUST_NO = SALES.CUST_NO
				  AND (C.ON_HOLD IS NULL OR C.ON_HOLD <> '*')
			  )
		`, nextStatus, poNumber)
	} else {
		result, errExec = tx.ExecContext(ctx, `
			UPDATE SALES 
			SET ORDER_STATUS = ? 
			WHERE PO_NUMBER = ?
		`, nextStatus, poNumber)
	}
	if errExec != nil {
		return fmt.Errorf("failed to update sales status: %w", errExec)
	}

	if _, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	return nil
}

// CallShipOrder calls the SHIP_ORDER stored procedure
func (wo *WriteOperations) CallShipOrder(ctx context.Context, tx *sql.Tx) error {
	// Get a random sales order that can be shipped
	var poNumber string

	err := tx.QueryRowContext(ctx, `
		SELECT S.PO_NUMBER 
		FROM SALES S
		JOIN CUSTOMER C ON C.CUST_NO = S.CUST_NO
		WHERE S.ORDER_STATUS IN ('new', 'open', 'waiting')
		  AND (C.ON_HOLD IS NULL OR C.ON_HOLD <> '*')
		ORDER BY RAND()
		ROWS 1
	`).Scan(&poNumber)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil // nothing shippable right now
		}
		return fmt.Errorf("failed to get random sales order: %w", err)
	}

	// Call SHIP_ORDER procedure
	// Note: SHIP_ORDER can raise CUSTOMER_CHECK exception if customer has overdue balance
	// This is expected business logic and should be handled gracefully
	_, err = tx.ExecContext(ctx, "EXECUTE PROCEDURE SHIP_ORDER(?)", poNumber)
	if err != nil {
		errStr := err.Error()
		// Check for expected exceptions
		if contains(errStr, "CUSTOMER_CHECK") ||
			contains(errStr, "ORDER_ALREADY_SHIPPED") ||
			contains(errStr, "customer_check") ||
			contains(errStr, "order_already_shipped") {
			// Expected business logic error - not a real failure
			return nil
		}
		return fmt.Errorf("failed to call SHIP_ORDER(%s): %w", poNumber, err)
	}

	return nil
}

// contains checks if a string contains a substring (case-insensitive)
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsIgnoreCase(s, substr))
}

func containsIgnoreCase(s, substr string) bool {
	s = strings.ToLower(s)
	substr = strings.ToLower(substr)
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// UpdateEmployeeSalary updates the salary of a random employee within job band
// and SALARY_HISTORY percent_change CHECK (±50%).
func (wo *WriteOperations) UpdateEmployeeSalary(ctx context.Context, tx *sql.Tx) error {
	var empNo int
	var minSalary, maxSalary, currentSalary float64

	err := tx.QueryRowContext(ctx, `
		SELECT E.EMP_NO, J.MIN_SALARY, J.MAX_SALARY, E.SALARY
		FROM EMPLOYEE E
		JOIN JOB J ON E.JOB_CODE = J.JOB_CODE
			AND E.JOB_GRADE = J.JOB_GRADE
			AND E.JOB_COUNTRY = J.JOB_COUNTRY
		ORDER BY RAND()
		ROWS 1
	`).Scan(&empNo, &minSalary, &maxSalary, &currentSalary)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("no employees found to update salary")
		}
		return fmt.Errorf("failed to get random employee: %w", err)
	}

	newSalary := wo.cache.SalaryWithinPercentCap(currentSalary, minSalary, maxSalary, 45)
	if newSalary == currentSalary {
		return nil
	}

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

// CallAddEmpProj calls the ADD_EMP_PROJ stored procedure for a pair that is not yet assigned.
func (wo *WriteOperations) CallAddEmpProj(ctx context.Context, tx *sql.Tx) error {
	empNo := wo.cache.RandomEmpNo()
	projId := wo.cache.RandomProjId()

	var exists int
	err := tx.QueryRowContext(ctx, `
		SELECT 1 FROM EMPLOYEE_PROJECT WHERE EMP_NO = ? AND PROJ_ID = ? ROWS 1
	`, empNo, projId).Scan(&exists)
	if err == nil {
		// Already assigned — pick any unassigned pair if possible
		err2 := tx.QueryRowContext(ctx, `
			SELECT E.EMP_NO, P.PROJ_ID
			FROM EMPLOYEE E
			CROSS JOIN PROJECT P
			WHERE NOT EXISTS (
				SELECT 1 FROM EMPLOYEE_PROJECT EP
				WHERE EP.EMP_NO = E.EMP_NO AND EP.PROJ_ID = P.PROJ_ID
			)
			ORDER BY RAND()
			ROWS 1
		`).Scan(&empNo, &projId)
		if err2 == sql.ErrNoRows {
			return nil // nothing left to assign
		}
		if err2 != nil {
			return fmt.Errorf("failed to find unassigned emp/proj: %w", err2)
		}
	} else if err != sql.ErrNoRows {
		return fmt.Errorf("failed to check emp/proj: %w", err)
	}

	_, err = tx.ExecContext(ctx, "EXECUTE PROCEDURE ADD_EMP_PROJ(?, ?)", empNo, projId)
	if err != nil {
		return fmt.Errorf("failed to call ADD_EMP_PROJ(%d, %s): %w", empNo, projId, err)
	}

	return nil
}

// DeleteEmpProj deletes a random employee-project assignment
func (wo *WriteOperations) DeleteEmpProj(ctx context.Context, tx *sql.Tx) error {
	var empNo int
	var projId string

	err := tx.QueryRowContext(ctx, `
		SELECT EMP_NO, PROJ_ID 
		FROM EMPLOYEE_PROJECT 
		ORDER BY RAND()
		ROWS 1
	`).Scan(&empNo, &projId)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil // nothing to delete
		}
		return fmt.Errorf("failed to get random employee-project assignment: %w", err)
	}

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

	// Concurrent workers may delete the same row — treat as benign.
	if rowsAffected == 0 {
		return nil
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
	// Truncate to fit ADDRESSLINE domain (VARCHAR(30))
	if len(address1) > 30 {
		address1 = address1[:30]
	}
	address2 = "" // Optional, leave empty
	city = wo.cache.RandomCitySimple()
	// Truncate to fit VARCHAR(25)
	if len(city) > 25 {
		city = city[:25]
	}
	state = wo.cache.RandomCountry() // Simplified - use country as state
	// Truncate to fit VARCHAR(15)
	if len(state) > 15 {
		state = state[:15]
	}
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

// TruncateString truncates a string to the specified length
func truncateString(s string, maxLen int) string {
	s = strings.TrimSpace(s)
	if len(s) > maxLen {
		return s[:maxLen]
	}
	return s
}
