package ops

import (
	"context"
	"database/sql"
	"fmt"

	"fb-loadgen/db"
)

// ReadOperations provides all read operations for the simulation
type ReadOperations struct {
	connFactory *db.ConnectionFactory
	cache       *Cache
}

// NewReadOperations creates a new ReadOperations instance
func NewReadOperations(connFactory *db.ConnectionFactory, cache *Cache) *ReadOperations {
	return &ReadOperations{
		connFactory: connFactory,
		cache:       cache,
	}
}

// CallOrgChart calls the ORG_CHART stored procedure
// Returns: HEAD_DEPT, DEPARTMENT, MNGR_NAME, TITLE, EMP_CNT
func (ro *ReadOperations) CallOrgChart(ctx context.Context, tx *sql.Tx) error {
	// ORG_CHART is a selectable SP that returns department hierarchy
	// Columns: HEAD_DEPT, DEPARTMENT, MNGR_NAME, TITLE, EMP_CNT
	rows, err := tx.QueryContext(ctx, "SELECT HEAD_DEPT, DEPARTMENT, MNGR_NAME, TITLE, EMP_CNT FROM ORG_CHART")
	if err != nil {
		return fmt.Errorf("failed to call ORG_CHART: %w", err)
	}
	defer rows.Close()

	// Consume all rows to simulate real workload
	var count int
	for rows.Next() {
		var headDept, department, mgrName, title sql.NullString
		var empCnt sql.NullInt64
		if err := rows.Scan(&headDept, &department, &mgrName, &title, &empCnt); err != nil {
			return fmt.Errorf("failed to scan ORG_CHART row: %w", err)
		}
		count++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("ORG_CHART rows error: %w", err)
	}

	return nil
}

// CallDeptBudget calls the DEPT_BUDGET stored procedure
// Returns: TOT (single column)
func (ro *ReadOperations) CallDeptBudget(ctx context.Context, tx *sql.Tx) error {
	// Pick a random department
	deptNo := ro.cache.RandomDeptNo()

	// DEPT_BUDGET is a selectable SP that takes a department number
	// Returns: TOT
	rows, err := tx.QueryContext(ctx, "SELECT TOT FROM DEPT_BUDGET(?)", deptNo)
	if err != nil {
		return fmt.Errorf("failed to call DEPT_BUDGET(%s): %w", deptNo, err)
	}
	defer rows.Close()

	// Consume all rows
	var count int
	for rows.Next() {
		var tot sql.NullFloat64
		if err := rows.Scan(&tot); err != nil {
			return fmt.Errorf("failed to scan DEPT_BUDGET row: %w", err)
		}
		count++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("DEPT_BUDGET rows error: %w", err)
	}

	return nil
}

// CallMailLabel calls the MAIL_LABEL stored procedure
// Returns: LINE1, LINE2, LINE3, LINE4, LINE5, LINE6
func (ro *ReadOperations) CallMailLabel(ctx context.Context, tx *sql.Tx) error {
	// Pick a random customer
	custNo := ro.cache.RandomCustNo()

	// MAIL_LABEL is a selectable SP that takes a customer number
	// Returns: LINE1, LINE2, LINE3, LINE4, LINE5, LINE6
	rows, err := tx.QueryContext(ctx, "SELECT LINE1, LINE2, LINE3, LINE4, LINE5, LINE6 FROM MAIL_LABEL(?)", custNo)
	if err != nil {
		return fmt.Errorf("failed to call MAIL_LABEL(%d): %w", custNo, err)
	}
	defer rows.Close()

	// Consume all rows
	var count int
	for rows.Next() {
		var line1, line2, line3, line4, line5, line6 sql.NullString
		if err := rows.Scan(&line1, &line2, &line3, &line4, &line5, &line6); err != nil {
			return fmt.Errorf("failed to scan MAIL_LABEL row: %w", err)
		}
		count++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("MAIL_LABEL rows error: %w", err)
	}

	return nil
}

// CallGetEmpProj calls the GET_EMP_PROJ stored procedure
// Returns: PROJ_ID
func (ro *ReadOperations) CallGetEmpProj(ctx context.Context, tx *sql.Tx) error {
	// Pick a random employee
	empNo := ro.cache.RandomEmpNo()

	// GET_EMP_PROJ is a selectable SP that takes an employee number
	// Returns: PROJ_ID
	rows, err := tx.QueryContext(ctx, "SELECT PROJ_ID FROM GET_EMP_PROJ(?)", empNo)
	if err != nil {
		return fmt.Errorf("failed to call GET_EMP_PROJ(%d): %w", empNo, err)
	}
	defer rows.Close()

	// Consume all rows
	var count int
	for rows.Next() {
		var projId sql.NullString
		if err := rows.Scan(&projId); err != nil {
			return fmt.Errorf("failed to scan GET_EMP_PROJ row: %w", err)
		}
		count++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("GET_EMP_PROJ rows error: %w", err)
	}

	return nil
}

// CallSubTotBudget calls the SUB_TOT_BUDGET stored procedure
// Returns: TOT_BUDGET, AVG_BUDGET, MIN_BUDGET, MAX_BUDGET
func (ro *ReadOperations) CallSubTotBudget(ctx context.Context, tx *sql.Tx) error {
	// Pick a random department
	deptNo := ro.cache.RandomDeptNo()

	// SUB_TOT_BUDGET is a selectable SP that takes a department number
	// Returns: TOT_BUDGET, AVG_BUDGET, MIN_BUDGET, MAX_BUDGET
	rows, err := tx.QueryContext(ctx, "SELECT TOT_BUDGET, AVG_BUDGET, MIN_BUDGET, MAX_BUDGET FROM SUB_TOT_BUDGET(?)", deptNo)
	if err != nil {
		return fmt.Errorf("failed to call SUB_TOT_BUDGET(%s): %w", deptNo, err)
	}
	defer rows.Close()

	// Consume all rows
	var count int
	for rows.Next() {
		var totBudget, avgBudget, minBudget, maxBudget sql.NullFloat64
		if err := rows.Scan(&totBudget, &avgBudget, &minBudget, &maxBudget); err != nil {
			return fmt.Errorf("failed to scan SUB_TOT_BUDGET row: %w", err)
		}
		count++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("SUB_TOT_BUDGET rows error: %w", err)
	}

	return nil
}

// SelectEmployeeDeptJob performs a multi-table JOIN query
func (ro *ReadOperations) SelectEmployeeDeptJob(ctx context.Context, tx *sql.Tx) error {
	// This simulates a complex read query joining EMPLOYEE, DEPARTMENT, and JOB tables
	// DEPARTMENT table has 'DEPARTMENT' column, not 'DEPT_NAME'
	// Note: PHONE_EXT can be NULL in the database
	rows, err := tx.QueryContext(ctx, `
		SELECT 
			E.EMP_NO,
			E.FIRST_NAME,
			E.LAST_NAME,
			E.PHONE_EXT,
			E.HIRE_DATE,
			E.DEPT_NO,
			D.DEPARTMENT,
			D.BUDGET,
			E.JOB_CODE,
			J.JOB_GRADE,
			E.JOB_COUNTRY,
			J.MIN_SALARY,
			J.MAX_SALARY
		FROM EMPLOYEE E
		JOIN DEPARTMENT D ON E.DEPT_NO = D.DEPT_NO
		JOIN JOB J ON E.JOB_CODE = J.JOB_CODE
		ORDER BY E.EMP_NO
		ROWS 100
	`)
	if err != nil {
		return fmt.Errorf("failed to execute employee dept job query: %w", err)
	}
	defer rows.Close()

	// Consume all rows
	var count int
	for rows.Next() {
		var empNo int
		var firstName, lastName, department, jobCode, jobCountry string
		var hireDate string
		var budget, minSalary, maxSalary float64
		var jobGrade int
		var phoneExtNull, deptNoNull sql.NullString
		if err := rows.Scan(&empNo, &firstName, &lastName, &phoneExtNull, &hireDate, &deptNoNull, &department, &budget, &jobCode, &jobGrade, &jobCountry, &minSalary, &maxSalary); err != nil {
			return fmt.Errorf("failed to scan employee dept job row: %w", err)
		}
		// PHONE_EXT and DEPT_NO can be NULL, just use the NullString values for now
		_ = phoneExtNull
		_ = deptNoNull
		count++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("employee dept job rows error: %w", err)
	}

	return nil
}

// SelectCustomerStats performs a read query to get customer statistics
func (ro *ReadOperations) SelectCustomerStats(ctx context.Context, tx *sql.Tx) error {
	// Get customer statistics
	// Note: CUSTOMER table has CUST_NO, not EMP_NO
	rows, err := tx.QueryContext(ctx, `
		SELECT 
			COUNT(*) as total_customers,
			COUNT(CASE WHEN ON_HOLD = '*' THEN 1 END) as on_hold_customers,
			AVG(CUST_NO) as avg_cust_no
		FROM CUSTOMER
	`)
	if err != nil {
		return fmt.Errorf("failed to execute customer stats query: %w", err)
	}
	defer rows.Close()

	// Consume the single row
	if rows.Next() {
		var totalCustomers, onHoldCustomers int
		var avgCustNo sql.NullFloat64
		if err := rows.Scan(&totalCustomers, &onHoldCustomers, &avgCustNo); err != nil {
			return fmt.Errorf("failed to scan customer stats row: %w", err)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("customer stats rows error: %w", err)
	}

	return nil
}

// SelectSalesStats performs a read query to get sales statistics
func (ro *ReadOperations) SelectSalesStats(ctx context.Context, tx *sql.Tx) error {
	// Get sales statistics
	rows, err := tx.QueryContext(ctx, `
		SELECT 
			COUNT(*) as total_orders,
			COUNT(CASE WHEN ORDER_STATUS = 'new' THEN 1 END) as new_orders,
			COUNT(CASE WHEN ORDER_STATUS = 'open' THEN 1 END) as open_orders,
			COUNT(CASE WHEN ORDER_STATUS = 'shipped' THEN 1 END) as shipped_orders,
			COUNT(CASE WHEN ORDER_STATUS = 'waiting' THEN 1 END) as waiting_orders,
			AVG(DISCOUNT) as avg_discount
		FROM SALES
	`)
	if err != nil {
		return fmt.Errorf("failed to execute sales stats query: %w", err)
	}
	defer rows.Close()

	// Consume the single row
	if rows.Next() {
		var totalOrders, newOrders, openOrders, shippedOrders, waitingOrders int
		var avgDiscount sql.NullFloat64
		if err := rows.Scan(&totalOrders, &newOrders, &openOrders, &shippedOrders, &waitingOrders, &avgDiscount); err != nil {
			return fmt.Errorf("failed to scan sales stats row: %w", err)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("sales stats rows error: %w", err)
	}

	return nil
}

// SelectEmployeeCountByDept performs a read query to get employee counts by department
func (ro *ReadOperations) SelectEmployeeCountByDept(ctx context.Context, tx *sql.Tx) error {
	// Get employee counts by department
	// DEPARTMENT table has 'DEPARTMENT' column, not 'DEPT_NAME'
	rows, err := tx.QueryContext(ctx, `
		SELECT 
			D.DEPT_NO,
			D.DEPARTMENT,
			COUNT(E.EMP_NO) as employee_count
		FROM DEPARTMENT D
		LEFT JOIN EMPLOYEE E ON D.DEPT_NO = E.DEPT_NO
		GROUP BY D.DEPT_NO, D.DEPARTMENT
		ORDER BY D.DEPT_NO
	`)
	if err != nil {
		return fmt.Errorf("failed to execute employee count by dept query: %w", err)
	}
	defer rows.Close()

	// Consume all rows
	var count int
	for rows.Next() {
		var deptNo, department string
		var employeeCount int
		if err := rows.Scan(&deptNo, &department, &employeeCount); err != nil {
			return fmt.Errorf("failed to scan employee count row: %w", err)
		}
		count++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("employee count by dept rows error: %w", err)
	}

	return nil
}

// ReadOperation represents a single read operation
type ReadOperation func(ctx context.Context, tx *sql.Tx) error

// ReadOperationInfo holds information about a read operation
type ReadOperationInfo struct {
	Name     string
	Weight   int
	Function ReadOperation
}

// GetReadOperations returns all available read operations with their weights
func (ro *ReadOperations) GetReadOperations() []ReadOperationInfo {
	return []ReadOperationInfo{
		{"CallOrgChart", 30, ro.CallOrgChart},
		{"CallDeptBudget", 25, ro.CallDeptBudget},
		{"CallMailLabel", 15, ro.CallMailLabel},
		{"CallGetEmpProj", 10, ro.CallGetEmpProj},
		{"CallSubTotBudget", 10, ro.CallSubTotBudget},
		{"SelectEmployeeDeptJob", 5, ro.SelectEmployeeDeptJob},
		{"SelectCustomerStats", 3, ro.SelectCustomerStats},
		{"SelectSalesStats", 2, ro.SelectSalesStats},
	}
}

// GetReadOperationByName returns a specific read operation by name
func (ro *ReadOperations) GetReadOperationByName(name string) (ReadOperation, bool) {
	ops := ro.GetReadOperations()
	for _, op := range ops {
		if op.Name == name {
			return op.Function, true
		}
	}
	return nil, false
}

// ExecuteRandomReadOperation executes a random read operation based on weights
func (ro *ReadOperations) ExecuteRandomReadOperation(ctx context.Context, tx *sql.Tx) error {
	ops := ro.GetReadOperations()

	// Calculate total weight
	totalWeight := 0
	for _, op := range ops {
		totalWeight += op.Weight
	}

	// Pick a random operation based on weight
	target := ro.cache.rng.Intn(totalWeight)
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
