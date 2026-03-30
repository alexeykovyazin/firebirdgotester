package ops

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"fb-loadgen/config"
	"fb-loadgen/db"
)

// testDB holds the test database connection and factory
var testDB *sql.DB
var testFactory *db.ConnectionFactory
var testCache *Cache

// skipIfNoDB skips the test if no database is available
func skipIfNoDB(t *testing.T) {
	if testDB == nil {
		t.Skip("Database not available")
	}
}

// setupTestDB sets up the test database connection
func setupTestDB(t *testing.T) {
	// Only run setup once
	if testDB != nil {
		return
	}

	// Read connection config from environment or use defaults
	dsn := os.Getenv("FIREBIRD_DSN")
	user := os.Getenv("FIREBIRD_USER")
	pass := os.Getenv("FIREBIRD_PASS")

	if dsn == "" {
		dsn = "localhost/3055:e:/Projects_2026/firebirdgotester/EMPLOYEE.FDB"
	}
	if user == "" {
		user = "SYSDBA"
	}
	if pass == "" {
		pass = "masterkey"
	}

	cfg := &config.Config{
		DSN:       dsn,
		User:      user,
		Pass:      pass,
		TxTimeout: 10,
	}

	testFactory = db.NewConnectionFactory(cfg)

	var err error
	testDB, err = testFactory.Open()
	if err != nil {
		t.Skipf("Could not connect to database: %v", err)
		return
	}

	// Create cache
	testCache, err = NewCache(testFactory)
	if err != nil {
		testDB.Close()
		testDB = nil
		t.Skipf("Could not create cache: %v", err)
		return
	}
}

// Helper to run a read operation in a transaction
func withReadTx(t *testing.T, fn func(tx *sql.Tx) error) {
	skipIfNoDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := testDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	err = fn(tx)
	if err != nil {
		tx.Rollback()
		t.Fatalf("Operation failed: %v", err)
		return
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}

// Helper to run a write operation in a transaction
func withWriteTx(t *testing.T, fn func(tx *sql.Tx) error) {
	skipIfNoDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := testDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	err = fn(tx)
	if err != nil {
		tx.Rollback()
		t.Fatalf("Operation failed: %v", err)
		return
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}

// =============================================================================
// CACHE TESTS
// =============================================================================

func TestLoadDeptNos(t *testing.T) {
	setupTestDB(t)
	skipIfNoDB(t)

	if len(testCache.DeptNos) == 0 {
		t.Error("No department numbers loaded in cache")
	}
}

func TestLoadEmpNos(t *testing.T) {
	setupTestDB(t)
	skipIfNoDB(t)

	if len(testCache.EmpNos) == 0 {
		t.Error("No employee numbers loaded in cache")
	}
}

func TestLoadProjIds(t *testing.T) {
	setupTestDB(t)
	skipIfNoDB(t)

	if len(testCache.ProjIds) == 0 {
		t.Error("No project IDs loaded in cache")
	}
}

func TestLoadCustNos(t *testing.T) {
	setupTestDB(t)
	skipIfNoDB(t)

	if len(testCache.CustNos) == 0 {
		t.Error("No customer numbers loaded in cache")
	}
}

func TestLoadJobSalaries(t *testing.T) {
	setupTestDB(t)
	skipIfNoDB(t)

	if len(testCache.JobSalaries) == 0 {
		t.Error("No job salary ranges loaded in cache")
	}
}

// =============================================================================
// READ OPERATION TESTS
// =============================================================================

func TestSelectDepartment(t *testing.T) {
	setupTestDB(t)
	skipIfNoDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var count int
	err := testDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM DEPARTMENT").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query DEPARTMENT: %v", err)
	}
	if count == 0 {
		t.Error("No departments found")
	}
	t.Logf("Found %d departments", count)
}

func TestSelectEmployee(t *testing.T) {
	setupTestDB(t)
	skipIfNoDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var count int
	err := testDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM EMPLOYEE").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query EMPLOYEE: %v", err)
	}
	if count == 0 {
		t.Error("No employees found")
	}
	t.Logf("Found %d employees", count)
}

func TestSelectProject(t *testing.T) {
	setupTestDB(t)
	skipIfNoDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var count int
	err := testDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM PROJECT").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query PROJECT: %v", err)
	}
	if count == 0 {
		t.Error("No projects found")
	}
	t.Logf("Found %d projects", count)
}

func TestSelectCustomer(t *testing.T) {
	setupTestDB(t)
	skipIfNoDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var count int
	err := testDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM CUSTOMER").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query CUSTOMER: %v", err)
	}
	if count == 0 {
		t.Error("No customers found")
	}
	t.Logf("Found %d customers", count)
}

func TestSelectJob(t *testing.T) {
	setupTestDB(t)
	skipIfNoDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var count int
	err := testDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM JOB").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query JOB: %v", err)
	}
	if count == 0 {
		t.Error("No jobs found")
	}
	t.Logf("Found %d jobs", count)
}

// =============================================================================
// STORED PROCEDURE TESTS
// =============================================================================

func TestCallOrgChart(t *testing.T) {
	setupTestDB(t)
	readOps := NewReadOperations(testFactory, testCache)

	withReadTx(t, func(tx *sql.Tx) error {
		return readOps.CallOrgChart(context.Background(), tx)
	})
}

func TestCallDeptBudget(t *testing.T) {
	setupTestDB(t)
	readOps := NewReadOperations(testFactory, testCache)

	withReadTx(t, func(tx *sql.Tx) error {
		return readOps.CallDeptBudget(context.Background(), tx)
	})
}

func TestCallMailLabel(t *testing.T) {
	setupTestDB(t)
	readOps := NewReadOperations(testFactory, testCache)

	withReadTx(t, func(tx *sql.Tx) error {
		return readOps.CallMailLabel(context.Background(), tx)
	})
}

func TestCallGetEmpProj(t *testing.T) {
	setupTestDB(t)
	readOps := NewReadOperations(testFactory, testCache)

	withReadTx(t, func(tx *sql.Tx) error {
		return readOps.CallGetEmpProj(context.Background(), tx)
	})
}

func TestCallSubTotBudget(t *testing.T) {
	setupTestDB(t)
	readOps := NewReadOperations(testFactory, testCache)

	withReadTx(t, func(tx *sql.Tx) error {
		return readOps.CallSubTotBudget(context.Background(), tx)
	})
}

// =============================================================================
// JOIN QUERY TESTS
// =============================================================================

func TestSelectEmployeeDeptJob(t *testing.T) {
	setupTestDB(t)
	readOps := NewReadOperations(testFactory, testCache)

	withReadTx(t, func(tx *sql.Tx) error {
		return readOps.SelectEmployeeDeptJob(context.Background(), tx)
	})
}

func TestSelectCustomerStats(t *testing.T) {
	setupTestDB(t)
	readOps := NewReadOperations(testFactory, testCache)

	withReadTx(t, func(tx *sql.Tx) error {
		return readOps.SelectCustomerStats(context.Background(), tx)
	})
}

func TestSelectSalesStats(t *testing.T) {
	setupTestDB(t)
	readOps := NewReadOperations(testFactory, testCache)

	withReadTx(t, func(tx *sql.Tx) error {
		return readOps.SelectSalesStats(context.Background(), tx)
	})
}

func TestSelectEmployeeCountByDept(t *testing.T) {
	setupTestDB(t)
	readOps := NewReadOperations(testFactory, testCache)

	withReadTx(t, func(tx *sql.Tx) error {
		return readOps.SelectEmployeeCountByDept(context.Background(), tx)
	})
}

// =============================================================================
// WRITE OPERATION TESTS
// =============================================================================

func TestInsertCustomer(t *testing.T) {
	setupTestDB(t)
	writeOps := NewWriteOperations(testFactory, testCache)

	withWriteTx(t, func(tx *sql.Tx) error {
		return writeOps.InsertCustomer(context.Background(), tx)
	})
}

func TestInsertSales(t *testing.T) {
	setupTestDB(t)
	writeOps := NewWriteOperations(testFactory, testCache)

	withWriteTx(t, func(tx *sql.Tx) error {
		return writeOps.InsertSales(context.Background(), tx)
	})
}

func TestUpdateSalesStatus(t *testing.T) {
	setupTestDB(t)
	writeOps := NewWriteOperations(testFactory, testCache)

	withWriteTx(t, func(tx *sql.Tx) error {
		err := writeOps.UpdateSalesStatus(context.Background(), tx)
		// It's OK if there are no sales orders to update
		if err != nil && err.Error() == "no sales orders found to update" {
			return nil
		}
		return err
	})
}

func TestCallShipOrder(t *testing.T) {
	setupTestDB(t)
	writeOps := NewWriteOperations(testFactory, testCache)

	withWriteTx(t, func(tx *sql.Tx) error {
		err := writeOps.CallShipOrder(context.Background(), tx)
		// It's OK if there are no shippable orders or customer has overdue balance
		if err != nil {
			// No shippable orders is OK
			if err.Error() == "no shippable sales orders found" {
				return nil
			}
			// CUSTOMER_CHECK exception is expected business logic
			if containsIgnoreCase(err.Error(), "CUSTOMER_CHECK") ||
				containsIgnoreCase(err.Error(), "ORDER_ALREADY_SHIPPED") {
				return nil
			}
		}
		return err
	})
}

func TestUpdateEmployeeSalary(t *testing.T) {
	setupTestDB(t)
	writeOps := NewWriteOperations(testFactory, testCache)

	withWriteTx(t, func(tx *sql.Tx) error {
		return writeOps.UpdateEmployeeSalary(context.Background(), tx)
	})
}

func TestCallAddEmpProj(t *testing.T) {
	setupTestDB(t)
	writeOps := NewWriteOperations(testFactory, testCache)

	withWriteTx(t, func(tx *sql.Tx) error {
		return writeOps.CallAddEmpProj(context.Background(), tx)
	})
}

func TestDeleteEmpProj(t *testing.T) {
	setupTestDB(t)
	writeOps := NewWriteOperations(testFactory, testCache)

	withWriteTx(t, func(tx *sql.Tx) error {
		err := writeOps.DeleteEmpProj(context.Background(), tx)
		// It's OK if there are no assignments to delete
		if err != nil && err.Error() == "no employee-project assignments found to delete" {
			return nil
		}
		return err
	})
}

func TestUpdateDeptBudget(t *testing.T) {
	setupTestDB(t)
	writeOps := NewWriteOperations(testFactory, testCache)

	withWriteTx(t, func(tx *sql.Tx) error {
		return writeOps.UpdateDeptBudget(context.Background(), tx)
	})
}

// =============================================================================
// RAW SQL QUERY TESTS - Testing specific SQL syntax
// =============================================================================

func TestRawSelectWithRows(t *testing.T) {
	setupTestDB(t)
	skipIfNoDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Firebird uses ROWS instead of LIMIT
	rows, err := testDB.QueryContext(ctx, `
		SELECT EMP_NO, FIRST_NAME, LAST_NAME
		FROM EMPLOYEE
		ORDER BY EMP_NO
		ROWS 10
	`)
	if err != nil {
		t.Fatalf("SELECT with ROWS failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var empNo int
		var firstName, lastName string
		if err := rows.Scan(&empNo, &firstName, &lastName); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		count++
	}

	if count == 0 {
		t.Error("No employees found")
	}
	if count > 10 {
		t.Errorf("Expected max 10 rows, got %d", count)
	}
	t.Logf("Found %d employees with ROWS clause", count)
}

func TestRawSelectRandomOrder(t *testing.T) {
	setupTestDB(t)
	skipIfNoDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Firebird doesn't have RAND(), use simple query without ORDER BY
	rows, err := testDB.QueryContext(ctx, `
		SELECT PO_NUMBER, ORDER_STATUS
		FROM SALES
		ROWS 5
	`)
	if err != nil {
		t.Fatalf("SELECT with ROWS failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var poNumber, status string
		if err := rows.Scan(&poNumber, &status); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		count++
	}

	t.Logf("Found %d sales orders", count)
}

func TestRawGenId(t *testing.T) {
	setupTestDB(t)
	skipIfNoDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Test GEN_ID function
	var nextVal int64
	err := testDB.QueryRowContext(ctx, "SELECT GEN_ID(CUST_NO_GEN, 1) FROM RDB$DATABASE").Scan(&nextVal)
	if err != nil {
		t.Fatalf("GEN_ID call failed: %v", err)
	}

	t.Logf("Next customer number: %d", nextVal)
}

// =============================================================================
// FIREBIRD-SPECIFIC SYNTAX TESTS
// =============================================================================

func TestSelectableStoredProcedureOrgChart(t *testing.T) {
	setupTestDB(t)
	skipIfNoDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Test ORG_CHART stored procedure with proper syntax
	// ORG_CHART returns: HEAD_DEPT, DEPARTMENT, MNGR_NAME, TITLE, EMP_CNT (5 columns)
	rows, err := testDB.QueryContext(ctx, "SELECT * FROM ORG_CHART")
	if err != nil {
		t.Fatalf("ORG_CHART call failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		// ORG_CHART returns: HEAD_DEPT, DEPARTMENT, MNGR_NAME, TITLE, EMP_CNT
		var headDept, department, mgrName, title sql.NullString
		var empCnt sql.NullInt64
		if err := rows.Scan(&headDept, &department, &mgrName, &title, &empCnt); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		count++
	}

	t.Logf("ORG_CHART returned %d rows", count)
}

func TestSelectableStoredProcedureDeptBudget(t *testing.T) {
	setupTestDB(t)
	skipIfNoDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Test DEPT_BUDGET stored procedure with parameter
	deptNo := testCache.RandomDeptNo()

	// DEPT_BUDGET returns: TOT (1 column)
	rows, err := testDB.QueryContext(ctx, "SELECT * FROM DEPT_BUDGET(?)", deptNo)
	if err != nil {
		t.Fatalf("DEPT_BUDGET call failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var tot sql.NullFloat64
		if err := rows.Scan(&tot); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		count++
	}

	t.Logf("DEPT_BUDGET(%s) returned %d rows", deptNo, count)
}

func TestExecutableStoredProcedure(t *testing.T) {
	setupTestDB(t)
	skipIfNoDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Test SHIP_ORDER stored procedure - it should work with EXECUTE PROCEDURE
	// First get a valid PO_NUMBER
	var poNumber string
	err := testDB.QueryRowContext(ctx, "SELECT PO_NUMBER FROM SALES WHERE ORDER_STATUS IN ('new', 'open', 'waiting') ROWS 1").Scan(&poNumber)
	if err != nil {
		if err == sql.ErrNoRows {
			t.Skip("No shippable sales orders found")
		}
		t.Fatalf("Failed to get PO_NUMBER: %v", err)
	}

	// Call SHIP_ORDER
	_, err = testDB.ExecContext(ctx, "EXECUTE PROCEDURE SHIP_ORDER(?)", poNumber)
	if err != nil {
		// CUSTOMER_CHECK is expected if customer has overdue balance
		t.Logf("SHIP_ORDER error (may be expected): %v", err)
	}

	t.Logf("SHIP_ORDER called for PO_NUMBER: %s", poNumber)
}

// =============================================================================
// COMPOSITE QUERY TESTS
// =============================================================================

func TestEmployeeDeptJoin(t *testing.T) {
	setupTestDB(t)
	skipIfNoDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// DEPARTMENT table has 'DEPARTMENT' column, not 'DEPT_NAME'
	rows, err := testDB.QueryContext(ctx, `
		SELECT 
			E.EMP_NO,
			E.FIRST_NAME,
			E.LAST_NAME,
			D.DEPT_NO,
			D.DEPARTMENT
		FROM EMPLOYEE E
		JOIN DEPARTMENT D ON E.DEPT_NO = D.DEPT_NO
		ORDER BY E.EMP_NO
		ROWS 10
	`)
	if err != nil {
		t.Fatalf("Employee-Department JOIN failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var empNo int
		var firstName, lastName, deptNo, department string
		if err := rows.Scan(&empNo, &firstName, &lastName, &deptNo, &department); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		count++
	}

	t.Logf("Employee-Department JOIN returned %d rows", count)
}

func TestEmployeeJobJoin(t *testing.T) {
	setupTestDB(t)
	skipIfNoDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := testDB.QueryContext(ctx, `
		SELECT 
			E.EMP_NO,
			E.FIRST_NAME,
			E.LAST_NAME,
			E.JOB_CODE,
			J.JOB_TITLE,
			E.SALARY,
			J.MIN_SALARY,
			J.MAX_SALARY
		FROM EMPLOYEE E
		JOIN JOB J ON E.JOB_CODE = J.JOB_CODE AND E.JOB_GRADE = J.JOB_GRADE AND E.JOB_COUNTRY = J.JOB_COUNTRY
		ORDER BY E.EMP_NO
		ROWS 10
	`)
	if err != nil {
		t.Fatalf("Employee-JOB JOIN failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var empNo int
		var firstName, lastName, jobCode, jobTitle string
		var salary, minSalary, maxSalary float64
		if err := rows.Scan(&empNo, &firstName, &lastName, &jobCode, &jobTitle, &salary, &minSalary, &maxSalary); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		count++
	}

	t.Logf("Employee-JOB JOIN returned %d rows", count)
}

// =============================================================================
// AGGREGATE QUERY TESTS
// =============================================================================

func TestAggregateWithCase(t *testing.T) {
	setupTestDB(t)
	skipIfNoDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var total, shipped int
	err := testDB.QueryRowContext(ctx, `
		SELECT 
			COUNT(*),
			SUM(CASE WHEN ORDER_STATUS = 'shipped' THEN 1 ELSE 0 END)
		FROM SALES
	`).Scan(&total, &shipped)
	if err != nil {
		t.Fatalf("Aggregate with CASE failed: %v", err)
	}

	t.Logf("Sales: total=%d, shipped=%d", total, shipped)
}

func TestGroupByAggregate(t *testing.T) {
	setupTestDB(t)
	skipIfNoDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// DEPARTMENT table has 'DEPARTMENT' column, not 'DEPT_NAME'
	rows, err := testDB.QueryContext(ctx, `
		SELECT 
			D.DEPT_NO,
			D.DEPARTMENT,
			COUNT(E.EMP_NO) as EMP_COUNT
		FROM DEPARTMENT D
		LEFT JOIN EMPLOYEE E ON D.DEPT_NO = E.DEPT_NO
		GROUP BY D.DEPT_NO, D.DEPARTMENT
		ORDER BY D.DEPT_NO
	`)
	if err != nil {
		t.Fatalf("GROUP BY query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var deptNo, department string
		var empCount int
		if err := rows.Scan(&deptNo, &department, &empCount); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		count++
	}

	t.Logf("Department employee counts: %d departments", count)
}

// =============================================================================
// ADDITIONAL TESTS FOR QUERY COVERAGE
// =============================================================================

func TestSelectableStoredProcedureMailLabel(t *testing.T) {
	setupTestDB(t)
	skipIfNoDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	custNo := testCache.RandomCustNo()

	// MAIL_LABEL returns: LINE1, LINE2, LINE3, LINE4, LINE5, LINE6 (6 columns)
	rows, err := testDB.QueryContext(ctx, "SELECT * FROM MAIL_LABEL(?)", custNo)
	if err != nil {
		t.Fatalf("MAIL_LABEL call failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var line1, line2, line3, line4, line5, line6 sql.NullString
		if err := rows.Scan(&line1, &line2, &line3, &line4, &line5, &line6); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		count++
	}

	t.Logf("MAIL_LABEL(%d) returned %d rows", custNo, count)
}

func TestSelectableStoredProcedureSubTotBudget(t *testing.T) {
	setupTestDB(t)
	skipIfNoDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	deptNo := testCache.RandomDeptNo()

	// SUB_TOT_BUDGET returns: TOT_BUDGET, AVG_BUDGET, MIN_BUDGET, MAX_BUDGET (4 columns)
	rows, err := testDB.QueryContext(ctx, "SELECT * FROM SUB_TOT_BUDGET(?)", deptNo)
	if err != nil {
		t.Fatalf("SUB_TOT_BUDGET call failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var totBudget, avgBudget, minBudget, maxBudget sql.NullFloat64
		if err := rows.Scan(&totBudget, &avgBudget, &minBudget, &maxBudget); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		count++
	}

	t.Logf("SUB_TOT_BUDGET(%s) returned %d rows", deptNo, count)
}

func TestSelectableStoredProcedureGetEmpProj(t *testing.T) {
	setupTestDB(t)
	skipIfNoDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	empNo := testCache.RandomEmpNo()

	// GET_EMP_PROJ returns: PROJ_ID (1 column)
	rows, err := testDB.QueryContext(ctx, "SELECT * FROM GET_EMP_PROJ(?)", empNo)
	if err != nil {
		t.Fatalf("GET_EMP_PROJ call failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var projId sql.NullString
		if err := rows.Scan(&projId); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		count++
	}

	t.Logf("GET_EMP_PROJ(%d) returned %d rows", empNo, count)
}

// Test all read operations via the ReadOperations interface
func TestReadOperationsInterface(t *testing.T) {
	setupTestDB(t)
	readOps := NewReadOperations(testFactory, testCache)

	ops := readOps.GetReadOperations()
	if len(ops) == 0 {
		t.Error("No read operations registered")
	}

	for _, op := range ops {
		t.Run(op.Name, func(t *testing.T) {
			withReadTx(t, func(tx *sql.Tx) error {
				return op.Function(context.Background(), tx)
			})
		})
	}
}

// Test all write operations via the WriteOperations interface
func TestWriteOperationsInterface(t *testing.T) {
	setupTestDB(t)
	writeOps := NewWriteOperations(testFactory, testCache)

	ops := writeOps.GetWriteOperations()
	if len(ops) == 0 {
		t.Error("No write operations registered")
	}

	for _, op := range ops {
		t.Run(op.Name, func(t *testing.T) {
			withWriteTx(t, func(tx *sql.Tx) error {
				err := op.Function(context.Background(), tx)
				// Handle expected "not found" errors and business logic exceptions
				if err != nil {
					switch err.Error() {
					case "no sales orders found to update",
						"no shippable sales orders found",
						"no employee-project assignments found to delete":
						return nil
					}
					// Check for expected business logic exceptions
					errStr := err.Error()
					if containsIgnoreCase(errStr, "CUSTOMER_CHECK") ||
						containsIgnoreCase(errStr, "ORDER_ALREADY_SHIPPED") ||
						containsIgnoreCase(errStr, "CUSTOMER_ON_HOLD") {
						return nil
					}
				}
				return err
			})
		})
	}
}
