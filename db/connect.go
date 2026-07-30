package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"fb-loadgen/config"

	_ "github.com/nakagami/firebirdsql"
)

// ConnectionFactory handles Firebird database connections
type ConnectionFactory struct {
	dial DialSettings
}

// NewConnectionFactory creates a factory from CLI/session config.
func NewConnectionFactory(cfg *config.Config) *ConnectionFactory {
	return NewConnectionFactoryDial(DialFromConfig(cfg))
}

// NewConnectionFactoryDial creates a factory from explicit dial settings.
func NewConnectionFactoryDial(dial DialSettings) *ConnectionFactory {
	if dial.TxTimeout <= 0 {
		dial.TxTimeout = 10 * time.Second
	}
	return &ConnectionFactory{dial: dial}
}

// Open creates a new database connection
func (cf *ConnectionFactory) Open() (*sql.DB, error) {
	dsn := cf.dial.DriverDSN

	db, err := sql.Open("firebirdsql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection: %w", err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	ctx, cancel := context.WithTimeout(context.Background(), cf.dial.TxTimeout)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return db, nil
}

// Close safely closes a database connection
func (cf *ConnectionFactory) Close(db *sql.DB) error {
	if db == nil {
		return nil
	}
	return db.Close()
}

// TestConnection performs a basic query to verify the database is accessible
func (cf *ConnectionFactory) TestConnection(db *sql.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), cf.dial.TxTimeout)
	defer cancel()

	var count int
	err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM EMPLOYEE").Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to query EMPLOYEE table: %w", err)
	}

	expectedTables := []string{"CUSTOMER", "SALES", "EMPLOYEE", "EMPLOYEE_PROJECT", "DEPARTMENT"}
	for _, table := range expectedTables {
		var exists string
		query := fmt.Sprintf("SELECT RDB$RELATION_NAME FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = '%s'", table)
		err := db.QueryRowContext(ctx, query).Scan(&exists)
		if err != nil {
			return fmt.Errorf("failed to verify table %s exists: %w", table, err)
		}
	}

	return nil
}

// CheckEmployeeSchema verifies the EMPLOYEE table exists (schema gate).
func (cf *ConnectionFactory) CheckEmployeeSchema() error {
	db, err := cf.Open()
	if err != nil {
		return err
	}
	defer cf.Close(db)

	ctx, cancel := context.WithTimeout(context.Background(), cf.dial.TxTimeout)
	defer cancel()

	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM EMPLOYEE").Scan(&count); err != nil {
		return fmt.Errorf("EMPLOYEE schema check failed: %w", err)
	}
	return nil
}

// ValidateSchemaGate opens a connection and runs the full schema validation.
func (cf *ConnectionFactory) ValidateSchemaGate() error {
	db, err := cf.Open()
	if err != nil {
		return err
	}
	defer cf.Close(db)
	return cf.ValidateSchema(db)
}

// GetDSN returns the full connection string for the firebirdsql driver
func (cf *ConnectionFactory) GetDSN() string {
	return cf.dial.DriverDSN
}

// ConnectionInfo returns connection details for logging
func (cf *ConnectionFactory) ConnectionInfo() string {
	return cf.dial.String()
}

// Dial returns a copy of the dial settings
func (cf *ConnectionFactory) Dial() DialSettings {
	return cf.dial
}

// ValidateSchema checks that the EMPLOYEE database has the expected structure
func (cf *ConnectionFactory) ValidateSchema(db *sql.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), cf.dial.TxTimeout)
	defer cancel()

	// Columns must match the classic Firebird EMPLOYEE sample (see EMPLOYEE_metadata.sql).
	// Firebird pads RDB$* CHAR names, so comparisons use TRIM.
	requiredTables := map[string][]string{
		"CUSTOMER":         {"CUST_NO", "ON_HOLD"},
		"SALES":            {"PO_NUMBER", "ORDER_STATUS", "PAID"},
		"EMPLOYEE":         {"EMP_NO", "SALARY"},
		"EMPLOYEE_PROJECT": {"EMP_NO", "PROJ_ID"},
		"DEPARTMENT":       {"DEPT_NO", "BUDGET"},
		"COUNTRY":          {"COUNTRY"},
		"JOB":              {"JOB_CODE", "MIN_SALARY", "MAX_SALARY"},
		"PROJECT":          {"PROJ_ID"},
		"SALARY_HISTORY":   {"EMP_NO", "CHANGE_DATE"},
	}

	for table, columns := range requiredTables {
		var tableName string
		query := `SELECT TRIM(RDB$RELATION_NAME) FROM RDB$RELATIONS WHERE TRIM(RDB$RELATION_NAME) = ?`
		err := db.QueryRowContext(ctx, query, table).Scan(&tableName)
		if err != nil {
			return fmt.Errorf("required table %s does not exist: %w", table, err)
		}

		for _, column := range columns {
			var colName string
			colQuery := `SELECT TRIM(RDB$FIELD_NAME) FROM RDB$RELATION_FIELDS
				WHERE TRIM(RDB$RELATION_NAME) = ? AND TRIM(RDB$FIELD_NAME) = ?`
			err := db.QueryRowContext(ctx, colQuery, table, column).Scan(&colName)
			if err != nil {
				return fmt.Errorf("required column %s.%s does not exist: %w", table, column, err)
			}
		}
	}

	return nil
}

// GetTableCounts returns row counts for key tables
func (cf *ConnectionFactory) GetTableCounts(db *sql.DB) (map[string]int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cf.dial.TxTimeout)
	defer cancel()

	tables := []string{"CUSTOMER", "SALES", "EMPLOYEE", "EMPLOYEE_PROJECT", "DEPARTMENT", "PROJECT"}
	counts := make(map[string]int)

	for _, table := range tables {
		var count int
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
		err := db.QueryRowContext(ctx, query).Scan(&count)
		if err != nil {
			return nil, fmt.Errorf("failed to count rows in %s: %w", table, err)
		}
		counts[table] = count
	}

	return counts, nil
}

// ConnectionStats returns connection pool statistics
func (cf *ConnectionFactory) ConnectionStats(db *sql.DB) string {
	stats := db.Stats()
	return fmt.Sprintf("Open: %d, InUse: %d, Idle: %d, WaitCount: %d, WaitDuration: %v",
		stats.OpenConnections, stats.InUse, stats.Idle, stats.WaitCount, stats.WaitDuration)
}

// WithTimeout executes a function with a timeout
func (cf *ConnectionFactory) WithTimeout(db *sql.DB, timeout time.Duration, fn func(*sql.Tx) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if err := fn(tx); err != nil {
		return err
	}

	return tx.Commit()
}
