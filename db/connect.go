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
	cfg *config.Config
}

// NewConnectionFactory creates a new connection factory
func NewConnectionFactory(cfg *config.Config) *ConnectionFactory {
	return &ConnectionFactory{
		cfg: cfg,
	}
}

// Open creates a new database connection
func (cf *ConnectionFactory) Open() (*sql.DB, error) {
	dsn := cf.cfg.DSNString()

	// Open connection
	db, err := sql.Open("firebirdsql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection: %w", err)
	}

	// Set connection limits
	db.SetMaxOpenConns(1) // Each worker owns one connection
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0) // No limit

	// Test the connection
	ctx, cancel := context.WithTimeout(context.Background(), cf.cfg.GetTxTimeout())
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
	ctx, cancel := context.WithTimeout(context.Background(), cf.cfg.GetTxTimeout())
	defer cancel()

	var count int
	err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM EMPLOYEE").Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to query EMPLOYEE table: %w", err)
	}

	// Verify we can see the expected tables
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

// GetDSN returns the DSN string for debugging
func (cf *ConnectionFactory) GetDSN() string {
	return cf.cfg.DSNString()
}

// ConnectionInfo returns connection details for logging
func (cf *ConnectionFactory) ConnectionInfo() string {
	return fmt.Sprintf("Firebird connection: %s (user: %s)", cf.cfg.DSN, cf.cfg.User)
}

// ValidateSchema checks that the EMPLOYEE database has the expected structure
func (cf *ConnectionFactory) ValidateSchema(db *sql.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), cf.cfg.GetTxTimeout())
	defer cancel()

	// Check that required tables exist and have expected columns
	requiredTables := map[string][]string{
		"CUSTOMER":         {"CUST_NO", "ON_HOLD"},
		"SALES":            {"PO_NUMBER", "ORDER_STATUS", "PAID"},
		"EMPLOYEE":         {"EMP_NO", "SALARY"},
		"EMPLOYEE_PROJECT": {"EMP_NO", "PROJ_ID"},
		"DEPARTMENT":       {"DEPT_NO", "BUDGET"},
		"COUNTRY":          {"COUNTRY"},
		"JOB":              {"JOB_CODE", "MIN_SALARY", "MAX_SALARY"},
		"PROJECT":          {"PROJ_ID"},
		"SALARY_HISTORY":   {"EMP_NO", "CHANGEDATE"},
	}

	for table, columns := range requiredTables {
		// Check table exists
		var tableName string
		query := fmt.Sprintf("SELECT RDB$RELATION_NAME FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = '%s'", table)
		err := db.QueryRowContext(ctx, query).Scan(&tableName)
		if err != nil {
			return fmt.Errorf("required table %s does not exist: %w", table, err)
		}

		// Check columns exist
		for _, column := range columns {
			var colName string
			colQuery := fmt.Sprintf("SELECT RDB$FIELD_NAME FROM RDB$RELATION_FIELDS WHERE RDB$RELATION_NAME = '%s' AND RDB$FIELD_NAME = '%s'", table, column)
			err := db.QueryRowContext(ctx, colQuery).Scan(&colName)
			if err != nil {
				return fmt.Errorf("required column %s.%s does not exist: %w", table, column, err)
			}
		}
	}

	return nil
}

// GetTableCounts returns row counts for key tables
func (cf *ConnectionFactory) GetTableCounts(db *sql.DB) (map[string]int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cf.cfg.GetTxTimeout())
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
