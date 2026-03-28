package ops

import (
	"fmt"
	"strings"
)

// FirebirdException represents a classified Firebird exception
type FirebirdException struct {
	Code       string
	Message    string
	IsExpected bool
}

// Error implements the error interface
func (fe FirebirdException) Error() string {
	return fmt.Sprintf("Firebird exception %s: %s", fe.Code, fe.Message)
}

// IsExpectedException checks if the given error is an expected Firebird exception
func IsExpectedException(err error) bool {
	if fe, ok := err.(FirebirdException); ok {
		return fe.IsExpected
	}
	return false
}

// ClassifyError classifies a database error as expected or unexpected
func ClassifyError(err error) (bool, error) {
	if err == nil {
		return true, nil
	}

	// Convert to string for pattern matching
	errStr := err.Error()
	errStr = strings.ToLower(errStr)

	// Known expected exceptions (business logic rejections)
	expectedExceptions := map[string]string{
		"order_already_shipped": "Order already shipped",
		"customer_on_hold":      "Customer on hold",
		"customer_check":        "Customer constraint violation",
		"unknown_emp_id":        "Unknown employee ID",
		"reassign_sales":        "Sales reassignment",
		"po_number":             "PO number constraint",
		"order_status":          "Order status constraint",
		"paid":                  "Paid status constraint",
		"discount":              "Discount constraint",
		"cust_no":               "Customer number constraint",
		"emp_no":                "Employee number constraint",
		"dept_no":               "Department number constraint",
		"job_code":              "Job code constraint",
		"min_salary":            "Minimum salary constraint",
		"max_salary":            "Maximum salary constraint",
		"budget":                "Budget constraint",
		"proj_id":               "Project ID constraint",
	}

	// Check for expected exceptions
	for code, description := range expectedExceptions {
		if strings.Contains(errStr, code) || strings.Contains(errStr, description) {
			return true, FirebirdException{
				Code:       code,
				Message:    err.Error(),
				IsExpected: true,
			}
		}
	}

	// Check for specific expected exception patterns
	if strings.Contains(errStr, "order_already_shipped") ||
		strings.Contains(errStr, "order already shipped") {
		return true, FirebirdException{
			Code:       "order_already_shipped",
			Message:    err.Error(),
			IsExpected: true,
		}
	}

	if strings.Contains(errStr, "customer_on_hold") ||
		strings.Contains(errStr, "customer on hold") {
		return true, FirebirdException{
			Code:       "customer_on_hold",
			Message:    err.Error(),
			IsExpected: true,
		}
	}

	if strings.Contains(errStr, "customer_check") ||
		strings.Contains(errStr, "customer constraint") {
		return true, FirebirdException{
			Code:       "customer_check",
			Message:    err.Error(),
			IsExpected: true,
		}
	}

	if strings.Contains(errStr, "unknown_emp_id") ||
		strings.Contains(errStr, "unknown employee") {
		return true, FirebirdException{
			Code:       "unknown_emp_id",
			Message:    err.Error(),
			IsExpected: true,
		}
	}

	if strings.Contains(errStr, "reassign_sales") ||
		strings.Contains(errStr, "reassign sales") {
		return true, FirebirdException{
			Code:       "reassign_sales",
			Message:    err.Error(),
			IsExpected: true,
		}
	}

	// Check for constraint violations that might be expected
	if strings.Contains(errStr, "violation of foreign key constraint") ||
		strings.Contains(errStr, "foreign key") {
		return true, FirebirdException{
			Code:       "foreign_key_violation",
			Message:    err.Error(),
			IsExpected: true,
		}
	}

	if strings.Contains(errStr, "violation of unique constraint") ||
		strings.Contains(errStr, "unique constraint") {
		return true, FirebirdException{
			Code:       "unique_constraint_violation",
			Message:    err.Error(),
			IsExpected: true,
		}
	}

	if strings.Contains(errStr, "check constraint") ||
		strings.Contains(errStr, "constraint violation") {
		return true, FirebirdException{
			Code:       "check_constraint_violation",
			Message:    err.Error(),
			IsExpected: true,
		}
	}

	// Check for deadlock and lock timeout (these are expected in high concurrency)
	if strings.Contains(errStr, "deadlock") ||
		strings.Contains(errStr, "lock conflict") ||
		strings.Contains(errStr, "lock timeout") {
		return true, FirebirdException{
			Code:       "lock_conflict",
			Message:    err.Error(),
			IsExpected: true,
		}
	}

	// Check for connection issues (these are expected and should trigger reconnection)
	if strings.Contains(errStr, "connection") ||
		strings.Contains(errStr, "network") ||
		strings.Contains(errStr, "timeout") ||
		strings.Contains(errStr, "unavailable") {
		return false, FirebirdException{
			Code:       "connection_error",
			Message:    err.Error(),
			IsExpected: false,
		}
	}

	// If we can't classify it, treat it as unexpected
	return false, err
}

// HandleExpectedError logs expected errors without spam
func HandleExpectedError(err error, operation string) {
	if fe, ok := err.(FirebirdException); ok && fe.IsExpected {
		// Log expected errors at debug level or skip logging entirely
		// For now, we'll just return without logging to avoid spam
		return
	}

	// For unexpected errors, you might want to log them
	// log.Printf("Unexpected error in %s: %v", operation, err)
}

// HandleTransactionError handles transaction errors with appropriate logging
func HandleTransactionError(err error, operation string) error {
	isExpected, classifiedErr := ClassifyError(err)
	if isExpected {
		// Expected error - just return it, don't log
		return classifiedErr
	}

	// Unexpected error - log it and return
	return fmt.Errorf("unexpected transaction error in %s: %w", operation, classifiedErr)
}

// IsRetryableError checks if an error should trigger a retry
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())

	// Connection issues are retryable
	if strings.Contains(errStr, "connection") ||
		strings.Contains(errStr, "network") ||
		strings.Contains(errStr, "timeout") ||
		strings.Contains(errStr, "unavailable") {
		return true
	}

	// Deadlocks are retryable
	if strings.Contains(errStr, "deadlock") ||
		strings.Contains(errStr, "lock conflict") {
		return true
	}

	// Other errors are not retryable
	return false
}

// GetErrorCode extracts a simple error code from an error
func GetErrorCode(err error) string {
	if err == nil {
		return "success"
	}

	if fe, ok := err.(FirebirdException); ok {
		return fe.Code
	}

	errStr := strings.ToLower(err.Error())

	// Map common error patterns to codes
	if strings.Contains(errStr, "order_already_shipped") {
		return "order_already_shipped"
	}
	if strings.Contains(errStr, "customer_on_hold") {
		return "customer_on_hold"
	}
	if strings.Contains(errStr, "customer_check") {
		return "customer_check"
	}
	if strings.Contains(errStr, "unknown_emp_id") {
		return "unknown_emp_id"
	}
	if strings.Contains(errStr, "reassign_sales") {
		return "reassign_sales"
	}
	if strings.Contains(errStr, "foreign key") {
		return "foreign_key_violation"
	}
	if strings.Contains(errStr, "unique constraint") {
		return "unique_constraint_violation"
	}
	if strings.Contains(errStr, "check constraint") {
		return "check_constraint_violation"
	}
	if strings.Contains(errStr, "deadlock") {
		return "deadlock"
	}
	if strings.Contains(errStr, "connection") {
		return "connection_error"
	}

	return "unknown_error"
}

// ErrorStats tracks error statistics
type ErrorStats struct {
	TotalErrors      int
	ExpectedErrors   int
	UnexpectedErrors int
	RetryableErrors  int
	ErrorCounts      map[string]int
}

// NewErrorStats creates a new ErrorStats instance
func NewErrorStats() *ErrorStats {
	return &ErrorStats{
		ErrorCounts: make(map[string]int),
	}
}

// RecordError records an error for statistics
func (es *ErrorStats) RecordError(err error) {
	if err == nil {
		return
	}

	es.TotalErrors++
	errorCode := GetErrorCode(err)
	es.ErrorCounts[errorCode]++

	isExpected, _ := ClassifyError(err)
	if isExpected {
		es.ExpectedErrors++
	} else {
		es.UnexpectedErrors++
	}

	if IsRetryableError(err) {
		es.RetryableErrors++
	}
}

// GetStats returns a summary of error statistics
func (es *ErrorStats) GetStats() string {
	return fmt.Sprintf("Total: %d, Expected: %d, Unexpected: %d, Retryable: %d",
		es.TotalErrors, es.ExpectedErrors, es.UnexpectedErrors, es.RetryableErrors)
}

// Reset resets all error statistics
func (es *ErrorStats) Reset() {
	es.TotalErrors = 0
	es.ExpectedErrors = 0
	es.UnexpectedErrors = 0
	es.RetryableErrors = 0
	for k := range es.ErrorCounts {
		delete(es.ErrorCounts, k)
	}
}
