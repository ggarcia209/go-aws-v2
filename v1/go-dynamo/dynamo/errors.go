package dynamo

import (
	"errors"
	"fmt"
)

var (
	ErrTableNotFound = errors.New("table not found")
	// ErrTxConditionCheckFailed is returned when a transaction item fails it's conditional check.
	// This error cannot be retried.
	ErrTxConditionCheckFailed = errors.New("TX_CONDITION_CHECK_FAILED")
	// ErrTxConflict is returned when another transaction is in progress for a transaction item.
	// This error can be retried.
	ErrTxConflict = errors.New("TX_CONFLICT")
	// ErrTxInProgress is returned when multiple transactions are attempted with the same idempotency key.
	// This error cannot be retried.
	ErrTxInProgress = errors.New("TX_IN_PROGRESS")
	// ErrTxThrottled is returned when a transaction item fails due to throttling.
	// This error can be retried.
	ErrTxThrottled        = errors.New("TX_THROTTLED")
	ErrInvalidRequestType = errors.New("INVALID_REQUEST_TYPE")

	ErrRateLimitExceeded      = errors.New("rate limit exceeded")
	ErrResourceNotFound       = errors.New("resource not found")
	ErrCollectionSizeExceeded = errors.New("collection size exceeded")
	ErrReferenceObjectsCount  = errors.New("number of reference objects does not match number of queries")
	ErrResourceInUse          = errors.New("resource in use")
)

type TableNotFoundErr struct {
	tableName string
}

func (e *TableNotFoundErr) Error() string {
	return fmt.Sprintf("table %s not found", e.tableName)
}

func NewTableNotFoundErr(tableName string) *TableNotFoundErr {
	return &TableNotFoundErr{tableName: tableName}
}

type ConditionCheckFailedErr struct {
	msg string
}

func (e *ConditionCheckFailedErr) Error() string {
	return fmt.Sprintf("condition check failed: %s", e.msg)
}

func NewConditionCheckFailedErr(msg string) *ConditionCheckFailedErr {
	return &ConditionCheckFailedErr{msg: msg}
}
