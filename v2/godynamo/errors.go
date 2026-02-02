package godynamo

import (
	"fmt"

	"github.com/ggarcia209/go-aws-v2/v2/goaws"
)

type TableNotFoundError struct {
	*goaws.ClientErr
}

func NewTableNotFoundError(tableName string) *TableNotFoundError {
	return &TableNotFoundError{goaws.NewClientError(fmt.Errorf("table not found: %s", tableName))}
}

type ConditionCheckFailedError struct {
	*goaws.ClientErr
}

func NewConditionCheckFailedError(msg string) *ConditionCheckFailedError {
	return &ConditionCheckFailedError{goaws.NewClientError(fmt.Errorf("condition check failed: %s", msg))}
}

type RateLimitExceededError struct {
	*goaws.RetryableClientError
}

func NewRateLimitExceededError() *RateLimitExceededError {
	return &RateLimitExceededError{goaws.NewRetryableClientError(fmt.Errorf("rate limit exceeded"))}
}

type ResourceNotFoundError struct {
	*goaws.ClientErr
}

func NewResourceNotFoundError(resource string) *ResourceNotFoundError {
	return &ResourceNotFoundError{goaws.NewClientError(fmt.Errorf("resource not found: %s", resource))}
}

type CollectionSizeExceededError struct {
	*goaws.ClientErr
}

func NewCollectionSizeExceededError(size int) *CollectionSizeExceededError {
	return &CollectionSizeExceededError{goaws.NewClientError(fmt.Errorf("collection size exceeded: %d", size))}
}

type ReferenceObjectsCountError struct {
	*goaws.ClientErr
}

func NewReferenceObjectsCountError() *ReferenceObjectsCountError {
	return &ReferenceObjectsCountError{goaws.NewClientError(fmt.Errorf("number of reference objects does not match number of queries"))}
}

type ResourceInUseError struct {
	*goaws.RetryableClientError
}

func NewResourceInUseError(resource string) *ResourceInUseError {
	return &ResourceInUseError{goaws.NewRetryableClientError(fmt.Errorf("resource in use: %s", resource))}
}

type MaxRetriesExceededError struct {
	*goaws.ClientErr
}

func NewMaxRetriesExceededError() *MaxRetriesExceededError {
	return &MaxRetriesExceededError{goaws.NewClientError(fmt.Errorf("max retries exceeded"))}
}

type BadTxRequestError struct {
	*goaws.ClientErr
}

func NewBadTxRequestError() *BadTxRequestError {
	return &BadTxRequestError{goaws.NewClientError(fmt.Errorf("bad transaction request"))}
}

type TxConditonCheckFailedError struct {
	*goaws.ClientErr
}

func NewTxConditonCheckFailedError(msg string) *TxConditonCheckFailedError {
	return &TxConditonCheckFailedError{goaws.NewClientError(fmt.Errorf("transaction condition check failed: %s", msg))}
}

type TxThrottledError struct {
	*goaws.RetryableClientError
}

func NewTxThrottledError() *TxThrottledError {
	return &TxThrottledError{goaws.NewRetryableClientError(fmt.Errorf("transaction throttled"))}
}

type InvalidRequestTypeError struct {
	*goaws.ClientErr
}

func NewInvalidRequestTypeError() *InvalidRequestTypeError {
	return &InvalidRequestTypeError{goaws.NewClientError(fmt.Errorf("invalid request type"))}
}

type TxConflictError struct {
	*goaws.RetryableClientError
}

func NewTxConflictError() *TxConflictError {
	return &TxConflictError{goaws.NewRetryableClientError(fmt.Errorf("transaction conflict"))}
}

type TxInProgressError struct {
	*goaws.ClientErr
}

func NewTxInProgressError() *TxInProgressError {
	return &TxInProgressError{goaws.NewClientError(fmt.Errorf("transaction in progress"))}
}

type TxItemsExceedsLimitError struct {
	*goaws.ClientErr
}

func NewTxItemsExceedsLimitError() *TxItemsExceedsLimitError {
	return &TxItemsExceedsLimitError{goaws.NewClientError(fmt.Errorf("transaction items exceeds limit of 25"))}
}
