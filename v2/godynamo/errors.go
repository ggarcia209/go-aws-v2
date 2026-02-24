package godynamo

import (
	"errors"
	"fmt"

	"github.com/ggarcia209/go-aws-v2/v2/goaws"
)

type TableNotFoundError struct {
	*goaws.ClientErr
}

func NewTableNotFoundError(tableName string) *TableNotFoundError {
	return &TableNotFoundError{goaws.NewClientError(fmt.Errorf("table not found: %s", tableName))}
}

type NilModelError struct {
	*goaws.ClientErr
}

func NewNilModelError() *NilModelError {
	return &NilModelError{goaws.NewClientError(errors.New("input model is nil"))}
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
	return &RateLimitExceededError{goaws.NewRetryableClientError(errors.New("rate limit exceeded"))}
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
	return &ReferenceObjectsCountError{goaws.NewClientError(errors.New("number of reference objects does not match number of queries"))}
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
	return &MaxRetriesExceededError{goaws.NewClientError(errors.New("max retries exceeded"))}
}

type BadTxRequestError struct {
	*goaws.ClientErr
}

func NewBadTxRequestError() *BadTxRequestError {
	return &BadTxRequestError{goaws.NewClientError(errors.New("bad transaction request"))}
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
	return &TxThrottledError{goaws.NewRetryableClientError(errors.New("transaction throttled"))}
}

type InvalidRequestTypeError struct {
	*goaws.ClientErr
}

func NewInvalidRequestTypeError() *InvalidRequestTypeError {
	return &InvalidRequestTypeError{goaws.NewClientError(errors.New("invalid request type"))}
}

type TxConflictError struct {
	*goaws.RetryableClientError
}

func NewTxConflictError() *TxConflictError {
	return &TxConflictError{goaws.NewRetryableClientError(errors.New("transaction conflict"))}
}

type TxInProgressError struct {
	*goaws.ClientErr
}

func NewTxInProgressError() *TxInProgressError {
	return &TxInProgressError{goaws.NewClientError(errors.New("transaction in progress"))}
}

type TxItemsExceedsLimitError struct {
	*goaws.ClientErr
}

func NewTxItemsExceedsLimitError() *TxItemsExceedsLimitError {
	return &TxItemsExceedsLimitError{goaws.NewClientError(errors.New("transaction items exceeds limit of 25"))}
}
