package gosqs

import (
	"errors"
	"fmt"

	"github.com/ggarcia209/go-aws-v2/v2/goaws"
)

type EmptyQueueUrlInRequestError struct {
	*goaws.ClientErr
}

func NewEmptyQueueUrlInRequestError() *EmptyQueueUrlInRequestError {
	return &EmptyQueueUrlInRequestError{
		goaws.NewClientError(errors.New("empty queue url in request")),
	}
}

type EmptyQueueUrlInResponseError struct {
	*goaws.ClientErr
}

func NewEmptyQueueUrlInResponseError() *EmptyQueueUrlInResponseError {
	return &EmptyQueueUrlInResponseError{
		goaws.NewClientError(errors.New("empty queue url in response")),
	}
}

type InvalidMessageContentError struct {
	*goaws.ClientErr
}

func NewInvalidMessageContentError(content *string) *InvalidMessageContentError {
	var msg = "nil body"
	if content != nil {
		msg = *content
	}
	return &InvalidMessageContentError{
		goaws.NewClientError(fmt.Errorf("invalid message content: %s", msg)),
	}
}

type InvalidReceiptHandlesError struct {
	*goaws.ClientErr
}

func NewInvalidReceiptHandlesError(receiptHandles, messageIds int) *InvalidReceiptHandlesError {
	return &InvalidReceiptHandlesError{
		goaws.NewClientError(fmt.Errorf("must be equal number of message IDs (%d) and receipt handles (%d)", messageIds, receiptHandles)),
	}
}

type NoMessageIDsInBatchRequestError struct {
	*goaws.ClientErr
}

func NewNoMessageIDsInBatchRequestError() *NoMessageIDsInBatchRequestError {
	return &NoMessageIDsInBatchRequestError{
		goaws.NewClientError(errors.New("no message IDs in request")),
	}
}

type MaxMessagesInBatchRequestError struct {
	*goaws.ClientErr
}

func NewMaxMessagesExceededError(msgs int) *MaxMessagesInBatchRequestError {
	return &MaxMessagesInBatchRequestError{
		goaws.NewClientError(fmt.Errorf("max 10 messages per request (%d)", msgs)),
	}
}

type QueueNotFoundError struct {
	*goaws.ClientErr
}

func NewQueueNotFoundError(name string) *QueueNotFoundError {
	return &QueueNotFoundError{
		goaws.NewClientError(fmt.Errorf("queue '%s' does not exixt", name)),
	}
}

type InvalidAddressError struct {
	*goaws.ClientErr
}

func NewInvalidAddressError(address string) *InvalidAddressError {
	return &InvalidAddressError{
		goaws.NewClientError(fmt.Errorf("invalid address '%s'", address)),
	}
}
