package goses

import (
	"fmt"

	"github.com/ggarcia209/go-aws-v2/v2/goaws"
)

type InvalidRecipientError struct {
	*goaws.ClientErr
}

func NewInvalidRecipientError() *InvalidRecipientError {
	return &InvalidRecipientError{
		goaws.NewClientError(fmt.Errorf("invalid recipient")),
	}
}

type UnverifiedDomainError struct {
	*goaws.ClientErr
}

func NewUnverifiedDomainError(domain string) *UnverifiedDomainError {
	return &UnverifiedDomainError{
		goaws.NewClientError(fmt.Errorf("unverified domain: %s", domain)),
	}
}

type InvalidSendRequestError struct {
	*goaws.ClientErr
}

func NewInvalidSendRequestError(message string) *InvalidSendRequestError {
	return &InvalidSendRequestError{
		goaws.NewClientError(fmt.Errorf("invalid send request: %s", message)),
	}
}
