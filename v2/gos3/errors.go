package gos3

import (
	"errors"
	"fmt"

	"github.com/ggarcia209/go-aws-v2/v2/goaws"
)

type ItemNotFoundError struct {
	*goaws.ClientErr
}

func NewItemNotFoundError(item string) error {
	return &ItemNotFoundError{
		goaws.NewClientError(fmt.Errorf("item not found: %s", item)),
	}
}

type MissingChecksumError struct {
	*goaws.InternalError
}

func NewMissingChecksumError() error {
	return &MissingChecksumError{
		goaws.NewInternalError(errors.New("missing checksum")),
	}
}
