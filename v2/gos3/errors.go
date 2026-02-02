package gos3

import (
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
