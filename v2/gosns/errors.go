package gosns

import (
	"errors"
	"fmt"

	"github.com/ggarcia209/go-aws-v2/v2/goaws"
)

var (
	ErrInvalidProtocol = errors.New("invalid protocol")
)

type InvalidProtocolError struct {
	*goaws.ClientErr
}

func NewInvalidProtocolError(protocol string) error {
	return &InvalidProtocolError{goaws.NewClientError(fmt.Errorf("invalid protocol: %s", protocol))}
}
