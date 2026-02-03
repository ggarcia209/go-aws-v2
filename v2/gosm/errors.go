package gosm

import (
	"fmt"

	"github.com/ggarcia209/go-aws-v2/v2/goaws"
)

type SecretNotFoundError struct {
	*goaws.ClientErr
}

func NewSecretNotFoundError(key string) *SecretNotFoundError {
	return &SecretNotFoundError{
		goaws.NewClientError(fmt.Errorf("secret not found: %s", key)),
	}
}

type SecretPermissionsError struct {
	*goaws.ClientErr
}

func NewSecretPermissionsError(key string) *SecretPermissionsError {
	return &SecretPermissionsError{
		goaws.NewClientError(fmt.Errorf("secret permissions error: %s", key)),
	}
}
