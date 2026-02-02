package goaws

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewGenericError(t *testing.T) {
	var tests = []struct {
		name      string
		retryable bool
		clientErr bool
	}{
		{name: "internal", retryable: false, clientErr: false},
		{name: "internal retryable", retryable: true, clientErr: false},
		{name: "client", retryable: false, clientErr: true},
		{name: "client retryable", retryable: true, clientErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := errors.New("test error")
			ge := NewGenericError(err, tt.retryable, tt.clientErr)
			require.Error(t, ge)
			assert.Equal(t, tt.retryable, ge.Retryable())
			assert.Equal(t, tt.clientErr, ge.ClientError())
			assert.Implements(t, (*AwsError)(nil), ge)
		})
	}
}

func TestNewInternalError(t *testing.T) {
	err := errors.New("test error")
	ie := NewInternalError(err)
	require.Error(t, ie)
	assert.Equal(t, err.Error(), ie.Error())
	assert.Equal(t, false, ie.Retryable())
	assert.Equal(t, false, ie.ClientError())
	assert.Implements(t, (*AwsError)(nil), ie)
}

func TestNewClientError(t *testing.T) {
	err := errors.New("test error")
	ce := NewClientError(err)
	require.Error(t, ce)
	assert.Equal(t, err.Error(), ce.Error())
	assert.Equal(t, false, ce.Retryable())
	assert.Equal(t, true, ce.ClientError())
	assert.Implements(t, (*AwsError)(nil), ce)
}

func TestNewRetryableInternalError(t *testing.T) {
	err := errors.New("test error")
	ie := NewRetryableInternalError(err)
	require.Error(t, ie)
	assert.Equal(t, err.Error(), ie.Error())
	assert.Equal(t, true, ie.Retryable())
	assert.Equal(t, false, ie.ClientError())
	assert.Implements(t, (*AwsError)(nil), ie)
}

func TestNewRetryableClientError(t *testing.T) {
	err := errors.New("test error")
	ce := NewRetryableClientError(err)
	require.Error(t, ce)
	assert.Equal(t, err.Error(), ce.Error())
	assert.Equal(t, true, ce.Retryable())
	assert.Equal(t, true, ce.ClientError())
	assert.Implements(t, (*AwsError)(nil), ce)
}
