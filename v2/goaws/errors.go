package goaws

// AwsError is a generic interface for implementing
// error handling for each service.
type AwsError interface {
	Error() string
	Retryable() bool
	ClientError() bool
}

type GenericError struct {
	msg       string
	retryable bool
	clientErr bool
}

func (e *GenericError) Error() string {
	return e.msg
}

func (e *GenericError) Retryable() bool {
	return e.retryable
}

func (e *GenericError) ClientError() bool {
	return e.clientErr
}

func NewGenericError(err error, retryable bool, clientErr bool) *GenericError {
	if err == nil {
		return nil
	}
	return &GenericError{
		msg:       err.Error(),
		retryable: retryable,
		clientErr: clientErr,
	}
}

type InternalError struct {
	msg string
}

func (e *InternalError) Error() string {
	return e.msg
}

func (e *InternalError) Retryable() bool {
	return false
}

func (e *InternalError) ClientError() bool {
	return false
}

func NewInternalError(err error) *InternalError {
	if err == nil {
		return nil
	}
	return &InternalError{
		msg: err.Error(),
	}
}

type ClientErr struct {
	msg string
}

func (e *ClientErr) Error() string {
	return e.msg
}

func (e *ClientErr) Retryable() bool {
	return false
}

func (e *ClientErr) ClientError() bool {
	return true
}

func NewClientError(err error) *ClientErr {
	if err == nil {
		return nil
	}
	return &ClientErr{
		msg: err.Error(),
	}
}

type RetryableInternalError struct {
	msg string
}

func (e *RetryableInternalError) Error() string {
	return e.msg
}

func (e *RetryableInternalError) Retryable() bool {
	return true
}

func (e *RetryableInternalError) ClientError() bool {
	return false
}

func NewRetryableInternalError(err error) *RetryableInternalError {
	if err == nil {
		return nil
	}
	return &RetryableInternalError{
		msg: err.Error(),
	}
}

type RetryableClientError struct {
	msg string
}

func (e *RetryableClientError) Error() string {
	return e.msg
}

func (e *RetryableClientError) Retryable() bool {
	return true
}

func (e *RetryableClientError) ClientError() bool {
	return true
}

func NewRetryableClientError(err error) *RetryableClientError {
	if err == nil {
		return nil
	}
	return &RetryableClientError{
		msg: err.Error(),
	}
}
