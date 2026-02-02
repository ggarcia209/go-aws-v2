package gosqs

import (
	"context"
	"errors"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/ggarcia209/go-aws-v2/v2/goaws"

	"fmt"
)

// QueuesLogic defines common methods for SQS Queues
//
//go:generate mockgen -destination=../mocks/gosqsmock/queues.go -package=gosqsmock . QueuesLogic
type QueuesLogic interface {
	CreateQueue(ctx context.Context, name string, options QueueOptions, tags map[string]string) (*CreateQueueResponse, error)
	GetQueueURL(ctx context.Context, name string) (*GetQueueUrlResponse, error)
	DeleteQueue(ctx context.Context, url string) error
	PurgeQueue(ctx context.Context, url string) error
}

// SQSQueuesClientAPI defines the interface for the AWS SQS client methods used by this package.
//
//go:generate mockgen -destination=./queues_client_api_test.go -package=gosqs . SQSQueuesClientAPI
type SQSQueuesClientAPI interface {
	CreateQueue(ctx context.Context, params *sqs.CreateQueueInput, optFns ...func(*sqs.Options)) (*sqs.CreateQueueOutput, error)
	GetQueueUrl(ctx context.Context, params *sqs.GetQueueUrlInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error)
	DeleteQueue(ctx context.Context, params *sqs.DeleteQueueInput, optFns ...func(*sqs.Options)) (*sqs.DeleteQueueOutput, error)
	PurgeQueue(ctx context.Context, params *sqs.PurgeQueueInput, optFns ...func(*sqs.Options)) (*sqs.PurgeQueueOutput, error)
}

// SQSQueuesLogic implements Queues logic
// for interacting with AWS SQS Queues
type Queues struct {
	svc SQSQueuesClientAPI
}

func NewQueues(svc SQSQueuesClientAPI) *Queues {
	return &Queues{
		svc: svc,
	}
}

// CreateQueue creates a new SQS queue per the given name, options, & tags arguments and returns the url of the queue and/or error
func (s *Queues) CreateQueue(ctx context.Context, name string, options QueueOptions, tags map[string]string) (*CreateQueueResponse, error) {
	input := &sqs.CreateQueueInput{
		QueueName: &name,
		Attributes: map[string]string{
			"DelaySeconds":                  options.DelaySeconds,
			"MaximumMessageSize":            options.MaximumMessageSize,
			"MessageRetentionPeriod":        options.MessageRetentionPeriod,
			"Policy":                        options.Policy,
			"ReceiveMessageWaitTimeSeconds": options.ReceiveMessageWaitTimeSeconds,
			"RedrivePolicy":                 options.RedrivePolicy,
			"VisibilityTimeout":             options.VisibilityTimeout,
			"KmsMasterKeyId":                options.KmsMasterKeyId,
			"KmsDataKeyReusePeriodSeconds":  options.KmsDataKeyReusePeriodSeconds,
		},
	}
	// set FIFO Queue options
	if options.FifoQueue == "true" {
		input.Attributes["FifoQueue"] = "true"
		input.Attributes["ContentBasedDeduplication"] = options.ContentBasedDeduplication
		input.Attributes["DeduplicationScope"] = options.DeduplicationScope
		input.Attributes["FifoThroughputLimit"] = options.FifoThroughputLimit
	}
	// set tags
	if len(tags) > 0 {
		input.Tags = tags
	}
	result, err := s.svc.CreateQueue(ctx, input)
	if err != nil {
		return nil, goaws.NewInternalError(fmt.Errorf("s.svc.CreateQueue: %w", err))
	}

	if result.QueueUrl == nil {
		return nil, NewEmptyQueueUrlInResponseError()
	}
	return &CreateQueueResponse{
		QueueUrl: *result.QueueUrl,
	}, nil
}

// GetQueueURL retrives the URL for the given queue name
func (s *Queues) GetQueueURL(ctx context.Context, name string) (*GetQueueUrlResponse, error) {
	result, err := s.svc.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: &name,
	})
	if err != nil {
		var notExist *types.QueueDoesNotExist
		var re *awshttp.ResponseError
		switch {
		case errors.As(err, &notExist):
			return nil, NewQueueNotFoundError(name)
		case errors.As(err, &re):
			if re.ResponseError == nil {
				return nil, goaws.NewInternalError(fmt.Errorf("s.svc.GetQueueUrl: %w", re.Err))
			}
			switch re.ResponseError.HTTPStatusCode() {
			case http.StatusNotFound:
				return nil, NewQueueNotFoundError(name)
			default:
				return nil, goaws.NewInternalError(fmt.Errorf("s.svc.GetQueueUrl: %w", re.Err))
			}
		default:
			return nil, goaws.NewInternalError(fmt.Errorf("s.svc.GetQueueUrl: %w", err))
		}
	}

	if result.QueueUrl == nil {
		return nil, NewEmptyQueueUrlInResponseError()
	}
	return &GetQueueUrlResponse{
		QueueUrl: *result.QueueUrl,
	}, nil
}

// DeleteQueue deletes the queue at the given URL
func (s *Queues) DeleteQueue(ctx context.Context, url string) error {
	if _, err := s.svc.DeleteQueue(ctx, &sqs.DeleteQueueInput{
		QueueUrl: aws.String(url),
	}); err != nil {
		var notExist *types.QueueDoesNotExist
		var re *awshttp.ResponseError
		switch {
		case errors.As(err, &notExist):
			return NewQueueNotFoundError(url)
		case errors.As(err, &re):
			if re.ResponseError == nil {
				return goaws.NewInternalError(fmt.Errorf("s.svc.DeleteQueue: %w", re.Err))
			}
			switch re.ResponseError.HTTPStatusCode() {
			case http.StatusNotFound:
				return NewQueueNotFoundError(url)
			default:
				return goaws.NewInternalError(fmt.Errorf("s.svc.DeleteQueue: %w", re.Err))
			}
		default:
			return goaws.NewInternalError(fmt.Errorf("s.svc.DeleteQueue: %w", err))
		}
	}

	return nil
}

// PurgeQueue purges the specified queue.
func (s *Queues) PurgeQueue(ctx context.Context, url string) error {
	if _, err := s.svc.PurgeQueue(ctx, &sqs.PurgeQueueInput{
		QueueUrl: aws.String(url),
	}); err != nil {
		var notExist *types.QueueDoesNotExist
		var re *awshttp.ResponseError
		switch {
		case errors.As(err, &notExist):
			return NewQueueNotFoundError(url)
		case errors.As(err, &re):
			if re.ResponseError == nil {
				return goaws.NewInternalError(fmt.Errorf("s.svc.PurgeQueue: %w", re.Err))
			}
			switch re.ResponseError.HTTPStatusCode() {
			case http.StatusNotFound:
				return NewQueueNotFoundError(url)
			default:
				return goaws.NewInternalError(fmt.Errorf("s.svc.PurgeQueue: %w", re.Err))
			}
		default:
			return goaws.NewInternalError(fmt.Errorf("s.svc.PurgeQueue: %w", err))
		}
	}

	return nil
}
