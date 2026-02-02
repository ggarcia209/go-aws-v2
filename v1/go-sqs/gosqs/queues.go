package gosqs

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/ggarcia209/go-aws-v2/v1/goaws"
)

const ErrAWSNonExistentQueue = "AWS.SimpleQueueService.NonExistentQueue"

// QueueDefault contains the default attribute values for new SQS Queue objects
var QueueDefault = QueueOptions{
	DelaySeconds:                  "0",
	MaximumMessageSize:            "262144",
	MessageRetentionPeriod:        "345600",
	Policy:                        "",
	ReceiveMessageWaitTimeSeconds: "0",
	RedrivePolicy:                 "",
	VisibilityTimeout:             "30",
	KmsMasterKeyId:                "",
	KmsDataKeyReusePeriodSeconds:  "300",
	FifoQueue:                     "false",
	ContentBasedDeduplication:     "false",
	// * high throughput preview *
	// only available in us-east-1, us-east-2, us-west-2, eu-west-1
	DeduplicationScope:  "queue",
	FifoThroughputLimit: "perQueue",
	// *  *
}

// QueueOptions contains struct fields for setting custom options when creating a new SQS queue
type QueueOptions struct {
	DelaySeconds                  string
	MaximumMessageSize            string
	MessageRetentionPeriod        string
	Policy                        string // IAM Policy
	ReceiveMessageWaitTimeSeconds string
	RedrivePolicy                 string
	VisibilityTimeout             string
	KmsMasterKeyId                string
	KmsDataKeyReusePeriodSeconds  string
	FifoQueue                     string
	ContentBasedDeduplication     string
	DeduplicationScope            string
	FifoThroughputLimit           string
}

// QueueTags is a map object that enables tags when creating a new queue with CreateQueue()
type QueueTags map[string]*string

type SqsQueuesLogic interface {
	CreateQueue(name string, options QueueOptions, tags map[string]*string) (string, error)
	GetQueueURL(name string) (string, error)
	DeleteQueue(url string) error
	PurgeQueue(url string)
}

type SqsQueues struct {
	svc *sqs.SQS
}

func NewSqsQueues(sess goaws.Session) *SqsQueues {
	return &SqsQueues{
		svc: sqs.New(sess.GetSession()),
	}
}

// InitSesh initializes a new session with default config/credentials.
// Returns the *sqs.SQS object as interface{} type. The *sqs.SQS type is
// asserted when passed to the methods in the gosqs package.
func InitSesh(sess goaws.Session) *sqs.SQS {
	return sqs.New(sess.GetSession())
}

// CreateQueue creates a new SQS queue per the given name, options, & tags arguments and returns the url of the queue and/or error
func (s *SqsQueues) CreateQueue(name string, options QueueOptions, tags map[string]*string) (string, error) {
	url := ""
	input := &sqs.CreateQueueInput{
		QueueName: &name,
		Attributes: map[string]*string{
			"DelaySeconds":                  aws.String(options.DelaySeconds),
			"MaximumMessageSize":            aws.String(options.MaximumMessageSize),
			"MessageRetentionPeriod":        aws.String(options.MessageRetentionPeriod),
			"Policy":                        aws.String(options.Policy),
			"ReceiveMessageWaitTimeSeconds": aws.String(options.ReceiveMessageWaitTimeSeconds),
			"RedrivePolicy":                 aws.String(options.RedrivePolicy),
			"VisibilityTimeout":             aws.String(options.VisibilityTimeout),
			"KmsMasterKeyId":                aws.String(options.KmsMasterKeyId),
			"KmsDataKeyReusePeriodSeconds":  aws.String(options.KmsDataKeyReusePeriodSeconds),
		},
	}
	// set FIFO Queue options
	if options.FifoQueue == "true" {
		input.Attributes["FifoQueue"] = aws.String("true")
		input.Attributes["ContentBasedDeduplication"] = aws.String(options.ContentBasedDeduplication)
		input.Attributes["DeduplicationScope"] = aws.String(options.DeduplicationScope)
		input.Attributes["FifoThroughputLimit"] = aws.String(options.FifoThroughputLimit)
	}
	// set tags
	if len(tags) > 0 {
		input.Tags = tags
	}
	result, err := s.svc.CreateQueue(input)
	if err != nil {
		return url, fmt.Errorf("s.svc.CreateQueue: %w", err)
	}

	url = *result.QueueUrl
	return url, nil
}

// GetQueueURL retrives the URL for the given queue name
func (s *SqsQueues) GetQueueURL(name string) (string, error) {
	result, err := s.svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: &name,
	})
	if err != nil {
		return "", fmt.Errorf("s.svc.GetQueueUrl: %w", err)
	}
	return *result.QueueUrl, nil
}

// DeleteQueue deletes the queue at the given URL
func (s *SqsQueues) DeleteQueue(url string) error {
	if _, err := s.svc.DeleteQueue(&sqs.DeleteQueueInput{
		QueueUrl: aws.String(url),
	}); err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == ErrAWSNonExistentQueue {
				return fmt.Errorf("s.svc.DeleteQueue: %s", awsErr.Code())
			}
			return fmt.Errorf("s.svc.DeleteQueue: %w", err)
		}
		return fmt.Errorf("s.svc.DeleteQueue: %w", err)
	}

	return nil
}

// PurgeQueue purges the specified queue.
func (s *SqsQueues) PurgeQueue(url string) error {
	if _, err := s.svc.PurgeQueue(&sqs.PurgeQueueInput{
		QueueUrl: aws.String(url),
	}); err != nil {
		return fmt.Errorf("s.svc.PurgeQueue: %w", err)
	}

	return nil
}
