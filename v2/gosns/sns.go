// gosns contains common methods for interacting with AWS SNS
package gosns

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/ggarcia209/go-aws-v2/v2/goaws"

	"fmt"
)

//go:generate mockgen -destination=../mocks/gosnsmock/sns.go -package=gosnsmock . SNSLogic
type SNSLogic interface {
	ListTopics(ctx context.Context) (*ListTopicsResponse, error)
	CreateTopic(ctx context.Context, name string) (*CreateTopicResponse, error)
	Subscribe(ectx context.Context, ndpoint, protocol, topicArn string) (*SubscribeResponse, error)
	Publish(ctx context.Context, msgStr, topicArn string) (*PublishResponse, error)
}

// SNSClientAPI defines the interface for the AWS SNS client methods used by this package.
//
//go:generate mockgen -destination=./sns_client_api_test.go -package=gosns . SNSClientAPI
type SNSClientAPI interface {
	ListTopics(ctx context.Context, params *sns.ListTopicsInput, optFns ...func(*sns.Options)) (*sns.ListTopicsOutput, error)
	CreateTopic(ctx context.Context, params *sns.CreateTopicInput, optFns ...func(*sns.Options)) (*sns.CreateTopicOutput, error)
	Subscribe(ctx context.Context, params *sns.SubscribeInput, optFns ...func(*sns.Options)) (*sns.SubscribeOutput, error)
	Publish(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error)
}

type SNS struct {
	svc SNSClientAPI
}

func NewSNS(config goaws.AwsConfig) *SNS {
	return &SNS{
		svc: sns.New(sns.Options{
			Credentials: config.Config.Credentials,
			Region:      config.Config.Region,
		}),
	}
}

// ListTopics prints and returns a list of all SNS topics' ARNs in the AWS account.
func (s *SNS) ListTopics(ctx context.Context) (*ListTopicsResponse, error) {
	arns := make([]string, 0)

	// get topics
	result, err := s.svc.ListTopics(ctx, &sns.ListTopicsInput{})
	if err != nil {
		return nil, goaws.NewInternalError(fmt.Errorf("s.svc.ListTopics: %w", err))
	}

	// print topic ARNs
	for _, t := range result.Topics {
		arns = append(arns, *t.TopicArn)
	}

	return &ListTopicsResponse{TopicArns: arns}, nil
}

// CreateTopic creates a new SNS topic with the given name.
func (s *SNS) CreateTopic(ctx context.Context, name string) (*CreateTopicResponse, error) {
	result, err := s.svc.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String(name),
	})
	if err != nil {
		return nil, goaws.NewInternalError(fmt.Errorf("s.svc.CreateTopic: %w", err))
	}

	var topicArn string
	if result.TopicArn != nil {
		topicArn = *result.TopicArn
	}

	return &CreateTopicResponse{TopicArn: topicArn}, nil
}

// Subscribe creates a new subscription for an endpoint.
func (s *SNS) Subscribe(ctx context.Context, endpoint, protocol, topicArn string) (*SubscribeResponse, error) {
	validProtocols := map[string]bool{
		"http":        true,
		"https":       true,
		"email":       true,
		"email-json":  true,
		"sms":         true,
		"sqs":         true,
		"application": true,
		"lambda":      true,
		"firehose":    true,
	}
	if !validProtocols[protocol] {
		return nil, NewInvalidProtocolError(protocol)
	}

	result, err := s.svc.Subscribe(ctx, &sns.SubscribeInput{
		Endpoint:              aws.String(endpoint),
		Protocol:              aws.String(protocol),
		ReturnSubscriptionArn: true, // Return the ARN, even if user has yet to confirm
		TopicArn:              aws.String(topicArn),
	})
	if err != nil {
		return nil, goaws.NewInternalError(fmt.Errorf("s.svc.Subscribe: %w", err))
	}

	var subscriptionArn string
	if result.SubscriptionArn != nil {
		subscriptionArn = *result.SubscriptionArn
	}

	return &SubscribeResponse{SubscriptionArn: subscriptionArn}, nil
}

// Publish publishes a new message to a Topic and returns the message ID
// of the published message.
func (s *SNS) Publish(ctx context.Context, msgStr, topicArn string) (*PublishResponse, error) {
	result, err := s.svc.Publish(ctx, &sns.PublishInput{
		Message:  aws.String(msgStr),
		TopicArn: aws.String(topicArn),
	})
	if err != nil {
		return nil, goaws.NewInternalError(fmt.Errorf("s.svc.Publish: %w", err))
	}

	var messageId string
	if result.MessageId != nil {
		messageId = *result.MessageId
	}

	return &PublishResponse{MessageId: messageId}, nil
}
