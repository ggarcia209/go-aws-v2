package gosns

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/ggarcia209/go-aws-v2/v1/goaws"

	"fmt"
)

// InvalidSvcArgErr is returned when an interface object passed as the svc argument to
// the package methods is not the AWS *sns.SNS type.
const InvalidSvcArgErr = "INVALID_SVC_ARG_TYPE"

// InvliadProtocolErr is returned when an invalid value is passed to the Subscribe function.
const InvalidProtocolErr = "INVALID_SUBSCRIPTION_PROTOCOL"

type SnsLogic interface {
	ListTopics() ([]string, error)
	CreateTopic(name string) (string, error)
	Subscribe(endpoint, protocol, topicArn string) (string, error)
	Publish(msgStr, topicArn string) (string, error)
}

type SNS struct {
	svc *sns.SNS
}

func NewSNS(sess goaws.Session) *SNS {
	return &SNS{
		svc: sns.New(sess.GetSession()),
	}
}

// InitSesh intitializes a new SNS client session and returns the AWS *sns.SNS object
// as an interface type to maintain encapsulation of the AWS sns package. The *sns.SNS
// type is asserted by the methods used in this package, which return the InvalidSvcArgErr
// if the type is invalid.
func InitSesh(sess goaws.Session) *sns.SNS {
	return sns.New(sess.GetSession())
}

// ListTopics prints and returns a list of all SNS topics' ARNs in the AWS account.
func (s *SNS) ListTopics() ([]string, error) {
	arns := []string{}

	// get topics
	result, err := s.svc.ListTopics(nil)
	if err != nil {
		return arns, fmt.Errorf("s.svc.ListTopics: %w", err)
	}

	// print topic ARNs
	for _, t := range result.Topics {
		fmt.Println(*t.TopicArn)
		arns = append(arns, *t.TopicArn)
	}

	return arns, nil
}

// CreateTopic creates a new SNS topic with the given name.
func (s *SNS) CreateTopic(name string) (string, error) {
	result, err := s.svc.CreateTopic(&sns.CreateTopicInput{
		Name: aws.String(name),
	})
	if err != nil {
		return "", fmt.Errorf("s.svc.CreateTopic: %w", err)
	}

	return *result.TopicArn, nil
}

// Subscribe creates a new subscription for an endpoint.
func (s *SNS) Subscribe(endpoint, protocol, topicArn string) (string, error) {
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
		return "", errors.New(InvalidProtocolErr)
	}

	result, err := s.svc.Subscribe(&sns.SubscribeInput{
		Endpoint:              aws.String(endpoint),
		Protocol:              aws.String(protocol),
		ReturnSubscriptionArn: aws.Bool(true), // Return the ARN, even if user has yet to confirm
		TopicArn:              aws.String(topicArn),
	})
	if err != nil {
		return "", fmt.Errorf("s.svc.Subscribe: %w", err)
	}

	return *result.SubscriptionArn, nil
}

// Publish publishes a new message to a Topic and returns the message ID
// of the published message.
func (s *SNS) Publish(msgStr, topicArn string) (string, error) {
	result, err := s.svc.Publish(&sns.PublishInput{
		Message:  aws.String(msgStr),
		TopicArn: aws.String(topicArn),
	})
	if err != nil {
		return "", fmt.Errorf("s.svc.Publish: %w", err)
	}

	return *result.MessageId, nil
}
