// gosqs contains common methods for interacting with AWS SQS
package gosqs

import (
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/ggarcia209/go-aws-v2/v2/goaws"
)

type SQS struct {
	Queues   QueuesLogic
	Messages MessagesLogic
}

func NewSQS(config goaws.AwsConfig) *SQS {
	svc := sqs.New(sqs.Options{
		Credentials: config.Config.Credentials,
		Region:      config.Config.Region,
	})
	return &SQS{
		Queues:   NewQueues(svc),
		Messages: NewMessages(svc),
	}
}
