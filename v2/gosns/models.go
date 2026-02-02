package gosns

type ListTopicsResponse struct {
	TopicArns []string
}

type CreateTopicResponse struct {
	TopicArn string
}

type SubscribeResponse struct {
	SubscriptionArn string
}

type PublishResponse struct {
	MessageId string
}
