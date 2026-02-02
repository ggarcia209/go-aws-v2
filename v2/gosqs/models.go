package gosqs

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

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
	DeduplicationScope:            "queue",
	FifoThroughputLimit:           "perQueue",
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

type CreateQueueResponse struct {
	QueueUrl string `json:"queue_url"`
}

type GetQueueUrlResponse struct {
	QueueUrl string `json:"queue_url"`
}

// SendMsgDefault contains the default options for the sqs.SendMessageInput object.
var SendMsgDefault = SendMsgOptions{
	DelaySeconds:            0,
	MessageAttributes:       nil,
	MessageBody:             "",
	MessageDeduplicationId:  "",
	MessageGroupId:          "",
	MessageSystemAttributes: nil,
	QueueURL:                "",
}

// SendMsgOptions is used to pass send message options to the sqs.SendMessageInput object.
type SendMsgOptions struct {
	DelaySeconds            int32
	MessageAttributes       map[string]types.MessageAttributeValue
	MessageBody             string
	MessageDeduplicationId  string
	MessageGroupId          string
	MessageSystemAttributes map[string]types.MessageSystemAttributeValue
	QueueURL                string
}

// SendMessageResponse wraps the sqs.SendMessageOutput object
type SendMsgResponse struct {
	MD5OfMessageAttributes       string `json:"md5_of_message_attributes"`
	MD5OfMessageBody             string `json:"md5_of_message_body"`
	MD5OfMessageSystemAttributes string `json:"md5_of_message_system_attributes"`
	MessageId                    string `json:"message_id"`
	SequenceNumber               string `json:"sequence_number"`
}

// RecMsgDefault contains the default values for the sqs.ReceiveMessageInput object.
var RecMsgDefault = RecMsgOptions{
	AttributeNames:          []types.QueueAttributeName{"All"},
	MaxNumberOfMessages:     1,
	MessageAttributeNames:   []string{"All"},
	QueueURL:                "",
	ReceiveRequestAttemptId: "",
	VisibilityTimeout:       30,
	WaitTimeSeconds:         0,
}

// RecMsgOptions is used to pass receive message options to the sqs.ReceiveMessageInput object.
type RecMsgOptions struct {
	AttributeNames          []types.QueueAttributeName
	MaxNumberOfMessages     int32
	MessageAttributeNames   []string
	QueueURL                string
	ReceiveRequestAttemptId string
	VisibilityTimeout       int32
	WaitTimeSeconds         int32
}

// ReceiveMessageResponse contains an array of messages received from SQS
type ReceiveMessageResponse struct {
	Messages []*Message `json:"messages"`
}

// Message wraps the sqs.Message type.
type Message struct {
	Attributes              map[string]string `json:"attributes"`
	Body                    string            `json:"body"`
	MD5OfBody               string            `json:"md5_of_body"`
	MD5OfMessagefAttributes string            `json:"md5_of_message_attributes"`
	MessageAttributes       map[string]MsgAV  `json:"message_attributes"`
	MessageId               string            `json:"message_id"`
	ReceiptHandle           string            `json:"receipt_handle"`
}

// MsgAV represents a single sqs.MessageAttributeValue or sqs.MessageSystemAttributeValue object.
// Limited to StringValue types; BinaryValue not supported.
type MsgAV struct {
	Key      string
	DataType string
	Value    string
}

// DeleteMessageBatchRequest is used to create a new BatchDelete request.
// len(MessageIDs) must equal len(ReceiptHandles). DeleteMessageBatch
// assumes the order of MessageIDs corresponds to the order ReceiptHandles.
type DeleteMessageBatchRequest struct {
	QueueURL       string   `json:"queue_url"`
	MessageIDs     []string `json:"message_ids"`
	ReceiptHandles []string `json:"receipt_handles"`
}

// DeleteMessageBatchResponse wraps the sqs.DeleteMessageBatchOutput type.
type DeleteMessageBatchResponse struct {
	Failed     []BatchDeleteErrEntry    `json:"failed"`
	Successful []BatchDeleteResultEntry `json:"successful"`
}

// BatchDeleteErrEntry wraps the sqs.BatchResultErrorEntry type.
type BatchDeleteErrEntry struct {
	ErrorCode     string `json:"error_code"`
	MessageID     string `json:"message_id"`
	ReceiptHandle string `json:"receipt_handle"` // not in sqs.BatchResultErrorEntry type - added for utility
	ErrorMessage  string `json:"error_message"`
	SenderFault   bool   `json:"sender_fault"`
}

// BatchDeleteResultEntry wraps the sqs.DeleteMessageBatchResultEntry type.
type BatchDeleteResultEntry struct {
	MessageID string `json:"message_id"`
}

// BatchUpdateVisibilityTimeoutRequest is used as input to the
// BatchUpdateVisibilityTimeout function.
type BatchUpdateVisibilityTimeoutRequest struct {
	QueueURL       string   `json:"queue_url"`
	MessageIDs     []string `json:"message_ids"`
	ReceiptHandles []string `json:"receipt_handles"`
	TimeoutSeconds int32    `json:"timeout_seconds"`
}

// BatchUpdateVisibilityTimeoutRequest wraps the output of the
// sqs.ChangeMessageVisibilityTImeout function (*sqs.ChangeMessageVisibilityOutput).
type BatchUpdateVisibilityTimeoutResponse struct {
	Failed     []BatchUpdateVisibilityTimeoutErrEntry `json:"failed"`
	Successful []BatchUpdateVisibilityTimeoutEntry    `json:"successful"`
}

// BatchUpdateVisibilityTimeoutErrEntry wraps the output *sqs.BatchResultErrorEntry object
// returned from BatchChangeMessageVisiblity timeout operations.
type BatchUpdateVisibilityTimeoutErrEntry struct {
	ErrorCode    string `json:"code"`
	MessageId    string `json:"id"`
	ErrorMessage string `json:"message"`
	SenderFault  bool   `json:"sender_fault"`
}

// BatchUpdateVisibilityTimeoutEntry wraps the output *sqs.ChangeMessageVisibilityBatchResult object.
type BatchUpdateVisibilityTimeoutEntry struct {
	MessageID string `json:"message_id"`
}

type msgErr struct {
	Code    string
	Message string
}

func (e *msgErr) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// CreateMsgAttributes creates a MessageAttributeValue map from a list of MsgAV objects.
// Limited to StringValue types; BinaryValue not supported.
func CreateMsgAttributes(attributes []MsgAV) map[string]*types.MessageAttributeValue {
	msgAttr := make(map[string]*types.MessageAttributeValue)
	for _, av := range attributes {
		attribute := &types.MessageAttributeValue{
			DataType:    aws.String(av.DataType),
			StringValue: aws.String(av.Value),
		}
		msgAttr[av.Key] = attribute
	}
	return msgAttr
}

// CreateMsgSystemAttributes creates a MessageSystemAttributeValue map from a list of MsgAV objects
// Limited to StringValue types; BinaryValue not supported
func CreateMsgSystemAttributes(attributes []MsgAV) map[string]*types.MessageSystemAttributeValue {
	msgSysAttr := make(map[string]*types.MessageSystemAttributeValue)
	for _, av := range attributes {
		attribute := &types.MessageSystemAttributeValue{
			DataType:    aws.String(av.DataType),
			StringValue: aws.String(av.Value),
		}
		msgSysAttr[av.Key] = attribute
	}
	return msgSysAttr
}

// CreateMsgAttribute constructs a MsgAV object from the given parameters
func CreateMsgAttribute(key, dataType, value string) MsgAV {
	av := MsgAV{
		Key:      key,
		DataType: dataType,
		Value:    value,
	}
	return av
}
