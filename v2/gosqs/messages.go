package gosqs

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/ggarcia209/go-aws-v2/v2/goaws"
)

// MessagesLogic defines common methods for SQS Messages
//
//go:generate mockgen -destination=../mocks/gosqsmock/messages.go -package=gosqsmock . MessagesLogic
type MessagesLogic interface {
	SendMessage(ctx context.Context, options SendMsgOptions) (*SendMsgResponse, error)
	ReceiveMessage(ctx context.Context, options RecMsgOptions) (*ReceiveMessageResponse, error)
	DeleteMessage(ctx context.Context, url, handle string) error
	DeleteMessageBatch(ctx context.Context, req DeleteMessageBatchRequest) (*DeleteMessageBatchResponse, error)
	ChangeMessageVisibilityBatch(ctx context.Context, req BatchUpdateVisibilityTimeoutRequest) (*BatchUpdateVisibilityTimeoutResponse, error)
}

// SQSMessagesClientAPI defines the interface for the AWS SQS client methods used by this package.
//
//go:generate mockgen -destination=./messages_client_api_test.go -package=gosqs . SQSMessagesClientAPI
type SQSMessagesClientAPI interface {
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	DeleteMessageBatch(ctx context.Context, params *sqs.DeleteMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error)
	ChangeMessageVisibilityBatch(ctx context.Context, params *sqs.ChangeMessageVisibilityBatchInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error)
}

type Messages struct {
	svc SQSMessagesClientAPI
}

func NewMessages(svc SQSMessagesClientAPI) *Messages {
	return &Messages{
		svc: svc,
	}
}

// SendMessage sends a new message to a queue per the options argument.
// Unique MD5 checksums are generated for the MessageDeduplicationID
// and MessageGroupID fields if not set for messages sent to FIFO Queues.
func (s *Messages) SendMessage(ctx context.Context, options SendMsgOptions) (*SendMsgResponse, error) {
	// ensure values are valid
	if options.DelaySeconds < 0 {
		options.DelaySeconds = 0
	}
	if options.DelaySeconds > 900 {
		options.DelaySeconds = 900
	}
	input := &sqs.SendMessageInput{
		DelaySeconds:            options.DelaySeconds,
		MessageAttributes:       options.MessageAttributes,
		MessageBody:             aws.String(options.MessageBody),
		MessageSystemAttributes: options.MessageSystemAttributes,
		QueueUrl:                aws.String(options.QueueURL),
	}
	// set FIFO queue options
	if checkFifo(options.QueueURL) {
		if options.MessageDeduplicationId != "" {
			input.MessageDeduplicationId = aws.String(options.MessageDeduplicationId)
		} else {
			input.MessageDeduplicationId = aws.String(GenerateDedupeID(options.QueueURL))
		}
		if options.MessageGroupId != "" {
			input.MessageGroupId = aws.String(options.MessageGroupId)
		} else {
			input.MessageGroupId = aws.String(GenerateDedupeID(options.QueueURL))
		}
	}

	out, err := s.svc.SendMessage(ctx, input)
	if err != nil {
		var notExist *types.QueueDoesNotExist
		var invalidAddress *types.InvalidAddress
		var badContent *types.InvalidMessageContents
		var re *awshttp.ResponseError
		switch {
		case errors.As(err, &notExist):
			return nil, NewQueueNotFoundError(options.QueueURL)
		case errors.As(err, &invalidAddress):
			return nil, NewInvalidAddressError(options.QueueURL)
		case errors.As(err, &badContent):
			return nil, NewInvalidMessageContentError(input.MessageBody)
		case errors.As(err, &re):
			if re.ResponseError == nil {
				return nil, goaws.NewInternalError(fmt.Errorf("s.svc.DeleteMessage: %w", re.Err))
			}
			switch re.HTTPStatusCode() {
			case http.StatusBadRequest:
				return nil, NewInvalidMessageContentError(input.MessageBody)
			case http.StatusNotFound:
				return nil, NewQueueNotFoundError(options.QueueURL)
			default:
				return nil, goaws.NewInternalError(fmt.Errorf("s.svc.DeleteMessage: %w", re.Err))
			}
		default:
			return nil, goaws.NewInternalError(fmt.Errorf("s.svc.DeleteMessage: %w", err))
		}
	}
	resp := wrapSendMsgOutput(out)
	return resp, nil
}

// ReceiveMessage receives a message from a queue per the options argument
func (s *Messages) ReceiveMessage(ctx context.Context, options RecMsgOptions) (*ReceiveMessageResponse, error) {
	var msgs = make([]*Message, 0)

	// ensure values are valid
	if options.MaxNumberOfMessages < 1 {
		options.MaxNumberOfMessages = 1
	}
	if options.MaxNumberOfMessages > 10 {
		options.MaxNumberOfMessages = 10
	}
	if options.VisibilityTimeout < 0 {
		options.VisibilityTimeout = 0
	}
	if options.VisibilityTimeout > 43200 {
		options.VisibilityTimeout = 43200
	}
	if options.WaitTimeSeconds < 1 {
		options.WaitTimeSeconds = 1
	}
	if options.WaitTimeSeconds > 20 {
		options.WaitTimeSeconds = 20
	}
	// set ReceiveRequestAttemptID for FIFO queues if not set
	if checkFifo(options.QueueURL) && options.ReceiveRequestAttemptId == "" {
		options.ReceiveRequestAttemptId = GenerateDedupeID(options.QueueURL)
	}

	msgResult, err := s.svc.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		AttributeNames:          options.AttributeNames,
		MaxNumberOfMessages:     options.MaxNumberOfMessages,
		MessageAttributeNames:   options.MessageAttributeNames,
		QueueUrl:                aws.String(options.QueueURL),
		ReceiveRequestAttemptId: aws.String(options.ReceiveRequestAttemptId),
		VisibilityTimeout:       options.VisibilityTimeout,
		WaitTimeSeconds:         options.WaitTimeSeconds,
	})
	if err != nil {
		return nil, goaws.NewInternalError(fmt.Errorf("s.svc.ReceiveMessage: %w", err))
	}
	for _, msg := range msgResult.Messages {
		conv := convertMessage(msg)
		msgs = append(msgs, conv)
	}
	return &ReceiveMessageResponse{Messages: msgs}, nil
}

func wrapSendMsgOutput(out *sqs.SendMessageOutput) *SendMsgResponse {
	resp := new(SendMsgResponse)
	if out.MD5OfMessageAttributes != nil {
		resp.MD5OfMessageAttributes = *out.MD5OfMessageAttributes
	}
	if out.MD5OfMessageBody != nil {
		resp.MD5OfMessageBody = *out.MD5OfMessageBody
	}
	if out.MD5OfMessageSystemAttributes != nil {
		resp.MD5OfMessageSystemAttributes = *out.MD5OfMessageSystemAttributes
	}
	if out.MessageId != nil {
		resp.MessageId = *out.MessageId
	}
	if out.SequenceNumber != nil {
		resp.SequenceNumber = *out.SequenceNumber
	}
	return resp
}

// convert *sqsMessage type to Message struct
func convertMessage(msg types.Message) *Message {
	attributes := make(map[string]string)
	for k, v := range msg.Attributes {
		attributes[k] = v
	}
	msgAttributes := make(map[string]MsgAV)
	for k, v := range msg.MessageAttributes {
		var value string
		var dataType string
		if v.DataType != nil {
			dataType = *v.DataType
		}
		if v.StringValue != nil {
			value = *v.StringValue
		}
		av := MsgAV{
			Key:      k,
			DataType: dataType,
			Value:    value,
		}
		msgAttributes[k] = av
	}

	var (
		body                   string
		md5OfBody              string
		messageId              string
		receiptHandle          string
		md5OfMessageAttributes string
	)
	if msg.Body != nil {
		body = *msg.Body
	}
	if msg.MD5OfBody != nil {
		md5OfBody = *msg.MD5OfBody
	}
	if msg.MessageId != nil {
		messageId = *msg.MessageId
	}
	if msg.ReceiptHandle != nil {
		receiptHandle = *msg.ReceiptHandle
	}
	if msg.MD5OfMessageAttributes != nil {
		md5OfMessageAttributes = *msg.MD5OfMessageAttributes
	}

	return &Message{
		Attributes:              attributes,
		Body:                    body,
		MD5OfBody:               md5OfBody,
		MessageAttributes:       msgAttributes,
		MessageId:               messageId,
		ReceiptHandle:           receiptHandle,
		MD5OfMessagefAttributes: md5OfMessageAttributes,
	}
}

// determine if FIFO queue from url (".fifo")
func checkFifo(url string) bool {
	spl := strings.Split(url, ".")
	if len(spl) > 1 {
		appendix := spl[len(spl)-1]
		if appendix == "fifo" {
			return true
		}
	}
	return false
}

// GenerateDedupeID generates a MD5 hash from a
// timestamp of the current time + the given queue url.
func GenerateDedupeID(msgBody string) string {
	hash := md5.Sum([]byte(msgBody))
	hashStr := hex.EncodeToString(hash[:])
	return hashStr
}

// DeleteMessage deletes a message from the specified queue (by url) with the
// given handle.
func (s *Messages) DeleteMessage(ctx context.Context, url, handle string) error {
	if _, err := s.svc.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(url),
		ReceiptHandle: aws.String(handle),
	}); err != nil {
		var notExist *types.InvalidAddress
		var re *awshttp.ResponseError
		switch {
		case errors.As(err, &notExist):
			return NewInvalidAddressError(url)
		case errors.As(err, &re):
			if re.ResponseError == nil {
				return goaws.NewInternalError(fmt.Errorf("s.svc.DeleteMessage: %w", re.Err))
			}
			switch re.HTTPStatusCode() {
			case http.StatusNotFound:
				return NewInvalidAddressError(url)
			default:
				return goaws.NewInternalError(fmt.Errorf("s.svc.DeleteMessage: %w", re.Err))
			}
		default:
			return goaws.NewInternalError(fmt.Errorf("s.svc.DeleteMessage: %w", err))
		}
	}
	return nil
}

// DeleteMessageBatch deletes a batch of messages
func (s *Messages) DeleteMessageBatch(ctx context.Context, req DeleteMessageBatchRequest) (*DeleteMessageBatchResponse, error) {
	if req.QueueURL == "" {
		return nil, NewEmptyQueueUrlInRequestError()
	}
	if len(req.MessageIDs) != len(req.ReceiptHandles) {
		return nil, NewInvalidReceiptHandlesError(len(req.MessageIDs), len(req.ReceiptHandles))
	}
	if len(req.MessageIDs) == 0 {
		return nil, NewNoMessageIDsInBatchRequestError()
	}
	if len(req.MessageIDs) > 10 {
		return nil, NewMaxMessagesExceededError(len(req.MessageIDs))
	}

	handles := make(map[string]string)
	entries := make([]types.DeleteMessageBatchRequestEntry, 0)
	for i, handle := range req.ReceiptHandles {
		msgID := req.MessageIDs[i]
		entry := types.DeleteMessageBatchRequestEntry{
			Id:            aws.String(msgID),
			ReceiptHandle: aws.String(handle),
		}
		handles[msgID] = handle
		entries = append(entries, entry)
	}
	batchRequest := &sqs.DeleteMessageBatchInput{
		Entries:  entries,
		QueueUrl: aws.String(req.QueueURL),
	}
	result, err := s.svc.DeleteMessageBatch(ctx, batchRequest)
	if err != nil {
		wrap := wrapBatchDeleteOutput(result, handles)
		return wrap, goaws.NewInternalError(fmt.Errorf("s.svc.DeleteMessageBatch: %w", err))
	}
	wrap := wrapBatchDeleteOutput(result, handles)
	return wrap, nil
}

// wrap sqs.DeleteMessageBatchOutput object
func wrapBatchDeleteOutput(output *sqs.DeleteMessageBatchOutput, handles map[string]string) *DeleteMessageBatchResponse {
	wrapSuccessful := make([]BatchDeleteResultEntry, 0)
	wrapFailed := make([]BatchDeleteErrEntry, 0)
	successful := output.Successful
	failed := output.Failed

	for _, entry := range successful {
		wrap := BatchDeleteResultEntry{
			MessageID: *entry.Id,
		}
		wrapSuccessful = append(wrapSuccessful, wrap)
	}
	for _, entry := range failed {
		msgID := *entry.Id
		wrap := BatchDeleteErrEntry{
			ErrorCode:     *entry.Code,
			MessageID:     msgID,
			ReceiptHandle: handles[msgID],
			ErrorMessage:  *entry.Message,
			SenderFault:   entry.SenderFault,
		}
		wrapFailed = append(wrapFailed, wrap)
	}
	return &DeleteMessageBatchResponse{
		Successful: wrapSuccessful,
		Failed:     wrapFailed,
	}
}

// ChangeMessageVisibilityBatch updates the visibility timeout for a batch of messages
// represented by the given MessageIds and ReceiptHandles. Assumes msgIDs[i] and handles[i] args
// are in order and correspond to the same message.
func (s *Messages) ChangeMessageVisibilityBatch(ctx context.Context, req BatchUpdateVisibilityTimeoutRequest) (*BatchUpdateVisibilityTimeoutResponse, error) {
	if req.QueueURL == "" {
		return nil, NewEmptyQueueUrlInRequestError()
	}
	if len(req.MessageIDs) != len(req.ReceiptHandles) {
		return nil, NewInvalidReceiptHandlesError(len(req.MessageIDs), len(req.ReceiptHandles))
	}
	if len(req.MessageIDs) == 0 {
		return nil, NewNoMessageIDsInBatchRequestError()
	}
	if len(req.MessageIDs) > 10 {
		return nil, NewMaxMessagesExceededError(len(req.MessageIDs))
	}

	input := new(sqs.ChangeMessageVisibilityBatchInput)
	input.QueueUrl = aws.String(req.QueueURL)
	entries := make([]types.ChangeMessageVisibilityBatchRequestEntry, 0)
	for i, id := range req.MessageIDs {
		entry := types.ChangeMessageVisibilityBatchRequestEntry{
			Id:                aws.String(id),
			ReceiptHandle:     aws.String(req.ReceiptHandles[i]),
			VisibilityTimeout: req.TimeoutSeconds,
		}
		entries = append(entries, entry)
	}
	input.Entries = entries

	output, err := s.svc.ChangeMessageVisibilityBatch(ctx, input)
	if err != nil {
		return nil, goaws.NewInternalError(fmt.Errorf("s.svc.ChangeMessageVisibilityBatch: %w", err))
	}

	return wrapBatchUpdateVisibilityTimeoutOutput(output), nil
}

// wrap sqs.DeleteMessageBatchOutput object
func wrapBatchUpdateVisibilityTimeoutOutput(output *sqs.ChangeMessageVisibilityBatchOutput) *BatchUpdateVisibilityTimeoutResponse {
	wrapSuccessful := make([]BatchUpdateVisibilityTimeoutEntry, 0)
	wrapFailed := make([]BatchUpdateVisibilityTimeoutErrEntry, 0)

	for _, entry := range output.Successful {
		wrap := BatchUpdateVisibilityTimeoutEntry{
			MessageID: *entry.Id,
		}
		wrapSuccessful = append(wrapSuccessful, wrap)
	}
	for _, entry := range output.Failed {
		msgID := *entry.Id
		wrap := BatchUpdateVisibilityTimeoutErrEntry{
			ErrorCode:    *entry.Code,
			MessageId:    msgID,
			ErrorMessage: *entry.Message,
			SenderFault:  entry.SenderFault,
		}
		wrapFailed = append(wrapFailed, wrap)
	}
	return &BatchUpdateVisibilityTimeoutResponse{
		Successful: wrapSuccessful,
		Failed:     wrapFailed,
	}
}
