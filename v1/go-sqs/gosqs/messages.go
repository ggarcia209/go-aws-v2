package gosqs

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/ggarcia209/go-aws-v2/v1/goaws"
)

// error codes for invalid batch delete requests

// ErrAWSInvalidParameter is returned when ReceiptHandles/VisiblityTimeout are expired (AWS SDK Error).
const ErrAWSInvalidParameter = "InvalidParameterValue"

// ErrMissingParameter is returned when an empty ReceiptHandle value is passed to DeleteMessage.
const ErrAWSMissingParameter = "MissingParameter"

// ErrTooManyRequests is returned when a batch delete request is made with > 10 receipt handles for deletion.
const ErrTooManyRequests = "TOO_MANY_REQUESTS"

// ErrInvalidRequest is returned when the number of message IDs != the number of receipt handles in a batch delete request.
const ErrInvalidRequest = "IDS_NOT_EQUAL_HANDLES"

// ErrInvalidQueueURL is returned when a batch delete request is passed with an empty QueueURL field.
const ErrInvalidQueueURL = "INVALID_QUEUE_URL"

// SendMsgDefault contains the default options for the sqs.SendMessageInput object.
var SendMsgDefault = SendMsgOptions{
	DelaySeconds:            int64(0),
	MessageAttributes:       nil,
	MessageBody:             "",
	MessageDeduplicationId:  "",
	MessageGroupId:          "",
	MessageSystemAttributes: nil,
	QueueURL:                "",
}

// SendMsgOptions is used to pass send message options to the sqs.SendMessageInput object.
type SendMsgOptions struct {
	DelaySeconds            int64
	MessageAttributes       map[string]*sqs.MessageAttributeValue
	MessageBody             string
	MessageDeduplicationId  string
	MessageGroupId          string
	MessageSystemAttributes map[string]*sqs.MessageSystemAttributeValue
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
	AttributeNames:          []*string{aws.String("All")},
	MaxNumberOfMessages:     int64(1),
	MessageAttributeNames:   []*string{aws.String("All")},
	QueueURL:                "",
	ReceiveRequestAttemptId: "",
	VisibilityTimeout:       int64(30),
	WaitTimeSeconds:         int64(0),
}

// RecMsgOptions is used to pass receive message options to the sqs.ReceiveMessageInput object.
type RecMsgOptions struct {
	AttributeNames          []*string
	MaxNumberOfMessages     int64
	MessageAttributeNames   []*string
	QueueURL                string
	ReceiveRequestAttemptId string
	VisibilityTimeout       int64
	WaitTimeSeconds         int64
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
	TimeoutSeconds int      `json:"timeout_seconds"`
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
func CreateMsgAttributes(attributes []MsgAV) map[string]*sqs.MessageAttributeValue {
	msgAttr := make(map[string]*sqs.MessageAttributeValue)
	for _, av := range attributes {
		attribute := &sqs.MessageAttributeValue{
			DataType:    aws.String(av.DataType),
			StringValue: aws.String(av.Value),
		}
		msgAttr[av.Key] = attribute
	}
	return msgAttr
}

// CreateMsgSystemAttributes creates a MessageSystemAttributeValue map from a list of MsgAV objects
// Limited to StringValue types; BinaryValue not supported
func CreateMsgSystemAttributes(attributes []MsgAV) map[string]*sqs.MessageSystemAttributeValue {
	msgSysAttr := make(map[string]*sqs.MessageSystemAttributeValue)
	for _, av := range attributes {
		attribute := &sqs.MessageSystemAttributeValue{
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

type SqsMessagesLogic interface {
	SendMessage(options SendMsgOptions) (SendMsgResponse, error)
	ReceiveMessage(options RecMsgOptions) ([]Message, error)
	DeleteMessage(url, handle string) error
	DeleteMessageBatch(req DeleteMessageBatchRequest) (DeleteMessageBatchResponse, error)
	ChangeMessageVisibilityBatch(req BatchUpdateVisibilityTimeoutRequest) (BatchUpdateVisibilityTimeoutResponse, error)
}

type SqsMessages struct {
	svc *sqs.SQS
}

func NewSqsMessages(sess goaws.Session) *SqsMessages {
	return &SqsMessages{
		svc: sqs.New(sess.GetSession()),
	}
}

// SendMessage sends a new message to a queue per the options argument.
// Unique MD5 checksums are generated for the MessageDeduplicationID
// and MessageGroupID fields if not set for messages sent to FIFO Queues.
func (s *SqsMessages) SendMessage(options SendMsgOptions) (SendMsgResponse, error) {
	// ensure values are valid
	if options.DelaySeconds < 0 {
		options.DelaySeconds = 0
	}
	if options.DelaySeconds > 900 {
		options.DelaySeconds = 900
	}
	input := &sqs.SendMessageInput{
		DelaySeconds:            aws.Int64(options.DelaySeconds),
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

	out, err := s.svc.SendMessage(input)
	if err != nil {
		return SendMsgResponse{}, fmt.Errorf("s.svc.SendMessage: %w", err)
	}
	resp := wrapSendMsgOutput(out)
	return resp, nil
}

// ReceiveMessage receives a message from a queue per the options argument
func (s *SqsMessages) ReceiveMessage(options RecMsgOptions) ([]Message, error) {
	msgs := []Message{}

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

	msgResult, err := s.svc.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames:          options.AttributeNames,
		MaxNumberOfMessages:     aws.Int64(options.MaxNumberOfMessages),
		MessageAttributeNames:   options.MessageAttributeNames,
		QueueUrl:                aws.String(options.QueueURL),
		ReceiveRequestAttemptId: aws.String(options.ReceiveRequestAttemptId),
		VisibilityTimeout:       aws.Int64(options.VisibilityTimeout),
		WaitTimeSeconds:         aws.Int64(options.WaitTimeSeconds),
	})
	if err != nil {
		return msgs, fmt.Errorf("s.svc.ReceiveMessage: %w", err)
	}
	for _, msg := range msgResult.Messages {
		conv := convertMessage(msg)
		msgs = append(msgs, conv)
	}
	return msgs, nil
}

func wrapSendMsgOutput(out *sqs.SendMessageOutput) SendMsgResponse {
	resp := SendMsgResponse{}
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
func convertMessage(msg *sqs.Message) Message {
	attributes := make(map[string]string)
	for k, v := range msg.Attributes {
		attributes[k] = *v
	}
	msgAttributes := make(map[string]MsgAV)
	for k, v := range msg.MessageAttributes {
		av := MsgAV{
			Key:      k,
			DataType: *v.DataType,
			Value:    *v.StringValue,
		}
		msgAttributes[k] = av
	}
	conv := Message{
		Attributes:        attributes,
		Body:              *msg.Body,
		MD5OfBody:         *msg.MD5OfBody,
		MessageAttributes: msgAttributes,
		MessageId:         *msg.MessageId,
		ReceiptHandle:     *msg.ReceiptHandle,
	}
	if msg.MD5OfMessageAttributes != nil {
		conv.MD5OfMessagefAttributes = *msg.MD5OfMessageAttributes
	}
	return conv
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
func (s *SqsMessages) DeleteMessage(url, handle string) error {
	_, err := s.svc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(url),
		ReceiptHandle: aws.String(handle),
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == ErrAWSInvalidParameter || awsErr.Code() == ErrAWSMissingParameter {
				return fmt.Errorf("s.svc.DeleteMessage: %s", awsErr.Code())
			}
			return fmt.Errorf("s.svc.DeleteMessage: %w", err)
		}
		return fmt.Errorf("s.svc.DeleteMessage: %w", err)
	}
	return nil
}

// DeleteMessageBatch deletes a batch of messages
func (s *SqsMessages) DeleteMessageBatch(req DeleteMessageBatchRequest) (DeleteMessageBatchResponse, error) {
	if len(req.ReceiptHandles) > 10 {
		return DeleteMessageBatchResponse{}, errors.New(ErrTooManyRequests)
	}
	if len(req.MessageIDs) != len(req.ReceiptHandles) {
		return DeleteMessageBatchResponse{}, errors.New(ErrInvalidRequest)
	}
	if req.QueueURL == "" {
		return DeleteMessageBatchResponse{}, errors.New(ErrInvalidQueueURL)
	}

	handles := make(map[string]string)
	entries := []*sqs.DeleteMessageBatchRequestEntry{}
	for i, handle := range req.ReceiptHandles {
		msgID := req.MessageIDs[i]
		entry := &sqs.DeleteMessageBatchRequestEntry{
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
	result, err := s.svc.DeleteMessageBatch(batchRequest)
	if err != nil {
		wrap := wrapBatchDeleteOutput(result, handles)
		return wrap, fmt.Errorf("s.svc.DeleteMessageBatch: %w", err)
	}
	wrap := wrapBatchDeleteOutput(result, handles)
	return wrap, nil
}

// wrap sqs.DeleteMessageBatchOutput object
func wrapBatchDeleteOutput(output *sqs.DeleteMessageBatchOutput, handles map[string]string) DeleteMessageBatchResponse {
	wrapSuccessful := []BatchDeleteResultEntry{}
	wrapFailed := []BatchDeleteErrEntry{}
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
			SenderFault:   *entry.SenderFault,
		}
		wrapFailed = append(wrapFailed, wrap)
	}
	wrap := DeleteMessageBatchResponse{
		Successful: wrapSuccessful,
		Failed:     wrapFailed,
	}
	return wrap
}

// ChangeMessageVisibilityBatch updates the visibility timeout for a batch of messages
// represented by the given MessageIds and ReceiptHandles. Assumes msgIDs[i] and handles[i] args
// are in order and correspond to the same message.
func (s *SqsMessages) ChangeMessageVisibilityBatch(req BatchUpdateVisibilityTimeoutRequest) (BatchUpdateVisibilityTimeoutResponse, error) {
	resp := BatchUpdateVisibilityTimeoutResponse{}

	if len(req.MessageIDs) != len(req.ReceiptHandles) {
		log.Printf("ChangeMessageVisibilityBatch failed: Invalid request")
		return resp, fmt.Errorf("INVALID_REQUEST")
	}
	if len(req.MessageIDs) == 0 {
		log.Printf("ChangeMessageVisibilityBatch failed: Empty request")
		return resp, fmt.Errorf("EMPTY_REQUEST")
	}
	if len(req.MessageIDs) > 10 {
		log.Printf("ChangeMessageVisibilityBatch failed: Too many entries (%d); max 10", len(req.MessageIDs))
		return resp, fmt.Errorf("INVALID_REQUEST")
	}
	input := &sqs.ChangeMessageVisibilityBatchInput{}
	input.QueueUrl = aws.String(req.QueueURL)
	entries := []*sqs.ChangeMessageVisibilityBatchRequestEntry{}
	for i, id := range req.MessageIDs {
		entry := &sqs.ChangeMessageVisibilityBatchRequestEntry{
			Id:                aws.String(id),
			ReceiptHandle:     aws.String(req.ReceiptHandles[i]),
			VisibilityTimeout: aws.Int64(int64(req.TimeoutSeconds)),
		}
		entries = append(entries, entry)
	}
	input.Entries = entries
	output, err := s.svc.ChangeMessageVisibilityBatch(input)
	if err != nil {
		return resp, fmt.Errorf("s.svc.ChangeMessageVisibilityBatch: %w", err)
	}
	resp = wrapBatchUpdateVisibilityTimeoutOutput(output)

	return resp, nil
}

// wrap sqs.DeleteMessageBatchOutput object
func wrapBatchUpdateVisibilityTimeoutOutput(output *sqs.ChangeMessageVisibilityBatchOutput) BatchUpdateVisibilityTimeoutResponse {
	wrapSuccessful := []BatchUpdateVisibilityTimeoutEntry{}
	wrapFailed := []BatchUpdateVisibilityTimeoutErrEntry{}

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
			SenderFault:  *entry.SenderFault,
		}
		wrapFailed = append(wrapFailed, wrap)
	}
	wrap := BatchUpdateVisibilityTimeoutResponse{
		Successful: wrapSuccessful,
		Failed:     wrapFailed,
	}
	return wrap
}
