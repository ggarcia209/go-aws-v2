//go:build integration

package gosqs

import (
	"testing"

	"github.com/ggarcia209/go-aws-v2/v1/goaws"
)

var avs = []MsgAV{
	{
		Key:      "department",
		DataType: "String",
		Value:    "IT-Eng",
	},
	{
		Key:      "project",
		DataType: "String",
		Value:    "uBook",
	},
}

var avMap = CreateMsgAttributes(avs)

var msg1 = SendMsgOptions{
	DelaySeconds:            SendMsgDefault.DelaySeconds,
	MessageAttributes:       nil,
	MessageBody:             "msg-test001",
	MessageDeduplicationId:  SendMsgDefault.MessageDeduplicationId,
	MessageGroupId:          SendMsgDefault.MessageGroupId,
	MessageSystemAttributes: nil,
	QueueURL:                "",
}

var msg2 = SendMsgOptions{
	DelaySeconds:            SendMsgDefault.DelaySeconds,
	MessageAttributes:       avMap,
	MessageBody:             "msg-test002",
	MessageDeduplicationId:  "test-dedup-001",
	MessageGroupId:          "test-group-001",
	MessageSystemAttributes: nil,
	QueueURL:                "",
}

var msg3 = SendMsgOptions{
	DelaySeconds:            SendMsgDefault.DelaySeconds,
	MessageAttributes:       avMap,
	MessageBody:             "msg-test003",
	MessageDeduplicationId:  "",
	MessageGroupId:          "",
	MessageSystemAttributes: nil,
	QueueURL:                "",
}

var testMsgIDs = []string{
	"981f6cf7-27ed-44e5-86ca-7692cd75ac90",
	"e74924e1-e946-41fa-9625-5a3aa1cc618c",
}

var testRecHandles = []string{
	"AQEBgIDmFAUw0/Dm2t3a38mDgYu44xOsHNoEe9U06Q2HN2pHs7QoZ98VuMSnEMo5jqGanOBpjGjsh/Zz04fnznR4s/7+SaIvZe+yMeRG6TKyW6kgfaarLx6tUqXEXjxEznRpIOCpBrLQRD1xwFm30bJLN8xGWZAd7bZEG4/uu8QKSA/aQ2ldeTjb5nIoRuO/wFR4F+BckwoLl5Q8YMMzF58x3f3bqbqNihxKW84uS3SUKYCH5cuAmCGR98iZ02hmXsQoK450HgLzBt6Ys966SAl9z06T91h4lZSzjkqMBIwSeaA=",
	"AQEBcdPxL7Y+SDLmih+QU9t3s5s6RFFSDcu2X29fUBTJCWf+BFJR43P0iDeCVEFgV58EsK5sJ2+v1pEznJqzmwhB9aTLqb5pB6SDe/FuKZDMsb1oszC8N9ifmkXp1mjo/tnjRrajlYWN6cf8ztnjhFk8c9FdvzUPvG98NgxJuttgY5QQjIE8Kzrbq/EtS7kbEKNWeEFdrLVlSKQoAfz3/jbjdOPNAiVNvyoWYGn3KQynoYaFMWK8ZbnkMtdQWnT5gPRXNQVid1yLr7ywJ/PFQkvGQvNJC8R9tgb4miChWIvBISI=",
}

// 5/6/2021 - PASS
func TestSendMessage(t *testing.T) {
	var tests = []struct {
		name    string
		options SendMsgOptions
	}{
		{name: "test-001", options: msg1},
		{name: "test-002.fifo", options: msg2},
		{name: "test-002.fifo", options: msg3},
	}
	sess := goaws.NewDefaultSession()
	sqsQ := NewSqsQueues(sess)
	sqsM := NewSqsMessages(sess)
	for _, test := range tests {
		url, err := sqsQ.GetQueueURL(test.name)
		if err != nil {
			t.Errorf("GetQueueURL failed (%s): %v", test.name, err)
		}
		test.options.QueueURL = url
		_, err = sqsM.SendMessage(test.options)
		if err != nil {
			t.Errorf("SendMessage failed: %v", err)
		}
	}
}

// 5/6/2021 - PASS
func TestCheckFifo(t *testing.T) {
	var tests = []struct {
		input string
		want  bool
	}{
		{input: "https://sqs.us-west-2.amazonaws.com/12345678909/test-001", want: false},
		{input: "https://sqs.us-west-2.amazonaws.com/12345678909/test-002.fifo", want: true},
	}
	for _, test := range tests {
		fifo := checkFifo(test.input)
		if fifo != test.want {
			t.Errorf("fail - %v; want: %v", fifo, test.want)
		}
	}
}

// 5/6/2021 - PASS
func TestGenerateDedupID(t *testing.T) {
	var tests = []string{
		"https://sqs.us-west-2.amazonaws.com/12345678909/test-001",
		"https://sqs.us-west-2.amazonaws.com/12345678909/test-002.fifo",
	}
	for _, test := range tests {
		hash := GenerateDedupeID(test)
		t.Logf("hash: %s", hash)
	}
}

// 5/6/2021 - PASS
func TestReceiveMessage(t *testing.T) {
	var tests = []struct {
		name    string
		options RecMsgOptions
	}{
		{name: "test-001", options: RecMsgDefault},
		{name: "test-002.fifo", options: RecMsgDefault},
		{name: "test-002.fifo", options: RecMsgDefault},
	}
	sess := goaws.NewDefaultSession()
	sqsQ := NewSqsQueues(sess)
	sqsM := NewSqsMessages(sess)
	for _, test := range tests {
		url, err := sqsQ.GetQueueURL(test.name)
		if err != nil {
			t.Errorf("GetQueueURL failed (%s): %v", test.name, err)
		}
		test.options.QueueURL = url
		msgs, err := sqsM.ReceiveMessage(test.options)
		if err != nil {
			t.Errorf("SendMessage failed: %v", err)
		}
		t.Log(msgs)
	}
}

// 5/6/2021 - PASS
func TestDeleteMessage(t *testing.T) {
	var tests = []struct {
		name    string
		options RecMsgOptions
	}{
		{name: "test-001", options: RecMsgDefault},
		{name: "test-002.fifo", options: RecMsgDefault},
	}
	sess := goaws.NewDefaultSession()
	sqsQ := NewSqsQueues(sess)
	sqsM := NewSqsMessages(sess)
	for _, test := range tests {
		url, err := sqsQ.GetQueueURL(test.name)
		if err != nil {
			t.Errorf("GetQueueURL failed (%s): %v", test.name, err)
		}
		test.options.QueueURL = url
		msgs, err := sqsM.ReceiveMessage(test.options)
		if err != nil {
			t.Errorf("ReceiveMessage failed: %v", err)
		}
		for _, msg := range msgs {
			handle := msg.ReceiptHandle
			err = sqsM.DeleteMessage(url, handle)
			if err != nil {
				t.Errorf("DeleteMessage failed: %v", err)
			}
		}
	}
}

func TestDeleteMesssageBatch(t *testing.T) {
	var tests = []struct {
		input DeleteMessageBatchRequest
		want  error
	}{
		{input: DeleteMessageBatchRequest{ // test delete with non-empty queue
			QueueURL:       "https://sqs.us-west-2.amazonaws.com/12345678909/us-west-san_francisco.fifo",
			MessageIDs:     testMsgIDs,
			ReceiptHandles: testRecHandles,
		}, want: nil},
		{input: DeleteMessageBatchRequest{ // test delete with empty queue
			QueueURL:       "https://sqs.us-west-2.amazonaws.com/12345678909/us-west-san_francisco.fifo",
			MessageIDs:     testMsgIDs,
			ReceiptHandles: testRecHandles,
		}, want: nil},
	}
	sqsM := NewSqsMessages(goaws.NewDefaultSession())
	for _, test := range tests {
		resp, err := sqsM.DeleteMessageBatch(test.input)
		if err != test.want {
			t.Errorf("FAIL - error: %v", err)
		}
		t.Logf("succeeded: %v", resp.Successful)
		t.Logf("failed: %v", resp.Failed)
	}
}

func TestChangeMessageVisibilityBatch(t *testing.T) {
	var tests = []struct {
		input RecMsgOptions
		want  error
	}{
		{RecMsgDefault, nil},
	}
	sess := goaws.NewDefaultSession()
	sqsQ := NewSqsQueues(sess)
	sqsM := NewSqsMessages(sess)
	msgIDs, handles := []string{}, []string{}
	url, err := sqsQ.GetQueueURL("test-queue.fifo")
	if err != nil {
		t.Errorf("GetQueueURL failed: %v", err)
	}
	for _, test := range tests {
		test.input.MaxNumberOfMessages = 10
		test.input.QueueURL = url
		msgs, err := sqsM.ReceiveMessage(test.input)
		if err != nil {
			t.Errorf("ReceiveMessage failed: %v", err)
		}
		for _, msg := range msgs {
			msgIDs = append(msgIDs, msg.MessageId)
			handles = append(handles, msg.ReceiptHandle)
		}
		req := BatchUpdateVisibilityTimeoutRequest{
			QueueURL:       url,
			MessageIDs:     msgIDs,
			ReceiptHandles: handles,
			TimeoutSeconds: 5,
		}
		resp, err := sqsM.ChangeMessageVisibilityBatch(req)
		if err != nil {
			t.Errorf("FAIL: %v", err)
		}
		for _, fail := range resp.Failed {
			t.Errorf("FAIL - AWS err: %v", fail.ErrorCode)
		}
	}
}

func BenchmarkChangeMessageVisibilityBatch(b *testing.B) {
	sess := goaws.NewDefaultSession()
	sqsQ := NewSqsQueues(sess)
	sqsM := NewSqsMessages(sess)

	msgIDs, handles := []string{}, []string{}
	url, err := sqsQ.GetQueueURL("test-queue.fifo")
	if err != nil {
		b.Errorf("GetQueueURL failed: %v", err)
	}
	input := RecMsgDefault
	input.MaxNumberOfMessages = 10
	input.QueueURL = url
	msgs, err := sqsM.ReceiveMessage(input)
	if err != nil {
		b.Errorf("ReceiveMessage failed: %v", err)
	}
	for _, msg := range msgs {
		msgIDs = append(msgIDs, msg.MessageId)
		handles = append(handles, msg.ReceiptHandle)
	}
	req := BatchUpdateVisibilityTimeoutRequest{
		QueueURL:       url,
		MessageIDs:     msgIDs,
		ReceiptHandles: handles,
		TimeoutSeconds: 0,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := sqsM.ChangeMessageVisibilityBatch(req)
		if err != nil {
			b.Errorf("FAIL: %v", err)
		}
	}
}
