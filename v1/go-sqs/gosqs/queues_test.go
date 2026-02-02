//go:build integration

package gosqs

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/ggarcia209/go-aws-v2/v1/goaws"
)

var noTags = map[string]*string{}
var testTags = map[string]*string{
	"department": aws.String("it-eng"),
	"project":    aws.String("ubook"),
}

var defaultTest = QueueOptions{
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
	// ContentBasedDeduplication:     "false",
	// * high throughput preview *
	// only available in us-east-1, us-east-2, us-west-2, eu-west-1
	// DeduplicationScope:  "queue",
	// FifoThroughputLimit: "perQueue",
	// *  *
}

var fifoOptions1 = QueueOptions{
	DelaySeconds:                  "0",
	MaximumMessageSize:            "262144",
	MessageRetentionPeriod:        "345600",
	Policy:                        "",
	ReceiveMessageWaitTimeSeconds: "0",
	RedrivePolicy:                 "",
	VisibilityTimeout:             "30",
	KmsMasterKeyId:                "",
	KmsDataKeyReusePeriodSeconds:  "300",
	FifoQueue:                     "true",
	ContentBasedDeduplication:     "false",
	// * high throughput preview *
	// only available in us-east-1, us-east-2, us-west-2, eu-west-1
	DeduplicationScope:  "queue",
	FifoThroughputLimit: "perQueue",
}

var fifoOptions2 = QueueOptions{
	DelaySeconds:                  "0",
	MaximumMessageSize:            "262144",
	MessageRetentionPeriod:        "345600",
	Policy:                        "",
	ReceiveMessageWaitTimeSeconds: "0",
	RedrivePolicy:                 "",
	VisibilityTimeout:             "30",
	KmsMasterKeyId:                "",
	KmsDataKeyReusePeriodSeconds:  "300",
	FifoQueue:                     "true",
	ContentBasedDeduplication:     "true",
	// * high throughput preview *
	// only available in us-east-1, us-east-2, us-west-2, eu-west-1
	DeduplicationScope:  "queue",
	FifoThroughputLimit: "perQueue",
}

// 5/6/21 - PASS
func TestCreateQueue(t *testing.T) {
	var tests = []struct {
		name    string
		options QueueOptions
		tags    map[string]*string
	}{
		{name: "test-001", options: defaultTest, tags: noTags},
		{name: "test-002.fifo", options: fifoOptions1, tags: noTags},
		// {name: "test-003.fifo", options: fifoOptions2, tags: testTags},
	}
	sqs := NewSqsQueues(goaws.NewDefaultSession())
	for i, test := range tests {
		url, err := sqs.CreateQueue(test.name, test.options, test.tags)
		if err != nil {
			t.Errorf("test %d failed: %v", i, err)
		}
		t.Logf("%s URL: %s", test.name, url)
	}
}

// 5/6/21 - PASS
func TestGetQueueURL(t *testing.T) {
	var tests = []string{
		"test-001",
		"test-002.fifo",
		"test-003.fifo",
	}
	sqs := NewSqsQueues(goaws.NewDefaultSession())
	for _, test := range tests {
		url, err := sqs.GetQueueURL(test)
		if err != nil {
			t.Errorf("%s failed: %v", test, err)
		}
		t.Logf("%s URL: %s", test, url)
	}
}

// 5/6/2021 - PASS
func TestPurgeQueue(t *testing.T) {
	var tests = []string{
		"test-001",
		"test-002.fifo",
		"test-003.fifo",
	}
	sqs := NewSqsQueues(goaws.NewDefaultSession())
	for _, test := range tests {
		url, err := sqs.GetQueueURL(test)
		if err != nil {
			t.Errorf("GetQueueURL failed (%s): %v", test, err)
		}
		err = sqs.PurgeQueue(url)
		if err != nil {
			t.Errorf("PurgeQueue failed (%s): %v", test, err)
		} else {
			t.Logf("PurgeQueue succeeded")
		}
	}
}

func TestDeleteQueue(t *testing.T) {
	var tests = []struct {
		input string
		want  string // error code
	}{
		{"https://sqs.us-west-2.amazonaws.com/840111470667/test-001", ""},                     // delete existing
		{"https://sqs.us-west-2.amazonaws.com/840111470667/test-001", ErrAWSNonExistentQueue}, // delete non-existent
	}
	sqs := NewSqsQueues(goaws.NewDefaultSession())
	for _, test := range tests {
		err := sqs.DeleteQueue(test.input)
		if err != nil {
			if err.Error() != test.want {
				t.Errorf("DeleteQueue failed (%s): %v", test.input, err)
			}
		}
		if err == nil && test.want != "" {
			t.Errorf("DeleteQueue failed (%s): %v", test.input, err)
		}
	}
}
