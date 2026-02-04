# go-aws/v2

Golang packages that wrap AWS SDK v2 APIs for core AWS services, providing simplified common methods and interfaces for easier testing.

## V2 Improvements:
- Migrate all packages from aws-sdk-go to aws-sdk-go-v2
- Improved error handling with common AwsError interface
- Generated mocks for each service
- Unit tests for all methods
- Fixes errors and simplifies logic
- Fixes security vulnerabilities in older versions of go and aws-sdk-go

## Packages

*   **godynamo**: DynamoDB CRUD operations and transaction helpers.
*   **gosqs**: SQS message sending and receiving.
*   **gos3**: S3 object upload and download.
*   **gosns**: SNS topic creation, subscription, and publishing.
*   **goses**: SES email sending.

## Installation

```bash
go get -u github.com/ggarcia209/go-aws-v2/v2@latest
```

## Configuration

Initialize the standard AWS configuration using the `goaws` package.

```go
package main

import (
	"context"
	"log"

	"github.com/ggarcia209/go-aws/v2/goaws"
)

func main() {
	ctx := context.Background()
	
	// Load default config (credentials from env vars, profile, or IAM role)
	cfg, err := goaws.NewDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}
    
    // Use cfg.Config with service constructors...
}
```

## Usage Examples

### DynamoDB (`godynamo`)

```go
import (
	"context"
	"github.com/ggarcia209/go-aws/v2/godynamo"
)

func example(cfg *goaws.AwsConfig) {
    // Define your table schema
    tables := map[string]*godynamo.Table{
        "MyTable": godynamo.CreateNewTableObj("MyTable", "ID", "string", "", ""),
    }
    
    // Initialize DynamoDB wrapper
    db := godynamo.NewDynamoDB(*cfg, []*godynamo.Table{tables["MyTable"]}, nil)

    // Put Item
    item := map[string]interface{}{"ID": "123", "Data": "test"}
    err := db.Queries.CreateItem(context.TODO(), item, "MyTable")
}
```

### SQS (`gosqs`)

```go
import (
	"context"
	"github.com/ggarcia209/go-aws/v2/gosqs"
)

func example(cfg *goaws.AwsConfig) {
    client := gosqs.NewSQS(*cfg)

    // Send Message
    msg := "Hello World"
    id, err := client.Messages.SendMsg(context.TODO(), "queue-url", msg)
}
```

### S3 (`gos3`)

```go
import (
    "context"
    "strings"
	"github.com/ggarcia209/go-aws/v2/gos3"
)

func example(cfg *goaws.AwsConfig) {
    // 5MB partition size for multipart uploads
    client := gos3.NewS3(*cfg, 5*1024*1024) 

    // Upload
    // Note: UploadFile takes a struct request
    req := gos3.UploadFileRequest{
        Bucket: "bucket-name",
        Key:    "key",
        File:   strings.NewReader("content"),
    }
    resp, err := client.UploadFile(context.TODO(), req)
}
```

### SNS (`gosns`)

```go
import (
	"context"
	"github.com/ggarcia209/go-aws/v2/gosns"
)

func example(cfg *goaws.AwsConfig) {
    client := gosns.NewSNS(*cfg)

    // Create Topic
    topic, err := client.CreateTopic(context.TODO(), "my-topic")
    if err != nil {
        // handle error
    }

    // Publish
    _, err = client.Publish(context.TODO(), "message body", topic.TopicArn)
}
```

### SES (`goses`)

```go
import (
    "context"
	"github.com/ggarcia209/go-aws/v2/goses"
)

func example(cfg *goaws.AwsConfig) {
    client := goses.NewSES(*cfg)

    // Send Email
    params := goses.SendEmailParams{
        From:     "sender@example.com",
        To:       []string{"recipient@example.com"},
        Subject:  "Hello",
        TextBody: "Hello User",
    }
    err := client.SendEmail(context.TODO(), params)
}
```

### Secrets Manager (`gosm`)

```go
import (
	"context"
	"fmt"
	"github.com/ggarcia209/go-aws-v2/v2/gosm"
)

func example(cfg *goaws.AwsConfig) {
    var mySecretFromEnv string
	client := gosm.NewSecretsManager(*cfg)

	// Get Secret
	// Returns the secret string value
	resp, err := client.GetSecret(context.TODO(), "my-secret-key")
	if err != nil {
		// handle error
	}
	mySecretFromEnv = resp.Secret
}
```

## Unit Testing with Mocks

This library provides generated mocks for all client interfaces using `go.uber.org/mock/gomock`. You can import these mocks in your own projects to robustly test your code that interacts with `go-aws` wrappers.

### Importing Mocks

The mocks are located in `github.com/ggarcia209/go-aws/v2/mocks/...`.

### Example Test

```go
package mypackage

import (
    "testing"
    "github.com/stretchr/testify/assert"
    "go.uber.org/mock/gomock"
    
    // Import the mock package
    "github.com/ggarcia209/go-aws/v2/mocks/gosqsmock" 
)

func TestMyFunctionUsingSQS(t *testing.T) {
    ctrl := gomock.NewController(t)
    defer ctrl.Finish()

    // Create a mock of the SQSMessagesClientAPI
    mockSQS := gosqsmock.NewMockSQSMessagesClientAPI(ctrl)

    // Set expectation
    mockSQS.EXPECT().
        SendMsg(gomock.Any(), "queue-url", "test-message").
        Return("msg-id", nil)

    // Inject mock into your code
    myService := NewMyService(mockSQS)
    err := myService.DoSomething()
    
    assert.NoError(t, err)
}
```
