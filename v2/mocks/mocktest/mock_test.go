package mocktest

import (
	"testing"

	"github.com/ggarcia209/go-aws-v2/v2/godynamo"
	"github.com/ggarcia209/go-aws-v2/v2/gos3"
	"github.com/ggarcia209/go-aws-v2/v2/goses"
	"github.com/ggarcia209/go-aws-v2/v2/gosm"
	"github.com/ggarcia209/go-aws-v2/v2/gosns"
	"github.com/ggarcia209/go-aws-v2/v2/gosqs"
	"github.com/ggarcia209/go-aws-v2/v2/mocks/godynamomock"
	"github.com/ggarcia209/go-aws-v2/v2/mocks/gos3mock"
	"github.com/ggarcia209/go-aws-v2/v2/mocks/gosesmock"
	"github.com/ggarcia209/go-aws-v2/v2/mocks/gosmmock"
	"github.com/ggarcia209/go-aws-v2/v2/mocks/gosnsmock"

	"github.com/ggarcia209/go-aws-v2/v2/mocks/gosqsmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestDynamoTablesMock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := godynamomock.NewMockTablesLogic(ctrl)
	require.NotNil(t, mock)
	assert.Implements(t, (*godynamo.TablesLogic)(nil), mock)
}

func TestDynamoQueriesMock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := godynamomock.NewMockQueriesLogic(ctrl)
	require.NotNil(t, mock)
	assert.Implements(t, (*godynamo.QueriesLogic)(nil), mock)
}

func TestDynamoTransactionsMock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := godynamomock.NewMockTransactionsLogic(ctrl)
	require.NotNil(t, mock)
	assert.Implements(t, (*godynamo.TransactionsLogic)(nil), mock)
}

func TestS3Mock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockS3 := gos3mock.NewMockS3Logic(ctrl)
	require.NotNil(t, mockS3)
	assert.Implements(t, (*gos3.S3Logic)(nil), mockS3)
}

func TestSESMock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSES := gosesmock.NewMockSESLogic(ctrl)
	require.NotNil(t, mockSES)
	assert.Implements(t, (*goses.SESLogic)(nil), mockSES)
}

func TestSNSMock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSNS := gosnsmock.NewMockSNSLogic(ctrl)
	require.NotNil(t, mockSNS)
	assert.Implements(t, (*gosns.SNSLogic)(nil), mockSNS)
}

func TestSQSQueuesMock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSQS := gosqsmock.NewMockQueuesLogic(ctrl)
	require.NotNil(t, mockSQS)
	assert.Implements(t, (*gosqs.QueuesLogic)(nil), mockSQS)
}

func TestSQSMessagesMock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSQS := gosqsmock.NewMockMessagesLogic(ctrl)
	require.NotNil(t, mockSQS)
	assert.Implements(t, (*gosqs.MessagesLogic)(nil), mockSQS)
}

func TestSecretsManagerMock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSM := gosmmock.NewMockSecretsManagerLogic(ctrl)
	require.NotNil(t, mockSM)
	assert.Implements(t, (*gosm.SecretsManagerLogic)(nil), mockSM)
}
