package godynamo

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ggarcia209/go-aws-v2/v2/goaws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gomock "go.uber.org/mock/gomock"
)

func TestNewTransactions(t *testing.T) {
	cfg, err := goaws.NewDefaultConfig(context.Background())
	if err != nil {
		t.Errorf("goaws.NewDefaultConfig: %v", err)
		return
	}

	require.NotNil(t, cfg)

	// test interface implementation
	svc := dynamodb.New(dynamodb.Options{
		Credentials: cfg.Config.Credentials,
		Region:      cfg.Config.Region,
	})
	transactions := NewTransactions(svc, DefaultFailConfig)
	assert.NotNil(t, transactions)
	assert.NotNil(t, transactions.svc)
	assert.Implements(t, (*TransactionsLogic)(nil), transactions)
}

func TestTransactions_TxWrite(t *testing.T) {
	testTable := &Table{TableName: "test-table", PrimaryKeyName: "id", PrimaryKeyType: "S"}
	testItem := map[string]interface{}{"id": "1", "data": "value"}

	tests := []struct {
		name          string
		items         []TransactionItem
		requestToken  string
		mockSetup     func(ctrl *gomock.Controller) DynamoDBTransactionsClientAPI
		expectedError error
		expectedFail  int
	}{
		{
			name: "Success",
			items: []TransactionItem{
				NewCreateTxItem("create-1", testItem, testTable, nil, NewExpression()),
			},
			requestToken: "token-1",
			mockSetup: func(ctrl *gomock.Controller) DynamoDBTransactionsClientAPI {
				m := NewMockDynamoDBTransactionsClientAPI(ctrl)
				m.EXPECT().TransactWriteItems(gomock.Any(), gomock.Any(), gomock.Any()).Return(&dynamodb.TransactWriteItemsOutput{}, nil).Times(1)
				return m
			},
			expectedError: nil,
			expectedFail:  0,
		},
		{
			name: "TxItemsExceedsLimitError",
			items: func() []TransactionItem {
				items := make([]TransactionItem, 26)
				for i := 0; i < 26; i++ {
					items[i] = NewCreateTxItem("create", testItem, testTable, nil, NewExpression())
				}
				return items
			}(),
			mockSetup: func(ctrl *gomock.Controller) DynamoDBTransactionsClientAPI {
				return NewMockDynamoDBTransactionsClientAPI(ctrl)
			},
			expectedError: NewTxItemsExceedsLimitError(),
			expectedFail:  0,
		},
		{
			name: "TransactionCanceledException/ConditionalCheckFailed",
			items: []TransactionItem{
				NewCreateTxItem("create-1", testItem, testTable, nil, NewExpression()),
			},
			mockSetup: func(ctrl *gomock.Controller) DynamoDBTransactionsClientAPI {
				m := NewMockDynamoDBTransactionsClientAPI(ctrl)
				m.EXPECT().TransactWriteItems(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, &types.TransactionCanceledException{
					CancellationReasons: []types.CancellationReason{
						{Code: aws.String(string(types.BatchStatementErrorCodeEnumConditionalCheckFailed)), Message: aws.String("Condition failed")},
					},
				}).Times(1)
				return m
			},
			expectedError: NewTxConditonCheckFailedError("Condition failed"),
			expectedFail:  1,
		},
		{
			name: "TransactionCanceledException/Throttling",
			items: []TransactionItem{
				NewCreateTxItem("create-1", testItem, testTable, nil, NewExpression()),
			},
			mockSetup: func(ctrl *gomock.Controller) DynamoDBTransactionsClientAPI {
				m := NewMockDynamoDBTransactionsClientAPI(ctrl)
				m.EXPECT().TransactWriteItems(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, &types.TransactionCanceledException{
					CancellationReasons: []types.CancellationReason{
						{Code: aws.String(string(types.BatchStatementErrorCodeEnumThrottlingError)), Message: aws.String("Throttled")},
					},
				}).Times(1)
				return m
			},
			expectedError: NewTxThrottledError(),
			expectedFail:  1,
		},
		{
			name: "TransactionConflictException",
			items: []TransactionItem{
				NewCreateTxItem("create-1", testItem, testTable, nil, NewExpression()),
			},
			mockSetup: func(ctrl *gomock.Controller) DynamoDBTransactionsClientAPI {
				m := NewMockDynamoDBTransactionsClientAPI(ctrl)
				m.EXPECT().TransactWriteItems(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, &types.TransactionConflictException{}).Times(1)
				return m
			},
			expectedError: NewTxConflictError(),
			expectedFail:  0,
		},
		{
			name: "TransactionInProgressException",
			items: []TransactionItem{
				NewCreateTxItem("create-1", testItem, testTable, nil, NewExpression()),
			},
			mockSetup: func(ctrl *gomock.Controller) DynamoDBTransactionsClientAPI {
				m := NewMockDynamoDBTransactionsClientAPI(ctrl)
				m.EXPECT().TransactWriteItems(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, &types.TransactionInProgressException{}).Times(1)
				return m
			},
			expectedError: NewTxInProgressError(),
			expectedFail:  0,
		},
		{
			name: "OtherError",
			items: []TransactionItem{
				NewCreateTxItem("create-1", testItem, testTable, nil, NewExpression()),
			},
			mockSetup: func(ctrl *gomock.Controller) DynamoDBTransactionsClientAPI {
				m := NewMockDynamoDBTransactionsClientAPI(ctrl)
				m.EXPECT().TransactWriteItems(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("some error")).Times(1)
				return m
			},
			expectedError: goaws.NewInternalError(errors.New("s.svc.DeleteMessage: some error")),
			expectedFail:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockSvc := tt.mockSetup(ctrl)
			transactions := NewTransactions(mockSvc, nil)

			failed, err := transactions.TxWrite(context.Background(), tt.items, tt.requestToken)

			if tt.expectedError != nil {
				require.Error(t, err)
				assert.EqualError(t, err, tt.expectedError.Error())
				var awsErr goaws.AwsError
				assert.True(t, errors.As(err, &awsErr))
			} else {
				require.NoError(t, err)
			}
			assert.Len(t, failed, tt.expectedFail)
		})
	}
}
