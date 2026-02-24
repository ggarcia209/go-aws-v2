package godynamo

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ggarcia209/go-aws-v2/v2/goaws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gomock "go.uber.org/mock/gomock"
)

func TestNewQueries(t *testing.T) {
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
	testTables := map[string]*Table{
		"test": CreateNewTableObj("test", "id", "S", "", ""),
	}
	queries := NewQueries(svc, testTables, DefaultFailConfig)
	assert.NotNil(t, queries)
	assert.NotNil(t, queries.svc)
	assert.Implements(t, (*QueriesLogic)(nil), queries)
}

func TestQueries_CreateItem(t *testing.T) {
	tests := []struct {
		name          string
		tableName     string
		item          any
		mockSetup     func(ctrl *gomock.Controller) DynamoDBQueriesClientAPI
		expectedError error
	}{
		{
			name:      "Success",
			tableName: "test-table",
			item:      map[string]interface{}{"id": "1", "data": "value"},
			mockSetup: func(ctrl *gomock.Controller) DynamoDBQueriesClientAPI {
				m := NewMockDynamoDBQueriesClientAPI(ctrl)
				m.EXPECT().PutItem(gomock.Any(), gomock.Any(), gomock.Any()).Return(&dynamodb.PutItemOutput{}, nil).Times(1)
				return m
			},
			expectedError: nil,
		},
		{
			name:      "TableNotFound",
			tableName: "missing-table",
			item:      map[string]interface{}{"id": "1"},
			mockSetup: func(ctrl *gomock.Controller) DynamoDBQueriesClientAPI {
				return NewMockDynamoDBQueriesClientAPI(ctrl)
			},
			expectedError: NewTableNotFoundError("missing-table"),
		},
		{
			name:      "Error",
			tableName: "test-table",
			item:      map[string]interface{}{"id": "1"},
			mockSetup: func(ctrl *gomock.Controller) DynamoDBQueriesClientAPI {
				m := NewMockDynamoDBQueriesClientAPI(ctrl)
				m.EXPECT().PutItem(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("put error")).Times(1)
				return m
			},
			expectedError: goaws.NewInternalError(errors.New("q.svc.PutItem: put error")),
		},
		{
			name:      "NilItem",
			tableName: "test-table",
			item:      nil,
			mockSetup: func(ctrl *gomock.Controller) DynamoDBQueriesClientAPI {
				return NewMockDynamoDBQueriesClientAPI(ctrl)
			},
			expectedError: NewNilModelError(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockSvc := tt.mockSetup(ctrl)

			// Setup tables map
			tables := map[string]*Table{}
			if tt.tableName == "test-table" {
				tables["test-table"] = &Table{TableName: "test-table"}
			}

			q := NewQueries(mockSvc, tables, nil)

			err := q.CreateItem(context.Background(), tt.item, tt.tableName)

			if tt.expectedError != nil {
				require.Error(t, err)
				assert.EqualError(t, err, tt.expectedError.Error())
				assert.Implements(t, (*goaws.AwsError)(nil), err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestQueries_GetItem(t *testing.T) {
	type TestItem struct {
		ID   string `json:"id"`
		Data string `json:"data"`
	}

	tests := []struct {
		name          string
		tableName     string
		query         *Query
		mockSetup     func(ctrl *gomock.Controller) DynamoDBQueriesClientAPI
		expectedItem  *TestItem
		expectedError error
	}{
		{
			name:      "Success",
			tableName: "test-table",
			query:     CreateNewQueryObj("1", nil),
			mockSetup: func(ctrl *gomock.Controller) DynamoDBQueriesClientAPI {
				m := NewMockDynamoDBQueriesClientAPI(ctrl)
				m.EXPECT().GetItem(gomock.Any(), gomock.Any(), gomock.Any()).Return(&dynamodb.GetItemOutput{
					Item: map[string]types.AttributeValue{
						"id":   &types.AttributeValueMemberS{Value: "1"},
						"data": &types.AttributeValueMemberS{Value: "value"},
					},
				}, nil).Times(1)
				return m
			},
			expectedItem:  &TestItem{ID: "1", Data: "value"},
			expectedError: nil,
		},
		{
			name:      "TableNotFound",
			tableName: "missing-table",
			query:     CreateNewQueryObj("1", nil),
			mockSetup: func(ctrl *gomock.Controller) DynamoDBQueriesClientAPI {
				return NewMockDynamoDBQueriesClientAPI(ctrl)
			},
			expectedItem:  &TestItem{},
			expectedError: NewTableNotFoundError("missing-table"),
		},
		{
			name:      "Error",
			tableName: "test-table",
			query:     CreateNewQueryObj("1", nil),
			mockSetup: func(ctrl *gomock.Controller) DynamoDBQueriesClientAPI {
				m := NewMockDynamoDBQueriesClientAPI(ctrl)
				m.EXPECT().GetItem(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("get error")).Times(1)
				return m
			},
			expectedItem:  &TestItem{},
			expectedError: goaws.NewInternalError(errors.New("q.svc.GetItem: get error")),
		},
		{
			name:      "NilQuery",
			tableName: "test-table",
			query:     nil,
			mockSetup: func(ctrl *gomock.Controller) DynamoDBQueriesClientAPI {
				return NewMockDynamoDBQueriesClientAPI(ctrl)
			},
			expectedItem:  &TestItem{},
			expectedError: NewNilModelError(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockSvc := tt.mockSetup(ctrl)

			// Setup tables map
			tables := map[string]*Table{}
			if tt.tableName == "test-table" {
				tables["test-table"] = &Table{
					TableName:      "test-table",
					PrimaryKeyName: "id",
					PrimaryKeyType: "S",
				}
			}

			q := NewQueries(mockSvc, tables, nil)

			item := &TestItem{}
			err := q.GetItem(context.Background(), GetItemParams{
				Query:      tt.query,
				TableName:  tt.tableName,
				ItemPtr:    item,
				Expression: NewExpression(),
			})

			if tt.expectedError != nil {
				require.Error(t, err)
				assert.EqualError(t, err, tt.expectedError.Error())
				assert.Implements(t, (*goaws.AwsError)(nil), err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedItem, item)
			}
		})
	}
}

func TestQueries_UpdateItem(t *testing.T) {
	tests := []struct {
		name          string
		tableName     string
		query         *Query
		expr          Expression
		mockSetup     func(ctrl *gomock.Controller) DynamoDBQueriesClientAPI
		expectedError error
	}{
		{
			name:      "Success",
			tableName: "test-table",
			query:     CreateNewQueryObj("1", nil),
			expr:      NewExpression(),
			mockSetup: func(ctrl *gomock.Controller) DynamoDBQueriesClientAPI {
				m := NewMockDynamoDBQueriesClientAPI(ctrl)
				m.EXPECT().UpdateItem(gomock.Any(), gomock.Any(), gomock.Any()).Return(&dynamodb.UpdateItemOutput{}, nil).Times(1)
				return m
			},
			expectedError: nil,
		},
		{
			name:      "TableNotFound",
			tableName: "missing-table",
			query:     CreateNewQueryObj("1", nil),
			expr:      NewExpression(),
			mockSetup: func(ctrl *gomock.Controller) DynamoDBQueriesClientAPI {
				return NewMockDynamoDBQueriesClientAPI(ctrl)
			},
			expectedError: NewTableNotFoundError("missing-table"),
		},
		{
			name:      "Error",
			tableName: "test-table",
			query:     CreateNewQueryObj("1", nil),
			expr:      NewExpression(),
			mockSetup: func(ctrl *gomock.Controller) DynamoDBQueriesClientAPI {
				m := NewMockDynamoDBQueriesClientAPI(ctrl)
				m.EXPECT().UpdateItem(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("update error")).Times(1)
				return m
			},
			expectedError: goaws.NewInternalError(errors.New("q.svc.UpdateItem: update error")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockSvc := tt.mockSetup(ctrl)

			// Setup tables map
			tables := map[string]*Table{}
			if tt.tableName == "test-table" {
				tables["test-table"] = &Table{
					TableName:      "test-table",
					PrimaryKeyName: "id",
					PrimaryKeyType: "S",
				}
			}

			q := NewQueries(mockSvc, tables, nil)

			err := q.UpdateItem(context.Background(), tt.query, tt.tableName, tt.expr)

			if tt.expectedError != nil {
				require.Error(t, err)
				assert.EqualError(t, err, tt.expectedError.Error())
				assert.Implements(t, (*goaws.AwsError)(nil), err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestQueries_DeleteItem(t *testing.T) {
	tests := []struct {
		name          string
		tableName     string
		query         *Query
		mockSetup     func(ctrl *gomock.Controller) DynamoDBQueriesClientAPI
		expectedError error
	}{
		{
			name:      "Success",
			tableName: "test-table",
			query:     CreateNewQueryObj("1", nil),
			mockSetup: func(ctrl *gomock.Controller) DynamoDBQueriesClientAPI {
				m := NewMockDynamoDBQueriesClientAPI(ctrl)
				m.EXPECT().DeleteItem(gomock.Any(), gomock.Any(), gomock.Any()).Return(&dynamodb.DeleteItemOutput{}, nil).Times(1)
				return m
			},
			expectedError: nil,
		},
		{
			name:      "TableNotFound",
			tableName: "missing-table",
			query:     CreateNewQueryObj("1", nil),
			mockSetup: func(ctrl *gomock.Controller) DynamoDBQueriesClientAPI {
				return NewMockDynamoDBQueriesClientAPI(ctrl)
			},
			expectedError: NewTableNotFoundError("missing-table"),
		},
		{
			name:      "Error",
			tableName: "test-table",
			query:     CreateNewQueryObj("1", nil),
			mockSetup: func(ctrl *gomock.Controller) DynamoDBQueriesClientAPI {
				m := NewMockDynamoDBQueriesClientAPI(ctrl)
				m.EXPECT().DeleteItem(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("delete error")).Times(1)
				return m
			},
			expectedError: goaws.NewInternalError(errors.New("q.svc.DeleteItem: delete error")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockSvc := tt.mockSetup(ctrl)

			// Setup tables map
			tables := map[string]*Table{}
			if tt.tableName == "test-table" {
				tables["test-table"] = &Table{
					TableName:      "test-table",
					PrimaryKeyName: "id",
					PrimaryKeyType: "S",
				}
			}

			q := NewQueries(mockSvc, tables, nil)

			err := q.DeleteItem(context.Background(), tt.query, tt.tableName)

			if tt.expectedError != nil {
				require.Error(t, err)
				assert.EqualError(t, err, tt.expectedError.Error())
				assert.Implements(t, (*goaws.AwsError)(nil), err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestQueries_BatchWriteCreate(t *testing.T) {
	type TestItem struct {
		ID   string `json:"id"`
		Data string `json:"data"`
	}

	tests := []struct {
		name          string
		tableName     string
		items         []any
		mockSetup     func(ctrl *gomock.Controller) DynamoDBQueriesClientAPI
		expectedError error
	}{
		{
			name:      "Success",
			tableName: "test-table",
			items:     []any{TestItem{ID: "1", Data: "a"}},
			mockSetup: func(ctrl *gomock.Controller) DynamoDBQueriesClientAPI {
				m := NewMockDynamoDBQueriesClientAPI(ctrl)
				m.EXPECT().BatchWriteItem(gomock.Any(), gomock.Any(), gomock.Any()).Return(&dynamodb.BatchWriteItemOutput{}, nil).Times(1)
				return m
			},
			expectedError: nil,
		},
		{
			name:      "TooManyItems",
			tableName: "test-table",
			items:     make([]any, 26),
			mockSetup: func(ctrl *gomock.Controller) DynamoDBQueriesClientAPI {
				return NewMockDynamoDBQueriesClientAPI(ctrl)
			},
			expectedError: NewCollectionSizeExceededError(26),
		},
		{
			name:      "TableNotFound",
			tableName: "missing-table",
			items:     []any{TestItem{ID: "1", Data: "a"}},
			mockSetup: func(ctrl *gomock.Controller) DynamoDBQueriesClientAPI {
				return NewMockDynamoDBQueriesClientAPI(ctrl)
			},
			expectedError: NewTableNotFoundError("missing-table"),
		},
		{
			name:      "Error",
			tableName: "test-table",
			items:     []any{TestItem{ID: "1", Data: "a"}},
			mockSetup: func(ctrl *gomock.Controller) DynamoDBQueriesClientAPI {
				m := NewMockDynamoDBQueriesClientAPI(ctrl)
				m.EXPECT().BatchWriteItem(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("batch error")).Times(1)
				return m
			},
			expectedError: goaws.NewInternalError(errors.New("q.batchWriteUtil: q.svc.BatchWriteItem: batch error")),
		},
		{
			name:      "NilItems",
			tableName: "test-table",
			items:     nil,
			mockSetup: func(ctrl *gomock.Controller) DynamoDBQueriesClientAPI {
				return NewMockDynamoDBQueriesClientAPI(ctrl)
			},
			expectedError: NewNilModelError(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockSvc := tt.mockSetup(ctrl)

			// Setup tables map
			tables := map[string]*Table{}
			if tt.tableName == "test-table" {
				tables["test-table"] = &Table{
					TableName:      "test-table",
					PrimaryKeyName: "id",
					PrimaryKeyType: "S",
				}
			}

			q := NewQueries(mockSvc, tables, nil)

			err := q.BatchWriteCreate(context.Background(), tt.tableName, tt.items)

			if tt.expectedError != nil {
				require.Error(t, err)
				assert.EqualError(t, err, tt.expectedError.Error())

				var awsErr goaws.AwsError
				assert.Equal(t, true, errors.As(err, &awsErr))
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestQueries_ScanItems(t *testing.T) {
	tests := []struct {
		name          string
		params        QueryItemsParams
		mockSetup     func(ctrl *gomock.Controller) DynamoDBQueriesClientAPI
		expectedError error
	}{
		{
			name: "Success",
			params: QueryItemsParams{
				TableName:  "test-table",
				Expression: NewExpression(),
			},
			mockSetup: func(ctrl *gomock.Controller) DynamoDBQueriesClientAPI {
				m := NewMockDynamoDBQueriesClientAPI(ctrl)
				m.EXPECT().Scan(gomock.Any(), gomock.Any(), gomock.Any()).Return(&dynamodb.ScanOutput{
					Items: []map[string]types.AttributeValue{
						{
							"id": &types.AttributeValueMemberS{Value: "1"},
						},
					},
				}, nil).Times(1)
				return m
			},
			expectedError: nil,
		},
		{
			name: "TableNotFound",
			params: QueryItemsParams{
				TableName:  "missing-table",
				Expression: NewExpression(),
			},
			mockSetup: func(ctrl *gomock.Controller) DynamoDBQueriesClientAPI {
				return NewMockDynamoDBQueriesClientAPI(ctrl)
			},
			expectedError: NewTableNotFoundError("missing-table"),
		},
		{
			name: "Error",
			params: QueryItemsParams{
				TableName:  "test-table",
				Expression: NewExpression(),
			},
			mockSetup: func(ctrl *gomock.Controller) DynamoDBQueriesClientAPI {
				m := NewMockDynamoDBQueriesClientAPI(ctrl)
				m.EXPECT().Scan(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("scan error")).Times(1)
				return m
			},
			expectedError: goaws.NewInternalError(errors.New("q.svc.Scan: scan error")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockSvc := tt.mockSetup(ctrl)

			// Setup tables map
			tables := map[string]*Table{}
			if tt.params.TableName == "test-table" {
				tables["test-table"] = &Table{
					TableName:      "test-table",
					PrimaryKeyName: "id",
					PrimaryKeyType: "S",
				}
			}

			q := NewQueries(mockSvc, tables, nil)

			res, err := q.ScanItems(context.Background(), tt.params)

			if tt.expectedError != nil {
				require.Error(t, err)
				assert.EqualError(t, err, tt.expectedError.Error())
				assert.Implements(t, (*goaws.AwsError)(nil), err)
				assert.Nil(t, res)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, res)
				assert.Len(t, res.Rows, 1)
				assert.Equal(t, "1", res.Rows[0]["id"])
			}
		})
	}
}

func TestQueries_QueryItems(t *testing.T) {
	tests := []struct {
		name          string
		params        QueryItemsParams
		mockSetup     func(ctrl *gomock.Controller) DynamoDBQueriesClientAPI
		expectedError error
	}{
		{
			name: "Success",
			params: QueryItemsParams{
				TableName:  "test-table",
				Expression: NewExpression(),
			},
			mockSetup: func(ctrl *gomock.Controller) DynamoDBQueriesClientAPI {
				m := NewMockDynamoDBQueriesClientAPI(ctrl)
				m.EXPECT().Query(gomock.Any(), gomock.Any(), gomock.Any()).Return(&dynamodb.QueryOutput{
					Items: []map[string]types.AttributeValue{
						{
							"id": &types.AttributeValueMemberS{Value: "1"},
						},
					},
				}, nil).Times(1)
				return m
			},
			expectedError: nil,
		},
		{
			name: "TableNotFound",
			params: QueryItemsParams{
				TableName:  "missing-table",
				Expression: NewExpression(),
			},
			mockSetup: func(ctrl *gomock.Controller) DynamoDBQueriesClientAPI {
				return NewMockDynamoDBQueriesClientAPI(ctrl)
			},
			expectedError: NewTableNotFoundError("missing-table"),
		},
		{
			name: "Error",
			params: QueryItemsParams{
				TableName:  "test-table",
				Expression: NewExpression(),
			},
			mockSetup: func(ctrl *gomock.Controller) DynamoDBQueriesClientAPI {
				m := NewMockDynamoDBQueriesClientAPI(ctrl)
				m.EXPECT().Query(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("query error")).Times(1)
				return m
			},
			expectedError: goaws.NewInternalError(errors.New("q.svc.Query: query error")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockSvc := tt.mockSetup(ctrl)

			// Setup tables map
			tables := map[string]*Table{}
			if tt.params.TableName == "test-table" {
				tables["test-table"] = &Table{
					TableName:      "test-table",
					PrimaryKeyName: "id",
					PrimaryKeyType: "S",
				}
			}

			q := NewQueries(mockSvc, tables, nil)

			res, err := q.QueryItems(context.Background(), tt.params)

			if tt.expectedError != nil {
				require.Error(t, err)
				assert.EqualError(t, err, tt.expectedError.Error())
				assert.Implements(t, (*goaws.AwsError)(nil), err)
				assert.Nil(t, res)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, res)
				assert.Len(t, res.Rows, 1)
				assert.Equal(t, "1", res.Rows[0]["id"])
			}
		})
	}
}
