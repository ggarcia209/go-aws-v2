package godynamo

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/ggarcia209/go-aws-v2/v2/goaws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gomock "go.uber.org/mock/gomock"
)

func TestNewTables(t *testing.T) {
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
	tables := NewTables(svc, testTables)
	assert.NotNil(t, tables)
	assert.NotNil(t, tables.svc)
	assert.Implements(t, (*TablesLogic)(nil), tables)
}

func TestTables_ListTables(t *testing.T) {
	tests := []struct {
		name          string
		params        ListTableParams
		mockSetup     func(ctrl *gomock.Controller) DynamoDBTablesClientAPI
		expectedNames []string
		expectedCount int
		expectedError error
	}{
		{
			name:   "Success",
			params: ListTableParams{},
			mockSetup: func(ctrl *gomock.Controller) DynamoDBTablesClientAPI {
				m := NewMockDynamoDBTablesClientAPI(ctrl)
				m.EXPECT().ListTables(gomock.Any(), gomock.Any(), gomock.Any()).Return(&dynamodb.ListTablesOutput{
					TableNames: []string{"table1", "table2"},
				}, nil).Times(1)
				return m
			},
			expectedNames: []string{"table1", "table2"},
			expectedCount: 2,
			expectedError: nil,
		},
		{
			name:   "Pagination",
			params: ListTableParams{},
			mockSetup: func(ctrl *gomock.Controller) DynamoDBTablesClientAPI {
				m := NewMockDynamoDBTablesClientAPI(ctrl)
				// First call returns table1 and a LastEvaluatedTableName
				m.EXPECT().ListTables(gomock.Any(), gomock.Any(), gomock.Any()).Return(&dynamodb.ListTablesOutput{
					TableNames:             []string{"table1"},
					LastEvaluatedTableName: aws.String("table1"),
				}, nil).Times(1)
				// Second call starts from table1 and returns table2
				m.EXPECT().ListTables(gomock.Any(), gomock.Any(), gomock.Any()).Return(&dynamodb.ListTablesOutput{
					TableNames: []string{"table2"},
				}, nil).Times(1)
				return m
			},
			expectedNames: []string{"table1", "table2"},
			expectedCount: 2,
			expectedError: nil,
		},
		{
			name:   "Error",
			params: ListTableParams{},
			mockSetup: func(ctrl *gomock.Controller) DynamoDBTablesClientAPI {
				m := NewMockDynamoDBTablesClientAPI(ctrl)
				m.EXPECT().ListTables(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("list error")).Times(1)
				return m
			},
			expectedNames: nil,
			expectedCount: 0,
			expectedError: errors.New("t.svc.ListTables: list error"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockSvc := tt.mockSetup(ctrl)
			s := &Tables{svc: mockSvc, tables: make(map[string]*Table)}

			names, count, err := s.ListTables(context.Background(), tt.params)

			if tt.expectedError != nil {
				require.Error(t, err)
				assert.EqualError(t, err, tt.expectedError.Error())
				assert.Implements(t, (*goaws.AwsError)(nil), err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedNames, names)
				assert.Equal(t, tt.expectedCount, count)
			}
		})
	}
}

func TestTables_CreateTable(t *testing.T) {
	tests := []struct {
		name          string
		table         *Table
		mockSetup     func(ctrl *gomock.Controller) DynamoDBTablesClientAPI
		expectedError error
	}{
		{
			name: "Success",
			table: &Table{
				TableName:      "test-table",
				PrimaryKeyName: "id",
				PrimaryKeyType: "S",
				SortKeyName:    "sort",
				SortKeyType:    "N",
			},
			mockSetup: func(ctrl *gomock.Controller) DynamoDBTablesClientAPI {
				m := NewMockDynamoDBTablesClientAPI(ctrl)
				m.EXPECT().CreateTable(gomock.Any(), gomock.Any(), gomock.Any()).Return(&dynamodb.CreateTableOutput{}, nil).Times(1)
				return m
			},
			expectedError: nil,
		},
		{
			name: "Error",
			table: &Table{
				TableName:      "test-table",
				PrimaryKeyName: "id",
				PrimaryKeyType: "S",
			},
			mockSetup: func(ctrl *gomock.Controller) DynamoDBTablesClientAPI {
				m := NewMockDynamoDBTablesClientAPI(ctrl)
				m.EXPECT().CreateTable(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("create error")).Times(1)
				return m
			},
			expectedError: errors.New("t.svc.CreateTable: create error"), // Note: CreateTable wraps error in handleErr too? No, it wraps in fmt.Errorf("t.svc.CreateTable: %w", handleErr(err))
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockSvc := tt.mockSetup(ctrl)
			s := &Tables{svc: mockSvc, tables: make(map[string]*Table)}

			err := s.CreateTable(context.Background(), tt.table)

			if tt.expectedError != nil {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError.Error())
			} else {
				require.NoError(t, err)
				_, ok := s.tables[tt.table.TableName]
				assert.True(t, ok)
			}
		})
	}
}

func TestTables_DeleteTable(t *testing.T) {
	tests := []struct {
		name          string
		tableName     string
		existingTable *Table
		mockSetup     func(ctrl *gomock.Controller) DynamoDBTablesClientAPI
		expectedError error
	}{
		{
			name:      "Success",
			tableName: "test-table",
			existingTable: &Table{
				TableName: "test-table",
			},
			mockSetup: func(ctrl *gomock.Controller) DynamoDBTablesClientAPI {
				m := NewMockDynamoDBTablesClientAPI(ctrl)
				m.EXPECT().DeleteTable(gomock.Any(), gomock.Any(), gomock.Any()).Return(&dynamodb.DeleteTableOutput{}, nil).Times(1)
				return m
			},
			expectedError: nil,
		},
		{
			name:          "TableNotFound",
			tableName:     "missing-table",
			existingTable: nil, // Table not in local map
			mockSetup: func(ctrl *gomock.Controller) DynamoDBTablesClientAPI {
				// DeleteTable shouldn't be called if table is not in local map
				return NewMockDynamoDBTablesClientAPI(ctrl)
			},
			expectedError: NewTableNotFoundError("missing-table"),
		},
		{
			name:      "Error",
			tableName: "test-table",
			existingTable: &Table{
				TableName: "test-table",
			},
			mockSetup: func(ctrl *gomock.Controller) DynamoDBTablesClientAPI {
				m := NewMockDynamoDBTablesClientAPI(ctrl)
				m.EXPECT().DeleteTable(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("delete error")).Times(1)
				return m
			},
			expectedError: errors.New("t.svc.DeleteTable: delete error"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockSvc := tt.mockSetup(ctrl)
			// setup local table map
			tables := make(map[string]*Table)
			if tt.existingTable != nil {
				tables[tt.tableName] = tt.existingTable
			}
			s := &Tables{svc: mockSvc, tables: tables}

			err := s.DeleteTable(context.Background(), tt.tableName)

			if tt.expectedError != nil {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError.Error())
			} else {
				require.NoError(t, err)
				_, ok := s.tables[tt.tableName]
				assert.False(t, ok)
			}
		})
	}
}
