package godynamo

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// TablesLogic defines common methods interacting with dynamo db tables
//
//go:generate mockgen -destination=../mocks/godynamomock/tables.go -package=godynamomock . TablesLogic
type TablesLogic interface {
	ListTables(ctx context.Context, params ListTableParams) ([]string, int, error)
	CreateTable(ctx context.Context, table *Table) error
	DeleteTable(ctx context.Context, tableName string) error
}

// DynamoDBTablesClientAPI defines the interface for the AWS DynamoDB client methods used by this package.
//
//go:generate mockgen -destination=./tables_client_api_test.go -package=godynamo . DynamoDBTablesClientAPI
type DynamoDBTablesClientAPI interface {
	ListTables(ctx context.Context, params *dynamodb.ListTablesInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ListTablesOutput, error)
	CreateTable(ctx context.Context, params *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
	DeleteTable(ctx context.Context, params *dynamodb.DeleteTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteTableOutput, error)
}

type Tables struct {
	svc    DynamoDBTablesClientAPI
	tables map[string]*Table
}

func NewTables(svc DynamoDBTablesClientAPI, tables map[string]*Table) *Tables {
	return &Tables{svc: svc, tables: make(map[string]*Table)}
}

// ListTables lists the tables in the database.
func (t *Tables) ListTables(ctx context.Context, params ListTableParams) ([]string, int, error) {
	names := []string{}
	i := 0
	input := &dynamodb.ListTablesInput{
		ExclusiveStartTableName: params.StartTable,
		Limit:                   params.Limit,
	}

	for {
		// Get the list of tables
		result, err := t.svc.ListTables(ctx, input)
		if err != nil {
			return nil, 0, handleErr(fmt.Errorf("t.svc.ListTables: %w", err))
		}

		for _, n := range result.TableNames {
			names = append(names, n)
			i++
		}

		// assign the last read tablename as the start for our next call to the ListTables function
		// the maximum number of table names returned in a call is 100 (default), which requires us to make
		// multiple calls to the ListTables function to retrieve all table names
		input.ExclusiveStartTableName = result.LastEvaluatedTableName

		if result.LastEvaluatedTableName == nil {
			break
		}
	}
	return names, i, nil
}

// CreateTable creates a new table with the parameters passed to the Table struct.
// NOTE: CreateTable creates Table in * On-Demand * billing mode.
func (t *Tables) CreateTable(ctx context.Context, table *Table) error {
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{ // Primary Key
				AttributeName: aws.String(table.PrimaryKeyName),
				AttributeType: types.ScalarAttributeType(table.PrimaryKeyType),
			},
			{
				AttributeName: aws.String(table.SortKeyName),
				AttributeType: types.ScalarAttributeType(table.SortKeyType),
			},
		},
		BillingMode: types.BillingModePayPerRequest,
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(table.PrimaryKeyName),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String(table.SortKeyName),
				KeyType:       types.KeyTypeRange,
			},
		},
		TableName: aws.String(table.TableName),
	}

	if _, err := t.svc.CreateTable(ctx, input); err != nil {
		return handleErr(fmt.Errorf("t.svc.CreateTable: %w", err))
	}

	t.tables[table.TableName] = table

	return nil
}

// DeleteTable deletes the selected table.
func (t *Tables) DeleteTable(ctx context.Context, tableName string) error {
	// get table
	table, ok := t.tables[tableName]
	if !ok {
		return NewTableNotFoundError(tableName)
	}

	input := &dynamodb.DeleteTableInput{
		TableName: aws.String(table.TableName),
	}
	if _, err := t.svc.DeleteTable(ctx, input); err != nil {
		return handleErr(fmt.Errorf("t.svc.DeleteTable: %w", err))
	}

	delete(t.tables, tableName)

	return nil
}
