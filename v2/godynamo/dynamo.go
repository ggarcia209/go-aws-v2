// Package dynamo contains controls and objects for DynamoDB CRUD operations.
// Operations in this package are abstracted from all other application logic
// and are designed to be used with any DynamoDB table and any object schema.
// This file contains CRUD operations for working with DynamoDB.
package godynamo

/* TO DO:
- add expression logic to Create, Read, Delete operations
*/

import (
	"log"

	"github.com/ggarcia209/go-aws-v2/v2/goaws"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type DynamoDB struct {
	Tables       TablesLogic
	Queries      QueriesLogic
	Transactions TransactionsLogic
}

func NewDynamoDB(config goaws.AwsConfig, tables []*Table, failConfig *FailConfig) *DynamoDB {
	tm := make(map[string]*Table)
	for _, t := range tables {
		tm[t.TableName] = t
	}
	log.Printf("region: %s", config.Config.Region)
	svc := dynamodb.New(dynamodb.Options{
		Region:      config.Config.Region,
		Credentials: config.Config.Credentials,
	})
	return &DynamoDB{
		Queries:      NewQueries(svc, tm, failConfig),
		Tables:       NewTables(svc, tm),
		Transactions: NewTransactions(svc, failConfig),
	}
}
