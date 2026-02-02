package godynamo

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ggarcia209/go-aws-v2/v2/goaws"
)

// QueriesLogic defines common methods for querying dynamo db tables
//
//go:generate mockgen -destination=../mocks/godynamomock/queries.go -package=godynamomock . QueriesLogic
type QueriesLogic interface {
	CreateItem(ctx context.Context, item any, tableName string) error
	GetItem(ctx context.Context, query *Query, tableName string, item any, expr Expression) error
	UpdateItem(ctx context.Context, query *Query, tableName string, expr Expression) error
	DeleteItem(ctx context.Context, query *Query, tableName string) error
	BatchWriteCreate(ctx context.Context, tableName string, items []any) error
	BatchWriteDelete(ctx context.Context, tableName string, queries []*Query) error
	BatchGet(ctx context.Context, tableName string, queries []*Query, refObjs []any, expr Expression) ([]any, error)
	QueryItems(ctx context.Context, tableName string, model any, startKey any, expr Expression, perPage *int32) (*QueryResults, error)
	ScanItems(ctx context.Context, tableName string, model any, startKey any, expr Expression, perPage *int32) (*ScanResults, error)
}

// DynamoDBQueriesClientAPI defines the interface for the AWS DynamoDB client methods used by this package.
//
//go:generate mockgen -destination=./queries_client_api_test.go -package=godynamo . DynamoDBQueriesClientAPI
type DynamoDBQueriesClientAPI interface {
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
	BatchGetItem(ctx context.Context, params *dynamodb.BatchGetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error)
	Scan(ctx context.Context, params *dynamodb.ScanInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
	Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
}

type Queries struct {
	svc    DynamoDBQueriesClientAPI
	tables map[string]*Table
	fc     *FailConfig
}

func NewQueries(svc DynamoDBQueriesClientAPI, tables map[string]*Table, fc *FailConfig) *Queries {
	if fc == nil {
		fc = DefaultFailConfig
	}

	return &Queries{svc: svc, tables: tables, fc: fc}
}

// CreateItem puts a new item in the table.
func (q *Queries) CreateItem(ctx context.Context, item any, tableName string) error {
	// check if table exists
	t := q.tables[tableName]
	if t == nil {
		return NewTableNotFoundError(tableName)
	}

	av, err := attributevalue.MarshalMap(item)
	if err != nil {
		return goaws.NewInternalError(fmt.Errorf("attributevalue.MarshalMap: %w", err))
	}

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(tableName),
	}

	if _, err = q.svc.PutItem(ctx, input); err != nil {
		return goaws.NewInternalError(fmt.Errorf("q.svc.PutItem: %w", err))
	}

	return nil
}

// GetItem reads an item from the database and unmarshals it's attribute map into the provided itemPtr.
func (q *Queries) GetItem(ctx context.Context, query *Query, tableName string, itemPtr any, expr Expression) error {
	// get table
	t := q.tables[tableName]
	if t == nil {
		return NewTableNotFoundError(tableName)
	}

	key := keyMaker(query, t)
	input := &dynamodb.GetItemInput{
		TableName: aws.String(t.TableName),
		Key:       key,
	}
	if expr.Projection() != nil {
		input.ExpressionAttributeNames = expr.Names()
		input.ProjectionExpression = expr.Projection()
	}

	result, err := q.svc.GetItem(ctx, input)
	if err != nil {
		return handleErr(fmt.Errorf("q.svc.GetItem: %w", err))
	}

	if err = attributevalue.UnmarshalMap(result.Item, itemPtr); err != nil {
		return goaws.NewInternalError(fmt.Errorf("attributevalue.UnmarshalMap: %w", err))
	}

	return nil
}

// UpdateItem updates the specified item's attribute defined in the
// Query object with the UpdateValue defined in the Query.
func (q *Queries) UpdateItem(ctx context.Context, query *Query, tableName string, expr Expression) error {
	// get table
	t, ok := q.tables[tableName]
	if !ok {
		return NewTableNotFoundError(tableName)
	}

	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		TableName:                 aws.String(t.TableName),
		Key:                       keyMaker(query, t),
		ReturnValues:              "ALL_NEW",
		UpdateExpression:          expr.Update(),
	}
	if expr.Condition() != nil {
		input.ConditionExpression = expr.Condition()
	}
	if expr.Filter() != nil {
		input.ConditionExpression = expr.Filter()
	}
	if expr.KeyCondition() != nil {
		input.ConditionExpression = expr.KeyCondition()
	}
	if expr.Projection() != nil {
		input.ConditionExpression = expr.Projection()
	}

	if _, err := q.svc.UpdateItem(ctx, input); err != nil {
		return handleErr(fmt.Errorf("q.svc.UpdateItem: %w", err))
	}

	return nil
}

// DeleteItem deletes the specified item defined in the Query
func (q *Queries) DeleteItem(ctx context.Context, query *Query, tableName string) error {
	// get table
	t, ok := q.tables[tableName]
	if !ok {
		return NewTableNotFoundError(tableName)
	}

	input := &dynamodb.DeleteItemInput{
		Key:       keyMaker(query, t),
		TableName: aws.String(t.TableName),
	}

	if _, err := q.svc.DeleteItem(ctx, input); err != nil {
		return handleErr(fmt.Errorf("q.svc.DeleteItem: %w", err))
	}

	return nil
}

// BatchWriteCreate writes a list of items to the database.
func (q *Queries) BatchWriteCreate(ctx context.Context, tableName string, items []any) error {
	if len(items) > 25 {
		return NewCollectionSizeExceededError(len(items))
	}

	// get table
	t, ok := q.tables[tableName]
	if !ok {
		return NewTableNotFoundError(tableName)
	}

	// create map of RequestItems
	reqItems := make(map[string][]types.WriteRequest)
	wrs := make([]types.WriteRequest, 0)

	// create PutRequests for each item
	for _, item := range items {
		if item == nil {
			continue
		}

		// marshal each item
		av, err := attributevalue.MarshalMap(item)
		if err != nil {
			return goaws.NewInternalError(fmt.Errorf("attributevalue.MarshalMap: %w", err))
		}
		// create put request, reformat as write request, and add to list
		pr := &types.PutRequest{Item: av}
		wr := types.WriteRequest{PutRequest: pr}
		wrs = append(wrs, wr)
	}
	// populate reqItems map
	reqItems[t.TableName] = wrs

	// generate input from reqItems map
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: reqItems,
	}

	// batch write and error handling with exponential backoff retries for HTTP 5xx errors
	var result *dynamodb.BatchWriteItemOutput
	var err error
	retries := q.fc.NewRetries()
	for {
		result, err = q.batchWriteUtil(ctx, input)
		if err != nil {
			var throttled *RateLimitExceededError
			var awsErr goaws.AwsError
			switch {
			case errors.As(err, &throttled):
				input = &dynamodb.BatchWriteItemInput{
					RequestItems: result.UnprocessedItems,
				}
				if err := retries.ExponentialBackoff(); err != nil { // waits
					return fmt.Errorf("retries.ExponentialBackoff: %w", err)
				}
			case errors.As(err, &awsErr):
				if awsErr.Retryable() {
					input = &dynamodb.BatchWriteItemInput{
						RequestItems: result.UnprocessedItems,
					}
					if err := retries.ExponentialBackoff(); err != nil { // waits
						return fmt.Errorf("retries.ExponentialBackoff: %w", err)
					}
				} else {
					return fmt.Errorf("q.batchWriteUtil: %w", err)
				}
			default:
				return goaws.NewInternalError(fmt.Errorf("q.batchWriteUtil: %w", err))
			}
		}

		if len(result.UnprocessedItems) == 0 {
			break
		}

	}

	return nil
}

// BatchWriteDelete deletes a list of items from the database.
func (q *Queries) BatchWriteDelete(ctx context.Context, tableName string, queries []*Query) error {
	if len(queries) > 25 {
		return NewCollectionSizeExceededError(len(queries))
	}

	// get table
	t := q.tables[tableName]
	if t == nil {
		return NewTableNotFoundError(tableName)
	}

	// create map of RequestItems
	reqItems := make(map[string][]types.WriteRequest)
	wrs := make([]types.WriteRequest, 0)

	// create PutRequests for each item
	for _, q := range queries {
		if q == nil {
			continue
		}

		// create put request, reformat as write request, and add to list
		dr := &types.DeleteRequest{Key: keyMaker(q, t)}
		wr := types.WriteRequest{DeleteRequest: dr}
		wrs = append(wrs, wr)
	}
	// populate reqItems map
	reqItems[t.TableName] = wrs

	// generate input from reqItems map
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: reqItems,
	}

	// batch write and error handling with exponential backoff retries for HTTP 5xx errors
	var result *dynamodb.BatchWriteItemOutput
	var err error
	retries := q.fc.NewRetries()
	for {
		result, err = q.batchWriteUtil(ctx, input)
		if err != nil {
			var throttled *RateLimitExceededError
			var awsErr goaws.AwsError
			switch {
			case errors.As(err, &throttled):
				input = &dynamodb.BatchWriteItemInput{
					RequestItems: result.UnprocessedItems,
				}
				if err := retries.ExponentialBackoff(); err != nil { // waits
					return fmt.Errorf("retries.ExponentialBackoff: %w", err)
				}
			case errors.As(err, &awsErr):
				if awsErr.Retryable() {
					input = &dynamodb.BatchWriteItemInput{
						RequestItems: result.UnprocessedItems,
					}
					if err := retries.ExponentialBackoff(); err != nil { // waits
						return fmt.Errorf("retries.ExponentialBackoff: %w", err)
					}
				} else {
					return fmt.Errorf("q.batchWriteUtil: %w", err)
				}
			default:
				return goaws.NewInternalError(fmt.Errorf("q.batchWriteUtil: %w", err))
			}
		}

		if len(result.UnprocessedItems) == 0 {
			break
		}

	}

	return nil
}

// BatchGet retrieves a list of items from the database
// refObjs must be non-nil pointers of the same type,
// 1 for each query/object returneq.
//   - Returns err if len(queries) != len(refObjs).
func (q *Queries) BatchGet(ctx context.Context, tableName string, queries []*Query, refObjs []any, expr Expression) ([]any, error) {
	if len(queries) > 100 {
		return nil, NewCollectionSizeExceededError(len(queries))
	}

	if len(queries) != len(refObjs) {
		return nil, NewReferenceObjectsCountError()
	}

	// get table
	t := q.tables[tableName]
	if t == nil {
		return nil, NewTableNotFoundError(tableName)
	}

	items := make([]any, 0)

	// create map of RequestItems
	reqItems := make(map[string]types.KeysAndAttributes)
	keys := []map[string]types.AttributeValue{}

	// create Get requests for each query
	for _, q := range queries {
		if q == nil {
			continue
		}

		item := keyMaker(q, t)
		keys = append(keys, item)
	}
	// populate reqItems map
	reqItems[t.TableName] = types.KeysAndAttributes{Keys: keys}

	// generate input from reqItems map
	input := &dynamodb.BatchGetItemInput{
		RequestItems: reqItems,
	}

	// batch write and error handling with exponential backoff retries for HTTP 5xx errors
	var result *dynamodb.BatchGetItemOutput
	var err error
	retries := q.fc.NewRetries()
	for {
		result, err = q.batchGetUtil(ctx, input)
		if err != nil {
			var throttled *RateLimitExceededError
			var awsErr goaws.AwsError
			switch {
			case errors.As(err, &throttled):
				input = &dynamodb.BatchGetItemInput{
					RequestItems: result.UnprocessedKeys,
				}
				if err := retries.ExponentialBackoff(); err != nil { // waits
					return nil, fmt.Errorf("retries.ExponentialBackoff: %w", err)
				}
			case errors.As(err, &awsErr):
				if awsErr.Retryable() {
					input = &dynamodb.BatchGetItemInput{
						RequestItems: result.UnprocessedKeys,
					}
					if err := retries.ExponentialBackoff(); err != nil { // waits
						return nil, fmt.Errorf("retries.ExponentialBackoff: %w", err)
					}
				} else {
					return nil, fmt.Errorf("q.batchGetUtil: %w", err)
				}
			default:
				return nil, goaws.NewInternalError(fmt.Errorf("q.batchGetUtil: %w", err))
			}
		}

		for i, r := range result.Responses[t.TableName] {
			ref := refObjs[i]
			if err := attributevalue.UnmarshalMap(r, &ref); err != nil {
				return nil, goaws.NewInternalError(fmt.Errorf("attributevalue.UnmarshalMap, %w", err))
			}
			items = append(items, ref)
		}

		if len(result.UnprocessedKeys) == 0 {
			break
		}

	}

	return items, nil
}

// ScanItems scans the given Table for items matching the given expression parameters.
func (q *Queries) ScanItems(ctx context.Context, tableName string, model any, startKey any, expr Expression, perPage *int32) (*ScanResults, error) {
	// get table
	t := q.tables[tableName]
	if t == nil {
		return nil, NewTableNotFoundError(tableName)
	}

	items := make([]any, 0)

	// Build the query input parameters
	input := &dynamodb.ScanInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		ProjectionExpression:      expr.Projection(),
		TableName:                 aws.String(t.TableName),
		Limit:                     perPage,
	}

	if startKey != nil {
		av, err := attributevalue.MarshalMap(startKey)
		if err != nil {
			return nil, goaws.NewInternalError(fmt.Errorf("attributevalue.MarshalMap: %w", err))
		}
		input.ExclusiveStartKey = av
	}

	// Make the DynamoDB Query API call
	result, err := q.svc.Scan(ctx, input)
	if err != nil {
		return nil, handleErr(fmt.Errorf("q.svc.Scan: %w", err))
	}

	// get results
	for _, res := range result.Items {
		item := model
		if err = attributevalue.UnmarshalMap(res, &item); err != nil {
			return nil, goaws.NewInternalError(fmt.Errorf("attributevalue.UnmarshalMap: %w", err))
		}
		items = append(items, item)
	}

	for _, res := range result.Items {
		item := model
		err = attributevalue.UnmarshalMap(res, &item)
		if err != nil {
			return nil, goaws.NewInternalError(fmt.Errorf("attributevalue.UnmarshalMap: %w", err))
		}
	}

	scanResult := &ScanResults{
		Results: items,
		LastKey: result.LastEvaluatedKey,
	}

	if perPage != nil {
		scanResult.PerPage = *perPage
	}
	return scanResult, nil
}

// QueryItems queries the given Table for items matching the given expression parameters.
func (q *Queries) QueryItems(ctx context.Context, tableName string, model any, startKey any, expr Expression, perPage *int32) (*QueryResults, error) {
	// get table
	t := q.tables[tableName]
	if t == nil {
		return nil, NewTableNotFoundError(tableName)
	}

	items := make([]any, 0)

	// Build the query input parameters
	input := &dynamodb.QueryInput{
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		ProjectionExpression:      expr.Projection(),
		TableName:                 aws.String(t.TableName),
		Limit:                     perPage,
	}

	if startKey != nil {
		av, err := attributevalue.MarshalMap(startKey)
		if err != nil {
			return nil, goaws.NewInternalError(fmt.Errorf("attributevalue.MarshalMap: %w", err))
		}
		input.ExclusiveStartKey = av
	}

	// Make the DynamoDB Query API call
	result, err := q.svc.Query(ctx, input)
	if err != nil {
		return nil, handleErr(fmt.Errorf("q.svc.Scan: %w", err))
	}

	// get results
	for _, res := range result.Items {
		item := model
		if err = attributevalue.UnmarshalMap(res, &item); err != nil {
			return nil, goaws.NewInternalError(fmt.Errorf("attributevalue.UnmarshalMap: %w", err))
		}
		items = append(items, item)
	}

	for _, res := range result.Items {
		item := model
		err = attributevalue.UnmarshalMap(res, &item)
		if err != nil {
			return nil, goaws.NewInternalError(fmt.Errorf("attributevalue.UnmarshalMap: %w", err))
		}

		// get next page
		input.ExclusiveStartKey = result.LastEvaluatedKey
	}

	queryResult := &QueryResults{
		Results: items,
		LastKey: result.LastEvaluatedKey,
	}

	if perPage != nil {
		queryResult.PerPage = *perPage
	}

	return queryResult, nil
}

func (q *Queries) batchWriteUtil(ctx context.Context, input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
	result, err := q.svc.BatchWriteItem(ctx, input)
	if err != nil {
		return nil, handleErr(fmt.Errorf("q.svc.BatchWriteItem: %w", err))
	}
	return result, nil
}

func (q *Queries) batchGetUtil(ctx context.Context, input *dynamodb.BatchGetItemInput) (*dynamodb.BatchGetItemOutput, error) {
	result, err := q.svc.BatchGetItem(ctx, input)
	if err != nil {
		return nil, handleErr(fmt.Errorf("q.svc.BatchGetItem: %w", err))
	}
	return result, nil
}

func handleErr(err error) error {
	if err != nil {
		var (
			provisionedThroughputExceeded   *types.ProvisionedThroughputExceededException
			resourceNotFound                *types.ResourceNotFoundException
			itemCollectionSizeLimitExceeded *types.ItemCollectionSizeLimitExceededException
			requestLimitExceeded            *types.RequestLimitExceeded
			conditionalCheckFailed          *types.ConditionalCheckFailedException
		)
		switch {
		case errors.As(err, &provisionedThroughputExceeded):
			return NewRateLimitExceededError()
		case errors.As(err, &resourceNotFound):
			return NewResourceNotFoundError(resourceNotFound.ErrorMessage())
		case errors.As(err, &itemCollectionSizeLimitExceeded):
			return NewCollectionSizeExceededError(0)
		case errors.As(err, &requestLimitExceeded):
			return NewRateLimitExceededError()
		case errors.As(err, &conditionalCheckFailed):
			return NewConditionCheckFailedError(conditionalCheckFailed.ErrorMessage())
		default:
			return goaws.NewInternalError(err)
		}
	}
	return nil
}

// marshalMap marshals an interface object into an AttributeValue map
func marshalMap(input any) (map[string]types.AttributeValue, error) {
	marshal, err := attributevalue.MarshalMap(input)
	if err != nil {
		return nil, goaws.NewInternalError(fmt.Errorf("attributevalue.MarshalMap: %w", err))
	}
	return marshal, nil
}
