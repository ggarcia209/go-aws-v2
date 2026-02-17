package godynamo

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ggarcia209/go-aws-v2/v2/goaws"
)

// DynamoDBTransactionsClientAPI defines the interface for the AWS DynamoDB client methods used by this package.
//
//go:generate mockgen -destination=./transactions_client_api_test.go -package=godynamo . DynamoDBTransactionsClientAPI
type DynamoDBTransactionsClientAPI interface {
	TransactWriteItems(ctx context.Context, params *dynamodb.TransactWriteItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
}

//go:generate mockgen -destination=../mocks/godynamomock/transactions.go -package=godynamomock . TransactionsLogic
type TransactionsLogic interface {
	TxWrite(ctx context.Context, items []TransactionItem, requestToken string) ([]TransactionItem, error)
}

type Transactions struct {
	svc DynamoDBTransactionsClientAPI
	fc  *FailConfig
}

func NewTransactions(svc DynamoDBTransactionsClientAPI, fc *FailConfig) *Transactions {
	if fc == nil {
		fc = DefaultFailConfig
	}
	return &Transactions{
		svc: svc,
		fc:  fc,
	}
}

// TxConditionCheck checks that each conditional check for a list of transaction items passes. Failed condition checks
// return an error value, and a list of the TransactionItems that failed their condition checks. Successful condition
// checks return an empty list of TransactionItems and nil error value.
func (t *Transactions) TxWrite(ctx context.Context, items []TransactionItem, requestToken string) ([]TransactionItem, error) {
	// verify <= 25 tx items
	if len(items) > 25 {
		return nil, NewTxItemsExceedsLimitError()
	}

	txInput := &dynamodb.TransactWriteItemsInput{}
	// set client request token / idempotency key if provided
	if requestToken != "" {
		txInput.ClientRequestToken = aws.String(requestToken)
	}

	// create tx write items for input
	for _, ti := range items {
		txItem, err := newTxWriteItem(ti)
		if err != nil {
			return nil, fmt.Errorf("newTxWriteItem: %w", err)
		}
		txInput.TransactItems = append(txInput.TransactItems, *txItem)
	}

	failed := make([]TransactionItem, 0)

	if _, err := t.svc.TransactWriteItems(ctx, txInput); err != nil {
		var txCanceled *types.TransactionCanceledException
		var txConflict *types.TransactionConflictException
		var txInProgress *types.TransactionInProgressException
		var re *awshttp.ResponseError
		switch {
		case errors.As(err, &txCanceled):
			check := false     // denotes conditional checks failed
			throttled := false // denotes if tx failed due to throttling
			msg := ""

			for i, r := range txCanceled.CancellationReasons {
				if *r.Code == string(types.BatchStatementErrorCodeEnumConditionalCheckFailed) {
					check = true
					if r.Message != nil {
						msg = *r.Message
					}
					failed = append(failed, items[i])
				}
				if *r.Code == string(types.BatchStatementErrorCodeEnumThrottlingError) {
					throttled = true
					if r.Message != nil {
						msg = *r.Message
					} else {
						msg = "transaction request throttled"
					}
					failed = append(failed, items[i])
				}
			}

			if check {
				// no retry
				return failed, NewTxConditonCheckFailedError(msg)
			}
			if throttled {
				// retry
				return failed, NewTxThrottledError()
			}
			// no retry
			return failed, goaws.NewInternalError(fmt.Errorf("d.svc.TransactWriteItems: %w", err))
		case errors.As(err, &txConflict):
			// retry
			return failed, NewTxConflictError()
		case errors.As(err, &txInProgress):
			// no retry
			return failed, NewTxInProgressError()
		case errors.As(err, &re):
			if re.ResponseError == nil {
				return nil, goaws.NewInternalError(fmt.Errorf("s.svc.DeleteMessage: %w", err))
			}
			switch re.HTTPStatusCode() {
			case http.StatusBadRequest:
				return nil, NewBadTxRequestError()
			case http.StatusNotFound:
				return nil, NewResourceNotFoundError(re.Error())
			default:
				return nil, goaws.NewInternalError(fmt.Errorf("s.svc.DeleteMessage: %w", err))
			}
		default:
			return nil, goaws.NewInternalError(fmt.Errorf("s.svc.DeleteMessage: %w", err))
		}
	}

	return failed, nil
}

func newTxWriteItem(ti TransactionItem) (*types.TransactWriteItem, error) {
	req := ti.GetRequest()

	switch req {
	case "C":
		m, err := marshalMap(ti.Item)
		if err != nil {
			return nil, goaws.NewInternalError(fmt.Errorf("marshalMap: %w", err))
		}
		txItem := &types.TransactWriteItem{
			Put: &types.Put{
				Item:                      m,
				ConditionExpression:       ti.Expr.Condition(),
				ExpressionAttributeNames:  ti.Expr.Names(),
				ExpressionAttributeValues: ti.Expr.Values(),
				TableName:                 aws.String(ti.Table.TableName),
			},
		}
		return txItem, nil
	case "U":
		txItem := &types.TransactWriteItem{
			Update: &types.Update{
				ConditionExpression:       ti.Expr.Condition(),
				ExpressionAttributeNames:  ti.Expr.Names(),
				ExpressionAttributeValues: ti.Expr.Values(),
				TableName:                 aws.String(ti.Table.TableName),
				Key:                       keyMaker(ti.Query, ti.Table),
				UpdateExpression:          ti.Expr.Update(),
			},
		}
		return txItem, nil
	case "D":
		txItem := &types.TransactWriteItem{
			Delete: &types.Delete{
				ConditionExpression:       ti.Expr.Condition(),
				ExpressionAttributeNames:  ti.Expr.Names(),
				ExpressionAttributeValues: ti.Expr.Values(),
				TableName:                 aws.String(ti.Table.TableName),
				Key:                       keyMaker(ti.Query, ti.Table),
			},
		}
		return txItem, nil
	case "CC":
		txItem := &types.TransactWriteItem{
			ConditionCheck: &types.ConditionCheck{
				ConditionExpression:       ti.Expr.Condition(),
				ExpressionAttributeNames:  ti.Expr.Names(),
				ExpressionAttributeValues: ti.Expr.Values(),
				TableName:                 aws.String(ti.Table.TableName),
				Key:                       keyMaker(ti.Query, ti.Table),
			},
		}
		return txItem, nil
	default:
		return nil, NewInvalidRequestTypeError()
	}

}
