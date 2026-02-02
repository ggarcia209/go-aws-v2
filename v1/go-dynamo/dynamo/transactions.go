package dynamo

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// TransactionItem contains an item to create / update
// in a transaction operation.
type TransactionItem struct {
	Name    string // arbitrary name to reference transaction item
	request string // C,R,U,D, CC (condition check)
	Item    interface{}
	Table   *Table
	Query   *Query
	Expr    Expression
}

func (t *TransactionItem) GetRequest() string {
	return t.request
}

// NewCreateTxItem initializes a new TransactionItem object for create requests.
func NewCreateTxItem(name string, item interface{}, t *Table, q *Query, e Expression) TransactionItem {
	tx := TransactionItem{
		Name:    name,
		request: "C",
		Item:    item,
		Table:   t,
		Query:   q,
		Expr:    e,
	}
	return tx
}

// NewUpdateTxItem initializes a new TransactionItem object for update requests.
func NewUpdateTxItem(name string, t *Table, q *Query, e Expression) TransactionItem {
	tx := TransactionItem{
		Name:    name,
		request: "U",
		Table:   t,
		Query:   q,
		Expr:    e,
	}
	return tx
}

// NewReadTxItem initializes a new TransactionItem object for read requests.
func NewReadTxItem(name string, t *Table, q *Query, e Expression) TransactionItem {
	tx := TransactionItem{
		Name:    name,
		request: "R",
		Table:   t,
		Query:   q,
		Expr:    e,
	}
	return tx
}

// NeDeletewTxItem initializes a new TransactionItem object for delete requests.
func NewDeleteTxItem(name string, t *Table, q *Query, e Expression) TransactionItem {
	tx := TransactionItem{
		Name:    name,
		request: "D",
		Table:   t,
		Query:   q,
		Expr:    e,
	}
	return tx
}

// NewConditionalCheckTxItem initializes a new TransactionItem object for conditional check requests.
func NewConditionCheckTxItem(name string, t *Table, q *Query, e Expression) TransactionItem {
	tx := TransactionItem{
		Name:    name,
		request: "CC",
		Table:   t,
		Query:   q,
		Expr:    e,
	}
	return tx
}

// TxConditionCheck checks that each conditional check for a list of transaction items passes. Failed condition checks
// return an error value, and a list of the TransactionItems that failed their condition checks. Successful condition
// checks return an empty list of TransactionItems and nil error value.
func (d *DynamoDB) TxWrite(items []TransactionItem, requestToken string) ([]TransactionItem, error) {
	// verify <= 25 tx items
	if len(items) > 25 {
		return []TransactionItem{}, fmt.Errorf("TX_ITEMS_EXCEEDS_LIMIT")
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
			return []TransactionItem{}, fmt.Errorf("newTxWriteItem: %w", err)
		}
		txInput.TransactItems = append(txInput.TransactItems, txItem)
	}

	failed := []TransactionItem{}

	if _, err := d.svc.TransactWriteItems(txInput); err != nil {
		switch t := err.(type) {
		case *dynamodb.TransactionCanceledException:
			check := false     // denotes conditional checks failed
			throttled := false // denotes if tx failed due to throttling

			for i, r := range t.CancellationReasons {
				if *r.Code == "ConditionalCheckFailed" {
					check = true
					failed = append(failed, items[i])
				}
				if *r.Code == "ThrottlingError" {
					throttled = true
					failed = append(failed, items[i])
				}
			}

			if check {
				// no retry
				return failed, ErrTxConditionCheckFailed
			}
			if throttled {
				// retry
				return failed, ErrTxThrottled
			}
			// no retry
			return failed, fmt.Errorf("d.svc.TransactWriteItems: %w", err)
		case *dynamodb.TransactionConflictException:
			// retry
			return failed, ErrTxConflict
		case *dynamodb.TransactionInProgressException:
			// no retry
			return failed, ErrTxInProgress
		default:
			return failed, err
		}
	}

	return failed, nil
}

func newTxWriteItem(ti TransactionItem) (*dynamodb.TransactWriteItem, error) {
	req := ti.GetRequest()

	switch req {
	case "C":
		m, err := marshalMap(ti.Item)
		if err != nil {
			return nil, fmt.Errorf("marshalMap: %w", err)
		}
		txItem := &dynamodb.TransactWriteItem{
			Put: &dynamodb.Put{
				Item:                      m,
				ConditionExpression:       ti.Expr.Condition(),
				ExpressionAttributeNames:  ti.Expr.Names(),
				ExpressionAttributeValues: ti.Expr.Values(),
				TableName:                 aws.String(ti.Table.TableName),
			},
		}
		return txItem, nil
	case "U":
		txItem := &dynamodb.TransactWriteItem{
			Update: &dynamodb.Update{
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
		txItem := &dynamodb.TransactWriteItem{
			Delete: &dynamodb.Delete{
				ConditionExpression:       ti.Expr.Condition(),
				ExpressionAttributeNames:  ti.Expr.Names(),
				ExpressionAttributeValues: ti.Expr.Values(),
				TableName:                 aws.String(ti.Table.TableName),
				Key:                       keyMaker(ti.Query, ti.Table),
			},
		}
		return txItem, nil
	case "CC":
		txItem := &dynamodb.TransactWriteItem{
			ConditionCheck: &dynamodb.ConditionCheck{
				ConditionExpression:       ti.Expr.Condition(),
				ExpressionAttributeNames:  ti.Expr.Names(),
				ExpressionAttributeValues: ti.Expr.Values(),
				TableName:                 aws.String(ti.Table.TableName),
				Key:                       keyMaker(ti.Query, ti.Table),
			},
		}
		return txItem, nil
	default:
		return nil, ErrInvalidRequestType
	}

}
