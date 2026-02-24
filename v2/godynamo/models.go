package godynamo

import (
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

/* Tables */

// Table represents a table and holds basic information about it.
// This object is used to access the Dynamo Table requested for each CRUD op.
type Table struct {
	TableName      string
	PrimaryKeyName string
	PrimaryKeyType string
	SortKeyName    string
	SortKeyType    string
}

type ListTableParams struct {
	StartTable *string `json:"start_table"`
	Limit      *int32  `json:"limit"`
}

type GetItemParams struct {
	Query           *Query     `json:"query"`
	TableName       string     `json:"table_name"`
	ItemPtr         any        `json:"item_ptr"`
	Expression      Expression `json:"expression"`
	ConsistentReads bool       `json:"consistent_reads"`
}

type QueryItemsParams struct {
	TableName  string     `json:"table_name"`
	StartKey   any        `json:"start_key"`
	Expression Expression `json:"expression"`
	PerPage    *int32     `json:"per_page"`
}

// CreateNewTableObj creates a new Table struct.
// The Table's key's Go types must be declared as strings.
// ex: t := CreateNewTableObj("my_table", "Year", "int", "MovieName", "string")
func CreateNewTableObj(tableName, pKeyName, pType, sKeyName, sType string) *Table {
	typeMap := map[string]string{
		"[]byte":   "B",
		"[][]byte": "BS",
		"bool":     "BOOL",
		"list":     "L",
		"map":      "M",
		"int":      "N",
		"[]int":    "NS",
		"null":     "NULL",
		"string":   "S",
		"[]string": "SS",
	}

	pt := typeMap[pType]
	st := typeMap[sType]

	return &Table{tableName, pKeyName, pt, sKeyName, st}
}

/* Queries */

// Query holds the search values for both the Partition and Sort Keys.
// Query also holds data for updating a specific item in the UpdateFieldName column.
type Query struct {
	PrimaryValue    any
	SortValue       any
	UpdateFieldName string
	UpdateExprKey   string
	UpdateValue     any
}

type QueryResults struct {
	Rows    []QueryRow                      `json:"results"`
	PerPage int32                           `json:"per_page,omitempty"`
	LastKey map[string]types.AttributeValue `json:"last_key,omitempty"`
}

type QueryRow = map[string]any

type ScanResults struct {
	Rows    []QueryRow                      `json:"results"`
	PerPage int32                           `json:"per_page,omitempty"`
	LastKey map[string]types.AttributeValue `json:"last_key,omitempty"`
}

// New creates a new query by setting the Partition Key and Sort Key values.
func (q *Query) New(pv, sv any) { q.PrimaryValue, q.SortValue = pv, sv }

// UpdateCurrent sets the update fields for the current item.
func (q *Query) UpdateCurrent(fieldName string, value any) {
	q.UpdateFieldName, q.UpdateValue = fieldName, value
}

// UpdateNew selects a new item for an update.
func (q *Query) UpdateNew(pv, sv, fieldName string, value any) {
	q.PrimaryValue, q.SortValue, q.UpdateFieldName, q.UpdateValue = pv, sv, fieldName, value
}

// Reset clears all fields.
func (q *Query) Reset() {
	q.PrimaryValue, q.SortValue, q.UpdateValue, q.UpdateExprKey, q.UpdateFieldName = nil, nil, nil, "", ""
}

// CreateNewQueryObj creates a new Query struct.
// pval, sval == Primary/Partition key, Sort Key
func CreateNewQueryObj(pval, sval interface{}) *Query {
	return &Query{PrimaryValue: pval, SortValue: sval}
}

func createAV(val any) types.AttributeValue {

	if val == nil { // setNull
		av := &types.AttributeValueMemberNULL{Value: true}
		return av
	}
	if _, ok := val.([]byte); ok {
		av := &types.AttributeValueMemberB{Value: val.([]byte)}
		return av
	}
	if _, ok := val.(bool); ok {
		av := &types.AttributeValueMemberBOOL{Value: val.(bool)}
		return av
	}
	if _, ok := val.([][]byte); ok {
		av := &types.AttributeValueMemberBS{Value: val.([][]byte)}
		return av
	}
	if _, ok := val.([]types.AttributeValue); ok {
		av := &types.AttributeValueMemberL{Value: val.([]types.AttributeValue)}
		return av
	}
	if _, ok := val.(map[string]types.AttributeValue); ok {
		av := &types.AttributeValueMemberM{Value: val.(map[string]types.AttributeValue)}
		return av
	}

	if _, ok := val.(int); ok {
		av := &types.AttributeValueMemberN{Value: strconv.Itoa(val.(int))}
		return av
	}
	if _, ok := val.([]int); ok {

		ns := func(is []int) []string {
			list := []string{}
			for _, n := range is {
				str := strconv.Itoa(n)
				list = append(list, str)
			}
			return list
		}(val.([]int))

		av := &types.AttributeValueMemberNS{Value: ns}

		return av
	}
	if _, ok := val.(string); ok {
		av := &types.AttributeValueMemberS{Value: val.(string)}
		return av
	}
	if _, ok := val.(string); ok {
		av := &types.AttributeValueMemberS{Value: val.(string)}
		return av
	}
	return nil
}

// keyMaker creates a map of Partition and Sort Keys.
func keyMaker(q *Query, t *Table) map[string]types.AttributeValue {
	keys := make(map[string]types.AttributeValue)
	keys[t.PrimaryKeyName] = createAV(q.PrimaryValue)
	if t.SortKeyName == "" {
		return keys
	}
	keys[t.SortKeyName] = createAV(q.SortValue)
	return keys
}

/* Transactions */

// TransactionItem contains an item to create / update
// in a transaction operation.
type TransactionItem struct {
	Name    string // arbitrary name to reference transaction item
	request string // C,R,U,D, CC (condition check)
	Item    any
	Table   *Table
	Query   *Query
	Expr    Expression
}

func (t *TransactionItem) GetRequest() string {
	return t.request
}

// NewCreateTxItem initializes a new TransactionItem object for create requests.
func NewCreateTxItem(name string, item any, t *Table, q *Query, e Expression) TransactionItem {
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
