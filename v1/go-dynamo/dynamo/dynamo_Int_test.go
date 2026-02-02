//go:build integration

package dynamo

import (
	"fmt"
	"testing"

	"github.com/ggarcia209/go-aws-v2/v1/goaws"
)

const TableName = "go-dynamo-test"
const buildTreeErrProjection = "buildTree error: unset parameter: ProjectionBuilder"

var table = &Table{
	TableName:      TableName,
	PrimaryKeyName: "partition",
	PrimaryKeyType: "string",
	SortKeyName:    "uuid",
	SortKeyType:    "string",
}

type record struct {
	Partition string          `json:"partition"`
	UUID      string          `json:"uuid"`
	Count     int             `json:"count"`
	CountMap  map[string]int  `json:"count-map"`
	Price     float32         `json:"price"`
	Set       map[string]bool `json:"set"`
}

func TestCreateItem(t *testing.T) {
	svc := NewDynamoDB(goaws.NewDefaultSession(), []*Table{table}, nil)

	var tests = []struct {
		input record
		want  error
	}{
		{
			record{
				Partition: "A",
				UUID:      "001",
				Count:     3,
				Price:     19.95,
				Set:       map[string]bool{"A": true, "B": false, "C": true},
			}, nil,
		},
		{
			record{
				Partition: "A",
				UUID:      "002",
				Count:     5,
				Price:     9.95,
				Set:       map[string]bool{"A": false, "B": false, "C": true},
			}, nil,
		},
		{
			record{
				Partition: "B",
				UUID:      "003",
				Count:     10,
				Price:     10.00,
				Set:       map[string]bool{"A": false, "B": true, "C": true},
			}, nil,
		},
		{
			record{
				Partition: "C",
				UUID:      "004",
				Count:     0,
				Price:     0.00,
				Set:       map[string]bool{"A": false, "B": false, "C": false},
			}, nil,
		},
	}
	for _, test := range tests {
		err := svc.CreateItem(test.input, TableName)
		if err != test.want {
			t.Errorf("FAIL: %v", err)
		}
	}
}

func TestGetItem(t *testing.T) {
	svc := NewDynamoDB(goaws.NewDefaultSession(), []*Table{table}, nil)
	var tests = []struct {
		pk       string
		sk       string
		model    *record
		wantUuid string
		wantErr  error
	}{
		{pk: "A", sk: "001", model: &record{}, wantUuid: "001", wantErr: nil},
		{pk: "A", sk: "002", model: &record{}, wantUuid: "002", wantErr: nil},
		{pk: "B", sk: "003", model: &record{}, wantUuid: "003", wantErr: nil},
		{pk: "C", sk: "004", model: &record{}, wantUuid: "004", wantErr: nil},
	}
	expr := NewExpression()

	for _, test := range tests {
		q := CreateNewQueryObj(test.pk, test.sk)
		item, err := svc.GetItem(q, TableName, test.model, expr)
		if err != test.wantErr {
			t.Errorf("FAIL: %v; want: %v", err, test.wantErr)
		}
		if item.(*record).UUID != test.wantUuid {
			t.Errorf("FAIL - DATA: %v; want: %v", item.(*record).UUID, test.wantUuid)
		}
	}
}

func TestGetItemWithProjection(t *testing.T) {
	var tests = []struct {
		pk         string
		sk         string
		attributes []string
		size       string
		wantErr    error
	}{
		{pk: "A", sk: "001", attributes: []string{"count"}, size: "", wantErr: nil},                         // top-level value
		{pk: "B", sk: "003", attributes: []string{"count-map"}, size: "XL", wantErr: nil},                   // nested value - original count: 6
		{pk: "A", sk: "001", attributes: []string{"count", "price"}, size: "", wantErr: nil},                // multiple attributes
		{pk: "A", sk: "001", attributes: []string{}, size: "", wantErr: fmt.Errorf(buildTreeErrProjection)}, // no attributes specified
		{pk: "A", sk: "001", attributes: []string{"quantity"}, size: "", wantErr: nil},                      // non existent attribute
	}

	svc := NewDynamoDB(goaws.NewDefaultSession(), []*Table{table}, nil)

	for i, test := range tests {
		names := []string{}
		for _, name := range test.attributes {
			if name == "count-map" {
				name = fmt.Sprintf("%s.%s", name, test.size)
			}
			names = append(names, name)
		}

		q := CreateNewQueryObj(test.pk, test.sk)

		eb := NewExprBuilder()
		eb.SetProjection(names)
		expr, err := eb.BuildExpression()
		if err != nil {
			if test.wantErr == nil {
				t.Errorf("FAIL: %v", err)
				continue
			}
			if err.Error() != test.wantErr.Error() {
				t.Errorf("FAIL: %v; want: %v", err, test.wantErr)
				continue
			}
		}

		check, err := svc.GetItem(q, TableName, &record{}, expr)
		if err != nil {
			t.Errorf("FAIL: %v", err)
			continue
		}
		t.Logf("%d) check: %v", i, check)
	}
}

// func TestUpdateWithCondition(t *testing.T) {
// 	var tests = []struct {
// 		pk          string
// 		sk          string
// 		updateField string
// 		size        string
// 		variable    bool
// 		updateValue interface{}
// 		wantErr     error
// 	}{
// 		{pk: "A", sk: "001", updateField: "count", size: "", variable: true, updateValue: 2, wantErr: nil},                                  // top-level value
// 		{pk: "B", sk: "003", updateField: "count-map", size: "M", variable: true, updateValue: 2, wantErr: nil},                             // nested value - original count: 6
// 		{pk: "B", sk: "003", updateField: "count-map", size: "M", variable: true, updateValue: 2, wantErr: nil},                             // 4
// 		{pk: "B", sk: "003", updateField: "count-map", size: "M", variable: true, updateValue: 2, wantErr: nil},                             // 2
// 		{pk: "B", sk: "003", updateField: "count-map", size: "M", variable: true, updateValue: 2, wantErr: fmt.Errorf(ErrConditionalCheck)}, // 0 - Condition fail
// 		{pk: "Z", sk: "000", updateField: "count", size: "", variable: true, updateValue: 2, wantErr: nil},                                  // non-existent partition
// 		{pk: "A", sk: "000", updateField: "count", size: "", variable: true, updateValue: 2, wantErr: nil},                                  // non-existent item
// 		// {pk: "B", sk: "003", updateField: "price", exprKey: ":p", updateValue: 10.05, wantErr: nil},                                          // orig: "price": 10
// 		// {pk: "C", sk: "004", updateField: "set", exprKey: ":s", updateValue: map[string]bool{"A": true, "B": true, "C": true}, wantErr: nil}, // orig: "set": ["A":false, "B":false, "C":false]
// 	}

// 	svc := NewDynamoDB(goaws.NewDefaultSession(), []*Table{table}, nil)
// 	for _, test := range tests {
// 		qt := test.updateValue
// 		q := CreateNewQueryObj(test.pk, test.sk)
// 		q.UpdateCurrent(test.updateField, qt)

// 		keyName := test.updateField
// 		if test.size != "" {
// 			keyName = fmt.Sprintf("%s.%s", test.updateField, test.size)
// 		}

// 		// condition = if curr - quantity >= 0
// 		cond := NewCondition()
// 		cond.GreaterThanEqual(keyName, qt)

// 		// update = setMinus - current - quantity
// 		ud := NewUpdateExpr()
// 		ud.SetMinus(keyName, keyName, qt, test.variable)

// 		// build expression
// 		eb := NewExprBuilder()
// 		eb.SetCondition(cond)
// 		eb.SetUpdate(ud)
// 		expr, err := eb.BuildExpression()
// 		if err != nil {
// 			t.Errorf("FAIL %v", err)
// 		}

// 		err = svc.UpdateItem(q, TableName, expr)
// 		if err != nil && test.wantErr == nil {
// 			t.Errorf("FAIL: %v; want: %v", err, test.wantErr)
// 		}
// 		if err != nil && test.wantErr != nil {
// 			if err.Error() != test.wantErr.Error() {
// 				t.Errorf("FAIL: %v; want: %v", err, test.wantErr)
// 			}
// 		}
// 	}
// }
