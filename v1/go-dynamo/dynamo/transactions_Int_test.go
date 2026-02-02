//go:build integration

package dynamo

import (
	"log"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ggarcia209/go-aws-v2/v1/goaws"
)

func TestNewTxItem(t *testing.T) {
	var tests = []struct {
		n string
		t *Table
		q *Query
		e Expression
	}{
		{n: "t01", t: &Table{TableName: "test001"}, q: &Query{}, e: Expression{}},
	}

	for _, test := range tests {
		txc := NewCreateTxItem(test.n, nil, test.t, test.q, test.e)
		t.Logf("create: %v", txc)
		txu := NewUpdateTxItem(test.n, test.t, test.q, test.e)
		t.Logf("update: %v", txu)
		txcc := NewConditionCheckTxItem(test.n, test.t, test.q, test.e)
		t.Logf("condition check: %v", txcc)
	}
}

func TestNewTxWriteItem(t *testing.T) {
	var tests = []struct {
		n string
		t *Table
		q *Query
		e Expression
	}{
		{n: "t01", t: &Table{TableName: "test001"}, q: &Query{}, e: Expression{}},
	}

	for _, test := range tests {
		txc, err := newTxWriteItem(NewCreateTxItem(test.n, nil, test.t, test.q, test.e))
		if err != nil {
			t.Fatalf("FAIL: %v", err)
		}
		t.Logf("create: %v", txc)

		txu, err := newTxWriteItem(NewUpdateTxItem(test.n, test.t, test.q, test.e))
		if err != nil {
			t.Fatalf("FAIL: %v", err)
		}
		t.Logf("update: %v", txu)

		txcc, err := newTxWriteItem(NewConditionCheckTxItem(test.n, test.t, test.q, test.e))
		if err != nil {
			t.Fatalf("FAIL: %v", err)
		}
		t.Logf("condition check: %v", txcc)
	}
}

// SUCCESS
// All items fail condition check after subsequent calls
func TestTxWriteCreate(t *testing.T) {
	var tests = []struct {
		i record
		n string
		t *Table
		q *Query
		e Expression
	}{
		{
			i: record{
				Partition: "A",
				UUID:      "t001",
				Count:     10,
				Price:     24.99,
			},
			n: "t00", t: &Table{TableName: TableName}, q: &Query{}, e: createCondition(),
		},
		{
			i: record{
				Partition: "A",
				UUID:      "t002",
				Count:     20,
				Price:     10.99,
			},
			n: "t01", t: &Table{TableName: TableName}, q: &Query{}, e: createCondition(),
		},
		{
			i: record{
				Partition: "B",
				UUID:      "t001",
				Count:     30,
				Price:     24.99,
			},
			n: "t02", t: &Table{TableName: TableName}, q: &Query{}, e: createCondition(),
		},
		{
			i: record{
				Partition: "C",
				UUID:      "t004",
				Count:     15,
				Price:     12.99,
			},
			n: "t03", t: &Table{TableName: TableName}, q: &Query{}, e: createCondition(),
		},
		{
			i: record{
				Partition: "D",
				UUID:      "t005",
				Count:     10,
				Price:     24.99,
			},
			n: "t04", t: &Table{TableName: TableName}, q: &Query{}, e: createCondition(),
		},
	}

	txInput := []TransactionItem{}
	svc := NewDynamoDB(goaws.NewDefaultSession(), []*Table{table}, nil)

	for _, test := range tests {
		ti := NewCreateTxItem(test.n, test.i, test.t, test.q, test.e)
		txInput = append(txInput, ti)
	}

	tk := strconv.Itoa(int(rand.New(rand.NewSource(time.Now().UnixNano())).Uint64()))
	failed, err := svc.TxWrite(txInput, tk)

	if err != nil {
		t.Errorf("FAIL: %v\n failed: %v", err, failed)
	}

}

// SUCCESS
func TestTxWriteUpdate(t *testing.T) {
	var tests = []struct {
		i record
		n string
	}{
		{
			i: record{
				Partition: "A",
				UUID:      "t001",
				Count:     3,
				Price:     24.99,
			},
			n: "t00",
		},
		{
			i: record{
				Partition: "A",
				UUID:      "t002",
				Count:     10,
				Price:     10.99,
			},
			n: "t01",
		},
		{
			i: record{
				Partition: "B",
				UUID:      "t001",
				Count:     18,
				Price:     24.99,
			},
			n: "t02",
		},
		{
			i: record{
				Partition: "C",
				UUID:      "t004",
				Count:     15,
				Price:     12.99,
			},
			n: "t03",
		},
		{
			i: record{
				Partition: "D",
				UUID:      "t005",
				Count:     0,
				Price:     24.99,
			},
			n: "t04",
		},
	}

	txInput := []TransactionItem{}
	svc := NewDynamoDB(goaws.NewDefaultSession(), []*Table{table}, nil)
	for _, test := range tests {
		q, e := updateExpressionDec(test.i.Partition, test.i.UUID, test.i.Count)
		ti := NewUpdateTxItem(test.n, table, q, e)
		txInput = append(txInput, ti)
	}

	tk := strconv.Itoa(int(rand.New(rand.NewSource(time.Now().UnixNano())).Uint64()))
	failed, err := svc.TxWrite(txInput, tk)

	if err != nil {
		t.Errorf("FAIL: %v\n failed: %v", err, failed)
	}

}

func TestTxWriteCheckFail(t *testing.T) {
	var tests = []struct {
		i record
		n string
	}{
		{
			i: record{
				Partition: "A",
				UUID:      "t001",
				Count:     3,
				Price:     24.99,
			},
			n: "t00",
		},
		{
			i: record{
				Partition: "A",
				UUID:      "t002",
				Count:     10,
				Price:     10.99,
			},
			n: "t01",
		},
		{
			i: record{
				Partition: "B",
				UUID:      "t001",
				Count:     18,
				Price:     24.99,
			},
			n: "t02",
		},
		{
			i: record{
				Partition: "C",
				UUID:      "t004",
				Count:     15,
				Price:     12.99,
			},
			n: "t03",
		},
		{
			i: record{
				Partition: "D",
				UUID:      "t005",
				Count:     0,
				Price:     24.99,
			},
			n: "t04",
		},
	}

	txInput := []TransactionItem{}
	svc := NewDynamoDB(goaws.NewDefaultSession(), []*Table{table}, nil)
	for _, test := range tests {
		q, e := checkExpression(test.i.Partition, test.i.UUID, test.i.Count)
		ti := NewConditionCheckTxItem(test.n, table, q, e)
		txInput = append(txInput, ti)
	}

	tk := strconv.Itoa(int(rand.New(rand.NewSource(time.Now().UnixNano())).Uint64()))
	failed, err := svc.TxWrite(txInput, tk)

	if err != nil {
		if err.Error() != "TX_CONDITION_CHECK_FAILED" {
			t.Errorf("FAIL: %v\n failed: %v", err, failed)
			return
		}
		t.Logf("OK: CONDITION CHECK FAILED: %v\n failed: %v", err, failed)
		return
	}

}

func TestTxWriteCheckPass(t *testing.T) {
	var tests = []struct {
		i record
		n string
	}{
		{
			i: record{
				Partition: "A",
				UUID:      "t001",
				Count:     3,
				Price:     24.99,
			},
			n: "t00",
		},
		{
			i: record{
				Partition: "A",
				UUID:      "t002",
				Count:     10,
				Price:     10.99,
			},
			n: "t01",
		},
		{
			i: record{
				Partition: "B",
				UUID:      "t001",
				Count:     9,
				Price:     24.99,
			},
			n: "t02",
		},
		{
			i: record{
				Partition: "C",
				UUID:      "t004",
				Count:     0,
				Price:     12.99,
			},
			n: "t03",
		},
		{
			i: record{
				Partition: "D",
				UUID:      "t005",
				Count:     1,
				Price:     24.99,
			},
			n: "t04",
		},
	}

	txInput := []TransactionItem{}
	svc := NewDynamoDB(goaws.NewDefaultSession(), []*Table{table}, nil)
	for _, test := range tests {
		q, e := checkExpression(test.i.Partition, test.i.UUID, test.i.Count)
		ti := NewConditionCheckTxItem(test.n, table, q, e)
		txInput = append(txInput, ti)
	}

	tk := strconv.Itoa(int(rand.New(rand.NewSource(time.Now().UnixNano())).Uint64()))
	failed, err := svc.TxWrite(txInput, tk)

	if err != nil {
		t.Errorf("FAIL: %v\n failed: %v", err, failed)
	}

}

func createCondition() Expression {
	// condition = if curr - quantity >= 0
	cond := NewCondition()
	cond.AttributeNotExists("uuid")

	// build expression
	eb := NewExprBuilder()
	eb.SetCondition(cond)
	expr, err := eb.BuildExpression()

	if err != nil {
		os.Exit(1)
	}

	return expr
}

// decrement counts
func updateExpressionDec(pk, sk string, qt int) (*Query, Expression) {
	q := CreateNewQueryObj(pk, sk)
	keyName := "count"
	// q.UpdateCurrent(keyName, qt)

	log.Printf("query: %v", q)

	// condition = if curr - quantity >= 0
	cond := NewCondition()
	cond.GreaterThanEqual(keyName, qt)

	// update = setMinus - current - quantity
	ud := NewUpdateExpr()
	ud.SetMinus(keyName, keyName, qt, true)

	// build expression
	eb := NewExprBuilder()
	eb.SetCondition(cond)
	eb.SetUpdate(ud)
	expr, err := eb.BuildExpression()
	if err != nil {
		os.Exit(1)
	}

	log.Printf("expression: %v", expr)
	log.Printf("expression names: %v", *expr.Names()["#0"])
	log.Printf("expression values: %v", expr.Values())
	log.Printf("condition: %s", *expr.Condition())
	log.Printf("update expression: %s", *expr.Update())

	return q, expr
}

// increment counts
func updateExpressionInc(qt int) Expression {
	q := CreateNewQueryObj("partition", "uuid")
	keyName := "count"
	q.UpdateCurrent(keyName, qt)

	// update = setMinus - current - quantity
	ud := NewUpdateExpr()
	ud.SetPlus(keyName, keyName, qt, true)

	// build expression
	eb := NewExprBuilder()
	eb.SetUpdate(ud)
	expr, err := eb.BuildExpression()
	if err != nil {
		os.Exit(1)
	}

	return expr
}

func checkExpression(pk, sk string, qt int) (*Query, Expression) {
	q := CreateNewQueryObj(pk, sk)

	// condition = if curr - quantity >= 0
	cond := NewCondition()
	cond.GreaterThanEqual("count", qt)

	// build expression
	eb := NewExprBuilder()
	eb.SetCondition(cond)
	expr, err := eb.BuildExpression()
	if err != nil {
		os.Exit(1)
	}

	return q, expr
}
