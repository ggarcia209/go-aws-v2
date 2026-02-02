package dynamo

import (
	"log"

	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

/* Expression wrapper type & methods */

// Expression wraps the AWS expression.Expression object.
type Expression struct {
	Expression expression.Expression `json:"expression"`
}

// Condition returns the Condition expression.
func (e *Expression) Condition() *string {
	return e.Expression.Condition()
}

// Condition returns the Condition expression.
func (e *Expression) Filter() *string {
	return e.Expression.Filter()
}

// Condition returns the KeyCondition expression.
func (e *Expression) KeyCondition() *string {
	return e.Expression.KeyCondition()
}

// Condition returns the expression's Names.
func (e *Expression) Names() map[string]*string {
	return e.Expression.Names()
}

// Condition returns the Projection expression.
func (e *Expression) Projection() *string {
	return e.Expression.Projection()
}

// Condition returns the Update expression.
func (e *Expression) Update() *string {
	return e.Expression.Update()
}

// Condition returns the expression's Attribute Values.
func (e *Expression) Values() map[string]*dynamodb.AttributeValue {
	return e.Expression.Values()
}

/* ExprBuilder wrapper type & methods */

// ExprBuilder is used to contain Builder types (Condition, Filter, etc...)
// and build DynamoDB expressions.
type ExprBuilder struct {
	Condition    *expression.ConditionBuilder
	Filter       *expression.ConditionBuilder
	KeyCondition *expression.KeyConditionBuilder
	Projection   *expression.ProjectionBuilder
	Update       *expression.UpdateBuilder
}

// SetCondition creates a ConditionBuilder object with the given field name and value.
func (e *ExprBuilder) SetCondition(cond Conditions) {
	e.Condition = &cond.Condition
}

// SetFilter creates a ConditionBuilder object as a Filter with the given field name and value.
func (e *ExprBuilder) SetFilter(name string, value interface{}) {
	filt := expression.Name(name).Equal(expression.Value(value))
	e.Filter = &filt
}

// SetKeyCondition creates a KeyConditionBuilder object with the given field name and value.
func (e *ExprBuilder) SetKeyCondition(cond KeyConditions) {
	e.KeyCondition = cond.KeyCondition
}

// SetProjection creates a ProjectionBuilder object from the given list of field names.
func (e *ExprBuilder) SetProjection(names []string) {
	proj := expression.ProjectionBuilder{}
	for _, name := range names {
		proj = proj.AddNames(expression.Name(name))
	}
	e.Projection = &proj
}

// SetUpdate sets the Update field with a predefined UpdateExpr object.
func (e *ExprBuilder) SetUpdate(update UpdateExpr) {
	e.Update = &update.Update
}

// BuildExpression builds the expression from the ExprBuilder fields and returns the object.
func (e *ExprBuilder) BuildExpression() (Expression, error) {
	expr := NewExpression()
	eb := expression.NewBuilder()

	if e.Condition != nil {
		eb = eb.WithCondition(*e.Condition)
	}
	if e.Filter != nil {
		eb = eb.WithFilter(*e.Filter)
	}
	if e.KeyCondition != nil {
		eb = eb.WithKeyCondition(*e.KeyCondition)
	}
	if e.Projection != nil {
		eb = eb.WithProjection(*e.Projection)
	}
	if e.Update != nil {
		eb = eb.WithUpdate(*e.Update)
	}
	build, err := eb.Build()
	if err != nil {
		log.Printf("BuildExpression failed: %v", err)
		return Expression{}, err
	}
	expr.Expression = build
	return expr, nil
}

// Reset clears all values of the ExprBuilder object.
func (e *ExprBuilder) Reset() {
	e.Condition = nil
	e.Filter = nil
	e.KeyCondition = nil
	e.Projection = nil
	e.Update = nil
}

/* UpdateExpr wrapper type & methods */

// UpdateExpr is used to construct Update Expressions.
type UpdateExpr struct {
	Update expression.UpdateBuilder
}

// Add adds a new field and value to an object.
func (u *UpdateExpr) Add(name string, value interface{}) {
	update := u.Update.Add(expression.Name(name), expression.Value(value))
	u.Update = update
}

// Delete deletes the specified value set from the specified field name.
func (u *UpdateExpr) Delete(name string, value interface{}) {
	update := u.Update.Delete(expression.Name(name), expression.Value(value))
	u.Update = update
}

// Remove removes the specified field name entirely.
func (u *UpdateExpr) Remove(name string) {
	update := u.Update.Remove(expression.Name(name))
	u.Update = update
}

// Set sets the value for the given field name with no conditions.
func (u *UpdateExpr) Set(name string, value interface{}) {
	update := u.Update.Set(expression.Name(name), expression.Value(value))
	u.Update = update
}

// SetIfNotExists sets a new field + value conditionally, if the given field name does not exist.
func (u *UpdateExpr) SetIfNotExists(name string, value interface{}) {
	update := u.Update.Set(expression.Name(name), expression.IfNotExists(expression.Name(name), expression.Value(value)))
	u.Update = update
}

// SetPlus creates a new Set Update expression, where the value is the sum of the 'aug' and 'add' args.
// SetPlus uses the aug value as a string type expression variable when the variable value is set to true.
//
//	Ex: 'SET #name = #min + sub
func (u *UpdateExpr) SetPlus(name string, aug, add interface{}, variable bool) {
	_, ok := aug.(string)
	if variable && ok {
		update := u.Update.Set(expression.Name(name), expression.Plus(expression.Name(aug.(string)), expression.Value(add)))
		u.Update = update
		return
	}
	update := u.Update.Set(expression.Name(name), expression.Plus(expression.Value(aug), expression.Value(add)))
	u.Update = update
	return
}

// SetPlus creates a new Set Update expression, where the value is the difference of the 'min' and 'sub' args.
// SetMinus uses the min value as a string type expression variable when the variable value is set to true.
//
//	Ex: 'SET #name = #min - sub
func (u *UpdateExpr) SetMinus(name string, min, sub interface{}, variable bool) {
	_, ok := min.(string)
	if variable && ok {
		update := u.Update.Set(expression.Name(name), expression.Minus(expression.Name(min.(string)), expression.Value(sub)))
		u.Update = update
		return
	}
	update := u.Update.Set(expression.Name(name), expression.Minus(expression.Value(min), expression.Value(sub)))
	u.Update = update
	return
}

// SetListAppend creates a new Update expression to append the given list to the current value of the given field name.
func (u *UpdateExpr) SetListAppend(name string, list interface{}) {
	update := u.Update.Set(expression.Name(name), expression.ListAppend(expression.Name(name), expression.Value(list)))
	u.Update = update
}

// Reset clears the Update expression.
func (u *UpdateExpr) Reset() {
	u.Update = expression.UpdateBuilder{}
}

/* Object Constructors */
// NewExpression constructs a new Expression object.
func NewExpression() Expression {
	e := Expression{}
	e.Expression = expression.Expression{}
	return e
}

// NewExprBuilder constructs a new ExprBuilder object.
func NewExprBuilder() ExprBuilder {
	return ExprBuilder{}
}

// NewUpdateExpr constructs a new UpdateExpr object.
func NewUpdateExpr() UpdateExpr {
	u := UpdateExpr{}
	u.Update = expression.UpdateBuilder{}
	return u
}

func NewKeyCondition() KeyConditions {
	c := KeyConditions{}
	return c
}

func NewCondition() Conditions {
	c := Conditions{}
	c.Condition = expression.ConditionBuilder{}
	return c
}

/* Conditons wrapper and methods */

// KeyConditions wraps the expression.KeyConditionBuilder object.
type KeyConditions struct {
	KeyCondition *expression.KeyConditionBuilder
}

// And creates an AND boolean condition.
func (c *KeyConditions) BeginsWith(name string, prefix string) *KeyConditions {
	condition := expression.Key(name).BeginsWith(prefix)
	if c.KeyCondition != nil {
		newCond := c.KeyCondition.And(condition)
		c.KeyCondition = &newCond
	} else {
		c.KeyCondition = &condition
	}

	return c
}

func (c *KeyConditions) Between(name string, lower, upper interface{}) *KeyConditions {
	condition := expression.Key(name).Between(expression.Value(lower), expression.Value(upper))
	if c.KeyCondition != nil {
		newCond := c.KeyCondition.And(condition)
		c.KeyCondition = &newCond
	} else {
		c.KeyCondition = &condition
	}

	return c
}

func (c *KeyConditions) Equal(name string, value interface{}) *KeyConditions {
	condition := expression.Key(name).Equal(expression.Value(value))
	if c.KeyCondition != nil {
		newCond := c.KeyCondition.And(condition)
		c.KeyCondition = &newCond
	} else {
		c.KeyCondition = &condition
	}

	return c
}

func (c *KeyConditions) GreaterThan(name string, value interface{}) *KeyConditions {
	condition := expression.Key(name).GreaterThan(expression.Value(value))
	if c.KeyCondition != nil {
		newCond := c.KeyCondition.And(condition)
		c.KeyCondition = &newCond
	} else {
		c.KeyCondition = &condition
	}

	return c
}

func (c *KeyConditions) GreaterThanEqual(name string, value interface{}) *KeyConditions {
	condition := expression.Key(name).GreaterThanEqual(expression.Value(value))
	if c.KeyCondition != nil {
		newCond := c.KeyCondition.And(condition)
		c.KeyCondition = &newCond
	} else {
		c.KeyCondition = &condition
	}

	return c
}

func (c *KeyConditions) LessThan(name string, value interface{}) *KeyConditions {
	condition := expression.Key(name).LessThan(expression.Value(value))
	if c.KeyCondition != nil {
		newCond := c.KeyCondition.And(condition)
		c.KeyCondition = &newCond
	} else {
		c.KeyCondition = &condition
	}

	return c
}

func (c *KeyConditions) LessThanEqual(name string, value interface{}) *KeyConditions {
	condition := expression.Key(name).LessThanEqual(expression.Value(value))
	if c.KeyCondition != nil {
		newCond := c.KeyCondition.And(condition)
		c.KeyCondition = &newCond
	} else {
		c.KeyCondition = &condition
	}

	return c
}

/* Conditons wrapper and methods */

// Conditions wraps the expression.ConditionBuilder object.
type Conditions struct {
	Condition expression.ConditionBuilder
}

// And creates an AND boolean condition.
func (c *Conditions) And(left, right Conditions, other ...Conditions) {
	condition := expression.And(left.Condition, right.Condition)
	for _, cond := range other {
		condition = expression.And(condition, cond.Condition)
	}
	c.Condition = condition
}

// AttributeExists creates
func (c *Conditions) AttributeExists(name string) {
	condition := expression.AttributeExists(expression.Name(name))
	c.Condition = condition
}

func (c *Conditions) AttributeNotExists(name string) {
	condition := expression.AttributeNotExists(expression.Name(name))
	c.Condition = condition
}

func (c *Conditions) AttributeType() {
	// TO DO
}

func (c *Conditions) BeginsWith(name string, prefix string) {
	condition := expression.BeginsWith(expression.Name(name), prefix)
	c.Condition = condition
}

func (c *Conditions) Between(name string, lower, upper interface{}) {
	condition := expression.Between(expression.Name(name), expression.Value(lower), expression.Value(upper))
	c.Condition = condition
}

func (c *Conditions) Contains(name string, substr string) {
	condition := expression.Contains(expression.Name(name), substr)
	c.Condition = condition
}

func (c *Conditions) Equal(name string, value interface{}) {
	condition := expression.Equal(expression.Name(name), expression.Value(value))
	c.Condition = condition
}

func (c *Conditions) GreaterThan(name string, value interface{}) {
	condition := expression.GreaterThan(expression.Name(name), expression.Value(value))
	c.Condition = condition
}

func (c *Conditions) GreaterThanEqual(name string, value interface{}) {
	condition := expression.GreaterThanEqual(expression.Name(name), expression.Value(value))
	c.Condition = condition
}

// TEST
func (c *Conditions) In(name string, values ...interface{}) {
	condition := expression.In(expression.Name(name), expression.Value(values))
	c.Condition = condition
}

func (c *Conditions) LessThan(name string, value interface{}) {
	condition := expression.LessThan(expression.Name(name), expression.Value(value))
	c.Condition = condition
}

func (c *Conditions) LessThanEqual(name string, value interface{}) {
	condition := expression.LessThanEqual(expression.Name(name), expression.Value(value))
	c.Condition = condition
}

// Not negates the given Condition.
func (c *Conditions) Not(cond Conditions) {
	condition := expression.Not(cond.Condition)
	c.Condition = condition
}

func (c *Conditions) NotEqual(name string, value interface{}) {
	condition := expression.NotEqual(expression.Name(name), expression.Value(value))
	c.Condition = condition
}

func (c *Conditions) Or(left, right Conditions, other ...Conditions) {
	condition := expression.Or(left.Condition, right.Condition)
	for _, cond := range other {
		condition = expression.Or(condition, cond.Condition)
	}
	c.Condition = condition
}
