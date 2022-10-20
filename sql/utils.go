package sql

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/btnguyen2k/prom/sql"
)

// PlaceholderGenerator is a function that generates placeholder used in prepared statement.
type PlaceholderGenerator func(field string) string

// NewPlaceholderGenerator is a function that creates PlaceholderGenerator instances.
type NewPlaceholderGenerator func() PlaceholderGenerator

// NewPlaceholderGeneratorQuestion creates a placeholder function that uses "?" as placeholder.
//
// Note: "?" placeholder is used by MySQL.
func NewPlaceholderGeneratorQuestion() PlaceholderGenerator {
	return func(field string) string {
		return "?"
	}
}

// NewPlaceholderGeneratorDollarN creates a placeholder function that uses "$<n>" as placeholder.
//
// Note: "$<n>" placeholder is used by PostgreSQL.
func NewPlaceholderGeneratorDollarN() PlaceholderGenerator {
	var lock sync.Mutex
	n := 0
	return func(field string) string {
		lock.Lock()
		defer lock.Unlock()
		n++
		return "$" + strconv.Itoa(n)
	}
}

// NewPlaceholderGeneratorColonN creates a placeholder function that uses ":<n>" as placeholder.
//
// Note: ":<n>" placeholder is used by Oracle.
func NewPlaceholderGeneratorColonN() PlaceholderGenerator {
	var lock sync.Mutex
	n := 0
	return func(field string) string {
		lock.Lock()
		defer lock.Unlock()
		n++
		return ":" + strconv.Itoa(n)
	}
}

// NewPlaceholderGeneratorAtpiN creates a placeholder function that uses "@p<n>" as placeholder.
//
// Note: "@p<n>" placeholder is used by MSSQL.
func NewPlaceholderGeneratorAtpiN() PlaceholderGenerator {
	var lock sync.Mutex
	n := 0
	return func(field string) string {
		lock.Lock()
		defer lock.Unlock()
		n++
		return "@p" + strconv.Itoa(n)
	}
}

/*----------------------------------------------------------------------*/

// OptTableAlias is used to prefix table alias to field name when building ISorting, IFilter or ISqlBuilder.
type OptTableAlias struct {
	TableAlias string
}

// OptDbFlavor is used to specify the db flavor that affects the generated SQL statement.
type OptDbFlavor struct {
	Flavor sql.DbFlavor
}

func extractOptTableAlias(opts ...interface{}) string {
	for _, opt := range opts {
		switch opt.(type) {
		case OptTableAlias:
			return opt.(OptTableAlias).TableAlias + "."
		case *OptTableAlias:
			return opt.(*OptTableAlias).TableAlias + "."
		}
	}
	return ""
}

func removeOptTableAlias(opts ...interface{}) []interface{} {
	result := make([]interface{}, 0, len(opts))
	for _, opt := range opts {
		switch opt.(type) {
		case OptTableAlias, *OptTableAlias:
			continue
		}
		result = append(result, opt)
	}
	return result
}

func extractOptDbFlavor(opts ...interface{}) sql.DbFlavor {
	for _, opt := range opts {
		switch opt.(type) {
		case OptDbFlavor:
			return opt.(OptDbFlavor).Flavor
		case *OptDbFlavor:
			return opt.(*OptDbFlavor).Flavor
		}
	}
	return sql.FlavorDefault
}

// StmGeneratorBetween generates custom "BETWEEN" clause of the SQL statement.
//
// Available since v0.6.1
type StmGeneratorBetween func(pg PlaceholderGenerator, field string, leftValue, rightValue interface{}, opts ...interface{}) (string, []interface{})

// OptBetweenGenerator is used to specify the generator used to generate custom "BETWEEN" clause of the SQL statement.
//
// Available since v0.6.1
type OptBetweenGenerator struct {
	Generator StmGeneratorBetween
}

func extractOptBetweenGenerator(opts ...interface{}) StmGeneratorBetween {
	for _, opt := range opts {
		switch opt.(type) {
		case OptBetweenGenerator:
			return opt.(OptBetweenGenerator).Generator
		case *OptBetweenGenerator:
			return opt.(*OptBetweenGenerator).Generator
		}
	}
	return nil
}

/*----------------------------------------------------------------------*/

var isortingType = reflect.TypeOf((*ISorting)(nil)).Elem()

// ISorting provides API interface to build elements of 'ORDER BY' clause.
type ISorting interface {
	// Build builds the 'ORDER BY' clause (without "ORDER BY" keyword).
	Build(opts ...interface{}) string
}

// GenericSorting is a generic implementation of ISorting.
type GenericSorting struct {
	Flavor   sql.DbFlavor
	Ordering []string // list of fields to sort on. Field is in the following format: <field_name[<:order>]>, where '1' means 'ascending' and '-1' means 'descending'.
}

// WithFlavor sets the database flavor associated with this sorting instance.
//
// Available since v0.6.1
func (o *GenericSorting) WithFlavor(flavor sql.DbFlavor) *GenericSorting {
	o.Flavor = flavor
	return o
}

// Add appends an ordering element to the list.
//
// order is in the following format: <field_name[<:order>]>, where '1' means 'ascending' and '-1' means 'descending'.
func (o *GenericSorting) Add(order string) *GenericSorting {
	if strings.TrimSpace(order) != "" {
		o.Ordering = append(o.Ordering, strings.TrimSpace(order))
	}
	return o
}

// Build implements ISorting.Build.
func (o *GenericSorting) Build(opts ...interface{}) string {
	if len(o.Ordering) == 0 {
		return ""
	}

	tableAlias := extractOptTableAlias(opts...)
	elements := make([]string, 0, len(o.Ordering))
	for _, v := range o.Ordering {
		tokens := strings.Split(v, ":")
		order := tokens[0]
		if !reColnamePrefixedTblname.MatchString(order) {
			order = tableAlias + order
		}
		if len(tokens) > 1 {
			ord := strings.TrimSpace(strings.ToUpper(tokens[1]))
			if ord == "ASC" || ord == "DESC" {
				order += " " + tokens[1]
			} else if ord != "" && ord[0] == '-' {
				order += " DESC"
			} else {
				order += " ASC"
			}
		}
		elements = append(elements, order)
	}
	return strings.Join(elements, ",")
}

/*----------------------------------------------------------------------*/

var ifilterType = reflect.TypeOf((*IFilter)(nil)).Elem()

// IFilter provides API interface to build element of 'WHERE' clause for SQL statement.
type IFilter interface {
	// Build builds the 'WHERE' clause (without "WHERE" keyword) with placeholders and list of values for placeholders in order.
	Build(placeholderGenerator PlaceholderGenerator, opts ...interface{}) (string, []interface{})
}

/*----------------------------------------------------------------------*/

// FilterAndOr combines two or more filters using AND/OR clause.
// It is recommended to use FilterAnd and FilterOr instead of using FilterAndOr directly.
//
// Available since v0.3.0
type FilterAndOr struct {
	Filters  []IFilter
	Operator string // literal form of the operator
}

// Clone returns a cloned instance of this filter.
//
// Available since v0.6.1
func (f *FilterAndOr) Clone() *FilterAndOr {
	clone := &FilterAndOr{Operator: f.Operator, Filters: make([]IFilter, len(f.Filters))}
	copy(clone.Filters, f.Filters)
	return clone
}

// WithOperator assigns an operation to this filter.
//
// Available since v0.6.1
func (f *FilterAndOr) WithOperator(op string) *FilterAndOr {
	f.Operator = op
	return f
}

// Add appends a filter to the list.
func (f *FilterAndOr) Add(filter IFilter) *FilterAndOr {
	if filter != nil {
		f.Filters = append(f.Filters, filter)
	}
	return f
}

// Build implements IFilter.Build.
func (f *FilterAndOr) Build(placeholderGenerator PlaceholderGenerator, opts ...interface{}) (string, []interface{}) {
	nFilters := len(f.Filters)
	if nFilters == 0 {
		return "", make([]interface{}, 0)
	}

	op := fmt.Sprintf(" %s ", strings.TrimSpace(f.Operator))
	clause, values := f.Filters[0].Build(placeholderGenerator, opts...)
	if nFilters > 1 {
		clause = fmt.Sprintf("(%s)", clause)
		for i := 1; i < nFilters; i++ {
			c, v := f.Filters[i].Build(placeholderGenerator, opts...)
			clause += op + fmt.Sprintf("(%s)", c)
			values = append(values, v...)
		}
	}

	return clause, values
}

// FilterAnd combines two or more filters using AND clause.
type FilterAnd struct {
	FilterAndOr
}

// Clone returns a cloned instance of this filter.
//
// Available since v0.6.1
func (f *FilterAnd) Clone() *FilterAnd {
	clone := &FilterAnd{*f.FilterAndOr.Clone()}
	return clone
}

// Add appends a filter to the list.
func (f *FilterAnd) Add(filter IFilter) *FilterAnd {
	f.FilterAndOr.Add(filter)
	return f
}

// Build implements IFilter.Build.
func (f *FilterAnd) Build(placeholderGenerator PlaceholderGenerator, opts ...interface{}) (string, []interface{}) {
	if strings.TrimSpace(f.Operator) == "" {
		return f.Clone().WithOperator("AND").Build(placeholderGenerator, opts...)
	}
	return f.FilterAndOr.Build(placeholderGenerator, opts...)
}

// FilterOr combines two filters using OR clause.
type FilterOr struct {
	FilterAndOr
}

// Clone returns a cloned instance of this filter.
//
// Available since v0.6.1
func (f *FilterOr) Clone() *FilterOr {
	clone := &FilterOr{*f.FilterAndOr.Clone()}
	return clone
}

// Add appends a filter to the list.
func (f *FilterOr) Add(filter IFilter) *FilterOr {
	f.FilterAndOr.Add(filter)
	return f
}

// Build implements IFilter.Build.
func (f *FilterOr) Build(placeholderGenerator PlaceholderGenerator, opts ...interface{}) (string, []interface{}) {
	if strings.TrimSpace(f.Operator) == "" {
		return f.Clone().WithOperator("OR").Build(placeholderGenerator, opts...)
	}
	return f.FilterAndOr.Build(placeholderGenerator, opts...)
}

/*----------------------------------------------------------------------*/

// FilterAsIs represents a single filter where the clause is passed as-is to the database driver.
//
// Available since v0.6.1
type FilterAsIs struct {
	Clause string
}

// WithClause assigns a clause to this filter.
func (f *FilterAsIs) WithClause(clause string) *FilterAsIs {
	f.Clause = clause
	return f
}

// Build implements IFilter.Build.
func (f *FilterAsIs) Build(_ PlaceholderGenerator, _ ...interface{}) (string, []interface{}) {
	return f.Clause, []interface{}{}
}

/*----------------------------------------------------------------------*/

// FilterBetween represents the single filter: <field> BETWEEN <value1> AND <value2>.
//
// Available since v0.4.0
type FilterBetween struct {
	Field      string      // field to check
	Operator   string      // the operator itself (default value is BETWEEN)
	ValueLeft  interface{} // left value of the BETWEEN operator
	ValueRight interface{} // right value of the BETWEEN operator
}

// Clone returns a cloned instance of this filter.
//
// Available since v0.6.1
func (f *FilterBetween) Clone() *FilterBetween {
	clone := &FilterBetween{
		Field:      f.Field,
		Operator:   f.Operator,
		ValueLeft:  f.ValueLeft,
		ValueRight: f.ValueRight,
	}
	return clone
}

// WithField assigns a field to this filter.
//
// Available since v0.6.1
func (f *FilterBetween) WithField(field string) *FilterBetween {
	f.Field = field
	return f
}

// WithOperator assigns an operator to this filter.
//
// Available since v0.6.1
func (f *FilterBetween) WithOperator(op string) *FilterBetween {
	f.Operator = op
	return f
}

// WithLeftValue assigns a value to the left operand.
//
// Available since v0.6.1
func (f *FilterBetween) WithLeftValue(val interface{}) *FilterBetween {
	f.ValueLeft = val
	return f
}

// WithRightValue assigns a value to the right operand.
//
// Available since v0.6.1
func (f *FilterBetween) WithRightValue(val interface{}) *FilterBetween {
	f.ValueRight = val
	return f
}

// Build implements IFilter.Build.
func (f *FilterBetween) Build(placeholderGenerator PlaceholderGenerator, opts ...interface{}) (string, []interface{}) {
	if strings.TrimSpace(f.Operator) == "" {
		return f.Clone().WithOperator("BETWEEN").Build(placeholderGenerator, opts...)
	}

	if customGenerator := extractOptBetweenGenerator(opts...); customGenerator != nil {
		return customGenerator(placeholderGenerator, f.Field, f.ValueLeft, f.ValueRight, opts...)
	}

	if placeholderGenerator == nil {
		return "", []interface{}{}
	}
	tableAlias := extractOptTableAlias(opts...)
	if reColnamePrefixedTblname.MatchString(f.Field) {
		tableAlias = ""
	}
	values := []interface{}{f.ValueLeft, f.ValueRight}
	clause := fmt.Sprintf("%s%s %s %s AND %s", tableAlias, f.Field, strings.TrimSpace(f.Operator), placeholderGenerator(f.Field), placeholderGenerator(f.Field))
	return clause, values
}

/*----------------------------------------------------------------------*/

// FilterFieldValue represents the single filter: <field> <operator> <value>.
type FilterFieldValue struct {
	Field    string      // field to check
	Operator string      // the operator to perform
	Value    interface{} // value to test against
}

// Clone returns a cloned instance of this filter.
//
// Available since v0.6.1
func (f *FilterFieldValue) Clone() *FilterFieldValue {
	clone := &FilterFieldValue{
		Field:    f.Field,
		Operator: f.Operator,
		Value:    f.Value,
	}
	return clone
}

// WithField assigns a field to this filter.
//
// Available since v0.6.1
func (f *FilterFieldValue) WithField(field string) *FilterFieldValue {
	f.Field = field
	return f
}

// WithOperator assigns an operator to this filter.
//
// Available since v0.6.1
func (f *FilterFieldValue) WithOperator(op string) *FilterFieldValue {
	f.Operator = op
	return f
}

// WithValue assigns a value to this filter.
//
// Available since v0.6.1
func (f *FilterFieldValue) WithValue(val interface{}) *FilterFieldValue {
	f.Value = val
	return f
}

// Build implements IFilter.Build.
func (f *FilterFieldValue) Build(placeholderGenerator PlaceholderGenerator, opts ...interface{}) (string, []interface{}) {
	tableAlias := extractOptTableAlias(opts...)
	if reColnamePrefixedTblname.MatchString(f.Field) {
		tableAlias = ""
	}
	flavor := extractOptDbFlavor(opts...)
	values := make([]interface{}, 0)
	var clause string
	if f.Value != nil {
		if placeholderGenerator == nil {
			return "", []interface{}{}
		}
		clause = fmt.Sprintf("%s%s %s %s", tableAlias, f.Field, strings.TrimSpace(f.Operator), placeholderGenerator(f.Field))
		values = append(values, f.Value)
	} else if flavor == sql.FlavorCosmosDb {
		clause = fmt.Sprintf("%s%s %s null", tableAlias, f.Field, strings.TrimSpace(f.Operator))
	} else {
		clause = fmt.Sprintf("%s%s %s NULL", tableAlias, f.Field, strings.TrimSpace(f.Operator))
	}
	return clause, values
}

/*----------------------------------------------------------------------*/

// FilterIsNull represents the single filter: <field> IS NULL.
//
// Available since v0.4.0
type FilterIsNull struct {
	FilterFieldValue
}

// Clone returns a cloned instance of this filter.
//
// Available since v0.6.1
func (f *FilterIsNull) Clone() *FilterIsNull {
	clone := &FilterIsNull{*f.FilterFieldValue.Clone()}
	return clone
}

// WithField assigns a field to this filter.
//
// Available since v0.6.1
func (f *FilterIsNull) WithField(field string) *FilterIsNull {
	f.FilterFieldValue.WithField(field)
	return f
}

// WithOperator assigns an operator to this filter.
//
// Available since v0.6.1
func (f *FilterIsNull) WithOperator(op string) *FilterIsNull {
	f.FilterFieldValue.WithOperator(op)
	return f
}

// Build implements IFilter.Build.
func (f *FilterIsNull) Build(placeholderGenerator PlaceholderGenerator, opts ...interface{}) (string, []interface{}) {
	if f.Value != nil || strings.TrimSpace(f.Operator) == "" {
		fclone := f.Clone()
		fclone.WithValue(nil)
		if strings.TrimSpace(fclone.Operator) == "" {
			fclone.WithOperator("IS")
		}
		return fclone.Build(placeholderGenerator, opts...)
	}

	flavor := extractOptDbFlavor(opts...)
	if flavor == sql.FlavorCosmosDb {
		return f.Clone().WithOperator("=").FilterFieldValue.Build(placeholderGenerator, opts...)
	}
	return f.FilterFieldValue.Build(placeholderGenerator, opts...)
}

// FilterIsNotNull represents the single filter: <field> IS NOT NULL.
//
// Available since v0.4.0
type FilterIsNotNull struct {
	FilterFieldValue
}

// Clone returns a cloned instance of this filter.
//
// Available since v0.6.1
func (f *FilterIsNotNull) Clone() *FilterIsNotNull {
	clone := &FilterIsNotNull{*f.FilterFieldValue.Clone()}
	return clone
}

// WithField assigns a field to this filter.
//
// Available since v0.6.1
func (f *FilterIsNotNull) WithField(field string) *FilterIsNotNull {
	f.FilterFieldValue.WithField(field)
	return f
}

// WithOperator assigns an operator to this filter.
//
// Available since v0.6.1
func (f *FilterIsNotNull) WithOperator(op string) *FilterIsNotNull {
	f.FilterFieldValue.WithOperator(op)
	return f
}

// Build implements IFilter.Build.
func (f *FilterIsNotNull) Build(placeholderGenerator PlaceholderGenerator, opts ...interface{}) (string, []interface{}) {
	if f.Value != nil || strings.TrimSpace(f.Operator) == "" {
		fclone := f.Clone()
		fclone.WithValue(nil)
		if strings.TrimSpace(fclone.Operator) == "" {
			fclone.WithOperator("IS NOT")
		}
		return fclone.Build(placeholderGenerator, opts...)
	}

	flavor := extractOptDbFlavor(opts...)
	if flavor == sql.FlavorCosmosDb {
		return f.Clone().WithOperator("!=").FilterFieldValue.Build(placeholderGenerator, opts...)
	}
	return f.FilterFieldValue.Build(placeholderGenerator, opts...)
}

/*----------------------------------------------------------------------*/

// FilterExpression represents the single filter: <left> <operator> <right>.
type FilterExpression struct {
	Left, Right string // left & right parts of the expression
	Operator    string // the operator to perform
}

// WithLeft assigns a left operand to this filter.
//
// Available since v0.6.1
func (f *FilterExpression) WithLeft(left string) *FilterExpression {
	f.Left = left
	return f
}

// WithRight assigns a right operand to this filter.
//
// Available since v0.6.1
func (f *FilterExpression) WithRight(right string) *FilterExpression {
	f.Right = right
	return f
}

// WithOperator assigns an operator to this filter.
//
// Available since v0.6.1
func (f *FilterExpression) WithOperator(op string) *FilterExpression {
	f.Operator = op
	return f
}

// Build implements IFilter.Build.
func (f *FilterExpression) Build(_ PlaceholderGenerator, opts ...interface{}) (string, []interface{}) {
	tableAlias := extractOptTableAlias(opts...)
	tableAliasLeft, tableAliasRight := tableAlias, tableAlias
	if reColnamePrefixedTblname.MatchString(f.Left) {
		tableAliasLeft = ""
	}
	if reColnamePrefixedTblname.MatchString(f.Right) {
		tableAliasRight = ""
	}
	clause := fmt.Sprintf("%s%s %s %s%s", tableAliasLeft, f.Left, strings.TrimSpace(f.Operator), tableAliasRight, f.Right)
	return clause, make([]interface{}, 0)
}

/*----------------------------------------------------------------------*/

// ISqlBuilder provides API interface to build the SQL statement.
type ISqlBuilder interface {
	Build(opts ...interface{}) (string, []interface{})
}

// BaseSqlBuilder is the base struct to implement DeleteBuilder, InsertBuilder, SelectBuilder and UpdateBuilder.
//
// Available since v0.3.0
type BaseSqlBuilder struct {
	Flavor               sql.DbFlavor
	PlaceholderGenerator PlaceholderGenerator
	Table                string
}

// Clone returns a cloned instance of this filter.
//
// Available since v0.6.1
func (b *BaseSqlBuilder) Clone() *BaseSqlBuilder {
	clone := &BaseSqlBuilder{Flavor: b.Flavor, PlaceholderGenerator: b.PlaceholderGenerator, Table: b.Table}
	return clone
}

// WithFlavor sets the SQL flavor that affect the generated SQL statement.
//
// Note: WithFlavor will reset the PlaceholderGenerator
func (b *BaseSqlBuilder) WithFlavor(flavor sql.DbFlavor) *BaseSqlBuilder {
	b.Flavor = flavor
	switch flavor {
	case sql.FlavorPgSql, sql.FlavorCosmosDb:
		return b.WithPlaceholderGenerator(NewPlaceholderGeneratorDollarN())
	case sql.FlavorMsSql:
		return b.WithPlaceholderGenerator(NewPlaceholderGeneratorAtpiN())
	case sql.FlavorOracle:
		return b.WithPlaceholderGenerator(NewPlaceholderGeneratorColonN())
	case sql.FlavorMySql, sql.FlavorSqlite:
		return b.WithPlaceholderGenerator(NewPlaceholderGeneratorQuestion())
	default:
		return b.WithPlaceholderGenerator(NewPlaceholderGeneratorQuestion())
	}
}

// WithPlaceholderGenerator sets the placeholder generator used to generate placeholders in the SQL statement.
func (b *BaseSqlBuilder) WithPlaceholderGenerator(placeholderGenerator PlaceholderGenerator) *BaseSqlBuilder {
	b.PlaceholderGenerator = placeholderGenerator
	return b
}

// WithTable sets name of the database table used to generate the SQL statement.
func (b *BaseSqlBuilder) WithTable(table string) *BaseSqlBuilder {
	b.Table = table
	return b
}

// DeleteBuilder is a builder that helps to build DELETE sql statement.
type DeleteBuilder struct {
	BaseSqlBuilder
	Filter IFilter
}

// NewDeleteBuilder constructs a new DeleteBuilder.
func NewDeleteBuilder() *DeleteBuilder {
	return &DeleteBuilder{}
}

// WithFlavor sets the SQL flavor that affect the generated SQL statement.
//
// Note: WithFlavor will reset the PlaceholderGenerator
func (b *DeleteBuilder) WithFlavor(flavor sql.DbFlavor) *DeleteBuilder {
	b.BaseSqlBuilder.WithFlavor(flavor)
	return b
}

// WithTable sets name of the database table used to generate the SQL statement.
func (b *DeleteBuilder) WithTable(table string) *DeleteBuilder {
	b.BaseSqlBuilder.WithTable(table)
	return b
}

// WithPlaceholderGenerator sets the placeholder generator used to generate placeholders in the SQL statement.
func (b *DeleteBuilder) WithPlaceholderGenerator(placeholderGenerator PlaceholderGenerator) *DeleteBuilder {
	b.BaseSqlBuilder.WithPlaceholderGenerator(placeholderGenerator)
	return b
}

// WithFilter sets the filter used to generate the WHERE clause.
func (b *DeleteBuilder) WithFilter(filter IFilter) *DeleteBuilder {
	b.Filter = filter
	return b
}

// Build constructs the DELETE sql statement, in the following format:
//
//     DELETE FROM <table> [WHERE <filter>]
//
// (since v0.3.0) the generated DELETE statement works with MySQL, MSSQL, PostgreSQL, Oracle, SQLite3 and btnguyen2k/gocosmos.
func (b *DeleteBuilder) Build(opts ...interface{}) (string, []interface{}) {
	if b.Flavor == sql.FlavorCosmosDb {
		opts = removeOptTableAlias(opts...)
	}
	tableAlias := extractOptTableAlias(opts...)
	if tableAlias != "" {
		if reTblNameWithAlias.MatchString(b.Table) {
			tableAlias = ""
		} else {
			tableAlias = " " + tableAlias[:len(tableAlias)-1]
		}
	}
	if b.Filter != nil {
		newOpts := append([]interface{}{OptDbFlavor{Flavor: b.Flavor}}, opts...)
		whereClause, values := b.Filter.Build(b.PlaceholderGenerator, newOpts...)
		sql := fmt.Sprintf("DELETE FROM %s%s WHERE %s", b.Table, tableAlias, whereClause)
		return sql, values
	}
	sql := fmt.Sprintf("DELETE FROM %s%s", b.Table, tableAlias)
	return sql, make([]interface{}, 0)
}

/*----------------------------------------------------------------------*/

// SelectBuilder is a builder that helps building SELECT sql statement.
type SelectBuilder struct {
	BaseSqlBuilder
	Columns                   []string
	Tables                    []string
	Filter                    IFilter
	GroupBy                   []string
	Having                    IFilter
	Sorting                   ISorting
	LimitNumRows, LimitOffset int
}

// NewSelectBuilder constructs a new SelectBuilder.
func NewSelectBuilder() *SelectBuilder {
	return &SelectBuilder{}
}

// WithFlavor sets the SQL flavor that affect the generated SQL statement.
//
// Note: WithFlavor will reset the PlaceholderGenerator
func (b *SelectBuilder) WithFlavor(flavor sql.DbFlavor) *SelectBuilder {
	b.BaseSqlBuilder.WithFlavor(flavor)
	return b
}

// WithPlaceholderGenerator sets the placeholder generator used to generate placeholders in the SQL statement.
func (b *SelectBuilder) WithPlaceholderGenerator(placeholderGenerator PlaceholderGenerator) *SelectBuilder {
	b.BaseSqlBuilder.WithPlaceholderGenerator(placeholderGenerator)
	return b
}

// WithColumns sets list of table columns used to generate the SQL statement.
func (b *SelectBuilder) WithColumns(columns ...string) *SelectBuilder {
	b.Columns = make([]string, len(columns))
	copy(b.Columns, columns)
	return b
}

// AddColumns appends columns to the existing list.
func (b *SelectBuilder) AddColumns(columns ...string) *SelectBuilder {
	b.Columns = append(b.Columns, columns...)
	return b
}

// WithTable sets name of the database table used to generate the SQL statement.
//
// Available since v0.3.0
func (b *SelectBuilder) WithTable(table string) *SelectBuilder {
	// b.BaseSqlBuilder.WithTable(table)
	b.Tables = []string{table}
	return b
}

// WithTables sets list of database tables used to generate the SQL statement.
func (b *SelectBuilder) WithTables(tables ...string) *SelectBuilder {
	b.Tables = make([]string, len(tables))
	copy(b.Tables, tables)
	return b
}

// AddTables appends tables to the existing list.
func (b *SelectBuilder) AddTables(tables ...string) *SelectBuilder {
	b.Tables = append(b.Tables, tables...)
	return b
}

// WithFilter sets the filter used to generate the WHERE clause.
func (b *SelectBuilder) WithFilter(filter IFilter) *SelectBuilder {
	b.Filter = filter
	return b
}

// WithGroupBy sets list of fields used to generate the GROUP BY clause.
func (b *SelectBuilder) WithGroupBy(fields ...string) *SelectBuilder {
	b.GroupBy = make([]string, len(fields))
	copy(b.GroupBy, fields)
	return b
}

// AddGroupBy appends fields to the existing list.
func (b *SelectBuilder) AddGroupBy(fields ...string) *SelectBuilder {
	b.GroupBy = append(b.GroupBy, fields...)
	return b
}

// WithHaving sets the filter used to generate the HAVING clause.
func (b *SelectBuilder) WithHaving(having IFilter) *SelectBuilder {
	b.Having = having
	return b
}

// WithSorting sets sorting builder used to generate the ORDER BY clause.
func (b *SelectBuilder) WithSorting(sorting ISorting) *SelectBuilder {
	b.Sorting = sorting
	return b
}

// WithLimit sets the value to generate the LIMIT/OFFSET clause.
func (b *SelectBuilder) WithLimit(numRows, offset int) *SelectBuilder {
	b.LimitNumRows = numRows
	b.LimitOffset = offset
	return b
}

var (
	reColnamePrefixedTblname = regexp.MustCompile(`^[\w_-]+\..+?$`)
	reTblNameWithAlias       = regexp.MustCompile(`(?i)^([\w_-]+)\s+(AS\s+)?(.+?)$`)
)

// Build constructs the SELECT sql statement, in the following format:
//
//     SELECT <columns> FROM <tables>
//     [WHERE <filter>]
//     [GROUP BY <group-by>]
//     [HAVING <having>]
//     [ORDER BY <sorting>]
//     [LIMIT <limit>]
//
// (since v0.3.0) the generated SELECT statement works with MySQL, MSSQL, PostgreSQL, Oracle, SQLite and btnguyen2k/gocosmos.
//
// Note: (since v0.6.1) if selecting from multiple tables, OptTableAlias (if any) will be discarded.
func (b *SelectBuilder) Build(opts ...interface{}) (string, []interface{}) {
	if len(b.Tables) > 1 {
		opts = removeOptTableAlias(opts...)
	}
	cosmosdbTblName := strings.TrimSpace(b.Tables[0])
	cosmosdbTblAlias := "c"
	optTableAlias := extractOptTableAlias(opts...)
	tablesClause := strings.Join(b.Tables, ",")
	if optTableAlias != "" {
		tblAlias := optTableAlias[:len(optTableAlias)-1]
		if !reTblNameWithAlias.MatchString(tablesClause) {
			tablesClause += fmt.Sprintf(" %s", tblAlias)
		}
		cosmosdbTblAlias = tblAlias
	}

	if b.Flavor == sql.FlavorCosmosDb {
		/* START: special case for gocosmos */
		if tokens := reTblNameWithAlias.FindStringSubmatch(cosmosdbTblName); tokens != nil {
			cosmosdbTblName = tokens[1]
			cosmosdbTblAlias = strings.TrimSpace(tokens[3])
		}
		tablesClause = fmt.Sprintf("%s %s", cosmosdbTblName, cosmosdbTblAlias)

		if optTableAlias == "" {
			optTableAlias = cosmosdbTblAlias + "."
			opts = append([]interface{}{OptTableAlias{TableAlias: cosmosdbTblAlias}}, opts...)
		}
		/* END: special case for gocosmos */
	}

	colsClause := optTableAlias + "*"
	if len(b.Columns) > 0 {
		cols := make([]string, len(b.Columns))
		copy(cols, b.Columns)
		for i, col := range b.Columns {
			if !reColnamePrefixedTblname.MatchString(col) {
				cols[i] = optTableAlias + col
			}
		}
		colsClause = strings.Join(cols, ",")
	}

	sqlStm := fmt.Sprintf("SELECT %s FROM %s", colsClause, tablesClause)
	values := make([]interface{}, 0)
	var tempValues []interface{}

	whereClause := ""
	if b.Filter != nil {
		newOpts := append([]interface{}{OptDbFlavor{b.Flavor}}, opts...)
		whereClause, tempValues = b.Filter.Build(b.PlaceholderGenerator, newOpts...)
		values = append(values, tempValues...)
	}
	if whereClause != "" {
		sqlStm += fmt.Sprintf(" WHERE %s", whereClause)
	}

	groupClause := ""
	if len(b.GroupBy) > 0 {
		groupByList := make([]string, len(b.GroupBy))
		copy(groupByList, b.GroupBy)
		for i, col := range groupByList {
			groupByList[i] = col
			if !reColnamePrefixedTblname.MatchString(col) {
				groupByList[i] = optTableAlias + col
			}
		}
		groupClause = strings.Join(groupByList, ",")
	}
	if groupClause != "" {
		sqlStm += fmt.Sprintf(" GROUP BY %s", groupClause)
	}

	havingClause := ""
	if b.Having != nil {
		newOpts := append([]interface{}{OptDbFlavor{b.Flavor}}, opts...)
		havingClause, tempValues = b.Having.Build(b.PlaceholderGenerator, newOpts...)
		values = append(values, tempValues...)
	}
	if havingClause != "" {
		sqlStm += fmt.Sprintf(" HAVING %s", havingClause)
	}

	orderClause := ""
	if b.Sorting != nil {
		orderClause = b.Sorting.Build(opts...)
	}
	if orderClause != "" {
		sqlStm += fmt.Sprintf(" ORDER BY %s", orderClause)
	}

	if b.LimitNumRows != 0 || b.LimitOffset != 0 {
		switch b.Flavor {
		case sql.FlavorMySql, sql.FlavorSqlite:
			sqlStm += fmt.Sprintf(" LIMIT %d,%d", b.LimitOffset, b.LimitNumRows)
		case sql.FlavorPgSql:
			sqlStm += fmt.Sprintf(" LIMIT %d OFFSET %d", b.LimitNumRows, b.LimitOffset)
		case sql.FlavorMsSql:
			if orderClause != "" {
				// available since SQL Server 2012 && Azure SQL Database
				sqlStm += fmt.Sprintf(" OFFSET %d ROWS FETCH NEXT %d ROWS ONLY", b.LimitOffset, b.LimitNumRows)
			}
		case sql.FlavorOracle:
			sqlStm += fmt.Sprintf(" OFFSET %d ROWS FETCH NEXT %d ROWS ONLY", b.LimitOffset, b.LimitNumRows)
		case sql.FlavorCosmosDb:
			sqlStm += fmt.Sprintf(" OFFSET %d LIMIT %d", b.LimitOffset, b.LimitNumRows)
		}
	}

	if b.Flavor == sql.FlavorCosmosDb {
		/* START: special case for gocosmos */
		// sqlStm += " WITH collection=" + cosmosdbTblName
		sqlStm += " WITH cross_partition=true"
		/* END: special case for gocosmos */
	}

	return sqlStm, values
}

/*----------------------------------------------------------------------*/

// InsertBuilder is a builder that helps building INSERT sql statement.
type InsertBuilder struct {
	BaseSqlBuilder
	Values map[string]interface{}
}

// NewInsertBuilder constructs a new InsertBuilder.
func NewInsertBuilder() *InsertBuilder {
	return &InsertBuilder{}
}

// WithFlavor sets the SQL flavor that affect the generated SQL statement.
//
// Note: WithFlavor will reset the PlaceholderGenerator
func (b *InsertBuilder) WithFlavor(flavor sql.DbFlavor) *InsertBuilder {
	b.BaseSqlBuilder.WithFlavor(flavor)
	return b
}

// WithTable sets name of the database table used to generate the SQL statement.
func (b *InsertBuilder) WithTable(table string) *InsertBuilder {
	b.BaseSqlBuilder.WithTable(table)
	return b
}

// WithPlaceholderGenerator sets the placeholder generator used to generate placeholders in the SQL statement.
func (b *InsertBuilder) WithPlaceholderGenerator(placeholderGenerator PlaceholderGenerator) *InsertBuilder {
	b.BaseSqlBuilder.WithPlaceholderGenerator(placeholderGenerator)
	return b
}

// WithValues sets list of column/value pairs used to generate the SQL statement.
func (b *InsertBuilder) WithValues(values map[string]interface{}) *InsertBuilder {
	b.Values = make(map[string]interface{})
	if values != nil {
		for k, v := range values {
			b.Values[k] = v
		}
	}
	return b
}

// AddValues adds column/value pairs to the existing list.
func (b *InsertBuilder) AddValues(values map[string]interface{}) *InsertBuilder {
	if b.Values == nil {
		b.Values = make(map[string]interface{})
	}
	if values != nil {
		for k, v := range values {
			b.Values[k] = v
		}
	}
	return b
}

// Build constructs the INSERT sql statement, in the following format:
//
//     INSERT INTO <table> (<columns>) VALUES (<placeholders>)
//
// (since v0.3.0) the generated INSERT statement works with MySQL, MSSQL, PostgreSQL, Oracle, SQLite and btnguyen2k/gocosmos.
func (b *InsertBuilder) Build(opts ...interface{}) (string, []interface{}) {
	if b.Flavor == sql.FlavorCosmosDb {
		opts = removeOptTableAlias(opts...)
	}
	cols := make([]string, 0, len(b.Values))
	placeholders := make([]string, 0, len(b.Values))
	values := make([]interface{}, 0, len(b.Values))

	for k := range b.Values {
		cols = append(cols, k)
	}
	sort.Strings(cols)
	for _, col := range cols {
		values = append(values, b.Values[col])
		placeholders = append(placeholders, b.PlaceholderGenerator(col))
	}

	tableAliasForField := extractOptTableAlias(opts...)
	tableAlias := ""
	if tableAliasForField != "" {
		if !reTblNameWithAlias.MatchString(b.Table) {
			tableAlias = " " + tableAliasForField[:len(tableAliasForField)-1]
		}
		for i, _ := range cols {
			if !reColnamePrefixedTblname.MatchString(cols[i]) {
				cols[i] = tableAliasForField + cols[i]
			}
		}
	}
	sql := fmt.Sprintf("INSERT INTO %s%s (%s) VALUES (%s)", b.Table, tableAlias, strings.Join(cols, ","), strings.Join(placeholders, ","))
	return sql, values
}

/*----------------------------------------------------------------------*/

// UpdateBuilder is a builder that helps building INSERT sql statement.
type UpdateBuilder struct {
	BaseSqlBuilder
	Values map[string]interface{}
	Filter IFilter
}

// NewUpdateBuilder constructs a new UpdateBuilder.
func NewUpdateBuilder() *UpdateBuilder {
	return &UpdateBuilder{}
}

// WithFlavor sets the SQL flavor that affect the generated SQL statement.
//
// Note: WithFlavor will reset the PlaceholderGenerator
func (b *UpdateBuilder) WithFlavor(flavor sql.DbFlavor) *UpdateBuilder {
	b.BaseSqlBuilder.WithFlavor(flavor)
	return b
}

// WithTable sets name of the database table used to generate the SQL statement.
func (b *UpdateBuilder) WithTable(table string) *UpdateBuilder {
	b.BaseSqlBuilder.WithTable(table)
	return b
}

// WithPlaceholderGenerator sets the placeholder generator used to generate placeholders in the SQL statement.
func (b *UpdateBuilder) WithPlaceholderGenerator(placeholderGenerator PlaceholderGenerator) *UpdateBuilder {
	b.BaseSqlBuilder.WithPlaceholderGenerator(placeholderGenerator)
	return b
}

// WithValues sets list of column/value pairs used to generate the SQL statement.
func (b *UpdateBuilder) WithValues(values map[string]interface{}) *UpdateBuilder {
	b.Values = make(map[string]interface{})
	if values != nil {
		for k, v := range values {
			b.Values[k] = v
		}
	}
	return b
}

// AddValues adds column/value pairs to the existing list.
func (b *UpdateBuilder) AddValues(values map[string]interface{}) *UpdateBuilder {
	if b.Values == nil {
		b.Values = make(map[string]interface{})
	}
	if values != nil {
		for k, v := range values {
			b.Values[k] = v
		}
	}
	return b
}

// WithFilter sets the filter used to generate the WHERE clause.
func (b *UpdateBuilder) WithFilter(filter IFilter) *UpdateBuilder {
	b.Filter = filter
	return b
}

// Build constructs the UPDATE sql statement, in the following format:
//
//     UPDATE <table> SET <col=value>[,<col=value>...] [WHERE <filter>]
//
// (since v0.3.0) the generated DELETE statement works with MySQL, MSSQL, PostgreSQL, Oracle, SQLite and btnguyen2k/gocosmos.
func (b *UpdateBuilder) Build(opts ...interface{}) (string, []interface{}) {
	if b.Flavor == sql.FlavorCosmosDb {
		opts = removeOptTableAlias(opts...)
	}
	tableAliasForField := extractOptTableAlias(opts...)
	tableAlias := ""
	if tableAliasForField != "" {
		if !reTblNameWithAlias.MatchString(b.Table) {
			tableAlias = " " + tableAliasForField[:len(tableAliasForField)-1]
		}
	}

	sql := fmt.Sprintf("UPDATE %s%s", b.Table, tableAlias)
	values := make([]interface{}, 0, len(b.Values))
	cols := make([]string, 0, len(b.Values))
	for k := range b.Values {
		cols = append(cols, k)
	}
	sort.Strings(cols)
	setList := make([]string, 0)
	for _, col := range cols {
		values = append(values, b.Values[col])
		alias := tableAliasForField
		if reColnamePrefixedTblname.MatchString(col) {
			alias = ""
		}
		setList = append(setList, fmt.Sprintf("%s%s=%s", alias, col, b.PlaceholderGenerator(col)))
	}
	sql += " SET " + strings.Join(setList, ",")

	whereClause := ""
	if b.Filter != nil {
		newOpts := append([]interface{}{OptDbFlavor{Flavor: b.Flavor}}, opts...)
		var tempValues []interface{}
		whereClause, tempValues = b.Filter.Build(b.PlaceholderGenerator, newOpts...)
		values = append(values, tempValues...)
	}
	if whereClause != "" {
		sql += " WHERE " + whereClause
	}

	return sql, values
}
