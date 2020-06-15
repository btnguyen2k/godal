package sql

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/btnguyen2k/prom"
)

/*
PlaceholderGenerator is a function that generates placeholder used in prepared statement.
*/
type PlaceholderGenerator func(field string) string

/*
NewPlaceholderGenerator is a function that creates PlaceholderGenerator
*/
type NewPlaceholderGenerator func() PlaceholderGenerator

/*
NewPlaceholderGeneratorQuestion creates a placeholder function that uses "?" as placeholder.

Note: "?" placeholder is used by MySQL.
*/
func NewPlaceholderGeneratorQuestion() PlaceholderGenerator {
	return func(field string) string {
		return "?"
	}
}

/*
NewPlaceholderGeneratorDollarN creates a placeholder function that uses "$<n>" as placeholder.

Note: "$<n>" placeholder is used by PostgreSQL.
*/
func NewPlaceholderGeneratorDollarN() PlaceholderGenerator {
	n := 0
	return func(field string) string {
		n++
		return "$" + strconv.Itoa(n)
	}
}

/*
NewPlaceholderGeneratorColonN creates a placeholder function that uses ":<n>" as placeholder.

Note: ":<n>" placeholder is used by Oracle.
*/
func NewPlaceholderGeneratorColonN() PlaceholderGenerator {
	n := 0
	return func(field string) string {
		n++
		return ":" + strconv.Itoa(n)
	}
}

/*
NewPlaceholderGeneratorAtpiN creates a placeholder function that uses "@p<n>" as placeholder.

Note: "@p<n>" placeholder is used by MSSQL.
*/
func NewPlaceholderGeneratorAtpiN() PlaceholderGenerator {
	n := 0
	return func(field string) string {
		n++
		return "@p" + strconv.Itoa(n)
	}
}

/*----------------------------------------------------------------------*/

/*
OptionOpLiteral controls literal forms of operations.
*/
type OptionOpLiteral struct {
	OpAnd      string // 'and' operation, default is "AND"
	OpOr       string // 'or' operation, default is "OR"
	OpEqual    string // 'equal' operation, default is "="
	OpNotEqual string // 'not equal' operation, default is "!="
}

var defaultOptionLiteralOperation = &OptionOpLiteral{
	OpAnd:      "AND",
	OpOr:       "OR",
	OpEqual:    "=",
	OpNotEqual: "!=",
}

/*----------------------------------------------------------------------*/

/*
ISorting provides API interface to build elements of 'ORDER BY' clause.
*/
type ISorting interface {
	/*
		Build builds the 'ORDER BY' clause (without "ORDER BY" keyword).
	*/
	Build() string
}

/*
GenericSorting is a generic implementation of #ISorting.
*/
type GenericSorting struct {
	Flavor prom.DbFlavor
	// Ordering defines list of fields to sort on. Field is in the following format: <field_name[<:order>]>, where 'order>=0' means 'ascending' and 'order<0' means 'descending'.
	Ordering []string
}

/*
Add appends an ordering element to the list.
*/
func (o *GenericSorting) Add(order string) *GenericSorting {
	if order != "" {
		o.Ordering = append(o.Ordering, order)
	}
	return o
}

/*
Build implements ISorting.Build()
*/
func (o *GenericSorting) Build() string {
	if o.Ordering == nil || len(o.Ordering) == 0 {
		return ""
	}
	elements := make([]string, 0)
	for _, v := range o.Ordering {
		tokens := strings.Split(v, ":")
		order := tokens[0]
		if len(tokens) > 1 {
			ord := strings.ToUpper(tokens[1])
			if ord == "ASC" || ord == "DESC" {
				order += " " + tokens[1]
			} else if ord != "" && ord[0] == '-' {
				order += " DESC"
			}
		}
		elements = append(elements, order)
	}
	return strings.Join(elements, ",")
}

/*----------------------------------------------------------------------*/

/*
IFilter provides API interface to build element of 'WHERE' clause for SQL statement
*/
type IFilter interface {
	/*
		Build builds the 'WHERE' clause (without "WHERE" keyword) with placeholders and list of values for placeholders in order.
	*/
	Build(placeholderGenerator PlaceholderGenerator) (string, []interface{})
}

var ifilterType = reflect.TypeOf((*IFilter)(nil)).Elem()
var isortingType = reflect.TypeOf((*ISorting)(nil)).Elem()

/*
FilterAnd combines two or more filters using AND clause.
*/
type FilterAnd struct {
	Filters  []IFilter
	Operator string // literal form or the 'and' operator, default is "AND"
}

/*
Add appends a filter to the list.
*/
func (f *FilterAnd) Add(filter IFilter) *FilterAnd {
	if filter != nil {
		f.Filters = append(f.Filters, filter)
	}
	return f
}

/*
Build implements IFilter.Build()
*/
func (f *FilterAnd) Build(placeholderGenerator PlaceholderGenerator) (string, []interface{}) {
	if f.Filters == nil || len(f.Filters) == 0 {
		return "", make([]interface{}, 0)
	}

	op := " AND "
	if f.Operator != "" {
		op = " " + f.Operator + " "
	}
	clause, values := f.Filters[0].Build(placeholderGenerator)

	for k, v := range f.Filters {
		if k > 0 {
			c, v := v.Build(placeholderGenerator)
			clause += op + c
			values = append(values, v...)
		}
	}

	return "(" + clause + ")", values
}

/*
FilterOr combines two filters using OR clause.
*/
type FilterOr struct {
	Filters  []IFilter
	Operator string // literal form or the 'or' operator, default is "OR"
}

/*
Add appends a filter to the list.
*/
func (f *FilterOr) Add(filter IFilter) *FilterOr {
	if filter != nil {
		f.Filters = append(f.Filters, filter)
	}
	return f
}

/*
Build implements IFilter.Build()
*/
func (f *FilterOr) Build(placeholderGenerator PlaceholderGenerator) (string, []interface{}) {
	if f.Filters == nil || len(f.Filters) == 0 {
		return "", make([]interface{}, 0)
	}

	op := " OR "
	if f.Operator != "" {
		op = " " + f.Operator + " "
	}
	clause, values := f.Filters[0].Build(placeholderGenerator)

	for k, v := range f.Filters {
		if k > 0 {
			c, v := v.Build(placeholderGenerator)
			clause += op + c
			values = append(values, v...)
		}
	}

	return "(" + clause + ")", values
}

/*
FilterFieldValue represents single filter <field> <operation> <value>.
*/
type FilterFieldValue struct {
	Field     string      // field to check
	Operation string      // operation to perform
	Value     interface{} // value to test against
}

/*
Build implements IFilter.Build()
*/
func (f *FilterFieldValue) Build(placeholderGenerator PlaceholderGenerator) (string, []interface{}) {
	values := make([]interface{}, 0)
	values = append(values, f.Value)
	clause := f.Field + " " + f.Operation + " " + placeholderGenerator(f.Field)
	return clause, values
}

/*
FilterExpression represents single filter <left> <operation> <right>.
*/
type FilterExpression struct {
	Left, Right string // left & right parts of the expression
	Operation   string // operation to perform
}

/*
Build implements IFilter.Build()
*/
func (f *FilterExpression) Build(placeholderGenerator PlaceholderGenerator) (string, []interface{}) {
	clause := f.Left + " " + f.Operation + " " + f.Right
	return clause, make([]interface{}, 0)
}

/*----------------------------------------------------------------------*/

type ISqlBuilder interface {
	Build() (string, []interface{})
}

/*
DeleteBuilder is a builder that helps building DELETE sql statement.
*/
type DeleteBuilder struct {
	Flavor               prom.DbFlavor
	Table                string
	Filter               IFilter
	PlaceholderGenerator PlaceholderGenerator
}

/*
NewDeleteBuilder constructs a new DeleteBuilder.
*/
func NewDeleteBuilder() *DeleteBuilder {
	return &DeleteBuilder{}
}

/*
WithFlavor sets the SqlFlavor that affect the generated SQL statement.

Note: WithFlavor will reset the PlaceholderGenerator
*/
func (b *DeleteBuilder) WithFlavor(flavor prom.DbFlavor) *DeleteBuilder {
	b.Flavor = flavor
	switch flavor {
	case prom.FlavorMySql:
		b.PlaceholderGenerator = NewPlaceholderGeneratorQuestion()
	case prom.FlavorPgSql:
		b.PlaceholderGenerator = NewPlaceholderGeneratorDollarN()
	case prom.FlavorMsSql:
		b.PlaceholderGenerator = NewPlaceholderGeneratorAtpiN()
	case prom.FlavorOracle:
		b.PlaceholderGenerator = NewPlaceholderGeneratorColonN()
	default:
		b.PlaceholderGenerator = NewPlaceholderGeneratorQuestion()
	}
	return b
}

/*
WithTable sets name of the database table used to generate the SQL statement.
*/
func (b *DeleteBuilder) WithTable(table string) *DeleteBuilder {
	b.Table = table
	return b
}

/*
WithFilter sets the filter used to generate the WHERE clause.
*/
func (b *DeleteBuilder) WithFilter(filter IFilter) *DeleteBuilder {
	b.Filter = filter
	return b
}

/*
WithPlaceholderGenerator sets the placeholder generator used to generate placeholders in the SQL statement.
*/
func (b *DeleteBuilder) WithPlaceholderGenerator(placeholderGenerator PlaceholderGenerator) *DeleteBuilder {
	b.PlaceholderGenerator = placeholderGenerator
	return b
}

/*
Build constructs the DELETE sql statement, in the following format:

	DELETE FROM <table> [WHERE <filter>]
*/
func (b *DeleteBuilder) Build() (string, []interface{}) {
	if b.Filter != nil {
		whereClause, values := b.Filter.Build(b.PlaceholderGenerator)
		sql := fmt.Sprintf("DELETE FROM %s WHERE %s", b.Table, whereClause)
		return sql, values
	}
	sql := fmt.Sprintf("DELETE FROM %s", b.Table)
	return sql, make([]interface{}, 0)
}

/*----------------------------------------------------------------------*/
/*
SelectBuilder is a builder that helps building SELECT sql statement.
*/
type SelectBuilder struct {
	Flavor                    prom.DbFlavor
	Columns                   []string
	Tables                    []string
	Filter                    IFilter
	GroupBy                   []string
	Having                    IFilter
	LimitNumRows, LimitOffset int
	PlaceholderGenerator      PlaceholderGenerator
	Sorting                   ISorting
}

/*
NewSelectBuilder constructs a new SelectBuilder.
*/
func NewSelectBuilder() *SelectBuilder {
	return &SelectBuilder{}
}

/*
WithFlavor sets the SqlFlavor that affect the generated SQL statement.

Note: WithFlavor will reset the PlaceholderGenerator
*/
func (b *SelectBuilder) WithFlavor(flavor prom.DbFlavor) *SelectBuilder {
	b.Flavor = flavor
	switch flavor {
	case prom.FlavorMySql:
		b.PlaceholderGenerator = NewPlaceholderGeneratorQuestion()
	case prom.FlavorPgSql:
		b.PlaceholderGenerator = NewPlaceholderGeneratorDollarN()
	case prom.FlavorMsSql:
		b.PlaceholderGenerator = NewPlaceholderGeneratorAtpiN()
	case prom.FlavorOracle:
		b.PlaceholderGenerator = NewPlaceholderGeneratorColonN()
	default:
		b.PlaceholderGenerator = NewPlaceholderGeneratorQuestion()
	}
	return b
}

/*
WithColumns sets list of table columns used to generate the SQL statement.
*/
func (b *SelectBuilder) WithColumns(columns ...string) *SelectBuilder {
	b.Columns = make([]string, len(columns))
	copy(b.Columns, columns)
	return b
}

/*
AddColumns appends columns to the existing list.
*/
func (b *SelectBuilder) AddColumns(columns ...string) *SelectBuilder {
	b.Columns = append(b.Columns, columns...)
	return b
}

/*
WithTables sets list of database tables used to generate the SQL statement.
*/
func (b *SelectBuilder) WithTables(tables ...string) *SelectBuilder {
	b.Tables = make([]string, len(tables))
	copy(b.Tables, tables)
	return b
}

/*
AddTables appends tables to the existing list.
*/
func (b *SelectBuilder) AddTables(tables ...string) *SelectBuilder {
	b.Tables = append(b.Tables, tables...)
	return b
}

/*
WithFilter sets the filter used to generate the WHERE clause.
*/
func (b *SelectBuilder) WithFilter(filter IFilter) *SelectBuilder {
	b.Filter = filter
	return b
}

/*
WithGroupBy sets list of fields used to generate the GROUP BY clause.
*/
func (b *SelectBuilder) WithGroupBy(fields ...string) *SelectBuilder {
	b.GroupBy = make([]string, len(fields))
	copy(b.GroupBy, fields)
	return b
}

/*
AddGroupBy appends fields to the existing list.
*/
func (b *SelectBuilder) AddGroupBy(fields ...string) *SelectBuilder {
	b.GroupBy = append(b.GroupBy, fields...)
	return b
}

/*
WithHaving sets the filter used to generate the HAVING clause.
*/
func (b *SelectBuilder) WithHaving(having IFilter) *SelectBuilder {
	b.Having = having
	return b
}

/*
WithPlaceholderGenerator sets the placeholder generator used to generate placeholders in the SQL statement.
*/
func (b *SelectBuilder) WithPlaceholderGenerator(placeholderGenerator PlaceholderGenerator) *SelectBuilder {
	b.PlaceholderGenerator = placeholderGenerator
	return b
}

/*
WithSorting sets sorting builder used to generate the ORDER BY clause.
*/
func (b *SelectBuilder) WithSorting(sorting ISorting) *SelectBuilder {
	b.Sorting = sorting
	return b
}

/*
WithLimit sets the value to generate the LIMIT/OFFSET clause.
*/
func (b *SelectBuilder) WithLimit(numRows, offset int) *SelectBuilder {
	b.LimitNumRows = numRows
	b.LimitOffset = offset
	return b
}

/*
Build constructs the SELECT sql statement, in the following format:

	SELECT <columns> FROM <tables>
	[WHERE <filter>]
	[GROUP BY <group-by>]
	[HAVING <having>]
	[ORDER BY <sorting>]
	[LIMIT <limit>]
*/
func (b *SelectBuilder) Build() (string, []interface{}) {
	cols := strings.Join(allColumns, ",")
	if b.Columns != nil && len(b.Columns) > 0 {
		cols = strings.Join(b.Columns, ",")
	}
	tables := strings.Join(b.Tables, ",")
	sql := fmt.Sprintf("SELECT %s FROM %s", cols, tables)
	values := make([]interface{}, 0)
	var tempValues []interface{}

	whereClause := ""
	if b.Filter != nil {
		whereClause, tempValues = b.Filter.Build(b.PlaceholderGenerator)
		values = append(values, tempValues...)
	}
	if whereClause != "" {
		sql += " WHERE " + whereClause
	}

	groupClause := ""
	if b.GroupBy != nil && len(b.GroupBy) > 0 {
		groupClause = strings.Join(b.GroupBy, ",")
	}
	if groupClause != "" {
		sql += " GROUP BY " + groupClause
	}

	havingClause := ""
	if b.Having != nil {
		havingClause, tempValues = b.Having.Build(b.PlaceholderGenerator)
		values = append(values, tempValues...)
	}
	if havingClause != "" {
		sql += " HAVING " + havingClause
	}

	orderClause := ""
	if b.Sorting != nil {
		orderClause = b.Sorting.Build()
	}
	if orderClause != "" {
		sql += " ORDER BY " + orderClause
	}

	if b.LimitNumRows != 0 {
		switch b.Flavor {
		case prom.FlavorMySql:
			sql += " LIMIT " + strconv.Itoa(b.LimitOffset) + "," + strconv.Itoa(b.LimitNumRows)
		case prom.FlavorPgSql:
			sql += " LIMIT " + strconv.Itoa(b.LimitNumRows) + " OFFSET " + strconv.Itoa(b.LimitOffset)
		case prom.FlavorMsSql:
			if orderClause != "" {
				// available since SQL Server 2012 && Azure SQL Database
				sql += " OFFSET " + strconv.Itoa(b.LimitOffset) + " ROWS FETCH NEXT " + strconv.Itoa(b.LimitNumRows) + " ROWS ONLY"
			}
		case prom.FlavorOracle:
			sql += " OFFSET " + strconv.Itoa(b.LimitOffset) + " ROWS FETCH NEXT " + strconv.Itoa(b.LimitNumRows) + " ROWS ONLY"
		}
	}

	return sql, values
}

/*----------------------------------------------------------------------*/

/*
InsertBuilder is a builder that helps building INSERT sql statement.
*/
type InsertBuilder struct {
	Flavor               prom.DbFlavor
	Table                string
	Values               map[string]interface{}
	PlaceholderGenerator PlaceholderGenerator
}

/*
NewInsertBuilder constructs a new InsertBuilder.
*/
func NewInsertBuilder() *InsertBuilder {
	return &InsertBuilder{}
}

/*
WithFlavor sets the SqlFlavor that affect the generated SQL statement.

Note: WithFlavor will reset the PlaceholderGenerator
*/
func (b *InsertBuilder) WithFlavor(flavor prom.DbFlavor) *InsertBuilder {
	b.Flavor = flavor
	switch flavor {
	case prom.FlavorMySql:
		b.PlaceholderGenerator = NewPlaceholderGeneratorQuestion()
	case prom.FlavorPgSql:
		b.PlaceholderGenerator = NewPlaceholderGeneratorDollarN()
	case prom.FlavorMsSql:
		b.PlaceholderGenerator = NewPlaceholderGeneratorAtpiN()
	case prom.FlavorOracle:
		b.PlaceholderGenerator = NewPlaceholderGeneratorColonN()
	default:
		b.PlaceholderGenerator = NewPlaceholderGeneratorQuestion()
	}
	return b
}

/*
WithTable sets name of the database table used to generate the SQL statement.
*/
func (b *InsertBuilder) WithTable(table string) *InsertBuilder {
	b.Table = table
	return b
}

/*
WithValues sets list of column/value pairs used to generate the SQL statement.
*/
func (b *InsertBuilder) WithValues(values map[string]interface{}) *InsertBuilder {
	b.Values = make(map[string]interface{})
	if values != nil {
		for k, v := range values {
			b.Values[k] = v
		}
	}
	return b
}

/*
AddValues adds column/value pairs to the existing list.
*/
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

/*
WithPlaceholderGenerator sets the placeholder generator used to generate placeholders in the SQL statement.
*/
func (b *InsertBuilder) WithPlaceholderGenerator(placeholderGenerator PlaceholderGenerator) *InsertBuilder {
	b.PlaceholderGenerator = placeholderGenerator
	return b
}

/*
Build constructs the INSERT sql statement, in the following format:

	INSERT INTO <table> (<columns>) VALUES (<placeholders>)
*/
func (b *InsertBuilder) Build() (string, []interface{}) {
	cols := make([]string, 0)
	placeholders := make([]string, 0)
	values := make([]interface{}, 0)
	for k, v := range b.Values {
		cols = append(cols, k)
		values = append(values, v)
		placeholders = append(placeholders, b.PlaceholderGenerator(k))
	}
	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", b.Table, strings.Join(cols, ","), strings.Join(placeholders, ","))
	return sql, values
}

/*----------------------------------------------------------------------*/

/*
UpdateBuilder is a builder that helps building INSERT sql statement.
*/
type UpdateBuilder struct {
	Flavor               prom.DbFlavor
	Table                string
	Values               map[string]interface{}
	Filter               IFilter
	PlaceholderGenerator PlaceholderGenerator
}

/*
NewUpdateBuilder constructs a new UpdateBuilder.
*/
func NewUpdateBuilder() *UpdateBuilder {
	return &UpdateBuilder{}
}

/*
WithFlavor sets the SqlFlavor that affect the generated SQL statement.

Note: WithFlavor will reset the PlaceholderGenerator
*/
func (b *UpdateBuilder) WithFlavor(flavor prom.DbFlavor) *UpdateBuilder {
	b.Flavor = flavor
	switch flavor {
	case prom.FlavorMySql:
		b.PlaceholderGenerator = NewPlaceholderGeneratorQuestion()
	case prom.FlavorPgSql:
		b.PlaceholderGenerator = NewPlaceholderGeneratorDollarN()
	case prom.FlavorMsSql:
		b.PlaceholderGenerator = NewPlaceholderGeneratorAtpiN()
	case prom.FlavorOracle:
		b.PlaceholderGenerator = NewPlaceholderGeneratorColonN()
	default:
		b.PlaceholderGenerator = NewPlaceholderGeneratorQuestion()
	}
	return b
}

/*
WithTable sets name of the database table used to generate the SQL statement.
*/
func (b *UpdateBuilder) WithTable(table string) *UpdateBuilder {
	b.Table = table
	return b
}

/*
WithValues sets list of column/value pairs used to generate the SQL statement.
*/
func (b *UpdateBuilder) WithValues(values map[string]interface{}) *UpdateBuilder {
	b.Values = make(map[string]interface{})
	if values != nil {
		for k, v := range values {
			b.Values[k] = v
		}
	}
	return b
}

/*
AddValues adds column/value pairs to the existing list.
*/
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

/*
WithFilter sets the filter used to generate the WHERE clause.
*/
func (b *UpdateBuilder) WithFilter(filter IFilter) *UpdateBuilder {
	b.Filter = filter
	return b
}

/*
WithPlaceholderGenerator sets the placeholder generator used to generate placeholders in the SQL statement.
*/
func (b *UpdateBuilder) WithPlaceholderGenerator(placeholderGenerator PlaceholderGenerator) *UpdateBuilder {
	b.PlaceholderGenerator = placeholderGenerator
	return b
}

/*
Build constructs the UPDATE sql statement, in the following format:

	UPDATE <table> SET <col=value>[,<col=value>...] [WHERE <filter>]
*/
func (b *UpdateBuilder) Build() (string, []interface{}) {
	sql := fmt.Sprintf("UPDATE %s", b.Table)
	values := make([]interface{}, 0)
	tempValues := make([]interface{}, 0)

	setList := make([]string, 0)
	for k, v := range b.Values {
		values = append(values, v)
		setList = append(setList, k+"="+b.PlaceholderGenerator(k))
	}
	sql += " SET " + strings.Join(setList, ",")

	whereClause := ""
	if b.Filter != nil {
		whereClause, tempValues = b.Filter.Build(b.PlaceholderGenerator)
		values = append(values, tempValues...)
	}
	if whereClause != "" {
		sql += " WHERE " + whereClause
	}

	return sql, values
}
