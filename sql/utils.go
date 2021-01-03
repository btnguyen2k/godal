package sql

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/btnguyen2k/prom"
)

// PlaceholderGenerator is a function that generates placeholder used in prepared statement.
type PlaceholderGenerator func(field string) string

// NewPlaceholderGenerator is a function that creates PlaceholderGenerator
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
	n := 0
	return func(field string) string {
		n++
		return "$" + strconv.Itoa(n)
	}
}

// NewPlaceholderGeneratorColonN creates a placeholder function that uses ":<n>" as placeholder.
//
// Note: ":<n>" placeholder is used by Oracle.
func NewPlaceholderGeneratorColonN() PlaceholderGenerator {
	n := 0
	return func(field string) string {
		n++
		return ":" + strconv.Itoa(n)
	}
}

// NewPlaceholderGeneratorAtpiN creates a placeholder function that uses "@p<n>" as placeholder.
//
// Note: "@p<n>" placeholder is used by MSSQL.
func NewPlaceholderGeneratorAtpiN() PlaceholderGenerator {
	n := 0
	return func(field string) string {
		n++
		return "@p" + strconv.Itoa(n)
	}
}

/*----------------------------------------------------------------------*/

// OptionOpLiteral controls literal forms of operations.
type OptionOpLiteral struct {
	OpAnd      string // 'and' operation, default is "AND"
	OpOr       string // 'or' operation, default is "OR"
	OpEqual    string // 'equal' operation, default is "="
	OpNotEqual string // 'not equal' operation, default is "!="
}

// DefaultOptionLiteralOperation uses "AND" for 'and' operation, "OR" for 'or' operation, "=" for equal and "!=" for not equal.
var DefaultOptionLiteralOperation = &OptionOpLiteral{
	OpAnd:      "AND",
	OpOr:       "OR",
	OpEqual:    "=",
	OpNotEqual: "!=",
}

/*----------------------------------------------------------------------*/

// ISorting provides API interface to build elements of 'ORDER BY' clause.
type ISorting interface {
	// Build builds the 'ORDER BY' clause (without "ORDER BY" keyword).
	Build() string
}

// GenericSorting is a generic implementation of ISorting.
type GenericSorting struct {
	Flavor prom.DbFlavor
	// Ordering defines list of fields to sort on. Field is in the following format: <field_name[<:order>]>, where 'order>=0' means 'ascending' and 'order<0' means 'descending'.
	Ordering []string
}

// Add appends an ordering element to the list.
func (o *GenericSorting) Add(order string) *GenericSorting {
	if strings.TrimSpace(order) != "" {
		o.Ordering = append(o.Ordering, strings.TrimSpace(order))
	}
	return o
}

// Build implements ISorting.Build.
func (o *GenericSorting) Build() string {
	if o.Ordering == nil || len(o.Ordering) == 0 {
		return ""
	}
	elements := make([]string, 0)
	for _, v := range o.Ordering {
		tokens := strings.Split(v, ":")
		order := tokens[0]
		if len(tokens) > 1 {
			ord := strings.TrimSpace(strings.ToUpper(tokens[1]))
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

// IFilter provides API interface to build element of 'WHERE' clause for SQL statement.
type IFilter interface {
	// Build builds the 'WHERE' clause (without "WHERE" keyword) with placeholders and list of values for placeholders in order.
	Build(placeholderGenerator PlaceholderGenerator) (string, []interface{})
}

var ifilterType = reflect.TypeOf((*IFilter)(nil)).Elem()
var isortingType = reflect.TypeOf((*ISorting)(nil)).Elem()

// FilterAndOr combines two or more filters using AND/OR clause.
// It is recommended to use FilterAnd and FilterOr instead of using FilterAndOr directly.
//
// Available since v0.3.0
type FilterAndOr struct {
	Filters  []IFilter
	Operator string // literal form for the operator
}

// Add appends a filter to the list.
func (f *FilterAndOr) Add(filter IFilter) *FilterAndOr {
	if filter != nil {
		f.Filters = append(f.Filters, filter)
	}
	return f
}

// Build implements IFilter.Build.
func (f *FilterAndOr) Build(placeholderGenerator PlaceholderGenerator) (string, []interface{}) {
	if f.Filters == nil || len(f.Filters) == 0 {
		return "", make([]interface{}, 0)
	}

	op := " " + strings.TrimSpace(f.Operator) + " "
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

// FilterAnd combines two or more filters using AND clause.
type FilterAnd struct {
	FilterAndOr
}

// Add appends a filter to the list.
func (f *FilterAnd) Add(filter IFilter) *FilterAnd {
	f.FilterAndOr.Add(filter)
	return f
}

// Build implements IFilter.Build.
func (f *FilterAnd) Build(placeholderGenerator PlaceholderGenerator) (string, []interface{}) {
	if strings.TrimSpace(f.Operator) == "" {
		f.Operator = "AND"
	}
	return f.FilterAndOr.Build(placeholderGenerator)
}

// FilterOr combines two filters using OR clause.
type FilterOr struct {
	FilterAndOr
}

// Add appends a filter to the list.
func (f *FilterOr) Add(filter IFilter) *FilterOr {
	f.FilterAndOr.Add(filter)
	return f
}

// Build implements IFilter.Build.
func (f *FilterOr) Build(placeholderGenerator PlaceholderGenerator) (string, []interface{}) {
	if strings.TrimSpace(f.Operator) == "" {
		f.Operator = "OR"
	}
	return f.FilterAndOr.Build(placeholderGenerator)
}

// FilterFieldValue represents single filter: <field> <operation> <value>.
type FilterFieldValue struct {
	Field     string      // field to check
	Operation string      // operation to perform
	Value     interface{} // value to test against
}

// Build implements IFilter.Build.
func (f *FilterFieldValue) Build(placeholderGenerator PlaceholderGenerator) (string, []interface{}) {
	values := make([]interface{}, 0)
	values = append(values, f.Value)
	clause := f.Field + " " + strings.TrimSpace(f.Operation) + " " + placeholderGenerator(f.Field)
	return clause, values
}

// FilterExpression represents single filter: <left> <operation> <right>.
type FilterExpression struct {
	Left, Right string // left & right parts of the expression
	Operation   string // operation to perform
}

// Build implements IFilter.Build.
func (f *FilterExpression) Build(_ PlaceholderGenerator) (string, []interface{}) {
	clause := f.Left + " " + strings.TrimSpace(f.Operation) + " " + f.Right
	return clause, make([]interface{}, 0)
}

/*----------------------------------------------------------------------*/

// ISqlBuilder provides API interface to build the SQL statement.
type ISqlBuilder interface {
	Build() (string, []interface{})
}

// BaseSqlBuilder is the base struct to implement DeleteBuilder, InsertBuilder, SelectBuilder and UpdateBuilder.
//
// Available since v0.3.0
type BaseSqlBuilder struct {
	Flavor               prom.DbFlavor
	PlaceholderGenerator PlaceholderGenerator
	Table                string
}

// WithFlavor sets the SQL flavor that affect the generated SQL statement.
//
// Note: WithFlavor will reset the PlaceholderGenerator
func (b *BaseSqlBuilder) WithFlavor(flavor prom.DbFlavor) *BaseSqlBuilder {
	b.Flavor = flavor
	switch flavor {
	case prom.FlavorPgSql, prom.FlavorCosmosDb:
		b.PlaceholderGenerator = NewPlaceholderGeneratorDollarN()
	case prom.FlavorMsSql:
		b.PlaceholderGenerator = NewPlaceholderGeneratorAtpiN()
	case prom.FlavorOracle:
		b.PlaceholderGenerator = NewPlaceholderGeneratorColonN()
	case prom.FlavorMySql, prom.FlavorSqlite:
		b.PlaceholderGenerator = NewPlaceholderGeneratorQuestion()
	default:
		b.PlaceholderGenerator = NewPlaceholderGeneratorQuestion()
	}
	return b
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

// DeleteBuilder is a builder that helps building DELETE sql statement.
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
func (b *DeleteBuilder) WithFlavor(flavor prom.DbFlavor) *DeleteBuilder {
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
// As of v0.3.0 the generated DELETE statement works with MySQL, MSSQL, PostgreSQL, Oracle, SQLite and btnguyen2k/gocosmos.
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
func (b *SelectBuilder) WithFlavor(flavor prom.DbFlavor) *SelectBuilder {
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
	b.BaseSqlBuilder.WithTable(table)
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
// As of v0.3.0 the generated DELETE statement works with MySQL, MSSQL, PostgreSQL, Oracle, SQLite and btnguyen2k/gocosmos.
func (b *SelectBuilder) Build() (string, []interface{}) {
	tablesClause := strings.Join(b.Tables, ",")
	singleTblName := strings.TrimSpace(b.Tables[0])
	singleTblAlias := "c"
	if b.Flavor == prom.FlavorCosmosDb {
		/* START: special case for gocosmos */
		if tokens := reTblNameWithAlias.FindStringSubmatch(singleTblName); tokens != nil {
			singleTblName = tokens[1]
			singleTblAlias = strings.TrimSpace(tokens[3])
		}
		/* END: special case for gocosmos */
	}
	colsClause := strings.Join(allColumns, ",")
	if b.Columns != nil && len(b.Columns) > 0 {
		cols := make([]string, len(b.Columns))
		copy(cols, b.Columns)
		if b.Flavor == prom.FlavorCosmosDb {
			/* START: special case for gocosmos */
			for i, col := range cols {
				col = strings.TrimSpace(col)
				if !reColnamePrefixedTblname.MatchString(col) {
					cols[i] = singleTblAlias + "." + col
				}
			}
			/* END: special case for gocosmos */
		}
		colsClause = strings.Join(cols, ",")
	}

	sql := fmt.Sprintf("SELECT %s FROM %s", colsClause, tablesClause)
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

	if b.LimitNumRows != 0 || b.LimitOffset != 0 {
		switch b.Flavor {
		case prom.FlavorMySql, prom.FlavorSqlite:
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
		case prom.FlavorCosmosDb:
			sql += " OFFSET " + strconv.Itoa(b.LimitOffset) + " LIMIT " + strconv.Itoa(b.LimitNumRows)
		}
	}

	if b.Flavor == prom.FlavorCosmosDb {
		/* START: special case for gocosmos */
		sql += " WITH collection=" + singleTblName
		/* END: special case for gocosmos */
	}

	return sql, values
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
func (b *InsertBuilder) WithFlavor(flavor prom.DbFlavor) *InsertBuilder {
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
// As of v0.3.0 the generated INSERT statement works with MySQL, MSSQL, PostgreSQL, Oracle, SQLite and btnguyen2k/gocosmos.
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
func (b *UpdateBuilder) WithFlavor(flavor prom.DbFlavor) *UpdateBuilder {
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
// As of v0.3.0 the generated DELETE statement works with MySQL, MSSQL, PostgreSQL, Oracle, SQLite and btnguyen2k/gocosmos.
func (b *UpdateBuilder) Build() (string, []interface{}) {
	sql := fmt.Sprintf("UPDATE %s", b.Table)
	values := make([]interface{}, 0)

	setList := make([]string, 0)
	for k, v := range b.Values {
		values = append(values, v)
		setList = append(setList, k+"="+b.PlaceholderGenerator(k))
	}
	sql += " SET " + strings.Join(setList, ",")

	whereClause := ""
	if b.Filter != nil {
		var tempValues []interface{}
		whereClause, tempValues = b.Filter.Build(b.PlaceholderGenerator)
		values = append(values, tempValues...)
	}
	if whereClause != "" {
		sql += " WHERE " + whereClause
	}

	return sql, values
}
