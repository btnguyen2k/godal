package sql

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

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

// // OptionOpLiteral controls literal forms of operators.
// type OptionOpLiteral struct {
// 	OpAnd      string // 'and' operator, default is "AND"
// 	OpOr       string // 'or' operator, default is "OR"
// 	OpEqual    string // 'equal' operator, default is "="
// 	OpNotEqual string // 'not equal' operator, default is "!="
// }

// // DefaultOptionLiteralOperator uses "AND" for 'and' operator, "OR" for 'or' operator, "=" for equal and "!=" for not equal.
// var DefaultOptionLiteralOperator = &OptionOpLiteral{
// 	OpAnd:      "AND",
// 	OpOr:       "OR",
// 	OpEqual:    "=",
// 	OpNotEqual: "!=",
// }

/*----------------------------------------------------------------------*/

// OptTableAlias is used to prefix table alias to field name when building ISorting, IFilter or ISqlBuilder.
type OptTableAlias struct {
	TableAlias string
}

// OptDbFlavor is used to specify the db flavor that affects the generated SQL statement.
type OptDbFlavor struct {
	Flavor prom.DbFlavor
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
func (o *GenericSorting) Build(opts ...interface{}) string {
	if len(o.Ordering) == 0 {
		return ""
	}

	tableAlias := ""
	for _, opt := range opts {
		switch opt.(type) {
		case OptTableAlias:
			tableAlias = opt.(OptTableAlias).TableAlias + "."
		case *OptTableAlias:
			tableAlias = opt.(*OptTableAlias).TableAlias + "."
		}
	}

	elements := make([]string, 0)
	for _, v := range o.Ordering {
		tokens := strings.Split(v, ":")
		order := tableAlias + tokens[0]
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
func (f *FilterAndOr) Build(placeholderGenerator PlaceholderGenerator, opts ...interface{}) (string, []interface{}) {
	nFilters := len(f.Filters)
	if nFilters == 0 {
		return "", make([]interface{}, 0)
	}

	op := " " + strings.TrimSpace(f.Operator) + " "
	clause, values := f.Filters[0].Build(placeholderGenerator, opts...)
	if nFilters > 1 {
		clause = "(" + clause + ")"
		for i := 1; i < nFilters; i++ {
			c, v := f.Filters[i].Build(placeholderGenerator, opts...)
			clause += op + "(" + c + ")"
			values = append(values, v...)
		}
	}

	return clause, values
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
func (f *FilterAnd) Build(placeholderGenerator PlaceholderGenerator, opts ...interface{}) (string, []interface{}) {
	if strings.TrimSpace(f.Operator) == "" {
		f.Operator = "AND"
	}
	return f.FilterAndOr.Build(placeholderGenerator, opts...)
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
func (f *FilterOr) Build(placeholderGenerator PlaceholderGenerator, opts ...interface{}) (string, []interface{}) {
	if strings.TrimSpace(f.Operator) == "" {
		f.Operator = "OR"
	}
	return f.FilterAndOr.Build(placeholderGenerator, opts...)
}

/*----------------------------------------------------------------------*/

// FilterBetween represents single filter: <field> BETWEEN <value1> AND <value2>.
//
// Available since v0.4.0
type FilterBetween struct {
	Field      string      // field to check
	Operator   string      // the operator itself (default value is BETWEEN)
	ValueLeft  interface{} // left value of the BETWEEN operator
	ValueRight interface{} // right value of the BETWEEN operator
}

// Build implements IFilter.Build.
func (f *FilterBetween) Build(placeholderGenerator PlaceholderGenerator, opts ...interface{}) (string, []interface{}) {
	if placeholderGenerator == nil {
		return "", []interface{}{}
	}
	if strings.TrimSpace(f.Operator) == "" {
		f.Operator = "BETWEEN"
	}
	tableAlias := ""
	for _, opt := range opts {
		switch opt.(type) {
		case OptTableAlias:
			tableAlias = opt.(OptTableAlias).TableAlias + "."
		case *OptTableAlias:
			tableAlias = opt.(*OptTableAlias).TableAlias + "."
		}
	}
	values := []interface{}{f.ValueLeft, f.ValueRight}
	clause := tableAlias + f.Field + " " + strings.TrimSpace(f.Operator) + " " + placeholderGenerator(f.Field) + " AND " + placeholderGenerator(f.Field)
	return clause, values
}

/*----------------------------------------------------------------------*/

// FilterFieldValue represents single filter: <field> <operator> <value>.
type FilterFieldValue struct {
	Field    string      // field to check
	Operator string      // the operator to perform
	Value    interface{} // value to test against
}

// Build implements IFilter.Build.
func (f *FilterFieldValue) Build(placeholderGenerator PlaceholderGenerator, opts ...interface{}) (string, []interface{}) {
	tableAlias := ""
	flavor := prom.FlavorDefault
	for _, opt := range opts {
		switch opt.(type) {
		case OptTableAlias:
			tableAlias = opt.(OptTableAlias).TableAlias + "."
		case *OptTableAlias:
			tableAlias = opt.(*OptTableAlias).TableAlias + "."
		case OptDbFlavor:
			flavor = opt.(OptDbFlavor).Flavor
		case *OptDbFlavor:
			flavor = opt.(*OptDbFlavor).Flavor
		}
	}
	values := make([]interface{}, 0)
	clause := tableAlias + f.Field + " " + strings.TrimSpace(f.Operator) + " NULL"
	if flavor == prom.FlavorCosmosDb {
		clause = tableAlias + f.Field + " " + strings.TrimSpace(f.Operator) + " null"
	}
	if f.Value != nil {
		clause = tableAlias + f.Field + " " + strings.TrimSpace(f.Operator) + " " + placeholderGenerator(f.Field)
		values = append(values, f.Value)
	}
	return clause, values
}

/*----------------------------------------------------------------------*/

// FilterIsNull represents single filter: <field> IS NULL.
//
// Available since v0.4.0
type FilterIsNull struct {
	FilterFieldValue
}

// Build implements IFilter.Build.
func (f *FilterIsNull) Build(placeholderGenerator PlaceholderGenerator, opts ...interface{}) (string, []interface{}) {
	f.Value = nil
	if strings.TrimSpace(f.Operator) == "" {
		f.Operator = "IS"
		flavor := prom.FlavorDefault
		for _, opt := range opts {
			switch opt.(type) {
			case OptDbFlavor:
				flavor = opt.(OptDbFlavor).Flavor
			case *OptDbFlavor:
				flavor = opt.(*OptDbFlavor).Flavor
			}
		}
		if flavor == prom.FlavorCosmosDb {
			f.Operator = "="
		}
	}
	return f.FilterFieldValue.Build(placeholderGenerator, opts...)
}

// FilterIsNotNull represents single filter: <field> IS NOT NULL.
//
// Available since v0.4.0
type FilterIsNotNull struct {
	FilterFieldValue
}

// Build implements IFilter.Build.
func (f *FilterIsNotNull) Build(placeholderGenerator PlaceholderGenerator, opts ...interface{}) (string, []interface{}) {
	f.Value = nil
	if strings.TrimSpace(f.Operator) == "" {
		f.Operator = "IS NOT"
		flavor := prom.FlavorDefault
		for _, opt := range opts {
			switch opt.(type) {
			case OptDbFlavor:
				flavor = opt.(OptDbFlavor).Flavor
			case *OptDbFlavor:
				flavor = opt.(*OptDbFlavor).Flavor
			}
		}
		if flavor == prom.FlavorCosmosDb {
			f.Operator = "!="
		}
	}
	return f.FilterFieldValue.Build(placeholderGenerator, opts...)
}

/*----------------------------------------------------------------------*/

// FilterExpression represents single filter: <left> <operator> <right>.
type FilterExpression struct {
	Left, Right string // left & right parts of the expression
	Operator    string // the operator to perform
}

// Build implements IFilter.Build.
func (f *FilterExpression) Build(_ PlaceholderGenerator, opts ...interface{}) (string, []interface{}) {
	clause := f.Left + " " + strings.TrimSpace(f.Operator) + " " + f.Right
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
		return b.WithPlaceholderGenerator(NewPlaceholderGeneratorDollarN())
	case prom.FlavorMsSql:
		return b.WithPlaceholderGenerator(NewPlaceholderGeneratorAtpiN())
	case prom.FlavorOracle:
		return b.WithPlaceholderGenerator(NewPlaceholderGeneratorColonN())
	case prom.FlavorMySql, prom.FlavorSqlite:
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
func (b *DeleteBuilder) Build(opts ...interface{}) (string, []interface{}) {
	if b.Filter != nil {
		opts := append(opts, OptDbFlavor{Flavor: b.Flavor})
		whereClause, values := b.Filter.Build(b.PlaceholderGenerator, opts...)
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
func (b *SelectBuilder) Build(opts ...interface{}) (string, []interface{}) {
	singleTblName := strings.TrimSpace(b.Tables[0])
	singleTblAlias := "c"
	optTableAlias := ""
	for _, opt := range opts {
		switch opt.(type) {
		case OptTableAlias:
			singleTblAlias = opt.(OptTableAlias).TableAlias
			optTableAlias = opt.(OptTableAlias).TableAlias + "."
		case *OptTableAlias:
			singleTblAlias = opt.(*OptTableAlias).TableAlias
			optTableAlias = opt.(*OptTableAlias).TableAlias + "."
		}
	}
	tablesClause := strings.Join(b.Tables, ",")

	if b.Flavor == prom.FlavorCosmosDb {
		/* START: special case for gocosmos */
		if tokens := reTblNameWithAlias.FindStringSubmatch(singleTblName); tokens != nil {
			singleTblName = tokens[1]
			singleTblAlias = strings.TrimSpace(tokens[3])
		}
		tablesClause = singleTblName + " " + singleTblAlias

		if optTableAlias == "" {
			optTableAlias = singleTblAlias + "."
			opts = append(opts, &OptTableAlias{TableAlias: singleTblAlias})
		}
		/* END: special case for gocosmos */
	}

	colsClause := "*"
	if len(b.Columns) > 0 {
		cols := make([]string, len(b.Columns))
		copy(cols, b.Columns)
		if b.Flavor == prom.FlavorCosmosDb {
			/* START: special case for gocosmos */
			for i, col := range cols {
				col = strings.TrimSpace(col)
				if !reColnamePrefixedTblname.MatchString(col) && col != "*" {
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
		opts := append(opts, OptDbFlavor{Flavor: b.Flavor})
		whereClause, tempValues = b.Filter.Build(b.PlaceholderGenerator, opts...)
		values = append(values, tempValues...)
	}
	if whereClause != "" {
		sql += " WHERE " + whereClause
	}

	groupClause := ""
	if len(b.GroupBy) > 0 {
		groupByList := make([]string, len(b.GroupBy))
		copy(groupByList, b.GroupBy)
		for i, col := range groupByList {
			groupByList[i] = optTableAlias + col
		}
		groupClause = strings.Join(groupByList, ",")
	}
	if groupClause != "" {
		sql += " GROUP BY " + groupClause
	}

	havingClause := ""
	if b.Having != nil {
		opts := append(opts, OptDbFlavor{Flavor: b.Flavor})
		havingClause, tempValues = b.Having.Build(b.PlaceholderGenerator, opts...)
		values = append(values, tempValues...)
	}
	if havingClause != "" {
		sql += " HAVING " + havingClause
	}

	orderClause := ""
	if b.Sorting != nil {
		orderClause = b.Sorting.Build(opts...)
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
		// sql += " WITH collection=" + singleTblName
		sql += " WITH cross_partition=true"
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
func (b *InsertBuilder) Build(_ ...interface{}) (string, []interface{}) {
	cols := make([]string, 0)
	placeholders := make([]string, 0)
	values := make([]interface{}, 0)

	for k := range b.Values {
		cols = append(cols, k)
	}
	sort.Strings(cols)
	for _, col := range cols {
		values = append(values, b.Values[col])
		placeholders = append(placeholders, b.PlaceholderGenerator(col))
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
func (b *UpdateBuilder) Build(opts ...interface{}) (string, []interface{}) {
	sql := fmt.Sprintf("UPDATE %s", b.Table)
	values := make([]interface{}, 0)

	cols := make([]string, 0)
	for k := range b.Values {
		cols = append(cols, k)
	}
	sort.Strings(cols)
	setList := make([]string, 0)
	for _, col := range cols {
		values = append(values, b.Values[col])
		setList = append(setList, col+"="+b.PlaceholderGenerator(col))
	}
	sql += " SET " + strings.Join(setList, ",")

	whereClause := ""
	if b.Filter != nil {
		opts := append(opts, OptDbFlavor{Flavor: b.Flavor})
		var tempValues []interface{}
		whereClause, tempValues = b.Filter.Build(b.PlaceholderGenerator, opts...)
		values = append(values, tempValues...)
	}
	if whereClause != "" {
		sql += " WHERE " + whereClause
	}

	return sql, values
}
