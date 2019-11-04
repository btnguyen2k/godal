package sql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/prom"
	"reflect"
	"regexp"
	"strings"
	"time"
)

var allColumns = []string{"*"}

/*
ColummNameTransformation specifies how database column name is transformed.
*/
type ColummNameTransformation int

/*
Predefined column name transformations
*/
const (
	/*
		ColNameTransIntact specifies that table column names are kept intact.
	*/
	ColNameTransIntact ColummNameTransformation = iota

	/*
		ColNameTransUpperCase specifies that table column names are upper-cased.
	*/
	ColNameTransUpperCase

	/*
		ColNameTransLowerCase specifies that table column names are lower-cased.
	*/
	ColNameTransLowerCase
)

/*
GenericRowMapperSql is a generic implementation of godal.IRowMapper for 'database/sql'.

Implementation rules:

	- ToRow: transform godal.IGenericBo to map[string]interface{}.
	  - Only top level fields are converted. Field names are transform according to 'ColNameTrans' setting.
	  - If field is bool or string or time.Time: its value is converted as-is
	  - If field is int, int32 or int64: its value is converted to int64
	  - If field is uint, uint32 or uint64: its value is converted to uint64
	  - If field is float32 or float64: its value is converted to float64
	  - Field is one of other types: its value is converted to JSON string
	- ToBo: expect input is a map[string]interface{}, transform it to godal.IGenericBo. Field names are transform according to 'ColNameTrans' setting.
*/
type GenericRowMapperSql struct {
	/*
		ColNameTrans specifies how field/column names are transformed. Default value: ColNameTransIntact
	*/
	ColNameTrans ColummNameTransformation

	/*
		ColumnsListMap holds mappings of {table-name:[list of column names]}
	*/
	ColumnsListMap map[string][]string
}

var typeTime = reflect.TypeOf(time.Time{})

func (mapper *GenericRowMapperSql) transformColumnName(colName string) string {
	if mapper.ColNameTrans == ColNameTransLowerCase {
		return strings.ToLower(colName)
	} else if mapper.ColNameTrans == ColNameTransUpperCase {
		return strings.ToUpper(colName)
	}
	return colName
}

/*
ToRow implements godal.IRowMapper.ToRow.
This function transforms godal.IGenericBo to map[string]interface{}:

	- Only top level fields are converted. Field names are transform according to 'ColNameTrans' setting.
	- If field is bool or string or time.Time: its value is converted as-is
	- If field is int, int32 or int64: its value is converted to int64
	- If field is uint, uint32 or uint64: its value is converted to uint64
	- If field is float32 or float64: its value is converted to float64
	- Field is one of other types: its value is converted to JSON string
*/
func (mapper *GenericRowMapperSql) ToRow(storageId string, gbo godal.IGenericBo) (interface{}, error) {
	if gbo == nil {
		return nil, nil
	}
	var row = make(map[string]interface{})
	var err error
	gbo.GboIterate(func(kind reflect.Kind, field interface{}, value interface{}) {
		if err != nil {
			return
		}
		var k string
		if k, err = reddo.ToString(field); err != nil {
			return
		} else {
			k = mapper.transformColumnName(k)
		}

		v := reflect.ValueOf(value)
		for ; v.Kind() == reflect.Ptr; v = v.Elem() {
		}
		switch v.Kind() {
		case reflect.Bool:
			row[k] = v.Bool()
		case reflect.String:
			row[k] = v.String()
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			row[k] = v.Int()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			row[k] = v.Uint()
		case reflect.Float32, reflect.Float64:
			row[k] = v.Float()
		default:
			if v.Type() == typeTime {
				row[k] = v.Interface().(time.Time)
			} else {
				if js, e := json.Marshal(v.Interface()); e != nil {
					err = e
				} else {
					row[k] = string(js)
				}
			}
		}
	})
	return row, err
}

/*
ToBo implements godal.IRowMapper.ToBo.
This function expects input to be a map[string]interface{}, or JSON data (string or array/slice of bytes), transforms it to godal.IGenericBo. Field names are transform according to 'ColNameTrans' setting.
*/
func (mapper *GenericRowMapperSql) ToBo(storageId string, row interface{}) (godal.IGenericBo, error) {
	if row == nil {
		return nil, nil
	}
	switch row.(type) {
	case map[string]interface{}:
		bo := godal.NewGenericBo()
		for k, v := range row.(map[string]interface{}) {
			bo.GboSetAttr(mapper.transformColumnName(k), v)
		}
		return bo, nil
	case string:
		var data interface{}
		json.Unmarshal([]byte(row.(string)), &data)
		return mapper.ToBo(storageId, data)
	case *string:
		var data interface{}
		json.Unmarshal([]byte(*row.(*string)), &data)
		return mapper.ToBo(storageId, data)
	case []byte:
		var data interface{}
		json.Unmarshal(row.([]byte), &data)
		return mapper.ToBo(storageId, data)
	case *[]byte:
		var data interface{}
		json.Unmarshal(*row.(*[]byte), &data)
		return mapper.ToBo(storageId, data)
	}

	v := reflect.ValueOf(row)
	for ; v.Kind() == reflect.Ptr; v = v.Elem() {
	}
	switch v.Kind() {
	case reflect.Map:
		bo := godal.NewGenericBo()
		for iter := v.MapRange(); iter.Next(); {
			key, _ := reddo.ToString(iter.Key().Interface())
			bo.GboSetAttr(mapper.transformColumnName(key), iter.Value().Interface())
		}
		return bo, nil
	case reflect.String:
		var data interface{}
		json.Unmarshal([]byte(v.Interface().(string)), &data)
		return mapper.ToBo(storageId, data)
	case reflect.Slice, reflect.Array:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			// input is []byte
			zero := make([]byte, 0)
			arr, err := reddo.ToSlice(v.Interface(), reflect.TypeOf(zero))
			if err != nil {
				return nil, err
			}
			var data interface{}
			json.Unmarshal(arr.([]byte), &data)
			return mapper.ToBo(storageId, data)
		}
	}
	return nil, errors.New(fmt.Sprintf("cannot construct godal.IGenericBo from input %v", row))
}

/*
ColumnsList implements godal.IRowMapper.ColumnsList.
This function lookups column-list from a 'columns-list map', returns []string{"*"} if not found
*/
func (mapper *GenericRowMapperSql) ColumnsList(storageId string) []string {
	if result, ok := mapper.ColumnsListMap[storageId]; ok {
		return result
	}
	if result, ok := mapper.ColumnsListMap["*"]; ok {
		return result
	}
	return allColumns
}

var (
	/*
		GenericRowMapperSqlInstance is a pre-created instance of GenericRowMapperSql that is ready to use.
	*/
	GenericRowMapperSqlInstance godal.IRowMapper = &GenericRowMapperSql{ColNameTrans: ColNameTransIntact, ColumnsListMap: nil}
)

/*----------------------------------------------------------------------*/

/*
NewGenericDaoSql constructs a new GenericDaoSql with 'txModeOnWrite=true'.
*/
func NewGenericDaoSql(sqlConnect *prom.SqlConnect, agdao *godal.AbstractGenericDao) *GenericDaoSql {
	dao := &GenericDaoSql{
		AbstractGenericDao:          agdao,
		sqlConnect:                  sqlConnect,
		sqlFlavor:                   prom.FlavorDefault,
		txModeOnWrite:               true,
		txIsolationLevel:            sql.LevelDefault,
		optionOpLiteral:             defaultOptionLiteralOperation,
		funcNewPlaceholderGenerator: NewPlaceholderGeneratorQuestion,
	}
	if dao.GetRowMapper() == nil {
		dao.SetRowMapper(GenericRowMapperSqlInstance)
	}
	return dao
}

/*
GenericDaoSql is 'database/sql' implementation of godal.IGenericDao.

Function implementations (n = No, y = Yes, i = inherited):

	(n) GdaoCreateFilter(storageId string, bo godal.IGenericBo) interface{}
	(Y) GdaoDelete(storageId string, bo godal.IGenericBo) (int, error)
	(y) GdaoDeleteMany(storageId string, filter interface{}) (int, error)
	(y) GdaoFetchOne(storageId string, filter interface{}) (godal.IGenericBo, error)
	(y) GdaoFetchMany(storageId string, filter interface{}, ordering interface{}, fromOffset, numItems int) ([]godal.IGenericBo, error)
	(y) GdaoCreate(storageId string, bo godal.IGenericBo) (int, error)
	(y) GdaoUpdate(storageId string, bo godal.IGenericBo) (int, error)
	(y) GdaoSave(storageId string, bo godal.IGenericBo) (int, error)
*/
type GenericDaoSql struct {
	*godal.AbstractGenericDao
	sqlConnect                  *prom.SqlConnect
	sqlFlavor                   prom.DbFlavor
	txModeOnWrite               bool
	txIsolationLevel            sql.IsolationLevel
	optionOpLiteral             *OptionOpLiteral
	funcNewPlaceholderGenerator NewPlaceholderGenerator
}

/*
GetSqlConnect returns the '*prom.SqlConnect' instance attached to this DAO.
*/
func (dao *GenericDaoSql) GetSqlConnect() *prom.SqlConnect {
	return dao.sqlConnect
}

/*
SetSqlConnect attaches a '*prom.SqlConnect' instance to this DAO.

Available since v0.0.2
*/
func (dao *GenericDaoSql) SetSqlConnect(sqlC *prom.SqlConnect) *GenericDaoSql {
	dao.sqlConnect = sqlC
	return dao
}

/*
GetSqlFlavor returns the sql flavor preference.
*/
func (dao *GenericDaoSql) GetSqlFlavor() prom.DbFlavor {
	return dao.sqlFlavor
}

/*
SetSqlFlavor set the sql flavor preference.
*/
func (dao *GenericDaoSql) SetSqlFlavor(sqlFlavor prom.DbFlavor) *GenericDaoSql {
	dao.sqlFlavor = sqlFlavor
	dao.sqlConnect.SetDbFlavor(sqlFlavor)
	switch sqlFlavor {
	case prom.FlavorMySql:
		dao.funcNewPlaceholderGenerator = NewPlaceholderGeneratorQuestion
	case prom.FlavorPgSql:
		dao.funcNewPlaceholderGenerator = NewPlaceholderGeneratorDollarN
	case prom.FlavorMsSql:
		dao.funcNewPlaceholderGenerator = NewPlaceholderGeneratorAtpiN
	case prom.FlavorOracle:
		dao.funcNewPlaceholderGenerator = NewPlaceholderGeneratorColonN
	case prom.FlavorDefault:
		dao.funcNewPlaceholderGenerator = NewPlaceholderGeneratorQuestion
	}
	return dao
}

/*
GetTransactionMode returns transaction mode settings.

Deprecated: since v0.1.0 use GetTxIsolationLevel/GetTxModeOnWrite instead.
*/
func (dao *GenericDaoSql) GetTransactionMode() (bool, sql.IsolationLevel) {
	return dao.txModeOnWrite, dao.txIsolationLevel
}

/*
SetTransactionMode enables/disables transaction mode.

Deprecated: since v0.1.0 use SetTxIsolationLevel/SetTxModeOnWrite instead.
*/
func (dao *GenericDaoSql) SetTransactionMode(enabled bool, txIsolationLevel sql.IsolationLevel) *GenericDaoSql {
	dao.txModeOnWrite = enabled
	dao.txIsolationLevel = txIsolationLevel
	return dao
}

/*
GetTxModeOnWrite returns 'true' if transaction mode is enabled on write operations, 'false' otherwise.

RDBMS/SQL's implementation of GdaoSave is "try update, if failed then insert". It can be done either in transaction (txModeOnWrite=true) or non-transaction (txModeOnWrite=false) mode.

Available: since v0.1.0
*/
func (dao *GenericDaoSql) GetTxModeOnWrite() bool {
	return dao.txModeOnWrite
}

/*
SetTxModeOnWrite enables/disables transaction mode on write operations.

RDBMS/SQL's implementation of GdaoSave is "try update, if failed then insert". It can be done either in transaction (txModeOnWrite=true) or non-transaction (txModeOnWrite=false) mode.
By default, GenericDaoSql is created with 'txModeOnWrite=true', and it is recommended setting.

Available: since v0.1.0
*/
func (dao *GenericDaoSql) SetTxModeOnWrite(enabled bool) *GenericDaoSql {
	dao.txModeOnWrite = enabled
	return dao
}

/*
GetTxIsolationLevel returns current transaction isolation level setting.

Available: since v0.1.0
*/
func (dao *GenericDaoSql) GetTxIsolationLevel() sql.IsolationLevel {
	return dao.txIsolationLevel
}

/*
SetTxIsolationLevel sets new transaction isolation level.

Available: since v0.1.0
*/
func (dao *GenericDaoSql) SetTxIsolationLevel(txIsolationLevel sql.IsolationLevel) *GenericDaoSql {
	dao.txIsolationLevel = txIsolationLevel
	return dao
}

/*
StartTx starts a new transaction.

	- if ctx is nil, this function will create a new context by calling sqlConnect.NewContext()

Available: since v0.1.0
*/
func (dao *GenericDaoSql) StartTx(ctx context.Context) (*sql.Tx, error) {
	if ctx == nil {
		ctx, _ = dao.sqlConnect.NewContext()
	}
	return dao.sqlConnect.GetDB().BeginTx(ctx, &sql.TxOptions{Isolation: dao.txIsolationLevel})
}

/*
GetOptionOpLiteral returns operation literal settings.
*/
func (dao *GenericDaoSql) GetOptionOpLiteral() *OptionOpLiteral {
	return dao.optionOpLiteral
}

/*
SetOptionOpLiteral sets operation literal settings.
*/
func (dao *GenericDaoSql) SetOptionOpLiteral(optionOpLiteral *OptionOpLiteral) *GenericDaoSql {
	dao.optionOpLiteral = optionOpLiteral
	return dao
}

/*
GetFuncNewPlaceholderGenerator returns the function creates 'PlaceholderGenerator'.
*/
func (dao *GenericDaoSql) GetFuncNewPlaceholderGenerator() NewPlaceholderGenerator {
	return dao.funcNewPlaceholderGenerator
}

/*
SetFuncNewPlaceholderGenerator sets the function used to create 'PlaceholderGenerator'.
*/
func (dao *GenericDaoSql) SetFuncNewPlaceholderGenerator(funcNewPlaceholderGenerator NewPlaceholderGenerator) *GenericDaoSql {
	dao.funcNewPlaceholderGenerator = funcNewPlaceholderGenerator
	return dao
}

/*
BuildFilter builds IFilter instance based on the following rules:

	- If 'filter' is nil: return nil
	- If 'filter' is IFilter: return 'filter'
	- If 'filter' is a map: build a FilterAnd combining all map entries, using operation "=", and return it
	- Otherwise, return error
*/
func (dao *GenericDaoSql) BuildFilter(filter interface{}) (IFilter, error) {
	if filter == nil {
		return nil, nil
	}
	v := reflect.ValueOf(filter)
	if v.Type().AssignableTo(ifilterType) {
		return filter.(IFilter), nil
	}
	for ; v.Kind() == reflect.Ptr; v = v.Elem() {
	}
	if v.Kind() == reflect.Map {
		result := &FilterAnd{Filters: make([]IFilter, 0)}
		ops := dao.optionOpLiteral
		if ops == nil {
			ops = defaultOptionLiteralOperation
		}
		for iter := v.MapRange(); iter.Next(); {
			key, _ := reddo.ToString(iter.Key().Interface())
			result.Add(&FilterFieldValue{Field: key, Operation: ops.OpEqual, Value: iter.Value().Interface()})
		}
		return result, nil
	}
	return nil, errors.New(fmt.Sprintf("cannot build filter from %v", filter))
}

/*
BuildOrdering builds elements for 'ORDER BY' clause, based on the following rules:

	- If 'ordering' is nil: return nil
	- If 'ordering' is ISorting: return 'ordering'
	- If 'ordering' is a map: build a GenericSorting combining all map entries, where map key is field name and map value is ordering specification (1 for ASC, -1 for DESC)
	- If 'ordering' is a slice/array: build a GenericSorting combining all list entries, assuming each entry is a string in the format '<field_name[<:order>]>' ('order>=0' means 'ascending' and 'order<0' means 'descending')
	- Otherwise, return error

Available since v0.0.2
*/
func (dao *GenericDaoSql) BuildOrdering(ordering interface{}) (ISorting, error) {
	if ordering == nil {
		return nil, nil
	}
	v := reflect.ValueOf(ordering)
	if v.Type().AssignableTo(isortingType) {
		return ordering.(ISorting), nil
	}
	for ; v.Kind() == reflect.Ptr; v = v.Elem() {
	}
	if v.Kind() == reflect.Map {
		result := &GenericSorting{Flavor: dao.sqlFlavor}
		for iter := v.MapRange(); iter.Next(); {
			key, _ := reddo.ToString(iter.Key().Interface())
			value, _ := reddo.ToString(iter.Value().Interface())
			result.Add(key + ":" + value)
		}
		return result, nil
	}
	return nil, errors.New(fmt.Sprintf("cannot build ordering from %v", ordering))
}

/*----------------------------------------------------------------------*/
/*
SqlExecute executes a non-SELECT SQL statement within a context/transaction.

	- If ctx is nil, SqlExecute creates a new context to use.
	- If tx is not nil, SqlExecute uses transaction context to execute the query.
	- If tx is nil, SqlExecute calls DB.ExecContext to execute the query.
*/
func (dao *GenericDaoSql) SqlExecute(ctx context.Context, tx *sql.Tx, sqlStm string, values ...interface{}) (sql.Result, error) {
	if ctx == nil {
		ctx, _ = dao.sqlConnect.NewContext()
	}
	if tx != nil {
		return tx.ExecContext(ctx, sqlStm, values...)
	}
	return dao.sqlConnect.GetDB().ExecContext(ctx, sqlStm, values...)
}

/*
SqlQuery executes a SELECT SQL statement within a context/transaction.

	- If ctx is nil, SqlQuery creates a new context to use.
	- If tx is not nil, SqlQuery uses transaction context to execute the query.
	- If tx is nil, SqlQuery calls DB.QueryContext to execute the query.
*/
func (dao *GenericDaoSql) SqlQuery(ctx context.Context, tx *sql.Tx, sqlStm string, values ...interface{}) (*sql.Rows, error) {
	if ctx == nil {
		ctx, _ = dao.sqlConnect.NewContext()
	}
	if tx != nil {
		return tx.QueryContext(ctx, sqlStm, values...)
	}
	return dao.sqlConnect.GetDB().QueryContext(ctx, sqlStm, values...)
}

/*
SqlDelete constructs a DELETE statement and executes it within a context/transaction.
*/
func (dao *GenericDaoSql) SqlDelete(ctx context.Context, tx *sql.Tx, table string, filter IFilter) (sql.Result, error) {
	builder := NewDeleteBuilder().WithFlavor(dao.sqlFlavor).WithTable(table).WithFilter(filter)
	if dao.funcNewPlaceholderGenerator != nil {
		builder.WithPlaceholderGenerator(dao.funcNewPlaceholderGenerator())
	}
	sqlStm, values := builder.Build()
	return dao.SqlExecute(ctx, tx, sqlStm, values...)
}

/*
SqlInsert constructs a INSERT statement and executes it within a context/transaction.
*/
func (dao *GenericDaoSql) SqlInsert(ctx context.Context, tx *sql.Tx, table string, colsAndVals map[string]interface{}) (sql.Result, error) {
	builder := NewInsertBuilder().WithFlavor(dao.sqlFlavor).WithTable(table).WithValues(colsAndVals)
	if dao.funcNewPlaceholderGenerator != nil {
		builder.WithPlaceholderGenerator(dao.funcNewPlaceholderGenerator())
	}
	sqlStm, values := builder.Build()
	return dao.SqlExecute(ctx, tx, sqlStm, values...)
}

/*
SqlSelect constructs a SELECT query and executes it within a context/transaction.
*/
func (dao *GenericDaoSql) SqlSelect(ctx context.Context, tx *sql.Tx, table string, columns []string, filter IFilter, sorting ISorting, fromOffset, numItems int) (*sql.Rows, error) {
	builder := NewSelectBuilder().WithFlavor(dao.sqlFlavor).
		WithColumns(columns...).WithTables(table).
		WithFilter(filter).
		WithSorting(sorting).
		WithLimit(numItems, fromOffset)
	if dao.funcNewPlaceholderGenerator != nil {
		builder.WithPlaceholderGenerator(dao.funcNewPlaceholderGenerator())
	}
	query, values := builder.Build()
	return dao.SqlQuery(ctx, tx, query, values...)
}

/*
SqlUpdate constructs an UPDATE query and executes it within a context/transaction.
*/
func (dao *GenericDaoSql) SqlUpdate(ctx context.Context, tx *sql.Tx, table string, colsAndVals map[string]interface{}, filter IFilter) (sql.Result, error) {
	builder := NewUpdateBuilder().WithFlavor(dao.sqlFlavor).WithTable(table).WithValues(colsAndVals).WithFilter(filter)
	if dao.funcNewPlaceholderGenerator != nil {
		builder.WithPlaceholderGenerator(dao.funcNewPlaceholderGenerator())
	}
	query, values := builder.Build()
	return dao.SqlExecute(ctx, tx, query, values...)
}

/*
FetchOne fetches a row from `sql.Rows` and transforms it to godal.IGenericBo.

	- FetchOne will NOT call dbRows.Close(), caller must take care of cleaning resource.
	- Caller should not call dbRows.Next(), FetchOne will do that.
*/
func (dao *GenericDaoSql) FetchOne(storageId string, dbRows *sql.Rows) (godal.IGenericBo, error) {
	var bo godal.IGenericBo
	var err error
	e := dao.sqlConnect.FetchRowsCallback(dbRows, func(row map[string]interface{}, e error) bool {
		if e == nil {
			bo, err = dao.GetRowMapper().ToBo(storageId, row)
		} else {
			err = e
		}
		return false
	})
	if err != nil {
		return bo, err
	}
	return bo, e
}

/*
FetchAll fetches all rows from `sql.Rows` and transforms to []godal.IGenericBo

	- FetchOne will NOT call dbRows.Close(), caller must take are of cleaning resource.
	- Caller should not call dbRows.Next(), FetchOne will do that.
*/
func (dao *GenericDaoSql) FetchAll(storageId string, dbRows *sql.Rows) ([]godal.IGenericBo, error) {
	boList := make([]godal.IGenericBo, 0)
	var err error
	e := dao.sqlConnect.FetchRowsCallback(dbRows, func(row map[string]interface{}, e error) bool {
		if e != nil {
			err = e
			return false
		}
		if bo, e := dao.GetRowMapper().ToBo(storageId, row); e != nil {
			err = e
			return false
		} else {
			boList = append(boList, bo)
		}
		return true
	})
	if err != nil {
		return boList, err
	}
	return boList, e
}

/*----------------------------------------------------------------------*/
/*
GdaoDelete implements godal.IGenericDao.GdaoDelete.

Available: since v0.1.0
*/
func (dao *GenericDaoSql) GdaoDelete(storageId string, bo godal.IGenericBo) (int, error) {
	return dao.GdaoDeleteWithTx(nil, nil, storageId, bo)
}

/*
GdaoDeleteWithTx is extended-implementation of godal.IGenericDao.GdaoDelete.

Available: since v0.1.0
*/
func (dao *GenericDaoSql) GdaoDeleteWithTx(ctx context.Context, tx *sql.Tx, storageId string, bo godal.IGenericBo) (int, error) {
	filter := dao.GdaoCreateFilter(storageId, bo)
	return dao.GdaoDeleteManyWithTx(ctx, tx, storageId, filter)
}

/*
GdaoDeleteMany implements godal.IGenericDao.GdaoDeleteMany.
*/
func (dao *GenericDaoSql) GdaoDeleteMany(storageId string, filter interface{}) (int, error) {
	return dao.GdaoDeleteManyWithTx(nil, nil, storageId, filter)
}

/*
GdaoDeleteManyWithTx is extended-implementation of godal.IGenericDao.GdaoDeleteMany.

Available: since v0.1.0
*/
func (dao *GenericDaoSql) GdaoDeleteManyWithTx(ctx context.Context, tx *sql.Tx, storageId string, filter interface{}) (int, error) {
	if f, err := dao.BuildFilter(filter); err != nil {
		return 0, err
	} else if result, err := dao.SqlDelete(ctx, tx, storageId, f); err != nil {
		return 0, err
	} else {
		numRows, err := result.RowsAffected()
		return int(numRows), err
	}
}

/*
GdaoFetchOne implements godal.IGenericDao.GdaoFetchOne.
*/
func (dao *GenericDaoSql) GdaoFetchOne(storageId string, filter interface{}) (godal.IGenericBo, error) {
	return dao.GdaoFetchOneWithTx(nil, nil, storageId, filter)
}

/*
GdaoFetchOneWithTx is extended-implementation of godal.IGenericDao.GdaoFetchOne.

Available: since v0.1.0
*/
func (dao *GenericDaoSql) GdaoFetchOneWithTx(ctx context.Context, tx *sql.Tx, storageId string, filter interface{}) (godal.IGenericBo, error) {
	if f, err := dao.BuildFilter(filter); err != nil {
		return nil, err
	} else {
		dbRows, err := dao.SqlSelect(ctx, tx, storageId, dao.GetRowMapper().ColumnsList(storageId), f, nil, 0, 0)
		if dbRows != nil {
			defer func() { _ = dbRows.Close() }()
		}
		if err != nil {
			return nil, err
		}
		return dao.FetchOne(storageId, dbRows)
	}
}

/*
GdaoFetchMany implements godal.IGenericDao.GdaoFetchMany.
*/
func (dao *GenericDaoSql) GdaoFetchMany(storageId string, filter interface{}, ordering interface{}, fromOffset, numRows int) ([]godal.IGenericBo, error) {
	return dao.GdaoFetchManyWithTx(nil, nil, storageId, filter, ordering, fromOffset, numRows)
}

/*
GdaoFetchManyWithTx is extended-implementation of godal.IGenericDao.GdaoFetchMany.

Available: since v0.1.0
*/
func (dao *GenericDaoSql) GdaoFetchManyWithTx(ctx context.Context, tx *sql.Tx, storageId string, filter interface{}, ordering interface{}, fromOffset, numRows int) ([]godal.IGenericBo, error) {
	if f, err := dao.BuildFilter(filter); err != nil {
		return nil, err
	} else {
		o, _ := dao.BuildOrdering(ordering)
		dbRows, err := dao.SqlSelect(ctx, tx, storageId, dao.GetRowMapper().ColumnsList(storageId), f, o, fromOffset, numRows)
		if dbRows != nil {
			defer func() { _ = dbRows.Close() }()
		}
		if err != nil {
			return nil, err
		}
		return dao.FetchAll(storageId, dbRows)
	}
}

func (dao *GenericDaoSql) isErrorDuplicatedEntry(err error) bool {
	if err == nil {
		return false
	}
	switch dao.sqlFlavor {
	case prom.FlavorMySql:
		return regexp.MustCompile(`\W1062\W`).FindString(err.Error()) != ""
	case prom.FlavorPgSql:
		return regexp.MustCompile(`\W23505\W`).FindString(fmt.Sprintf("%e", err)) != ""
	case prom.FlavorMsSql:
		return regexp.MustCompile(`\W2627\W|\W2601\W`).FindString(fmt.Sprintf("%e", err)) != ""
	case prom.FlavorOracle:
		return regexp.MustCompile(`\WORA\-00001\W`).FindString(fmt.Sprintf("%v", err)) != ""
	}
	return false
}

/*
GdaoCreate implements godal.IGenericDao.GdaoCreate.
*/
func (dao *GenericDaoSql) GdaoCreate(storageId string, bo godal.IGenericBo) (int, error) {
	return dao.GdaoCreateWithTx(nil, nil, storageId, bo)
}

/*
GdaoCreateWithTx is extended-implementation of godal.IGenericDao.GdaoCreate.

Available: since v0.1.0
*/
func (dao *GenericDaoSql) GdaoCreateWithTx(ctx context.Context, tx *sql.Tx, storageId string, bo godal.IGenericBo) (int, error) {
	// insert new document
	if row, err := dao.GetRowMapper().ToRow(storageId, bo); err != nil {
		return 0, err
	} else if colsAndVals, err := reddo.ToMap(row, reflect.TypeOf(map[string]interface{}{})); err != nil {
		return 0, err
	} else if result, err := dao.SqlInsert(ctx, tx, storageId, colsAndVals.(map[string]interface{})); err != nil {
		if dao.isErrorDuplicatedEntry(err) {
			return 0, godal.GdaoErrorDuplicatedEntry
		} else {
			return 0, err
		}
	} else {
		numRows, err := result.RowsAffected()
		return int(numRows), err
	}
}

/*
GdaoUpdate implements godal.IGenericDao.GdaoUpdate.
*/
func (dao *GenericDaoSql) GdaoUpdate(storageId string, bo godal.IGenericBo) (int, error) {
	return dao.GdaoUpdateWithTx(nil, nil, storageId, bo)
}

/*
GdaoUpdateWithTx is extended-implementation of godal.IGenericDao.GdaoUpdate.

Available: since v0.1.0
*/
func (dao *GenericDaoSql) GdaoUpdateWithTx(ctx context.Context, tx *sql.Tx, storageId string, bo godal.IGenericBo) (int, error) {
	filter, err := dao.BuildFilter(dao.GdaoCreateFilter(storageId, bo))
	if err != nil {
		return 0, err
	}
	row, err := dao.GetRowMapper().ToRow(storageId, bo)
	if err != nil {
		return 0, err
	}
	colsAndVals, err := reddo.ToMap(row, reflect.TypeOf(map[string]interface{}{}))
	if err != nil {
		return 0, err
	}
	if result, err := dao.SqlUpdate(ctx, tx, storageId, colsAndVals.(map[string]interface{}), filter); err != nil {
		if dao.isErrorDuplicatedEntry(err) {
			return 0, godal.GdaoErrorDuplicatedEntry
		}
		return 0, err
	} else {
		numRows, err := result.RowsAffected()
		return int(numRows), err
	}
}

/*
GdaoSave implements godal.IGenericDao.GdaoSave.
*/
func (dao *GenericDaoSql) GdaoSave(storageId string, bo godal.IGenericBo) (int, error) {
	var numRows int
	var err error
	if dao.txModeOnWrite {
		err = dao.WrapTransaction(nil, func(ctx context.Context, tx *sql.Tx) error {
			var e error
			numRows, e = dao.GdaoSaveWithTx(ctx, tx, storageId, bo)
			return e
		})
	} else {
		numRows, err = dao.GdaoSaveWithTx(nil, nil, storageId, bo)
	}
	return numRows, err

	// var tx *sql.Tx
	// var err error
	// defer func() {
	// 	if tx != nil {
	// 		if err != nil {
	// 			tx.Rollback()
	// 		} else {
	// 			err = tx.Commit()
	// 		}
	// 	}
	// }()
	// var ctx context.Context
	// if dao.txModeOnWrite {
	// 	ctx, _ = dao.sqlConnect.NewContext()
	// 	if tx, err = dao.sqlConnect.GetDB().BeginTx(ctx, &sql.TxOptions{Isolation: dao.txIsolationLevel}); err != nil {
	// 		return 0, err
	// 	}
	// }
	// numRows, err := dao.GdaoSaveWithTx(ctx, tx, storageId, bo)
	// return numRows, err
}

/*
GdaoSaveWithTx is extended-implementation of godal.IGenericDao.GdaoSave.

Available: since v0.1.0
*/
func (dao *GenericDaoSql) GdaoSaveWithTx(ctx context.Context, tx *sql.Tx, storageId string, bo godal.IGenericBo) (int, error) {
	filter, err := dao.BuildFilter(dao.GdaoCreateFilter(storageId, bo))
	if err != nil {
		return 0, err
	}
	row, err := dao.GetRowMapper().ToRow(storageId, bo)
	if err != nil {
		return 0, err
	}
	colsAndVals, err := reddo.ToMap(row, reflect.TypeOf(map[string]interface{}{}))
	if err != nil {
		return 0, err
	}

	// firstly: try to update row
	if result, err := dao.SqlUpdate(ctx, tx, storageId, colsAndVals.(map[string]interface{}), filter); err != nil {
		if dao.isErrorDuplicatedEntry(err) {
			return 0, godal.GdaoErrorDuplicatedEntry
		}
		return 0, err
	} else if numRows, err := result.RowsAffected(); err != nil || numRows > 0 {
		return int(numRows), err
	} else {
		// secondly: no row updated, try insert row
		if result, err := dao.SqlInsert(ctx, tx, storageId, colsAndVals.(map[string]interface{})); err != nil {
			if dao.isErrorDuplicatedEntry(err) {
				return 0, godal.GdaoErrorDuplicatedEntry
			}
			return 0, err
		} else {
			numRows, err := result.RowsAffected()
			return int(numRows), err
		}
	}
}

/*
WrapTransaction wraps a function inside a transaction.

	- txFunc: the function to wrap. If the function returns error, the transaction will be aborted, otherwise transaction is committed.

Available: since v0.1.0
*/
func (dao *GenericDaoSql) WrapTransaction(ctx context.Context, txFunc func(ctx context.Context, tx *sql.Tx) error) error {
	var tx *sql.Tx
	var err error
	defer func() {
		if tx != nil {
			if err != nil {
				tx.Rollback()
			} else {
				err = tx.Commit()
			}
		}
	}()
	if ctx == nil {
		ctx, _ = dao.sqlConnect.NewContext()
	}
	if tx, err = dao.sqlConnect.GetDB().BeginTx(ctx, &sql.TxOptions{Isolation: dao.txIsolationLevel}); err != nil {
		return err
	}
	err = txFunc(ctx, tx)
	return err
}
