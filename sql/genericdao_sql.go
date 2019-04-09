package sql

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/prom"
	"github.com/pkg/errors"
	"reflect"
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
func (mapper *GenericRowMapperSql) ToRow(storageId string, bo godal.IGenericBo) (interface{}, error) {
	if bo == nil {
		return nil, nil
	}
	var row = make(map[string]interface{})
	var err error
	bo.GboIterate(func(kind reflect.Kind, field interface{}, value interface{}) {
		if err != nil {
			return
		}
		k, e := reddo.ToString(field)
		if err != nil {
			err = e
			return
		}
		switch mapper.ColNameTrans {
		case ColNameTransLowerCase:
			k = strings.ToLower(k)
		case ColNameTransUpperCase:
			k = strings.ToUpper(k)
		}
		v := reflect.ValueOf(value)
		for v.Kind() == reflect.Ptr {
			v = v.Elem()
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
		case reflect.Struct:
			if v.Type() == typeTime {
				row[k] = v.Interface().(time.Time)
			} else {
				js, e := json.Marshal(v.Interface())
				if e == nil {
					row[k] = string(js)
				} else {
					err = e
				}
			}
		default:
			js, e := json.Marshal(v.Interface())
			if e == nil {
				row[k] = string(js)
			} else {
				err = e
			}
		}
	})
	return row, err
}

/*
ToBo implements godal.IRowMapper.ToBo.
This function expects input is a map[string]interface{}, transforms it to godal.IGenericBo. Field names are transform according to 'ColNameTrans' setting.
*/
func (mapper *GenericRowMapperSql) ToBo(storageId string, row interface{}) (godal.IGenericBo, error) {
	if row == nil {
		return nil, nil
	}
	v := reflect.ValueOf(row)
	for v.Kind() == reflect.Ptr && !v.IsNil() {
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Map:
		bo := godal.NewGenericBo()
		iter := v.MapRange()
		for iter.Next() {
			key, err := reddo.ToString(iter.Key().Interface())
			if err != nil {
				return bo, err
			}
			switch mapper.ColNameTrans {
			case ColNameTransLowerCase:
				key = strings.ToLower(key)
			case ColNameTransUpperCase:
				key = strings.ToUpper(key)
			}
			err = bo.GboSetAttr(key, iter.Value().Interface())
			if err != nil {
				return bo, err
			}
		}
		return bo, nil
	}
	return nil, errors.Errorf("cannot construct godal.IGenericBo from input %v", row)
}

/*
ColumnsList implements godal.IRowMapper.ColumnsList.
This function lookups column-list from a 'columns-list map', returns []string{"*"} if not found
*/
func (mapper *GenericRowMapperSql) ColumnsList(storageId string) []string {
	result, e := mapper.ColumnsListMap[storageId]
	if e {
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
NewGenericDaoSql constructs a new GenericDaoSql.
*/
func NewGenericDaoSql(sqlConnect *prom.SqlConnect, agdao *godal.AbstractGenericDao) *GenericDaoSql {
	dao := &GenericDaoSql{AbstractGenericDao: agdao, sqlConnect: sqlConnect}
	dao.optionOpLiteral = defaultOptionLiteralOperation
	dao.txMode = false
	dao.txIsolationLevel = sql.LevelDefault
	dao.funcNewPlaceholderGenerator = NewPlaceholderGeneratorQuestion
	if dao.GetRowMapper() == nil {
		dao.SetRowMapper(GenericRowMapperSqlInstance)
	}
	return dao
}

/*
GenericDaoSql is 'database/sql' implementation of godal.IGenericDao.

Function implementations (n = No, y = Yes, i = inherited):

	(n) GdaoCreateFilter(storageId string, bo godal.IGenericBo) interface{}
	(i) GdaoDelete(storageId string, bo godal.IGenericBo) (int, error)
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
	txMode                      bool
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
*/
func (dao *GenericDaoSql) GetTransactionMode() (bool, sql.IsolationLevel) {
	return dao.txMode, dao.txIsolationLevel
}

/*
SetTransactionMode enables/disables transaction mode.
*/
func (dao *GenericDaoSql) SetTransactionMode(enabled bool, txIsolationLevel sql.IsolationLevel) *GenericDaoSql {
	dao.txMode = enabled
	dao.txIsolationLevel = txIsolationLevel
	return dao
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
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() == reflect.Map {
		result := &FilterAnd{Filters: make([]IFilter, 0)}
		ops := dao.optionOpLiteral
		if ops == nil {
			ops = defaultOptionLiteralOperation
		}

		iter := v.MapRange()
		for iter.Next() {
			key, _ := reddo.ToString(iter.Key().Interface())
			value := iter.Value().Interface()
			f := FilterFieldValue{Field: key, Operation: ops.OpEqual, Value: value}
			result.Add(&f)
		}

		return result, nil
	}
	return nil, errors.Errorf("cannot build filter from %v", filter)
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
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() == reflect.Map {
		result := &GenericSorting{Flavor: dao.sqlFlavor}
		iter := v.MapRange()
		for iter.Next() {
			key, _ := reddo.ToString(iter.Key().Interface())
			value, _ := reddo.ToString(iter.Value().Interface())
			result.Add(key + ":" + value)
		}
		return result, nil
	}
	return nil, errors.Errorf("cannot build ordering from %v", ordering)
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
		ctx, _ = dao.sqlConnect.NewBackgroundContext()
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
	- If tx is nil, SqlQuery calls DB.ExecContext to execute the query.
*/
func (dao *GenericDaoSql) SqlQuery(ctx context.Context, tx *sql.Tx, sqlStm string, values ...interface{}) (*sql.Rows, error) {
	if ctx == nil {
		ctx, _ = dao.sqlConnect.NewBackgroundContext()
	}
	if tx != nil {
		return tx.QueryContext(ctx, sqlStm, values...)
	}
	return dao.sqlConnect.GetDB().QueryContext(ctx, sqlStm, values...)
}

/*
SqlDelete constructs a DELETE statement and executes it within a context/transaction.
If ctx is nil, SqlDelete creates a new context to use.
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
If ctx is nil, SqlDelete creates a new context to use.
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
If ctx is nil, SqlSelect creates a new context to use.
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
If ctx is nil, SqlUpdate creates a new context to use.
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
FetchOne fetches a row from a dataset and transforms it to godal.IGenericBo.

	- FetchOne will NOT call dbRows.Close(), caller must take care of closing dataset.
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
FetchAll fetches all rows from a dataset and transforms to []godal.IGenericBo

	- FetchOne will NOT call dbRows.Close(), caller must take are of closing dataset
	- Caller should not call dbRows.Next(), FetchOne will do that.
*/
func (dao *GenericDaoSql) FetchAll(storageId string, dbRows *sql.Rows) ([]godal.IGenericBo, error) {
	var boList []godal.IGenericBo
	var err error
	e := dao.sqlConnect.FetchRowsCallback(dbRows, func(row map[string]interface{}, e error) bool {
		if e != nil {
			return false
		}
		bo, e := dao.GetRowMapper().ToBo(storageId, row)
		if e != nil {
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
GdaoDeleteMany implements godal.IGenericDao.GdaoDeleteMany.
*/
func (dao *GenericDaoSql) GdaoDeleteMany(storageId string, filter interface{}) (int, error) {
	f, err := dao.BuildFilter(filter)
	if err != nil {
		return 0, err
	}
	result, err := dao.SqlDelete(nil, nil, storageId, f)
	if err != nil {
		return 0, err
	}
	numRows, err := result.RowsAffected()
	return int(numRows), err
}

/*
GdaoFetchOne implements godal.IGenericDao.GdaoFetchOne.
*/
func (dao *GenericDaoSql) GdaoFetchOne(storageId string, filter interface{}) (godal.IGenericBo, error) {
	f, err := dao.BuildFilter(filter)
	if err != nil {
		return nil, err
	}
	dbRows, err := dao.SqlSelect(nil, nil, storageId, dao.GetRowMapper().ColumnsList(storageId), f, nil, 0, 0)
	if dbRows != nil {
		defer func() { _ = dbRows.Close() }()
	}
	if err != nil {
		return nil, err
	}
	return dao.FetchOne(storageId, dbRows)
}

/*
GdaoFetchMany implements godal.IGenericDao.GdaoFetchMany.
*/
func (dao *GenericDaoSql) GdaoFetchMany(storageId string, filter interface{}, ordering interface{}, fromOffset, numRows int) ([]godal.IGenericBo, error) {
	f, err := dao.BuildFilter(filter)
	if err != nil {
		return nil, err
	}
	o, err := dao.BuildOrdering(ordering)
	dbRows, err := dao.SqlSelect(nil, nil, storageId, dao.GetRowMapper().ColumnsList(storageId), f, o, fromOffset, numRows)
	if dbRows != nil {
		defer func() { _ = dbRows.Close() }()
	}
	if err != nil {
		return nil, err
	}
	return dao.FetchAll(storageId, dbRows)
}

func (dao *GenericDaoSql) updateOrInsert(ctx context.Context, tx *sql.Tx, storageId string, bo godal.IGenericBo) (bool, error) {
	filter, err := dao.BuildFilter(dao.GdaoCreateFilter(storageId, bo))
	if err != nil {
		return false, err
	}
	row, err := dao.GetRowMapper().ToRow(storageId, bo)
	if err != nil {
		return false, err
	}
	colsAndVals, err := reddo.ToMap(row, reflect.TypeOf(map[string]interface{}{}))
	if err != nil {
		return false, err
	}

	// firstly: try to update row
	result, err := dao.SqlUpdate(ctx, tx, storageId, colsAndVals.(map[string]interface{}), filter)
	if err != nil {
		return false, err
	}
	numRows, err := result.RowsAffected()
	if numRows > 0 || err != nil {
		// update successful or error
		return true, err
	}

	// secondly: no row updated, try insert row
	result, err = dao.SqlInsert(ctx, tx, storageId, colsAndVals.(map[string]interface{}))
	if err != nil {
		return false, err
	}
	numRows, err = result.RowsAffected()
	return numRows > 0, err
}

func (dao *GenericDaoSql) insertIfNotExist(ctx context.Context, tx *sql.Tx, storageId string, bo godal.IGenericBo) (bool, error) {
	// first fetch existing document from storage
	filter, err := dao.BuildFilter(dao.GdaoCreateFilter(storageId, bo))
	if err != nil {
		return false, err
	}
	dbRows, err := dao.SqlSelect(ctx, tx, storageId, dao.GetRowMapper().ColumnsList(storageId), filter, nil, 0, 0)
	if dbRows != nil {
		defer func() { _ = dbRows.Close() }()
	}
	if err != nil {
		return false, err
	}

	existingBo, err := dao.FetchOne(storageId, dbRows)
	if err != nil || existingBo != nil {
		// error or document already existed
		return false, err
	}

	// insert new document
	row, err := dao.GetRowMapper().ToRow(storageId, bo)
	if err != nil {
		return false, err
	}
	colsAndVals, err := reddo.ToMap(row, reflect.TypeOf(map[string]interface{}{}))
	if err != nil {
		return false, err
	}
	result, err := dao.SqlInsert(ctx, tx, storageId, colsAndVals.(map[string]interface{}))
	if err != nil {
		return false, err
	}
	numRows, err := result.RowsAffected()
	return numRows > 0, err
}

/*
GdaoCreate implements godal.IGenericDao.GdaoCreate.
*/
func (dao *GenericDaoSql) GdaoCreate(storageId string, bo godal.IGenericBo) (int, error) {
	ctx, _ := dao.sqlConnect.NewBackgroundContext()
	var tx *sql.Tx
	var err error
	if dao.txMode {
		tx, err = dao.sqlConnect.GetDB().BeginTx(ctx, &sql.TxOptions{Isolation: dao.txIsolationLevel})
		defer func() { _ = tx.Rollback() }()
		if err != nil {
			return 0, err
		}
	}
	result, err := dao.insertIfNotExist(ctx, tx, storageId, bo)
	if tx != nil {
		_ = tx.Commit()
	}
	if result {
		return 1, err
	}
	return 0, err
}

/*
GdaoUpdate implements godal.IGenericDao.GdaoUpdate.
*/
func (dao *GenericDaoSql) GdaoUpdate(storageId string, bo godal.IGenericBo) (int, error) {
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
	result, err := dao.SqlUpdate(nil, nil, storageId, colsAndVals.(map[string]interface{}), filter)
	if err != nil {
		return 0, err
	}
	numRows, err := result.RowsAffected()
	return int(numRows), err
}

/*
GdaoSave implements godal.IGenericDao.GdaoSave.
*/
func (dao *GenericDaoSql) GdaoSave(storageId string, bo godal.IGenericBo) (int, error) {
	ctx, _ := dao.sqlConnect.NewBackgroundContext()
	var tx *sql.Tx
	var err error
	if dao.txMode {
		tx, err = dao.sqlConnect.GetDB().BeginTx(ctx, &sql.TxOptions{Isolation: dao.txIsolationLevel})
		defer func() { _ = tx.Rollback() }()
		if err != nil {
			return 0, err
		}
	}
	result, err := dao.updateOrInsert(ctx, tx, storageId, bo)
	if tx != nil {
		_ = tx.Commit()
	}
	if result {
		return 1, err
	}
	return 0, err
}
