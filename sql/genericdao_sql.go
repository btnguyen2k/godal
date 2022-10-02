/*
Package sql provides a generic 'database/sql' implementation of godal.IGenericDao.

General guideline:

	- DAOs must implement IGenericDao.GdaoCreateFilter(string, IGenericBo) FilterOpt.

Guideline: Use GenericDaoSql (and godal.IGenericBo) directly

	- Define a DAO struct that implements IGenericDao.GdaoCreateFilter(string, IGenericBo) FilterOpt.
	- Optionally, create a helper function to create DAO instances.

	// Remember to import database driver(s). The following example uses MySQL, hence driver "github.com/go-sql-driver/mysql".
	import (
		"github.com/btnguyen2k/consu/reddo"
		"github.com/btnguyen2k/godal"
		godalsql "github.com/btnguyen2k/godal/sql"
		promsql "github.com/btnguyen2k/prom/sql"

		_ "github.com/go-sql-driver/mysql"
	)

	type myGenericDaoMysql struct {
		*godalsql.GenericDaoSql
	}

	// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter.
	func (dao *myGenericDaoMysql) GdaoCreateFilter(tableName string, bo godal.IGenericBo) godal.FilterOpt {
		id := bo.GboGetAttrUnsafe(fieldId, reddo.TypeString)
		return &godal.FilterOptFieldOpValue{FieldName: fieldId, Operator: godal.FilterOpEqual, Value: id}
	}

	// newGenericDaoMysql is helper function to create myGenericDaoMysql instances.
	func newGenericDaoMysql(sqlc *promsql.SqlConnect, txModeOnWrite bool) godal.IGenericDao {
		rowMapper := &godalsql.GenericRowMapperSql{NameTransformation: sql.NameTransfLowerCase}
		dao := &myGenericDaoMysql{}
		dao.GenericDaoSql = godalsql.NewGenericDaoSql(sqlc, godal.NewAbstractGenericDao(dao))
		dao.SetTxModeOnWrite(txModeOnWrite)
		dao.SetRowMapper(rowMapper)
		return dao
	}

	In most cases, GenericRowMapperSql should be sufficient:
		- Column/Field names can be transformed to lower-cased, upper-cased or kept intact. Transformation rule is specified by GenericRowMapperSql.NameTransformation
		- Column names (after transformed) can be translated to field names via GenericRowMapperSql.ColNameToGboFieldTranslator,
		- and vice versa, field names (after transformed) can be translated to column names via GenericRowMapperSql.GboFieldToColNameTranslator

Guideline: Implement custom 'database/sql' business DAOs and BOs

	- Define and implement the business DAO (Note: DAOs must implement IGenericDao.GdaoCreateFilter(string, IGenericBo) FilterOpt).
	- Optionally, create a helper function to create DAO instances.
	- Define functions to transform godal.IGenericBo to business BO and vice versa.

	// Remember to import database driver(s). The following example uses MySQL, hence driver "github.com/go-sql-driver/mysql".
	import (
		"github.com/btnguyen2k/consu/reddo"
		"github.com/btnguyen2k/godal"
		godalsql "github.com/btnguyen2k/godal/sql"
		promsql "github.com/btnguyen2k/prom/sql"

		_ "github.com/go-sql-driver/mysql"
	)

	// BoApp defines business object app
	type BoApp struct {
		Id            string                 `json:"id"`
		Description   string                 `json:"desc"`
		Value         int                    `json:"val"`
	}

	func (app *BoApp) ToGbo() godal.IGenericBo {
		gbo := godal.NewGenericBo()

		// method 1: populate attributes one by one
		gbo.GboSetAttr("id"  , app.Id)
		gbo.GboSetAttr("desc", app.Description)
		gbo.GboSetAttr("val" , app.Value)

		// method 2: transfer all attributes at once
		if err := gbo.GboImportViaJson(app); err!=nil {
			panic(err)
		}

		return gbo
	}

	func NewBoAppFromGbo(gbo godal.IGenericBo) *BoApp {
		app := BoApp{}

		// method 1: populate attributes one by one
		app.Id          = gbo.GboGetAttrUnsafe("id", reddo.TypeString).(string)
		app.Description = gbo.GboGetAttrUnsafe("desc", reddo.TypeString).(string)
		app.Value       = int(gbo.GboGetAttrUnsafe("val", reddo.TypeInt).(int64))

		// method 2: transfer all attributes at once
		if err := gbo.GboTransferViaJson(&app); err!=nil {
			panic(err)
		}

		return &app
	}

	// DaoAppMysql is MySQL-implementation of business dao
	type DaoAppMysql struct {
		*godalsql.GenericDaoSql
		tableName string
	}

	// NewDaoAppMysql is helper function to create DaoAppMysql instances.
	func NewDaoAppMysql(sqlc *promsql.SqlConnect, taleName string, txModeOnWrite bool) *DaoAppMysql {
		dao := &DaoAppMysql{tableName: taleName}
		dao.GenericDaoSql = godalsql.NewGenericDaoSql(sqlc, godal.NewAbstractGenericDao(dao))
		dao.SetTxModeOnWrite(txModeOnWrite)
		dao.SetRowMapper(&godalsql.GenericRowMapperSql{NameTransformation: sql.NameTransfLowerCase})
		return dao
	}

	In most cases, GenericRowMapperSql should be sufficient:
		- Column/Field names can be transformed to lower-cased, upper-cased or kept intact. Transformation rule is specified by GenericRowMapperSql.NameTransformation
		- Column names (after transformed) can be translated to field names via GenericRowMapperSql.ColNameToGboFieldTranslator,
		- and vice versa, field names (after transformed) can be translated to column names via GenericRowMapperSql.GboFieldToColNameTranslator

See more examples in 'examples' directory on project's GitHub: https://github.com/btnguyen2k/godal/tree/master/examples

To create SqlConnect instances, see package github.com/btnguyen2k/prom/sql
*/
package sql

import (
	"context"
	gosql "database/sql"
	"errors"
	"fmt"
	"reflect"
	"regexp"

	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/prom/sql"

	"github.com/btnguyen2k/godal"
)

// NewGenericDaoSql constructs a new GenericDaoSql with 'txModeOnWrite=true'.
func NewGenericDaoSql(sqlConnect *sql.SqlConnect, agdao *godal.AbstractGenericDao) *GenericDaoSql {
	dao := &GenericDaoSql{
		AbstractGenericDao:           agdao,
		txModeOnWrite:                true,
		txIsolationLevel:             gosql.LevelDefault,
		funcFilterOperatorTranslator: DefaultFilterOperatorTranslator,
	}
	dao.SetSqlConnect(sqlConnect)
	if dao.GetRowMapper() == nil {
		dao.SetRowMapper(&GenericRowMapperSql{NameTransformation: NameTransfIntact})
	}
	return dao
}

var (
	typeMap = reflect.TypeOf(map[string]interface{}{})
)

// IGenericDaoSql is 'database/sql' reference implementation of godal.IGenericDao.
//
// Note: IGenericDaoSql and GenericDaoSql should be in sync.
//
// Available since v0.3.0
type IGenericDaoSql interface {
	// IGenericDao instance to inherit existing functions.
	godal.IGenericDao

	// GdaoDeleteWithTx is database/sql variant of GdaoDelete.
	GdaoDeleteWithTx(ctx context.Context, tx *gosql.Tx, tableName string, bo godal.IGenericBo) (int, error)

	// GdaoDeleteManyWithTx is database/sql variant of GdaoDeleteMany.
	GdaoDeleteManyWithTx(ctx context.Context, tx *gosql.Tx, tableName string, filter godal.FilterOpt) (int, error)

	// GdaoFetchOneWithTx is database/sql variant of GdaoFetchOne.
	GdaoFetchOneWithTx(ctx context.Context, tx *gosql.Tx, tableName string, filter godal.FilterOpt) (godal.IGenericBo, error)

	// GdaoFetchManyWithTx is database/sql variant of GdaoFetchMany.
	GdaoFetchManyWithTx(ctx context.Context, tx *gosql.Tx, tableName string, filter godal.FilterOpt, sorting *godal.SortingOpt, fromOffset, numRows int) ([]godal.IGenericBo, error)

	// GdaoCreateWithTx is database/sql variant of GdaoCreate.
	GdaoCreateWithTx(ctx context.Context, tx *gosql.Tx, tableName string, bo godal.IGenericBo) (int, error)

	// GdaoUpdateWithTx is database/sql variant of GdaoUpdate.
	GdaoUpdateWithTx(ctx context.Context, tx *gosql.Tx, tableName string, bo godal.IGenericBo) (int, error)

	// GdaoSaveWithTx is database/sql variant of godal.IGenericDao.GdaoSave.
	GdaoSaveWithTx(ctx context.Context, tx *gosql.Tx, tableName string, bo godal.IGenericBo) (int, error)

	// SetRowMapper attaches an IRowMapper to the DAO for latter use.
	SetRowMapper(rowMapper godal.IRowMapper) IGenericDaoSql

	// GetSqlConnect returns the SqlConnect instance attached to this DAO.
	GetSqlConnect() *sql.SqlConnect

	// SetSqlConnect attaches a SqlConnect instance to this DAO.
	SetSqlConnect(sqlC *sql.SqlConnect) IGenericDaoSql

	// GetSqlFlavor returns the sql flavor preference.
	GetSqlFlavor() sql.DbFlavor

	// SetSqlFlavor set the sql flavor preference.
	//
	// Deprecated since v0.6.0 (sql flavor should come from the attached SqlConnect instance).
	SetSqlFlavor(sqlFlavor sql.DbFlavor) IGenericDaoSql

	// GetTxModeOnWrite returns 'true' if transaction mode is enabled on write operations, 'false' otherwise.
	//
	// RDBMS/SQL's implementation of GdaoSave is "try update, if failed then insert". It can be done either in transactional (txModeOnWrite=true) or non-transactional (txModeOnWrite=false) mode.
	GetTxModeOnWrite() bool

	// SetTxModeOnWrite enables/disables transaction mode on write operations.
	//
	// RDBMS/SQL's implementation of GdaoSave is "try update, if failed then insert". It can be done either in transactional (txModeOnWrite=true) or non-transactional (txModeOnWrite=false) mode.
	SetTxModeOnWrite(enabled bool) IGenericDaoSql

	// GetTxIsolationLevel returns current transaction isolation level setting.
	GetTxIsolationLevel() gosql.IsolationLevel

	// SetTxIsolationLevel sets new transaction isolation level.
	SetTxIsolationLevel(txIsolationLevel gosql.IsolationLevel) IGenericDaoSql

	// StartTx starts a new transaction.
	StartTx(ctx context.Context) (*gosql.Tx, error)

	// GetFuncNewPlaceholderGenerator returns the function that creates 'PlaceholderGenerator' instances.
	GetFuncNewPlaceholderGenerator() NewPlaceholderGenerator

	// SetFuncNewPlaceholderGenerator sets the function used to create 'PlaceholderGenerator'.
	SetFuncNewPlaceholderGenerator(funcNewPlaceholderGenerator NewPlaceholderGenerator) IGenericDaoSql

	// BuildFilter transforms a godal.FilterOpt to IFilter.
	//
	// Available since v0.5.0
	BuildFilter(tableName string, filter godal.FilterOpt) (IFilter, error)

	// BuildSorting builds elements for 'ORDER BY' clause, based on the following rules:
	//
	// Available since v0.5.0
	BuildSorting(tableName string, ordering *godal.SortingOpt) (ISorting, error)

	// SqlExecute executes a non-SELECT SQL statement within a context/transaction.
	//   - If tx is not nil, the transaction context is used to execute the query.
	//   - If tx is nil, DB.ExecContext is used to execute the query.
	SqlExecute(ctx context.Context, tx *gosql.Tx, sql string, values ...interface{}) (gosql.Result, error)

	// SqlQuery executes a SELECT SQL statement within a context/transaction.
	//   - If tx is not nil, the transaction context is used to execute the query.
	//   - If tx is nil, DB.ExecContext is used to execute the query.
	SqlQuery(ctx context.Context, tx *gosql.Tx, sql string, values ...interface{}) (*gosql.Rows, error)

	// SqlDelete constructs a DELETE statement and executes it within a context/transaction.
	SqlDelete(ctx context.Context, tx *gosql.Tx, table string, filter IFilter) (gosql.Result, error)

	// SqlBuildDeleteEx is a utility function to construct the DELETE statement along with values for placeholders.
	SqlBuildDeleteEx(builder ISqlBuilder, table string, filter IFilter) (sql string, placeholderValues []interface{})

	// SqlDeleteEx is the extended version of SqlDelete that uses an external DeleteBuilder to construct the DELETE statement.
	SqlDeleteEx(ctx context.Context, builder ISqlBuilder, tx *gosql.Tx, table string, filter IFilter) (gosql.Result, error)

	// SqlInsert constructs a INSERT statement and executes it within a context/transaction.
	SqlInsert(ctx context.Context, tx *gosql.Tx, table string, colsAndVals map[string]interface{}) (gosql.Result, error)

	// SqlBuildInsertEx is a utility function to construct the INSERT statement along with values for placeholders.
	SqlBuildInsertEx(builder ISqlBuilder, table string, colsAndVals map[string]interface{}) (sql string, placeholderValues []interface{})

	// SqlInsertEx is the extended version of SqlInsert that uses an external InsertBuilder to construct the INSERT statement.
	SqlInsertEx(ctx context.Context, builder ISqlBuilder, tx *gosql.Tx, table string, colsAndVals map[string]interface{}) (gosql.Result, error)

	// SqlSelect constructs a SELECT query and executes it within a context/transaction.
	SqlSelect(ctx context.Context, tx *gosql.Tx, table string, columns []string, filter IFilter, sorting ISorting, fromOffset, numItems int) (*gosql.Rows, error)

	// SqlBuildSelectEx is a utility function to construct the SELECT statement along with values for placeholders.
	SqlBuildSelectEx(builder ISqlBuilder, table string, columns []string, filter IFilter, sorting ISorting, fromOffset, numItems int) (sql string, placeholderValues []interface{})

	// SqlSelectEx is the extended version of SqlSelect that uses an external SelectBuilder to construct the SELECT statement.
	SqlSelectEx(ctx context.Context, builder ISqlBuilder, tx *gosql.Tx, table string, columns []string, filter IFilter, sorting ISorting, fromOffset, numItems int) (*gosql.Rows, error)

	// SqlUpdate constructs an UPDATE query and executes it within a context/transaction.
	SqlUpdate(ctx context.Context, tx *gosql.Tx, table string, colsAndVals map[string]interface{}, filter IFilter) (gosql.Result, error)

	// SqlBuildUpdateEx is a utility function to construct the UPDATE statement along with values for placeholders.
	SqlBuildUpdateEx(builder ISqlBuilder, table string, colsAndVals map[string]interface{}, filter IFilter) (sql string, placeholderValues []interface{})

	// SqlUpdateEx is the extended version of SqlUpdate that uses an external UpdateBuilder to construct the UPDATE statement.
	SqlUpdateEx(ctx context.Context, builder ISqlBuilder, tx *gosql.Tx, table string, colsAndVals map[string]interface{}, filter IFilter) (gosql.Result, error)

	// FetchOne fetches a row from `sql.Rows` and transforms it to godal.IGenericBo.
	//   - FetchOne will NOT call dbRows.Close(), caller must take care of cleaning resource.
	//   - Caller should not call dbRows.Next(), FetchOne will do that.
	FetchOne(tableName string, dbRows *gosql.Rows) (godal.IGenericBo, error)

	// FetchAll fetches all rows from `sql.Rows` and transforms to []godal.IGenericBo.
	//   - FetchOne will NOT call dbRows.Close(), caller must take are of cleaning resource.
	//   - Caller should not call dbRows.Next(), FetchOne will do that.
	FetchAll(tableName string, dbRows *gosql.Rows) ([]godal.IGenericBo, error)

	// IsErrorDuplicatedEntry checks if the error was caused by conflicting in database table entries.
	IsErrorDuplicatedEntry(err error) bool

	// WrapTransaction wraps a function inside a transaction.
	//
	// txFunc: the function to wrap. If the function returns error, the transaction will be aborted, otherwise transaction is committed.
	WrapTransaction(ctx context.Context, txFunc func(ctx context.Context, tx *gosql.Tx) error) error
}

// FilterOperatorTranslator takes a godal.FilterOperator and translates to database-compatible operator string.
//
// Available since v0.5.0
type FilterOperatorTranslator func(op godal.FilterOperator) (string, error)

// DefaultFilterOperatorTranslator is a ready-to-use implementation of FilterOperatorTranslator
//
// Translation rule:
//   - "equal"                   : =
//   - "not equal"               : <>
//   - "greater than"            : >
//   - "greater than or equal to": >=
//   - "less than"               : <
//   - "less than or equal to"   : <=
//   - other                     : error
//
// Available since v0.5.0
func DefaultFilterOperatorTranslator(op godal.FilterOperator) (string, error) {
	switch op {
	case godal.FilterOpEqual:
		return "=", nil
	case godal.FilterOpNotEqual:
		return "<>", nil
	case godal.FilterOpGreater:
		return ">", nil
	case godal.FilterOpGreaterOrEqual:
		return ">=", nil
	case godal.FilterOpLess:
		return "<", nil
	case godal.FilterOpLessOrEqual:
		return "<=", nil
	}
	return "", fmt.Errorf("cannot translate operator %#v to db-compatible operator string", op)
}

// GenericDaoSql is 'database/sql' implementation of godal.IGenericDao & IGenericDaoSql.
//
// Function implementations (n = No, y = Yes, i = inherited):
//   - (n) GdaoCreateFilter(tableName string, bo godal.IGenericBo) godal.FilterOpt
//   - (Y) GdaoDelete(tableName string, bo godal.IGenericBo) (int, error)
//   - (y) GdaoDeleteMany(tableName string, filter godal.FilterOpt) (int, error)
//   - (y) GdaoFetchOne(tableName string, filter godal.FilterOpt) (godal.IGenericBo, error)
//   - (y) GdaoFetchMany(tableName string, filter godal.FilterOpt, sorting *godal.SortingOpt, fromOffset, numItems int) ([]godal.IGenericBo, error)
//   - (y) GdaoCreate(tableName string, bo godal.IGenericBo) (int, error)
//   - (y) GdaoUpdate(tableName string, bo godal.IGenericBo) (int, error)
//   - (y) GdaoSave(tableName string, bo godal.IGenericBo) (int, error)
//
// Note: IGenericDaoSql and GenericDaoSql should be in sync.
type GenericDaoSql struct {
	*godal.AbstractGenericDao
	sqlConnect                   *sql.SqlConnect
	txModeOnWrite                bool
	txIsolationLevel             gosql.IsolationLevel
	funcFilterOperatorTranslator FilterOperatorTranslator
	funcNewPlaceholderGenerator  NewPlaceholderGenerator
}

// SetRowMapper attaches an IRowMapper to the DAO for latter use.
//
// Available since v0.3.0.
func (dao *GenericDaoSql) SetRowMapper(rowMapper godal.IRowMapper) IGenericDaoSql {
	dao.AbstractGenericDao.SetRowMapper(rowMapper)
	return dao
}

// GetSqlConnect returns the SqlConnect instance attached to this DAO.
func (dao *GenericDaoSql) GetSqlConnect() *sql.SqlConnect {
	return dao.sqlConnect
}

// SetSqlConnect attaches a SqlConnect instance to this DAO.
//
// Available since v0.0.2
//
// Note: since v0.6.0 SetSqlConnect will also reset the funcNewPlaceholderGenerator attribute to match the sql flavor.
func (dao *GenericDaoSql) SetSqlConnect(sqlC *sql.SqlConnect) IGenericDaoSql {
	dao.sqlConnect = sqlC
	switch dao.sqlConnect.GetDbFlavor() {
	case sql.FlavorPgSql, sql.FlavorCosmosDb:
		dao.funcNewPlaceholderGenerator = NewPlaceholderGeneratorDollarN
	case sql.FlavorMsSql:
		dao.funcNewPlaceholderGenerator = NewPlaceholderGeneratorAtpiN
	case sql.FlavorOracle:
		dao.funcNewPlaceholderGenerator = NewPlaceholderGeneratorColonN
	case sql.FlavorMySql, sql.FlavorSqlite:
		dao.funcNewPlaceholderGenerator = NewPlaceholderGeneratorQuestion
	default:
		dao.funcNewPlaceholderGenerator = NewPlaceholderGeneratorQuestion
	}
	return dao
}

// GetSqlFlavor returns the sql flavor preference.
func (dao *GenericDaoSql) GetSqlFlavor() sql.DbFlavor {
	return dao.sqlConnect.GetDbFlavor()
}

// SetSqlFlavor sets the sql flavor preference.
//
// Deprecated since v0.6.0 (sql flavor should come from the attached SqlConnect instance).
func (dao *GenericDaoSql) SetSqlFlavor(sqlFlavor sql.DbFlavor) IGenericDaoSql {
	// DEPRECATED
	return dao
}

// GetTxModeOnWrite returns 'true' if transaction mode is enabled on write operations, 'false' otherwise.
//
// RDBMS/SQL's implementation of GdaoSave is "try update, if failed then insert". It can be done either in transaction (txModeOnWrite=true) or non-transaction (txModeOnWrite=false) mode.
//
// Available: since v0.1.0
func (dao *GenericDaoSql) GetTxModeOnWrite() bool {
	return dao.txModeOnWrite
}

// SetTxModeOnWrite enables/disables transaction mode on write operations.
//
// RDBMS/SQL's implementation of GdaoSave is "try update, if failed then insert". It can be done either in transaction (txModeOnWrite=true) or non-transaction (txModeOnWrite=false) mode.
// By default, GenericDaoSql is created with 'txModeOnWrite=true', and it is recommended setting.
//
// Available: since v0.1.0
func (dao *GenericDaoSql) SetTxModeOnWrite(enabled bool) IGenericDaoSql {
	dao.txModeOnWrite = enabled
	return dao
}

// GetTxIsolationLevel returns current transaction isolation level setting.
//
// Available: since v0.1.0
func (dao *GenericDaoSql) GetTxIsolationLevel() gosql.IsolationLevel {
	return dao.txIsolationLevel
}

// SetTxIsolationLevel sets new transaction isolation level.
//
// Available: since v0.1.0
func (dao *GenericDaoSql) SetTxIsolationLevel(txIsolationLevel gosql.IsolationLevel) IGenericDaoSql {
	dao.txIsolationLevel = txIsolationLevel
	return dao
}

// StartTx starts a new transaction.
//
// If ctx is nil, StartTx creates a new context to use.
// The isolation level is determined by 'txIsolationLevel' attribute. Setting new value for 'txIsolationLevel' will
// only affect new transactions, _not_ affecting existing ones.
//
// Available: since v0.1.0
func (dao *GenericDaoSql) StartTx(ctx context.Context) (*gosql.Tx, error) {
	return dao.sqlConnect.GetDB().BeginTx(dao.sqlConnect.NewContextIfNil(ctx), &gosql.TxOptions{Isolation: dao.txIsolationLevel})
}

// GetFuncNewPlaceholderGenerator returns the function that creates 'PlaceholderGenerator' instances.
func (dao *GenericDaoSql) GetFuncNewPlaceholderGenerator() NewPlaceholderGenerator {
	return dao.funcNewPlaceholderGenerator
}

// SetFuncNewPlaceholderGenerator sets the function used to create 'PlaceholderGenerator'.
func (dao *GenericDaoSql) SetFuncNewPlaceholderGenerator(funcNewPlaceholderGenerator NewPlaceholderGenerator) IGenericDaoSql {
	dao.funcNewPlaceholderGenerator = funcNewPlaceholderGenerator
	return dao
}

// BuildFilter transforms a godal.FilterOpt to IFilter.
//
// Available since v0.5.0
func (dao *GenericDaoSql) BuildFilter(tableName string, filter godal.FilterOpt) (IFilter, error) {
	if filter == nil {
		return nil, nil
	}
	rm := dao.GetRowMapper()
	if rm == nil {
		return nil, errors.New("row-mapper is required to build filter")
	}
	funcFoTranslator := dao.funcFilterOperatorTranslator
	if funcFoTranslator == nil {
		return nil, errors.New("filter-operator-translator is required to build filter")
	}

	switch filter.(type) {
	case godal.FilterOptFieldOpValue:
		f := filter.(godal.FilterOptFieldOpValue)
		return dao.BuildFilter(tableName, &f)
	case *godal.FilterOptFieldOpValue:
		f := filter.(*godal.FilterOptFieldOpValue)
		opStr, err := funcFoTranslator(f.Operator)
		result := &FilterFieldValue{
			Field:    rm.ToDbColName(tableName, f.FieldName),
			Operator: opStr,
			Value:    f.Value,
		}
		return result, err
	case godal.FilterOptFieldOpField:
		f := filter.(godal.FilterOptFieldOpField)
		return dao.BuildFilter(tableName, &f)
	case *godal.FilterOptFieldOpField:
		f := filter.(*godal.FilterOptFieldOpField)
		opStr, err := funcFoTranslator(f.Operator)
		result := &FilterExpression{
			Left:     rm.ToDbColName(tableName, f.FieldNameLeft),
			Operator: opStr,
			Right:    rm.ToDbColName(tableName, f.FieldNameRight),
		}
		return result, err
	case godal.FilterOptFieldIsNull:
		f := filter.(godal.FilterOptFieldIsNull)
		return dao.BuildFilter(tableName, &f)
	case *godal.FilterOptFieldIsNull:
		f := filter.(*godal.FilterOptFieldIsNull)
		result := &FilterIsNull{FilterFieldValue: FilterFieldValue{Field: rm.ToDbColName(tableName, f.FieldName)}}
		return result, nil
	case godal.FilterOptFieldIsNotNull:
		f := filter.(godal.FilterOptFieldIsNotNull)
		return dao.BuildFilter(tableName, &f)
	case *godal.FilterOptFieldIsNotNull:
		f := filter.(*godal.FilterOptFieldIsNotNull)
		result := &FilterIsNotNull{FilterFieldValue: FilterFieldValue{Field: rm.ToDbColName(tableName, f.FieldName)}}
		return result, nil
	case godal.FilterOptAnd:
		f := filter.(godal.FilterOptAnd)
		return dao.BuildFilter(tableName, &f)
	case *godal.FilterOptAnd:
		f := filter.(*godal.FilterOptAnd)
		result := &FilterAnd{}
		for _, innerF := range f.Filters {
			innerResult, err := dao.BuildFilter(tableName, innerF)
			if err != nil {
				return nil, err
			}
			result.Add(innerResult)
		}
		return result, nil
	case godal.FilterOptOr:
		f := filter.(godal.FilterOptOr)
		return dao.BuildFilter(tableName, &f)
	case *godal.FilterOptOr:
		f := filter.(*godal.FilterOptOr)
		result := &FilterOr{}
		for _, innerF := range f.Filters {
			innerResult, err := dao.BuildFilter(tableName, innerF)
			if err != nil {
				return nil, err
			}
			result.Add(innerResult)
		}
		return result, nil
	}

	return nil, fmt.Errorf("cannot build filter from %T", filter)
}

// BuildSorting builds elements for 'ORDER BY' clause.
//
// Available since v0.5.0
func (dao *GenericDaoSql) BuildSorting(tableName string, sorting *godal.SortingOpt) (ISorting, error) {
	if sorting == nil || len(sorting.Fields) == 0 {
		return nil, nil
	}
	rm := dao.GetRowMapper()
	if rm == nil {
		return nil, errors.New("row-mapper is needed to build sorting clause")
	}
	result := &GenericSorting{Flavor: dao.GetSqlFlavor()}
	for _, field := range sorting.Fields {
		colName := rm.ToDbColName(tableName, field.FieldName)
		if colName == "" {
			return nil, fmt.Errorf("cannot map field \"%s\" to db column name", field.FieldName)
		}
		if field.Descending {
			colName += ":-1"
		}
		result.Add(colName)
	}
	return result, nil
}

/*----------------------------------------------------------------------*/

// SqlExecute executes a non-SELECT SQL statement within a context/transaction.
//   - If ctx is nil, SqlExecute creates a new context to use.
//   - If tx is not nil, SqlExecute uses transaction context to execute the query.
//   - If tx is nil, SqlExecute calls DB.ExecContext to execute the query.
func (dao *GenericDaoSql) SqlExecute(ctx context.Context, tx *gosql.Tx, sql string, values ...interface{}) (gosql.Result, error) {
	ctx = dao.sqlConnect.NewContextIfNil(ctx)
	if tx != nil {
		pstm, err := tx.PrepareContext(ctx, sql)
		if err != nil {
			return nil, err
		}
		return pstm.ExecContext(ctx, values...)
	}
	db := dao.sqlConnect.GetDB()
	pstm, err := db.PrepareContext(ctx, sql)
	if err != nil {
		return nil, err
	}
	return pstm.ExecContext(ctx, values...)
}

// SqlQuery executes a SELECT SQL statement within a context/transaction.
//   - If ctx is nil, SqlQuery creates a new context to use.
//   - If tx is not nil, SqlQuery uses transaction context to execute the query.
//   - If tx is nil, SqlQuery calls DB.QueryContext to execute the query.
func (dao *GenericDaoSql) SqlQuery(ctx context.Context, tx *gosql.Tx, sql string, values ...interface{}) (*gosql.Rows, error) {
	ctx = dao.sqlConnect.NewContextIfNil(ctx)
	if tx != nil {
		pstm, err := tx.PrepareContext(ctx, sql)
		if err != nil {
			return nil, err
		}
		return pstm.QueryContext(ctx, values...)
	}
	db := dao.sqlConnect.GetDB()
	pstm, err := db.PrepareContext(ctx, sql)
	if err != nil {
		return nil, err
	}
	return pstm.QueryContext(ctx, values...)
}

// SqlDelete constructs a DELETE statement and executes it within a context/transaction.
func (dao *GenericDaoSql) SqlDelete(ctx context.Context, tx *gosql.Tx, table string, filter IFilter) (gosql.Result, error) {
	return dao.SqlDeleteEx(ctx, nil, tx, table, filter)
}

// SqlBuildDeleteEx is a utility function to construct the DELETE statement along with values for placeholders.
//
// Available since v0.3.0
func (dao *GenericDaoSql) SqlBuildDeleteEx(builder ISqlBuilder, table string, filter IFilter) (sql string, placeholderValues []interface{}) {
	if builder == nil {
		builder = NewDeleteBuilder().WithFlavor(dao.GetSqlFlavor()).WithTable(table).WithFilter(filter)
		if dao.funcNewPlaceholderGenerator != nil {
			builder.(*DeleteBuilder).WithPlaceholderGenerator(dao.funcNewPlaceholderGenerator())
		}
	}
	return builder.Build()
}

// SqlDeleteEx is the extended version of SqlDelete that uses an external DeleteBuilder to construct the DELETE statement.
//
// Available since v0.3.0
func (dao *GenericDaoSql) SqlDeleteEx(ctx context.Context, builder ISqlBuilder, tx *gosql.Tx, table string, filter IFilter) (gosql.Result, error) {
	sqlStm, values := dao.SqlBuildDeleteEx(builder, table, filter)
	return dao.SqlExecute(ctx, tx, sqlStm, values...)
}

// SqlInsert constructs a INSERT statement and executes it within a context/transaction.
func (dao *GenericDaoSql) SqlInsert(ctx context.Context, tx *gosql.Tx, table string, colsAndVals map[string]interface{}) (gosql.Result, error) {
	return dao.SqlInsertEx(ctx, nil, tx, table, colsAndVals)
}

// SqlBuildInsertEx is a utility function to construct the INSERT statement along with values for placeholders.
//
// Available since v0.3.0
func (dao *GenericDaoSql) SqlBuildInsertEx(builder ISqlBuilder, table string, colsAndVals map[string]interface{}) (sql string, placeholderValues []interface{}) {
	if builder == nil {
		builder = NewInsertBuilder().WithFlavor(dao.GetSqlFlavor()).WithTable(table).WithValues(colsAndVals)
		if dao.funcNewPlaceholderGenerator != nil {
			builder.(*InsertBuilder).WithPlaceholderGenerator(dao.funcNewPlaceholderGenerator())
		}
	}
	return builder.Build()
}

// SqlInsertEx is the extended version of SqlInsert that uses an external InsertBuilder to construct the INSERT statement.
//
// Available since v0.3.0
func (dao *GenericDaoSql) SqlInsertEx(ctx context.Context, builder ISqlBuilder, tx *gosql.Tx, table string, colsAndVals map[string]interface{}) (gosql.Result, error) {
	sqlStm, values := dao.SqlBuildInsertEx(builder, table, colsAndVals)
	return dao.SqlExecute(ctx, tx, sqlStm, values...)
}

// SqlSelect constructs a SELECT query and executes it within a context/transaction.
func (dao *GenericDaoSql) SqlSelect(ctx context.Context, tx *gosql.Tx, table string, columns []string, filter IFilter, sorting ISorting, fromOffset, numItems int) (*gosql.Rows, error) {
	return dao.SqlSelectEx(ctx, nil, tx, table, columns, filter, sorting, fromOffset, numItems)
}

// SqlBuildSelectEx is a utility function to construct the SELECT statement along with values for placeholders.
//
// Available since v0.3.0
func (dao *GenericDaoSql) SqlBuildSelectEx(builder ISqlBuilder, table string, columns []string, filter IFilter, sorting ISorting, fromOffset, numItems int) (sql string, placeholderValues []interface{}) {
	if builder == nil {
		builder = NewSelectBuilder().WithFlavor(dao.GetSqlFlavor()).
			WithColumns(columns...).WithTables(table).
			WithFilter(filter).
			WithSorting(sorting).
			WithLimit(numItems, fromOffset)
		if dao.funcNewPlaceholderGenerator != nil {
			builder.(*SelectBuilder).WithPlaceholderGenerator(dao.funcNewPlaceholderGenerator())
		}
	}
	return builder.Build()
}

// SqlSelectEx is the extended version of SqlSelect that uses an external SelectBuilder to construct the SELECT statement.
//
// Available since v0.3.0
func (dao *GenericDaoSql) SqlSelectEx(ctx context.Context, builder ISqlBuilder, tx *gosql.Tx, table string, columns []string, filter IFilter, sorting ISorting, fromOffset, numItems int) (*gosql.Rows, error) {
	query, values := dao.SqlBuildSelectEx(builder, table, columns, filter, sorting, fromOffset, numItems)
	return dao.SqlQuery(ctx, tx, query, values...)
}

// SqlUpdate constructs an UPDATE query and executes it within a context/transaction.
func (dao *GenericDaoSql) SqlUpdate(ctx context.Context, tx *gosql.Tx, table string, colsAndVals map[string]interface{}, filter IFilter) (gosql.Result, error) {
	return dao.SqlUpdateEx(ctx, nil, tx, table, colsAndVals, filter)
}

// SqlBuildUpdateEx is a utility function to construct the UPDATE statement along with values for placeholders.
//
// Available since v0.3.0
func (dao *GenericDaoSql) SqlBuildUpdateEx(builder ISqlBuilder, table string, colsAndVals map[string]interface{}, filter IFilter) (sql string, placeholderValues []interface{}) {
	if builder == nil {
		builder = NewUpdateBuilder().WithFlavor(dao.GetSqlFlavor()).WithTable(table).WithValues(colsAndVals).WithFilter(filter)
		if dao.funcNewPlaceholderGenerator != nil {
			builder.(*UpdateBuilder).WithPlaceholderGenerator(dao.funcNewPlaceholderGenerator())
		}
	}
	return builder.Build()
}

// SqlUpdateEx is the extended version of SqlUpdate that uses an external UpdateBuilder to construct the UPDATE statement.
//
// Available since v0.3.0
func (dao *GenericDaoSql) SqlUpdateEx(ctx context.Context, builder ISqlBuilder, tx *gosql.Tx, table string, colsAndVals map[string]interface{}, filter IFilter) (gosql.Result, error) {
	query, values := dao.SqlBuildUpdateEx(builder, table, colsAndVals, filter)
	return dao.SqlExecute(ctx, tx, query, values...)
}

/*----------------------------------------------------------------------*/

// FetchOne fetches a row from `sql.Rows` and transforms it to godal.IGenericBo.
//   - FetchOne will NOT call dbRows.Close(), caller must take care of cleaning up resource.
//   - Caller should not call dbRows.Next(), FetchOne will do that.
func (dao *GenericDaoSql) FetchOne(tableName string, dbRows *gosql.Rows) (godal.IGenericBo, error) {
	var bo godal.IGenericBo
	var err error
	e := dao.sqlConnect.FetchRowsCallback(dbRows, func(row map[string]interface{}, e error) bool {
		if e == nil {
			bo, err = dao.GetRowMapper().ToBo(tableName, row)
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

// FetchAll fetches all rows from `sql.Rows` and transforms to []godal.IGenericBo.
//   - FetchOne will NOT call dbRows.Close(), caller must take are of cleaning up resource.
//   - Caller should not call dbRows.Next(), FetchOne will do that.
func (dao *GenericDaoSql) FetchAll(tableName string, dbRows *gosql.Rows) ([]godal.IGenericBo, error) {
	boList := make([]godal.IGenericBo, 0)
	var err error
	e := dao.sqlConnect.FetchRowsCallback(dbRows, func(row map[string]interface{}, e error) bool {
		if e != nil {
			err = e
			return false
		}
		bo, e := dao.GetRowMapper().ToBo(tableName, row)
		if e != nil {
			err = e
			return false
		}
		boList = append(boList, bo)
		return true
	})
	if err != nil {
		return boList, err
	}
	return boList, e
}

/*----------------------------------------------------------------------*/

// GdaoDelete implements godal.IGenericDao.GdaoDelete.
//
// Available: since v0.1.0
func (dao *GenericDaoSql) GdaoDelete(tableName string, bo godal.IGenericBo) (int, error) {
	return dao.GdaoDeleteWithTx(nil, nil, tableName, bo)
}

// GdaoDeleteWithTx is database/sql variant of GdaoDelete.
//
// Available: since v0.1.0
func (dao *GenericDaoSql) GdaoDeleteWithTx(ctx context.Context, tx *gosql.Tx, tableName string, bo godal.IGenericBo) (int, error) {
	filter := dao.GdaoCreateFilter(tableName, bo)
	return dao.GdaoDeleteManyWithTx(ctx, tx, tableName, filter)
}

// GdaoDeleteMany implements godal.IGenericDao.GdaoDeleteMany.
func (dao *GenericDaoSql) GdaoDeleteMany(tableName string, filter godal.FilterOpt) (int, error) {
	return dao.GdaoDeleteManyWithTx(nil, nil, tableName, filter)
}

// GdaoDeleteManyWithTx is database/sql variant of GdaoDeleteMany.
//
// Available: since v0.1.0
func (dao *GenericDaoSql) GdaoDeleteManyWithTx(ctx context.Context, tx *gosql.Tx, tableName string, filter godal.FilterOpt) (int, error) {
	if f, err := dao.BuildFilter(tableName, filter); err != nil {
		return 0, err
	} else if result, err := dao.SqlDelete(ctx, tx, tableName, f); err != nil {
		return 0, err
	} else {
		numRows, err := result.RowsAffected()
		return int(numRows), err
	}
}

// GdaoFetchOne implements godal.IGenericDao.GdaoFetchOne.
func (dao *GenericDaoSql) GdaoFetchOne(tableName string, filter godal.FilterOpt) (godal.IGenericBo, error) {
	return dao.GdaoFetchOneWithTx(nil, nil, tableName, filter)
}

// GdaoFetchOneWithTx is database/sql variant of GdaoFetchOne.
//
// Available: since v0.1.0
func (dao *GenericDaoSql) GdaoFetchOneWithTx(ctx context.Context, tx *gosql.Tx, tableName string, filter godal.FilterOpt) (godal.IGenericBo, error) {
	f, err := dao.BuildFilter(tableName, filter)
	if err != nil {
		return nil, err
	}
	columns := dao.GetRowMapper().ColumnsList(tableName)
	dbRows, err := dao.SqlSelect(ctx, tx, tableName, columns, f, nil, 0, 0)
	if dbRows != nil {
		defer func() { _ = dbRows.Close() }()
	}
	if err != nil {
		return nil, err
	}
	return dao.FetchOne(tableName, dbRows)
}

// GdaoFetchMany implements godal.IGenericDao.GdaoFetchMany.
func (dao *GenericDaoSql) GdaoFetchMany(tableName string, filter godal.FilterOpt, sorting *godal.SortingOpt, fromOffset, numRows int) ([]godal.IGenericBo, error) {
	return dao.GdaoFetchManyWithTx(nil, nil, tableName, filter, sorting, fromOffset, numRows)
}

// GdaoFetchManyWithTx is database/sql variant of GdaoFetchMany.
//
// Available: since v0.1.0
func (dao *GenericDaoSql) GdaoFetchManyWithTx(ctx context.Context, tx *gosql.Tx, tableName string, filter godal.FilterOpt, sorting *godal.SortingOpt, fromOffset, numRows int) ([]godal.IGenericBo, error) {
	f, err := dao.BuildFilter(tableName, filter)
	if err != nil {
		return nil, err
	}
	o, err := dao.BuildSorting(tableName, sorting)
	if err != nil {
		return nil, err
	}
	dbRows, err := dao.SqlSelect(ctx, tx, tableName, dao.GetRowMapper().ColumnsList(tableName), f, o, fromOffset, numRows)
	if dbRows != nil {
		defer func() { _ = dbRows.Close() }()
	}
	if err != nil {
		return nil, err
	}
	return dao.FetchAll(tableName, dbRows)
}

// IsErrorDuplicatedEntry checks if the error was caused by conflicting in database table entries.
func (dao *GenericDaoSql) IsErrorDuplicatedEntry(err error) bool {
	if err == nil {
		return false
	}
	switch dao.GetSqlFlavor() {
	case sql.FlavorMySql:
		return regexp.MustCompile(`\W1062\W`).FindString(err.Error()) != ""
	case sql.FlavorPgSql:
		return regexp.MustCompile(`\W23505\W`).FindString(fmt.Sprintf("%e", err)) != ""
	case sql.FlavorMsSql:
		return regexp.MustCompile(`\W2627\W|\W2601\W`).FindString(fmt.Sprintf("%e", err)) != ""
	case sql.FlavorOracle:
		errStr := fmt.Sprintf("%e", err)
		return regexp.MustCompile(`\WORA\-00001\W`).FindString(errStr) != "" ||
			regexp.MustCompile(`\Wunique constraint.*?violated\W`).FindString(errStr) != ""
	case sql.FlavorSqlite:
		errStr := fmt.Sprintf("%e", err)
		return regexp.MustCompile(`\WErrNo=19\W`).FindString(errStr) != "" &&
			regexp.MustCompile(`\WErrNoExtended=1555\W|\WErrNoExtended=2067\W`).FindString(errStr) != ""
	}
	return false
}

// GdaoCreate implements godal.IGenericDao.GdaoCreate.
func (dao *GenericDaoSql) GdaoCreate(tableName string, bo godal.IGenericBo) (int, error) {
	return dao.GdaoCreateWithTx(nil, nil, tableName, bo)
}

// GdaoCreateWithTx is database/sql variant of GdaoCreate.
//
// Available: since v0.1.0
func (dao *GenericDaoSql) GdaoCreateWithTx(ctx context.Context, tx *gosql.Tx, tableName string, bo godal.IGenericBo) (int, error) {
	if row, err := dao.GetRowMapper().ToRow(tableName, bo); err != nil {
		return 0, err
	} else if colsAndVals, err := reddo.ToMap(row, typeMap); err != nil {
		return 0, err
	} else if result, err := dao.SqlInsert(ctx, tx, tableName, colsAndVals.(map[string]interface{})); err != nil {
		if dao.IsErrorDuplicatedEntry(err) {
			return 0, godal.ErrGdaoDuplicatedEntry
		}
		return 0, err
	} else {
		numRows, err := result.RowsAffected()
		return int(numRows), err
	}
}

// GdaoUpdate implements godal.IGenericDao.GdaoUpdate.
func (dao *GenericDaoSql) GdaoUpdate(tableName string, bo godal.IGenericBo) (int, error) {
	return dao.GdaoUpdateWithTx(nil, nil, tableName, bo)
}

// GdaoUpdateWithTx is database/sql variant of GdaoUpdate.
//
// Available: since v0.1.0
func (dao *GenericDaoSql) GdaoUpdateWithTx(ctx context.Context, tx *gosql.Tx, tableName string, bo godal.IGenericBo) (int, error) {
	filter, err := dao.BuildFilter(tableName, dao.GdaoCreateFilter(tableName, bo))
	if err != nil {
		return 0, err
	}
	row, err := dao.GetRowMapper().ToRow(tableName, bo)
	if err != nil {
		return 0, err
	}
	colsAndVals, err := reddo.ToMap(row, typeMap)
	if err != nil {
		return 0, err
	}
	result, err := dao.SqlUpdate(ctx, tx, tableName, colsAndVals.(map[string]interface{}), filter)
	if err != nil {
		if dao.IsErrorDuplicatedEntry(err) {
			return 0, godal.ErrGdaoDuplicatedEntry
		}
		return 0, err
	}
	numRows, err := result.RowsAffected()
	return int(numRows), err
}

// GdaoSave implements godal.IGenericDao.GdaoSave.
func (dao *GenericDaoSql) GdaoSave(tableName string, bo godal.IGenericBo) (int, error) {
	var numRows int
	var err error
	if dao.txModeOnWrite {
		err = dao.WrapTransaction(nil, func(ctx context.Context, tx *gosql.Tx) error {
			var e error
			numRows, e = dao.GdaoSaveWithTx(ctx, tx, tableName, bo)
			return e
		})
	} else {
		numRows, err = dao.GdaoSaveWithTx(nil, nil, tableName, bo)
	}
	return numRows, err
}

// GdaoSaveWithTx is extended-implementation of godal.IGenericDao.GdaoSave.
//
// Available: since v0.1.0
func (dao *GenericDaoSql) GdaoSaveWithTx(ctx context.Context, tx *gosql.Tx, tableName string, bo godal.IGenericBo) (int, error) {
	filter, err := dao.BuildFilter(tableName, dao.GdaoCreateFilter(tableName, bo))
	if err != nil {
		return 0, err
	}
	row, err := dao.GetRowMapper().ToRow(tableName, bo)
	if err != nil {
		return 0, err
	}
	colsAndVals, err := reddo.ToMap(row, typeMap)
	if err != nil {
		return 0, err
	}

	// firstly: try to update row
	if result, err := dao.SqlUpdate(ctx, tx, tableName, colsAndVals.(map[string]interface{}), filter); err != nil {
		if dao.IsErrorDuplicatedEntry(err) {
			return 0, godal.ErrGdaoDuplicatedEntry
		}
		return 0, err
	} else if numRows, err := result.RowsAffected(); err != nil || numRows > 0 {
		return int(numRows), err
	} else {
		// secondly: no row updated, try insert row
		result, err := dao.SqlInsert(ctx, tx, tableName, colsAndVals.(map[string]interface{}))
		if err != nil {
			if dao.IsErrorDuplicatedEntry(err) {
				return 0, godal.ErrGdaoDuplicatedEntry
			}
			return 0, err
		}
		numRows, err := result.RowsAffected()
		return int(numRows), err
	}
}

// WrapTransaction wraps a function inside a transaction.
//
// txFunc: the function to wrap. If the function returns error, the transaction will be aborted, otherwise transaction is committed.
//
// Available: since v0.1.0
func (dao *GenericDaoSql) WrapTransaction(ctx context.Context, txFunc func(ctx context.Context, tx *gosql.Tx) error) error {
	var tx *gosql.Tx
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
	ctx = dao.sqlConnect.NewContextIfNil(ctx)
	if tx, err = dao.sqlConnect.GetDB().BeginTx(ctx, &gosql.TxOptions{Isolation: dao.txIsolationLevel}); err != nil {
		return err
	}
	err = txFunc(ctx, tx)
	return err
}
