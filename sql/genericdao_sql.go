/*
Package sql provides a generic 'database/sql' implementation of godal.IGenericDao.

General guideline:

	- Dao must implement IGenericDao.GdaoCreateFilter(string, IGenericBo) interface{}.

Guideline: Use GenericDaoSql (and godal.IGenericBo) directly

	- Define a dao struct that implements IGenericDao.GdaoCreateFilter(string, IGenericBo) interface{}.
	- Optionally, create a helper function to create dao instances.

	// Remember to import database driver. The following example uses MySQL hence driver "github.com/go-sql-driver/mysql".
	import (
		"github.com/btnguyen2k/consu/reddo"
		"github.com/btnguyen2k/godal"
		"github.com/btnguyen2k/godal/sql"
		"github.com/btnguyen2k/prom"

		_ "github.com/go-sql-driver/mysql"
	)

	type myGenericDaoMysql struct {
		*sql.GenericDaoSql
	}

	// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter.
	func (dao *myGenericDaoMysql) GdaoCreateFilter(storageId string, bo godal.IGenericBo) interface{} {
		id := bo.GboGetAttrUnsafe(fieldId, reddo.TypeString)
		return map[string]interface{}{tableColumnId: id}
	}

	// newGenericDaoMysql is helper function to create myGenericDaoMysql instances.
	func newGenericDaoMysql(sqlc *prom.SqlConnect, txModeOnWrite bool) godal.IGenericDao {
		rowMapper := &sql.GenericRowMapperSql{NameTransformation: sql.NameTransfLowerCase}
		dao := &myGenericDaoMysql{}
		dao.GenericDaoSql = sql.NewGenericDaoSql(sqlc, godal.NewAbstractGenericDao(dao))
		dao.SetTxModeOnWrite(txModeOnWrite).SetSqlFlavor(prom.FlavorMySql)
		dao.SetRowMapper(rowMapper)
		return dao
	}

	In most cases, GenericRowMapperSql should be sufficient:
		- Column/Field names can be transformed to lower-cased, upper-cased or kept intact. Transformation rule is specified by GenericRowMapperSql.NameTransformation
		- Column names (after transformed) can be translated to field names via GenericRowMapperSql.ColNameToGboFieldTranslator,
		- and vice versa, field names (after transformed) can be translated to column names via GenericRowMapperSql.GboFieldToColNameTranslator

Guideline: Implement custom 'database/sql' business dao and bo

	- Define and implement the business dao (Note: dao must implement IGenericDao.GdaoCreateFilter(string, IGenericBo) interface{}).
	- Optionally, create a helper function to create dao instances.
	- Define functions to transform godal.IGenericBo to business bo and vice versa.

	// Remember to import database driver. The following example uses MySQL hence driver "github.com/go-sql-driver/mysql".
	import (
		"github.com/btnguyen2k/consu/reddo"
		"github.com/btnguyen2k/godal"
		"github.com/btnguyen2k/godal/sql"
		"github.com/btnguyen2k/prom"

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
		*sql.GenericDaoSql
		tableName string
	}

	// NewDaoAppMysql is helper function to create DaoAppMysql instances.
	func NewDaoAppMysql(sqlc *prom.SqlConnect, taleName string, txModeOnWrite bool) *DaoAppMysql {
		dao := &DaoAppMysql{tableName: taleName}
		dao.GenericDaoSql = mongo.NewGenericDaoSql(sqlc, godal.NewAbstractGenericDao(dao))
		dao.SetTxModeOnWrite(txModeOnWrite).SetSqlFlavor(prom.FlavorMySql)
		dao.SetRowMapper(&sql.GenericRowMapperSql{NameTransformation: sql.NameTransfLowerCase})
		return dao
	}

	In most cases, GenericRowMapperSql should be sufficient:
		- Column/Field names can be transformed to lower-cased, upper-cased or kept intact. Transformation rule is specified by GenericRowMapperSql.NameTransformation
		- Column names (after transformed) can be translated to field names via GenericRowMapperSql.ColNameToGboFieldTranslator,
		- and vice versa, field names (after transformed) can be translated to column names via GenericRowMapperSql.GboFieldToColNameTranslator

See more examples in 'examples' directory on project's GitHub: https://github.com/btnguyen2k/godal/tree/master/examples

To create prom.SqlConnect, see package github.com/btnguyen2k/prom
*/
package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"regexp"

	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/prom"

	"github.com/btnguyen2k/godal"
)

// NewGenericDaoSql constructs a new GenericDaoSql with 'txModeOnWrite=true'.
func NewGenericDaoSql(sqlConnect *prom.SqlConnect, agdao *godal.AbstractGenericDao) *GenericDaoSql {
	dao := &GenericDaoSql{
		AbstractGenericDao:          agdao,
		sqlConnect:                  sqlConnect,
		sqlFlavor:                   prom.FlavorDefault,
		txModeOnWrite:               true,
		txIsolationLevel:            sql.LevelDefault,
		optionOpLiteral:             DefaultOptionLiteralOperator,
		funcNewPlaceholderGenerator: NewPlaceholderGeneratorQuestion,
	}
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
	GdaoDeleteWithTx(ctx context.Context, tx *sql.Tx, storageId string, bo godal.IGenericBo) (int, error)

	// GdaoDeleteManyWithTx is database/sql variant of GdaoDeleteMany.
	GdaoDeleteManyWithTx(ctx context.Context, tx *sql.Tx, storageId string, filter interface{}) (int, error)

	// GdaoFetchOneWithTx is database/sql variant of GdaoFetchOne.
	GdaoFetchOneWithTx(ctx context.Context, tx *sql.Tx, storageId string, filter interface{}) (godal.IGenericBo, error)

	// GdaoFetchManyWithTx is database/sql variant of GdaoFetchMany.
	GdaoFetchManyWithTx(ctx context.Context, tx *sql.Tx, storageId string, filter interface{}, sorting *godal.SortingOpt, fromOffset, numRows int) ([]godal.IGenericBo, error)

	// GdaoCreateWithTx is database/sql variant of GdaoCreate.
	GdaoCreateWithTx(ctx context.Context, tx *sql.Tx, storageId string, bo godal.IGenericBo) (int, error)

	// GdaoUpdateWithTx is database/sql variant of GdaoUpdate.
	GdaoUpdateWithTx(ctx context.Context, tx *sql.Tx, storageId string, bo godal.IGenericBo) (int, error)

	// GdaoSaveWithTx is database/sql variant of godal.IGenericDao.GdaoSave.
	GdaoSaveWithTx(ctx context.Context, tx *sql.Tx, storageId string, bo godal.IGenericBo) (int, error)

	// SetRowMapper attaches an IRowMapper to the DAO for latter use.
	SetRowMapper(rowMapper godal.IRowMapper) IGenericDaoSql

	// GetSqlConnect returns the '*prom.SqlConnect' instance attached to this DAO.
	GetSqlConnect() *prom.SqlConnect

	// SetSqlConnect attaches a '*prom.SqlConnect' instance to this DAO.
	SetSqlConnect(sqlC *prom.SqlConnect) IGenericDaoSql

	// GetSqlFlavor returns the sql flavor preference.
	GetSqlFlavor() prom.DbFlavor

	// SetSqlFlavor set the sql flavor preference.
	SetSqlFlavor(sqlFlavor prom.DbFlavor) IGenericDaoSql

	// GetTxModeOnWrite returns 'true' if transaction mode is enabled on write operations, 'false' otherwise.
	//
	// RDBMS/SQL's implementation of GdaoSave is "try update, if failed then insert". It can be done either in transactional (txModeOnWrite=true) or non-transactional (txModeOnWrite=false) mode.
	GetTxModeOnWrite() bool

	// SetTxModeOnWrite enables/disables transaction mode on write operations.
	//
	// RDBMS/SQL's implementation of GdaoSave is "try update, if failed then insert". It can be done either in transactional (txModeOnWrite=true) or non-transactional (txModeOnWrite=false) mode.
	SetTxModeOnWrite(enabled bool) IGenericDaoSql

	// GetTxIsolationLevel returns current transaction isolation level setting.
	GetTxIsolationLevel() sql.IsolationLevel

	// SetTxIsolationLevel sets new transaction isolation level.
	SetTxIsolationLevel(txIsolationLevel sql.IsolationLevel) IGenericDaoSql

	// StartTx starts a new transaction.
	StartTx(ctx context.Context) (*sql.Tx, error)

	// GetOptionOpLiteral returns operation literal settings.
	GetOptionOpLiteral() *OptionOpLiteral

	// SetOptionOpLiteral sets operation literal settings.
	SetOptionOpLiteral(optionOpLiteral *OptionOpLiteral) IGenericDaoSql

	// GetFuncNewPlaceholderGenerator returns the function creates 'PlaceholderGenerator'.
	GetFuncNewPlaceholderGenerator() NewPlaceholderGenerator

	// SetFuncNewPlaceholderGenerator sets the function used to create 'PlaceholderGenerator'.
	SetFuncNewPlaceholderGenerator(funcNewPlaceholderGenerator NewPlaceholderGenerator) IGenericDaoSql

	// BuildFilter builds IFilter instance based on the following rules:
	//   - If 'filter' is nil: return nil.
	//   - If 'filter' is IFilter: return 'filter'.
	//   - If 'filter' is a map: build a FilterAnd combining all map entries, using operation "=", and return it.
	//   - Otherwise, return error.
	BuildFilter(filter interface{}) (IFilter, error)

	// BuildSorting builds elements for 'ORDER BY' clause, based on the following rules:
	//
	// Available since v0.5.0
	BuildSorting(storageId string, ordering *godal.SortingOpt) (ISorting, error)

	// SqlExecute executes a non-SELECT SQL statement within a context/transaction.
	//   - If tx is not nil, the transaction context is used to execute the query.
	//   - If tx is nil, DB.ExecContext is used to execute the query.
	SqlExecute(ctx context.Context, tx *sql.Tx, sql string, values ...interface{}) (sql.Result, error)

	// SqlQuery executes a SELECT SQL statement within a context/transaction.
	//   - If tx is not nil, the transaction context is used to execute the query.
	//   - If tx is nil, DB.ExecContext is used to execute the query.
	SqlQuery(ctx context.Context, tx *sql.Tx, sql string, values ...interface{}) (*sql.Rows, error)

	// SqlDelete constructs a DELETE statement and executes it within a context/transaction.
	SqlDelete(ctx context.Context, tx *sql.Tx, table string, filter IFilter) (sql.Result, error)

	// SqlBuildDeleteEx is a utility function to construct the DELETE statement along with values for placeholders.
	SqlBuildDeleteEx(builder ISqlBuilder, table string, filter IFilter) (sql string, placeholderValues []interface{})

	// SqlDeleteEx is the extended version of SqlDelete that uses an external DeleteBuilder to construct the DELETE statement.
	SqlDeleteEx(builder ISqlBuilder, ctx context.Context, tx *sql.Tx, table string, filter IFilter) (sql.Result, error)

	// SqlInsert constructs a INSERT statement and executes it within a context/transaction.
	SqlInsert(ctx context.Context, tx *sql.Tx, table string, colsAndVals map[string]interface{}) (sql.Result, error)

	// SqlBuildInsertEx is a utility function to construct the INSERT statement along with values for placeholders.
	SqlBuildInsertEx(builder ISqlBuilder, table string, colsAndVals map[string]interface{}) (sql string, placeholderValues []interface{})

	// SqlInsertEx is the extended version of SqlInsert that uses an external InsertBuilder to construct the INSERT statement.
	SqlInsertEx(builder ISqlBuilder, ctx context.Context, tx *sql.Tx, table string, colsAndVals map[string]interface{}) (sql.Result, error)

	// SqlSelect constructs a SELECT query and executes it within a context/transaction.
	SqlSelect(ctx context.Context, tx *sql.Tx, table string, columns []string, filter IFilter, sorting ISorting, fromOffset, numItems int) (*sql.Rows, error)

	// SqlBuildSelectEx is a utility function to construct the SELECT statement along with values for placeholders.
	SqlBuildSelectEx(builder ISqlBuilder, table string, columns []string, filter IFilter, sorting ISorting, fromOffset, numItems int) (sql string, placeholderValues []interface{})

	// SqlSelectEx is the extended version of SqlSelect that uses an external SelectBuilder to construct the SELECT statement.
	SqlSelectEx(builder ISqlBuilder, ctx context.Context, tx *sql.Tx, table string, columns []string, filter IFilter, sorting ISorting, fromOffset, numItems int) (*sql.Rows, error)

	// SqlUpdate constructs an UPDATE query and executes it within a context/transaction.
	SqlUpdate(ctx context.Context, tx *sql.Tx, table string, colsAndVals map[string]interface{}, filter IFilter) (sql.Result, error)

	// SqlBuildUpdateEx is a utility function to construct the UPDATE statement along with values for placeholders.
	SqlBuildUpdateEx(builder ISqlBuilder, table string, colsAndVals map[string]interface{}, filter IFilter) (sql string, placeholderValues []interface{})

	// SqlUpdateEx is the extended version of SqlUpdate that uses an external UpdateBuilder to construct the UPDATE statement.
	SqlUpdateEx(builder ISqlBuilder, ctx context.Context, tx *sql.Tx, table string, colsAndVals map[string]interface{}, filter IFilter) (sql.Result, error)

	// FetchOne fetches a row from `sql.Rows` and transforms it to godal.IGenericBo.
	//   - FetchOne will NOT call dbRows.Close(), caller must take care of cleaning resource.
	//   - Caller should not call dbRows.Next(), FetchOne will do that.
	FetchOne(storageId string, dbRows *sql.Rows) (godal.IGenericBo, error)

	// FetchAll fetches all rows from `sql.Rows` and transforms to []godal.IGenericBo.
	//   - FetchOne will NOT call dbRows.Close(), caller must take are of cleaning resource.
	//   - Caller should not call dbRows.Next(), FetchOne will do that.
	FetchAll(storageId string, dbRows *sql.Rows) ([]godal.IGenericBo, error)

	// IsErrorDuplicatedEntry checks if the error was caused by conflicting in database table entries.
	IsErrorDuplicatedEntry(err error) bool

	// WrapTransaction wraps a function inside a transaction.
	//
	// txFunc: the function to wrap. If the function returns error, the transaction will be aborted, otherwise transaction is committed.
	WrapTransaction(ctx context.Context, txFunc func(ctx context.Context, tx *sql.Tx) error) error
}

// GenericDaoSql is 'database/sql' implementation of godal.IGenericDao.
//
// Function implementations (n = No, y = Yes, i = inherited):
//   - (n) GdaoCreateFilter(storageId string, bo godal.IGenericBo) interface{}
//   - (Y) GdaoDelete(storageId string, bo godal.IGenericBo) (int, error)
//   - (y) GdaoDeleteMany(storageId string, filter interface{}) (int, error)
//   - (y) GdaoFetchOne(storageId string, filter interface{}) (godal.IGenericBo, error)
//   - (y) GdaoFetchMany(storageId string, filter interface{}, sorting *godal.SortingOpt, fromOffset, numItems int) ([]godal.IGenericBo, error)
//   - (y) GdaoCreate(storageId string, bo godal.IGenericBo) (int, error)
//   - (y) GdaoUpdate(storageId string, bo godal.IGenericBo) (int, error)
//   - (y) GdaoSave(storageId string, bo godal.IGenericBo) (int, error)
//
// Note: IGenericDaoSql and GenericDaoSql should be in sync.
type GenericDaoSql struct {
	*godal.AbstractGenericDao
	sqlConnect                  *prom.SqlConnect
	sqlFlavor                   prom.DbFlavor
	txModeOnWrite               bool
	txIsolationLevel            sql.IsolationLevel
	optionOpLiteral             *OptionOpLiteral
	funcNewPlaceholderGenerator NewPlaceholderGenerator
}

// SetRowMapper attaches an IRowMapper to the DAO for latter use.
//
// Available since v0.3.0.
func (dao *GenericDaoSql) SetRowMapper(rowMapper godal.IRowMapper) IGenericDaoSql {
	dao.AbstractGenericDao.SetRowMapper(rowMapper)
	return dao
}

// GetSqlConnect returns the '*prom.SqlConnect' instance attached to this DAO.
func (dao *GenericDaoSql) GetSqlConnect() *prom.SqlConnect {
	return dao.sqlConnect
}

// SetSqlConnect attaches a '*prom.SqlConnect' instance to this DAO.
//
// Available since v0.0.2
func (dao *GenericDaoSql) SetSqlConnect(sqlC *prom.SqlConnect) IGenericDaoSql {
	dao.sqlConnect = sqlC
	return dao
}

// GetSqlFlavor returns the sql flavor preference.
func (dao *GenericDaoSql) GetSqlFlavor() prom.DbFlavor {
	return dao.sqlFlavor
}

// SetSqlFlavor set the sql flavor preference.
//
// Note: SetSqlFlavor will reset the funcNewPlaceholderGenerator
func (dao *GenericDaoSql) SetSqlFlavor(sqlFlavor prom.DbFlavor) IGenericDaoSql {
	dao.sqlFlavor = sqlFlavor
	dao.sqlConnect.SetDbFlavor(sqlFlavor)
	switch sqlFlavor {
	case prom.FlavorPgSql, prom.FlavorCosmosDb:
		dao.funcNewPlaceholderGenerator = NewPlaceholderGeneratorDollarN
	case prom.FlavorMsSql:
		dao.funcNewPlaceholderGenerator = NewPlaceholderGeneratorAtpiN
	case prom.FlavorOracle:
		dao.funcNewPlaceholderGenerator = NewPlaceholderGeneratorColonN
	case prom.FlavorMySql, prom.FlavorSqlite:
		dao.funcNewPlaceholderGenerator = NewPlaceholderGeneratorQuestion
	default:
		dao.funcNewPlaceholderGenerator = NewPlaceholderGeneratorQuestion
	}
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
func (dao *GenericDaoSql) GetTxIsolationLevel() sql.IsolationLevel {
	return dao.txIsolationLevel
}

// SetTxIsolationLevel sets new transaction isolation level.
//
// Available: since v0.1.0
func (dao *GenericDaoSql) SetTxIsolationLevel(txIsolationLevel sql.IsolationLevel) IGenericDaoSql {
	dao.txIsolationLevel = txIsolationLevel
	return dao
}

// StartTx starts a new transaction.
//
// If ctx is nil, this function will create a new context by calling sqlConnect.NewContext()
//
// Available: since v0.1.0
func (dao *GenericDaoSql) StartTx(ctx context.Context) (*sql.Tx, error) {
	return dao.sqlConnect.GetDB().BeginTx(dao.sqlConnect.NewContextIfNil(ctx), &sql.TxOptions{Isolation: dao.txIsolationLevel})
}

// GetOptionOpLiteral returns operation literal settings.
func (dao *GenericDaoSql) GetOptionOpLiteral() *OptionOpLiteral {
	return dao.optionOpLiteral
}

// SetOptionOpLiteral sets operation literal settings.
func (dao *GenericDaoSql) SetOptionOpLiteral(optionOpLiteral *OptionOpLiteral) IGenericDaoSql {
	dao.optionOpLiteral = optionOpLiteral
	return dao
}

// GetFuncNewPlaceholderGenerator returns the function creates 'PlaceholderGenerator'.
func (dao *GenericDaoSql) GetFuncNewPlaceholderGenerator() NewPlaceholderGenerator {
	return dao.funcNewPlaceholderGenerator
}

// SetFuncNewPlaceholderGenerator sets the function used to create 'PlaceholderGenerator'.
func (dao *GenericDaoSql) SetFuncNewPlaceholderGenerator(funcNewPlaceholderGenerator NewPlaceholderGenerator) IGenericDaoSql {
	dao.funcNewPlaceholderGenerator = funcNewPlaceholderGenerator
	return dao
}

// BuildFilter builds IFilter instance based on the following rules:
//   - If 'filter' is nil: return nil.
//   - If 'filter' is IFilter: return 'filter'.
//   - If 'filter' is a map: build a FilterAnd combining all map entries, using operation "=", and return it.
//   - Otherwise, return error.
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
		result := &FilterAnd{FilterAndOr: FilterAndOr{Filters: make([]IFilter, 0)}}
		ops := dao.optionOpLiteral
		if ops == nil {
			ops = DefaultOptionLiteralOperator
		}
		for iter := v.MapRange(); iter.Next(); {
			key, _ := reddo.ToString(iter.Key().Interface())
			result.Add(&FilterFieldValue{Field: key, Operator: ops.OpEqual, Value: iter.Value().Interface()})
		}
		return result, nil
	}
	return nil, fmt.Errorf("cannot build filter from %v", filter)
}

// BuildSorting builds elements for 'ORDER BY' clause.
//
// Available since v0.5.0
func (dao *GenericDaoSql) BuildSorting(storageId string, sorting *godal.SortingOpt) (ISorting, error) {
	if sorting == nil || len(sorting.Fields) == 0 {
		return nil, nil
	}
	rm := dao.GetRowMapper()
	if rm == nil {
		return nil, errors.New("row-mapper is needed to build sorting clause")
	}
	result := &GenericSorting{Flavor: dao.sqlFlavor}
	for _, field := range sorting.Fields {
		colName := rm.ToDbColName(storageId, field.FieldName)
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
//
//   - If ctx is nil, SqlExecute creates a new context to use.
//   - If tx is not nil, SqlExecute uses transaction context to execute the query.
//   - If tx is nil, SqlExecute calls DB.ExecContext to execute the query.
func (dao *GenericDaoSql) SqlExecute(ctx context.Context, tx *sql.Tx, sql string, values ...interface{}) (sql.Result, error) {
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
//
//   - If ctx is nil, SqlQuery creates a new context to use.
//   - If tx is not nil, SqlQuery uses transaction context to execute the query.
//   - If tx is nil, SqlQuery calls DB.QueryContext to execute the query.
func (dao *GenericDaoSql) SqlQuery(ctx context.Context, tx *sql.Tx, sql string, values ...interface{}) (*sql.Rows, error) {
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
func (dao *GenericDaoSql) SqlDelete(ctx context.Context, tx *sql.Tx, table string, filter IFilter) (sql.Result, error) {
	return dao.SqlDeleteEx(nil, ctx, tx, table, filter)
}

// SqlBuildDeleteEx is a utility function to construct the DELETE statement along with values for placeholders.
//
// Available since v0.3.0
func (dao *GenericDaoSql) SqlBuildDeleteEx(builder ISqlBuilder, table string, filter IFilter) (sql string, placeholderValues []interface{}) {
	if builder == nil {
		builder = NewDeleteBuilder().WithFlavor(dao.sqlFlavor).WithTable(table).WithFilter(filter)
		if dao.funcNewPlaceholderGenerator != nil {
			builder.(*DeleteBuilder).WithPlaceholderGenerator(dao.funcNewPlaceholderGenerator())
		}
	}
	return builder.Build()
}

// SqlDeleteEx is the extended version of SqlDelete that uses an external DeleteBuilder to construct the DELETE statement.
//
// Available since v0.3.0
func (dao *GenericDaoSql) SqlDeleteEx(builder ISqlBuilder, ctx context.Context, tx *sql.Tx, table string, filter IFilter) (sql.Result, error) {
	sqlStm, values := dao.SqlBuildDeleteEx(builder, table, filter)
	return dao.SqlExecute(ctx, tx, sqlStm, values...)
}

// SqlInsert constructs a INSERT statement and executes it within a context/transaction.
func (dao *GenericDaoSql) SqlInsert(ctx context.Context, tx *sql.Tx, table string, colsAndVals map[string]interface{}) (sql.Result, error) {
	return dao.SqlInsertEx(nil, ctx, tx, table, colsAndVals)
}

// SqlBuildInsertEx is a utility function to construct the INSERT statement along with values for placeholders.
//
// Available since v0.3.0
func (dao *GenericDaoSql) SqlBuildInsertEx(builder ISqlBuilder, table string, colsAndVals map[string]interface{}) (sql string, placeholderValues []interface{}) {
	if builder == nil {
		builder = NewInsertBuilder().WithFlavor(dao.sqlFlavor).WithTable(table).WithValues(colsAndVals)
		if dao.funcNewPlaceholderGenerator != nil {
			builder.(*InsertBuilder).WithPlaceholderGenerator(dao.funcNewPlaceholderGenerator())
		}
	}
	return builder.Build()
}

// SqlInsertEx is the extended version of SqlInsert that uses an external InsertBuilder to construct the INSERT statement.
//
// Available since v0.3.0
func (dao *GenericDaoSql) SqlInsertEx(builder ISqlBuilder, ctx context.Context, tx *sql.Tx, table string, colsAndVals map[string]interface{}) (sql.Result, error) {
	sqlStm, values := dao.SqlBuildInsertEx(builder, table, colsAndVals)
	return dao.SqlExecute(ctx, tx, sqlStm, values...)
}

// SqlSelect constructs a SELECT query and executes it within a context/transaction.
func (dao *GenericDaoSql) SqlSelect(ctx context.Context, tx *sql.Tx, table string, columns []string, filter IFilter, sorting ISorting, fromOffset, numItems int) (*sql.Rows, error) {
	return dao.SqlSelectEx(nil, ctx, tx, table, columns, filter, sorting, fromOffset, numItems)
}

// SqlBuildSelectEx is a utility function to construct the SELECT statement along with values for placeholders.
//
// Available since v0.3.0
func (dao *GenericDaoSql) SqlBuildSelectEx(builder ISqlBuilder, table string, columns []string, filter IFilter, sorting ISorting, fromOffset, numItems int) (sql string, placeholderValues []interface{}) {
	if builder == nil {
		builder = NewSelectBuilder().WithFlavor(dao.sqlFlavor).
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
func (dao *GenericDaoSql) SqlSelectEx(builder ISqlBuilder, ctx context.Context, tx *sql.Tx, table string, columns []string, filter IFilter, sorting ISorting, fromOffset, numItems int) (*sql.Rows, error) {
	query, values := dao.SqlBuildSelectEx(builder, table, columns, filter, sorting, fromOffset, numItems)
	return dao.SqlQuery(ctx, tx, query, values...)
}

// SqlUpdate constructs an UPDATE query and executes it within a context/transaction.
func (dao *GenericDaoSql) SqlUpdate(ctx context.Context, tx *sql.Tx, table string, colsAndVals map[string]interface{}, filter IFilter) (sql.Result, error) {
	return dao.SqlUpdateEx(nil, ctx, tx, table, colsAndVals, filter)
}

// SqlBuildUpdateEx is a utility function to construct the UPDATE statement along with values for placeholders.
//
// Available since v0.3.0
func (dao *GenericDaoSql) SqlBuildUpdateEx(builder ISqlBuilder, table string, colsAndVals map[string]interface{}, filter IFilter) (sql string, placeholderValues []interface{}) {
	if builder == nil {
		builder = NewUpdateBuilder().WithFlavor(dao.sqlFlavor).WithTable(table).WithValues(colsAndVals).WithFilter(filter)
		if dao.funcNewPlaceholderGenerator != nil {
			builder.(*UpdateBuilder).WithPlaceholderGenerator(dao.funcNewPlaceholderGenerator())
		}
	}
	return builder.Build()
}

// SqlUpdateEx is the extended version of SqlUpdate that uses an external UpdateBuilder to construct the UPDATE statement.
//
// Available since v0.3.0
func (dao *GenericDaoSql) SqlUpdateEx(builder ISqlBuilder, ctx context.Context, tx *sql.Tx, table string, colsAndVals map[string]interface{}, filter IFilter) (sql.Result, error) {
	query, values := dao.SqlBuildUpdateEx(builder, table, colsAndVals, filter)
	return dao.SqlExecute(ctx, tx, query, values...)
}

/*----------------------------------------------------------------------*/

// FetchOne fetches a row from `sql.Rows` and transforms it to godal.IGenericBo.
//
//   - FetchOne will NOT call dbRows.Close(), caller must take care of cleaning resource.
//   - Caller should not call dbRows.Next(), FetchOne will do that.
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

// FetchAll fetches all rows from `sql.Rows` and transforms to []godal.IGenericBo.
//
//   - FetchOne will NOT call dbRows.Close(), caller must take are of cleaning resource.
//   - Caller should not call dbRows.Next(), FetchOne will do that.
func (dao *GenericDaoSql) FetchAll(storageId string, dbRows *sql.Rows) ([]godal.IGenericBo, error) {
	boList := make([]godal.IGenericBo, 0)
	var err error
	e := dao.sqlConnect.FetchRowsCallback(dbRows, func(row map[string]interface{}, e error) bool {
		if e != nil {
			err = e
			return false
		}
		bo, e := dao.GetRowMapper().ToBo(storageId, row)
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
func (dao *GenericDaoSql) GdaoDelete(storageId string, bo godal.IGenericBo) (int, error) {
	return dao.GdaoDeleteWithTx(nil, nil, storageId, bo)
}

// GdaoDeleteWithTx is database/sql variant of GdaoDelete.
//
// Available: since v0.1.0
func (dao *GenericDaoSql) GdaoDeleteWithTx(ctx context.Context, tx *sql.Tx, storageId string, bo godal.IGenericBo) (int, error) {
	filter := dao.GdaoCreateFilter(storageId, bo)
	return dao.GdaoDeleteManyWithTx(ctx, tx, storageId, filter)
}

// GdaoDeleteMany implements godal.IGenericDao.GdaoDeleteMany.
func (dao *GenericDaoSql) GdaoDeleteMany(storageId string, filter interface{}) (int, error) {
	return dao.GdaoDeleteManyWithTx(nil, nil, storageId, filter)
}

// GdaoDeleteManyWithTx is database/sql variant of GdaoDeleteMany.
//
// Available: since v0.1.0
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

// GdaoFetchOne implements godal.IGenericDao.GdaoFetchOne.
func (dao *GenericDaoSql) GdaoFetchOne(storageId string, filter interface{}) (godal.IGenericBo, error) {
	return dao.GdaoFetchOneWithTx(nil, nil, storageId, filter)
}

// GdaoFetchOneWithTx is database/sql variant of GdaoFetchOne.
//
// Available: since v0.1.0
func (dao *GenericDaoSql) GdaoFetchOneWithTx(ctx context.Context, tx *sql.Tx, storageId string, filter interface{}) (godal.IGenericBo, error) {
	f, err := dao.BuildFilter(filter)
	if err != nil {
		return nil, err
	}
	columns := dao.GetRowMapper().ColumnsList(storageId)
	dbRows, err := dao.SqlSelect(ctx, tx, storageId, columns, f, nil, 0, 0)
	if dbRows != nil {
		defer func() { _ = dbRows.Close() }()
	}
	if err != nil {
		return nil, err
	}
	return dao.FetchOne(storageId, dbRows)
}

// GdaoFetchMany implements godal.IGenericDao.GdaoFetchMany.
func (dao *GenericDaoSql) GdaoFetchMany(storageId string, filter interface{}, sorting *godal.SortingOpt, fromOffset, numRows int) ([]godal.IGenericBo, error) {
	return dao.GdaoFetchManyWithTx(nil, nil, storageId, filter, sorting, fromOffset, numRows)
}

// GdaoFetchManyWithTx is database/sql variant of GdaoFetchMany.
//
// Available: since v0.1.0
func (dao *GenericDaoSql) GdaoFetchManyWithTx(ctx context.Context, tx *sql.Tx, storageId string, filter interface{}, sorting *godal.SortingOpt, fromOffset, numRows int) ([]godal.IGenericBo, error) {
	f, err := dao.BuildFilter(filter)
	if err != nil {
		return nil, err
	}
	o, err := dao.BuildSorting(storageId, sorting)
	if err != nil {
		return nil, err
	}
	dbRows, err := dao.SqlSelect(ctx, tx, storageId, dao.GetRowMapper().ColumnsList(storageId), f, o, fromOffset, numRows)
	if dbRows != nil {
		defer func() { _ = dbRows.Close() }()
	}
	if err != nil {
		return nil, err
	}
	return dao.FetchAll(storageId, dbRows)
}

// IsErrorDuplicatedEntry checks if the error was caused by conflicting in database table entries.
func (dao *GenericDaoSql) IsErrorDuplicatedEntry(err error) bool {
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
		errStr := fmt.Sprintf("%e", err)
		return regexp.MustCompile(`\WORA\-00001\W`).FindString(errStr) != "" ||
			regexp.MustCompile(`\Wunique constraint.*?violated\W`).FindString(errStr) != ""
	case prom.FlavorSqlite:
		errStr := fmt.Sprintf("%e", err)
		return regexp.MustCompile(`\WErrNo=19\W`).FindString(errStr) != "" &&
			regexp.MustCompile(`\WErrNoExtended=1555\W|\WErrNoExtended=2067\W`).FindString(errStr) != ""
	}
	return false
}

// GdaoCreate implements godal.IGenericDao.GdaoCreate.
func (dao *GenericDaoSql) GdaoCreate(storageId string, bo godal.IGenericBo) (int, error) {
	return dao.GdaoCreateWithTx(nil, nil, storageId, bo)
}

// GdaoCreateWithTx is database/sql variant of GdaoCreate.
//
// Available: since v0.1.0
func (dao *GenericDaoSql) GdaoCreateWithTx(ctx context.Context, tx *sql.Tx, storageId string, bo godal.IGenericBo) (int, error) {
	if row, err := dao.GetRowMapper().ToRow(storageId, bo); err != nil {
		return 0, err
	} else if colsAndVals, err := reddo.ToMap(row, typeMap); err != nil {
		return 0, err
	} else if result, err := dao.SqlInsert(ctx, tx, storageId, colsAndVals.(map[string]interface{})); err != nil {
		if dao.IsErrorDuplicatedEntry(err) {
			return 0, godal.GdaoErrorDuplicatedEntry
		}
		return 0, err
	} else {
		numRows, err := result.RowsAffected()
		return int(numRows), err
	}
}

// GdaoUpdate implements godal.IGenericDao.GdaoUpdate.
func (dao *GenericDaoSql) GdaoUpdate(storageId string, bo godal.IGenericBo) (int, error) {
	return dao.GdaoUpdateWithTx(nil, nil, storageId, bo)
}

// GdaoUpdateWithTx is database/sql variant of GdaoUpdate.
//
// Available: since v0.1.0
func (dao *GenericDaoSql) GdaoUpdateWithTx(ctx context.Context, tx *sql.Tx, storageId string, bo godal.IGenericBo) (int, error) {
	filter, err := dao.BuildFilter(dao.GdaoCreateFilter(storageId, bo))
	if err != nil {
		return 0, err
	}
	row, err := dao.GetRowMapper().ToRow(storageId, bo)
	if err != nil {
		return 0, err
	}
	colsAndVals, err := reddo.ToMap(row, typeMap)
	if err != nil {
		return 0, err
	}
	result, err := dao.SqlUpdate(ctx, tx, storageId, colsAndVals.(map[string]interface{}), filter)
	if err != nil {
		if dao.IsErrorDuplicatedEntry(err) {
			return 0, godal.GdaoErrorDuplicatedEntry
		}
		return 0, err
	}
	numRows, err := result.RowsAffected()
	return int(numRows), err
}

// GdaoSave implements godal.IGenericDao.GdaoSave.
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
}

// GdaoSaveWithTx is extended-implementation of godal.IGenericDao.GdaoSave.
//
// Available: since v0.1.0
func (dao *GenericDaoSql) GdaoSaveWithTx(ctx context.Context, tx *sql.Tx, storageId string, bo godal.IGenericBo) (int, error) {
	filter, err := dao.BuildFilter(dao.GdaoCreateFilter(storageId, bo))
	if err != nil {
		return 0, err
	}
	row, err := dao.GetRowMapper().ToRow(storageId, bo)
	if err != nil {
		return 0, err
	}
	colsAndVals, err := reddo.ToMap(row, typeMap)
	if err != nil {
		return 0, err
	}

	// firstly: try to update row
	if result, err := dao.SqlUpdate(ctx, tx, storageId, colsAndVals.(map[string]interface{}), filter); err != nil {
		if dao.IsErrorDuplicatedEntry(err) {
			return 0, godal.GdaoErrorDuplicatedEntry
		}
		return 0, err
	} else if numRows, err := result.RowsAffected(); err != nil || numRows > 0 {
		return int(numRows), err
	} else {
		// secondly: no row updated, try insert row
		result, err := dao.SqlInsert(ctx, tx, storageId, colsAndVals.(map[string]interface{}))
		if err != nil {
			if dao.IsErrorDuplicatedEntry(err) {
				return 0, godal.GdaoErrorDuplicatedEntry
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
	ctx = dao.sqlConnect.NewContextIfNil(ctx)
	if tx, err = dao.sqlConnect.GetDB().BeginTx(ctx, &sql.TxOptions{Isolation: dao.txIsolationLevel}); err != nil {
		return err
	}
	err = txFunc(ctx, tx)
	return err
}
