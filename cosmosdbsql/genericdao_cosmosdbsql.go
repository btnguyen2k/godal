/*
Package cosmosdbsql provides a generic Azure Cosmos DB implementation of godal.IGenericDao using database/sql interface.

General guideline:

	- DAOs must implement IGenericDao.GdaoCreateFilter(string, IGenericBo) FilterOpt.

Guideline: Use GenericDaoCosmosdb (and godal.IGenericBo) directly

	- Define a DAO struct that implements IGenericDao.GdaoCreateFilter(string, IGenericBo) FilterOpt.
	- Configure either {collection-name:path-to-fetch-partition_key-value-from-genericbo} (via GenericDaoCosmosdb.CosmosSetPkGboMapPath)
	  or {collection-name:path-to-fetch-partition_key-value-from-dbrow} (via GenericDaoCosmosdb.CosmosSetPkRowMapPath).
	- Optionally, configure {collection-name:path-to-fetch-id-value-from-genericbo} via GenericDaoCosmosdb.CosmosSetIdGboMapPath.
	- Optionally, create a helper function to create DAO instances.

	// Remember to import the database driver, the only supported/available driver for now is "github.com/btnguyen2k/gocosmos".
	import (
		"github.com/btnguyen2k/consu/reddo"
		"github.com/btnguyen2k/godal"
		godalcosmosdb "github.com/btnguyen2k/godal/cosmosdbsql"
		promsql "github.com/btnguyen2k/prom/sql"

		_ "github.com/btnguyen2k/gocosmos"
	)

	type myGenericDaoCosmosdb struct {
		*godalcosmosdb.GenericDaoCosmosdb
	}

	// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter.
	func (dao *myGenericDaoCosmosdb) GdaoCreateFilter(collection string, bo godal.IGenericBo) godal.FilterOpt {
		id := bo.GboGetAttrUnsafe(fieldId, reddo.TypeString)
		return &godal.FilterOptFieldOpValue{FieldName: fieldId, Operator: godal.FilterOpEqual, Value: id}
	}

	// newGenericDaoCosmosdb is helper function to create myGenericDaoCosmosdb instances.
	func newGenericDaoCosmosdb(sqlc *promsql.SqlConnect) godal.IGenericDao {
		rowMapper := godalcosmosdb.GenericRowMapperCosmosdbInstance
		dao := &myGenericDaoCosmosdb{}
		dao.GenericDaoCosmosdb = godalcosmosdb.NewGenericDaoCosmosdb(sqlc, godal.NewAbstractGenericDao(dao))
		dao.SetSqlFlavor(sql.FlavorCosmosDb).SetRowMapper(rowMapper)
		dao.SetTxModeOnWrite(false)
		dao.CosmosSetPkGboMapPath(map[string]string{collectionName: fieldPk})
		return dao
	}

	Since Azure Cosmos DB is schema-less, GenericRowMapperCosmosdbInstance should be sufficient.

	txModeOnWrite should be disabled as btnguyen2k/gocosmosdb driver does not currently support transaction!

Guideline: Implement custom Azure Cosmos DB business DAOs and BOs

	- Define and implement the business DAO (Note: DAOs must implement IGenericDao.GdaoCreateFilter(string, IGenericBo) FilterOpt).
	- Define functions to transform godal.IGenericBo to business BO and vice versa.
	- Optionally, create a helper function to create DAO instances.

	// Remember to import the database driver, the only supported/available driver for now is "github.com/btnguyen2k/gocosmos".
	import (
		"github.com/btnguyen2k/consu/reddo"
		"github.com/btnguyen2k/godal"
		godalcosmosdb "github.com/btnguyen2k/godal/cosmosdbsql"
		promsql "github.com/btnguyen2k/prom/sql"

		_ "github.com/btnguyen2k/gocosmos"
	)

	// BoApp defines business object app
	type BoApp struct {
		Id            string                 `json:"id"`
		Description   string                 `json:"desc"`
		Value         int                    `json:"val"`
		Pk            string                 `json:"pk"` // it's a good idea to have a dedicated field for partition key
	}

	func (app *BoApp) ToGbo() godal.IGenericBo {
		gbo := godal.NewGenericBo()

		// method 1: populate attributes one by one
		gbo.GboSetAttr("id"  , app.Id)
		gbo.GboSetAttr("desc", app.Description)
		gbo.GboSetAttr("val" , app.Value)
		gbo.GboSetAttr("pk"  , app.Pk)

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
		app.Pk          = gbo.GboGetAttrUnsafe("pk", reddo.TypeString).(string)

		// method 2: transfer all attributes at once
		if err := gbo.GboTransferViaJson(&app); err!=nil {
			panic(err)
		}

		return &app
	}

	// DaoAppCosmosdb is Azure CosmosDB-implementation of business dao.
	type DaoAppCosmosdb struct {
		*godalcosmosdb.GenericDaoCosmosdb
		collectionName string
	}

	// NewDaoAppCosmosdb is helper function to create DaoAppCosmosdb instances.
	func NewDaoAppCosmosdb(sqlc *promsql.SqlConnect, collectionName string) *DaoAppCosmosdb {
		rowMapper := GenericRowMapperCosmosdbInstance
		dao := &DaoAppCosmosdb{collectionName: collectionName}
		dao.GenericDaoCosmosdb = godalcosmosdb.NewGenericDaoCosmosdb(sqlc, godal.NewAbstractGenericDao(dao))
		dao.SetSqlFlavor(sql.FlavorCosmosDb).SetRowMapper(rowMapper)
		dao.SetTxModeOnWrite(false)
		dao.CosmosSetPkGboMapPath(map[string]string{"*": fieldPk})
		return dao
	}

	Since Azure Cosmos DB is schema-less, GenericRowMapperCosmosdbInstance should be sufficient.

	txModeOnWrite should be disabled as btnguyen2k/gocosmosdb driver does not currently support transaction!

	Partition key (PK) is crucial to CosmosDB. PK value is needed in almost all document related operations. Hence, it's
	important to be able to extract PK value from BO. If using or extending GenericDaoCosmosdb, configure either
	{collection-name:path-to-fetch-partition_key-value-from-genericbo} (via GenericDaoCosmosdb.CosmosSetPkGboMapPath)
	or {collection-name:path-to-fetch-partition_key-value-from-dbrow} (via GenericDaoCosmosdb.CosmosSetPkRowMapPath).

See more examples in 'examples' directory on project's GitHub: https://github.com/btnguyen2k/godal/tree/master/examples

To create SqlConnect instances, see package github.com/btnguyen2k/prom/sql
*/
package cosmosdbsql

import (
	"context"
	gosql "database/sql"
	"fmt"
	"reflect"
	"regexp"

	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/consu/semita"
	"github.com/btnguyen2k/gocosmos"
	"github.com/btnguyen2k/prom/sql"

	"github.com/btnguyen2k/godal"
	godalsql "github.com/btnguyen2k/godal/sql"
)

// GenericRowMapperCosmosdb is a generic implementation of godal.IRowMapper for Azure Cosmos DB.
//
// Implementation rules:
//   - ToRow        : transform godal.IGenericBo "as-is" to map[string]interface{}.
//   - ToBo         : expects input is a map[string]interface{}, or JSON data (string or array/slice of bytes), transforms input to godal.IGenericBo via JSON unmarshalling.
//   - ColumnsList  : return []string{"*"} (CosmosDB is schema-free, hence column-list is not used).
//   - ToDbColName  : return the input field name "as-is".
//   - ToBoFieldName: return the input column name "as-is".
//
// Available: since v0.3.0
type GenericRowMapperCosmosdb struct {
}

// ToRow implements godal.IRowMapper.ToRow.
//
// This function transforms godal.IGenericBo to map[string]interface{}. Field names are kept intact.
func (mapper *GenericRowMapperCosmosdb) ToRow(_ string, bo godal.IGenericBo) (interface{}, error) {
	if bo == nil {
		return nil, nil
	}
	result := make(map[string]interface{})
	return result, bo.GboTransferViaJson(&result)
}

// ToBo implements godal.IRowMapper.ToBo.
//
// This function expects input to be a map[string]interface{}, or JSON data (string or array/slice of bytes), transforms it to godal.IGenericBo via JSON unmarshalling. Field names are kept intact.
func (mapper *GenericRowMapperCosmosdb) ToBo(collectionName string, row interface{}) (godal.IGenericBo, error) {
	if row == nil {
		return nil, nil
	}
	switch row.(type) {
	case *map[string]interface{}:
		// unwrap if pointer
		m := row.(*map[string]interface{})
		if m == nil {
			return nil, nil
		}
		return mapper.ToBo(collectionName, *m)
	case map[string]interface{}:
		bo := godal.NewGenericBo()
		for k, v := range row.(map[string]interface{}) {
			bo.GboSetAttr(k, v)
		}
		return bo, nil
	case string:
		bo := godal.NewGenericBo()
		return bo, bo.GboFromJson([]byte(row.(string)))
	case *string:
		// unwrap if pointer
		s := row.(*string)
		if s == nil {
			return nil, nil
		}
		return mapper.ToBo(collectionName, *s)
	case []byte:
		if row.([]byte) == nil {
			return nil, nil
		}
		bo := godal.NewGenericBo()
		return bo, bo.GboFromJson(row.([]byte))
	case *[]byte:
		// unwrap if pointer
		ba := row.(*[]byte)
		if ba == nil {
			return nil, nil
		}
		return mapper.ToBo(collectionName, *ba)
	}

	v := reflect.ValueOf(row)
	for ; v.Kind() == reflect.Ptr; v = v.Elem() {
		// unwrap if pointer
	}
	switch v.Kind() {
	case reflect.Map:
		bo := godal.NewGenericBo()
		for iter := v.MapRange(); iter.Next(); {
			key, _ := reddo.ToString(iter.Key().Interface())
			bo.GboSetAttr(key, iter.Value().Interface())
		}
		return bo, nil
	case reflect.String:
		bo := godal.NewGenericBo()
		return bo, bo.GboFromJson([]byte(v.Interface().(string)))
	case reflect.Slice, reflect.Array:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			// input is []byte
			zero := make([]byte, 0)
			arr, err := reddo.ToSlice(v.Interface(), reflect.TypeOf(zero))
			if err != nil || arr.([]byte) == nil || len(arr.([]byte)) == 0 {
				return nil, err
			}
			bo := godal.NewGenericBo()
			return bo, bo.GboFromJson(arr.([]byte))
		}
	case reflect.Interface:
		return mapper.ToBo(collectionName, v.Interface())
	case reflect.Invalid:
		return nil, nil
	}
	return nil, fmt.Errorf("cannot construct godal.IGenericBo from input %v", row)
}

// ColumnsList implements godal.IRowMapper.ColumnsList.
//
// This function returns []string{"*"} since CosmosDB is schema-free (hence column-list is not used).
func (mapper *GenericRowMapperCosmosdb) ColumnsList(_ string) []string {
	return []string{"*"}
}

// ToDbColName implements godal.IRowMapper.ToDbColName.
//
// This function returns the input field name "as-is".
func (mapper *GenericRowMapperCosmosdb) ToDbColName(_, fieldName string) string {
	return fieldName
}

// ToBoFieldName implements godal.IRowMapper.ToBoFieldName.
//
// This function returns the input column name "as-is".
func (mapper *GenericRowMapperCosmosdb) ToBoFieldName(_, colName string) string {
	return colName
}

var (
	// GenericRowMapperCosmosdbInstance is a pre-created instance of GenericRowMapperCosmosdb that is ready to use.
	GenericRowMapperCosmosdbInstance godal.IRowMapper = &GenericRowMapperCosmosdb{}
)

/*--------------------------------------------------------------------------------*/

// NewGenericDaoCosmosdb constructs a new Azure Cosmos DB implementation of 'godal.IGenericDao'.
func NewGenericDaoCosmosdb(sqlConnect *sql.SqlConnect, agdao *godal.AbstractGenericDao) *GenericDaoCosmosdb {
	sqlDao := godalsql.NewGenericDaoSql(sqlConnect, agdao)
	dao := &GenericDaoCosmosdb{IGenericDaoSql: sqlDao}
	return dao
}

var (
	typeMap = reflect.TypeOf(map[string]interface{}{})
)

// GenericDaoCosmosdb is Azure Cosmos DB implementation of godal.IGenericDao.
//
// Function implementations (n = No, y = Yes, i = inherited):
//   - (n) GdaoCreateFilter(collectionName string, bo godal.IGenericBo) godal.FilterOpt
//   - (y) GdaoDelete(collectionName string, bo godal.IGenericBo) (int, error)
//   - (y) GdaoDeleteMany(collectionName string, filter godal.FilterOpt) (int, error)
//   - (y) GdaoFetchOne(collectionName string, filter godal.FilterOpt) (godal.IGenericBo, error)
//   - (y) GdaoFetchMany(collectionName string, filter godal.FilterOpt, sorting *godal.SortingOpt, startOffset, numItems int) ([]godal.IGenericBo, error)
//   - (y) GdaoCreate(collectionName string, bo godal.IGenericBo) (int, error)
//   - (y) GdaoUpdate(collectionName string, bo godal.IGenericBo) (int, error)
//   - (y) GdaoSave(collectionName string, bo godal.IGenericBo) (int, error)
//
// Available: since v0.3.0
type GenericDaoCosmosdb struct {
	godalsql.IGenericDaoSql
	idGboPathMap map[string]string // mapping {collection-name:semita-path-to-fetch-id-value-from-genericbo}
	pkGboPathMap map[string]string // mapping {collection-name:semita-path-to-fetch-partition_key-value-from-genericbo}
	pkRowPathMap map[string]string // mapping {collection-name:semita-path-to-fetch-partition_key-value-from-dbrow}
}

// CosmosGetIdGboMapPath gets the mapping {collection-name:path-to-fetch-id-value-from-genericbo}.
//
// It is optional but highly recommended to specify such a mapping for performance reason. If no such mapping specified,
// extracting ID from a (generic) BO needs to go through a two-step process, which often imposes a negative impact
// on performance.
//
// Collection-name is "*" means "match any collection".
func (dao *GenericDaoCosmosdb) CosmosGetIdGboMapPath() map[string]string {
	result := make(map[string]string)
	for k, v := range dao.idGboPathMap {
		result[k] = v
	}
	return result
}

// CosmosSetIdGboMapPath sets the mapping {collection-name:path-to-fetch-id-value-from-genericbo}.
//
// It is optional but highly recommended to specify such a mapping for performance reason. If no such mapping specified,
// extracting ID from a (generic) BO needs to go through a two-step process, which often imposes a negative impact
// on performance.
//
// Collection-name is "*" means "match any collection".
func (dao *GenericDaoCosmosdb) CosmosSetIdGboMapPath(idGboPathMap map[string]string) *GenericDaoCosmosdb {
	temp := make(map[string]string)
	for k, v := range idGboPathMap {
		temp[k] = v
	}
	dao.idGboPathMap = temp
	return dao
}

// CosmosGetId extracts and returns ID value from a (generic) BO.
//
// If a mapping {collection-name:path-to-fetch-id-value-from-genericbo} has been specified, this function looks up the "id"
// value directly from the mapping. If the lookup is not successful, or there is no such mapping specified, the BO is
// then transformed to row (via the row-mapper) and the value of row's "id" field is returned.
//
// See CosmosSetIdGboMapPath.
func (dao *GenericDaoCosmosdb) CosmosGetId(collectionName string, bo godal.IGenericBo) string {
	for _, t := range []string{collectionName, "*"} {
		if path, ok := dao.idGboPathMap[t]; ok {
			if v, err := bo.GboGetAttr(path, reddo.TypeString); err == nil && v != nil {
				return v.(string)
			}
			// mapping path exists but cannot extract the value from the BO
			return ""
		}
	}
	if row, err := dao.GetRowMapper().ToRow(collectionName, bo); err == nil && row != nil {
		if rowMap, ok := reddo.ToMap(row, typeMap); ok == nil {
			if id, ok := rowMap.(map[string]interface{})["id"].(string); ok {
				return id
			}
		}
	}
	return ""
}

// CosmosGetPkGboMapPath gets the mapping {collection-name:path-to-fetch-partition_key-value-from-genericbo}.
//
// Note: at least one of {collection-name:path-to-fetch-partition_key-value-from-genericbo} and
// {collection-name:path-to-fetch-partition_key-value-from-dbrow} mappings must be configured. If not, client may encounter
// error "PartitionKey extracted from document doesn't match the one specified in the header".
//
// Collection-name is "*" means "match any collection".
func (dao *GenericDaoCosmosdb) CosmosGetPkGboMapPath() map[string]string {
	result := make(map[string]string)
	for k, v := range dao.pkGboPathMap {
		result[k] = v
	}
	return result
}

// CosmosSetPkGboMapPath sets the mapping {collection-name:path-to-fetch-partition_key-value-from-genericbo}.
//
// Note: at least one of {collection-name:path-to-fetch-partition_key-value-from-genericbo} and
// {collection-name:path-to-fetch-partition_key-value-from-dbrow} mappings must be configured. If not, client may encounter
// error "PartitionKey extracted from document doesn't match the one specified in the header".
//
// Collection-name is "*" means "match any collection".
func (dao *GenericDaoCosmosdb) CosmosSetPkGboMapPath(pkGboPathMap map[string]string) *GenericDaoCosmosdb {
	temp := make(map[string]string)
	for k, v := range pkGboPathMap {
		temp[k] = v
	}
	dao.pkGboPathMap = temp
	return dao
}

// CosmosGetPkRowMapPath gets the mapping {collection-name:path-to-fetch-partition_key-value-from-dbrow}.
//
// Note: at least one of {collection-name:path-to-fetch-partition_key-value-from-genericbo} and
// {collection-name:path-to-fetch-partition_key-value-from-dbrow} mappings must be configured. If not, client may encounter
// error "PartitionKey extracted from document doesn't match the one specified in the header".
//
// Collection-name is "*" means "match any collection".
func (dao *GenericDaoCosmosdb) CosmosGetPkRowMapPath() map[string]string {
	result := make(map[string]string)
	for k, v := range dao.pkRowPathMap {
		result[k] = v
	}
	return result
}

// CosmosSetPkRowMapPath sets the mapping {collection-name:path-to-fetch-partition_key-value-from-dbrow}.
//
// Note: at least one of {collection-name:path-to-fetch-partition_key-value-from-genericbo} and
// {collection-name:path-to-fetch-partition_key-value-from-dbrow} mappings must be configured. If not, client may encounter
// error "PartitionKey extracted from document doesn't match the one specified in the header".
//
// Collection-name is "*" means "match any collection".
func (dao *GenericDaoCosmosdb) CosmosSetPkRowMapPath(pkRowPathMap map[string]string) *GenericDaoCosmosdb {
	temp := make(map[string]string)
	for k, v := range pkRowPathMap {
		temp[k] = v
	}
	dao.pkRowPathMap = temp
	return dao
}

// CosmosGetPk extracts and returns partition key value from a BO.
//
// Firstly, the mapping {collection-name:path-to-fetch-partition_key-value-from-genericbo} is used to look up the PK value from
// the BO. If the lookup is not successful, the mapping {collection-name:path-to-fetch-partition_key-value-from-dbrow}
// is then used for lookup.
//
// Collection-name is "*" means "match any collection".
func (dao *GenericDaoCosmosdb) CosmosGetPk(collectionName string, bo godal.IGenericBo) interface{} {
	for _, t := range []string{collectionName, "*"} {
		if path, ok := dao.pkGboPathMap[t]; ok {
			if v, err := bo.GboGetAttr(path, reddo.TypeString); err == nil && v != nil {
				return v.(string)
			}
			// mapping path exists but cannot extract the value from the BO
			return ""
		}
	}
	if row, err := dao.GetRowMapper().ToRow(collectionName, bo); err == nil && row != nil {
		for _, t := range []string{collectionName, "*"} {
			if path, ok := dao.pkRowPathMap[t]; ok {
				s := semita.NewSemita(row)
				if v, err := s.GetValue(path); err == nil {
					return v
				}
			}
		}
	}
	return ""
}

// IsErrorDuplicatedEntry checks if the error was caused by document conflicting in collection.
func (dao *GenericDaoCosmosdb) IsErrorDuplicatedEntry(err error) bool {
	return err == gocosmos.ErrConflict
}

/*----------------------------------------------------------------------*/

// cosmosdbDeleteBuilder is CosmosDB variant of sql.DeleteBuilder.
type cosmosdbDeleteBuilder struct {
	*godalsql.DeleteBuilder
	pkValue interface{}
}

// Build implements ISqlBuilder.Build
func (b *cosmosdbDeleteBuilder) Build(opts ...interface{}) (string, []interface{}) {
	sql, values := b.DeleteBuilder.Build(opts...)
	values = append(values, b.pkValue)
	return sql, values
}

// GdaoDelete implements godal.IGenericDao.GdaoDelete.
func (dao *GenericDaoCosmosdb) GdaoDelete(collectionName string, bo godal.IGenericBo) (int, error) {
	return dao.GdaoDeleteWithTx(nil, nil, collectionName, bo)
}

// GdaoDeleteWithTx is database/sql variant of GdaoDelete.
func (dao *GenericDaoCosmosdb) GdaoDeleteWithTx(ctx context.Context, tx *gosql.Tx, collectionName string, bo godal.IGenericBo) (int, error) {
	f, err := dao.BuildFilter(collectionName, dao.GdaoCreateFilter(collectionName, bo))
	if err != nil {
		return 0, err
	}
	builder := &cosmosdbDeleteBuilder{
		pkValue:       dao.CosmosGetPk(collectionName, bo),
		DeleteBuilder: godalsql.NewDeleteBuilder().WithFlavor(dao.GetSqlFlavor()).WithTable(collectionName).WithFilter(f),
	}
	result, err := dao.SqlDeleteEx(ctx, builder, tx, collectionName, f)
	if err != nil {
		return 0, err
	}
	numRows, err := result.RowsAffected()
	return int(numRows), err
}

// GdaoDeleteMany implements godal.IGenericDao.GdaoDeleteMany.
func (dao *GenericDaoCosmosdb) GdaoDeleteMany(collectionName string, filter godal.FilterOpt) (int, error) {
	return dao.GdaoDeleteManyWithTx(nil, nil, collectionName, filter)
}

// GdaoDeleteManyWithTx is database/sql variant of GdaoDeleteMany.
//
// Note: this function firstly fetches all matched documents and then delete them one by one.
func (dao *GenericDaoCosmosdb) GdaoDeleteManyWithTx(ctx context.Context, tx *gosql.Tx, collectionName string, filter godal.FilterOpt) (int, error) {
	boList, err := dao.GdaoFetchManyWithTx(ctx, tx, collectionName, filter, nil, 0, 0)
	if err != nil {
		return 0, err
	}
	numRows := 0
	for _, bo := range boList {
		v, err := dao.GdaoDeleteWithTx(ctx, tx, collectionName, bo)
		if err != nil {
			return numRows, err
		}
		numRows += v
	}
	return numRows, nil
}

// cosmosdbSelectBuilder is CosmosDB variant of SelectBuilder.
type cosmosdbSelectBuilder struct {
	*godalsql.SelectBuilder
}

// Build implements ISqlBuilder.Build
func (b *cosmosdbSelectBuilder) Build(opts ...interface{}) (string, []interface{}) {
	opts = append(opts, godalsql.OptTableAlias{TableAlias: "c"})
	sql, values := b.SelectBuilder.Build(opts...)
	return sql, values
}

// GdaoFetchOne implements godal.IGenericDao.GdaoFetchOne.
func (dao *GenericDaoCosmosdb) GdaoFetchOne(collectionName string, filter godal.FilterOpt) (godal.IGenericBo, error) {
	return dao.GdaoFetchOneWithTx(nil, nil, collectionName, filter)
}

// GdaoFetchOneWithTx is database/sql variant of GdaoFetchOne.
func (dao *GenericDaoCosmosdb) GdaoFetchOneWithTx(ctx context.Context, tx *gosql.Tx, collectionName string, filter godal.FilterOpt) (godal.IGenericBo, error) {
	f, err := dao.BuildFilter(collectionName, filter)
	if err != nil {
		return nil, err
	}
	columns := dao.GetRowMapper().ColumnsList(collectionName)
	builder := &cosmosdbSelectBuilder{
		SelectBuilder: godalsql.NewSelectBuilder().WithFlavor(dao.GetSqlFlavor()).
			WithColumns(columns...).WithTables(collectionName).WithFilter(f),
	}
	dbRows, err := dao.SqlSelectEx(ctx, builder, tx, collectionName, columns, f, nil, 0, 0)
	if dbRows != nil {
		defer func() { _ = dbRows.Close() }()
	}
	if err != nil {
		return nil, err
	}
	return dao.FetchOne(collectionName, dbRows)
}

// GdaoFetchMany implements godal.IGenericDao.GdaoFetchMany.
func (dao *GenericDaoCosmosdb) GdaoFetchMany(collectionName string, filter godal.FilterOpt, sorting *godal.SortingOpt, fromOffset, numRows int) ([]godal.IGenericBo, error) {
	return dao.GdaoFetchManyWithTx(nil, nil, collectionName, filter, sorting, fromOffset, numRows)
}

// GdaoFetchManyWithTx is database/sql variant of GdaoFetchMany.
func (dao *GenericDaoCosmosdb) GdaoFetchManyWithTx(ctx context.Context, tx *gosql.Tx, collectionName string, filter godal.FilterOpt, sorting *godal.SortingOpt, fromOffset, numRows int) ([]godal.IGenericBo, error) {
	f, err := dao.BuildFilter(collectionName, filter)
	if err != nil {
		return nil, err
	}
	o, err := dao.BuildSorting(collectionName, sorting)
	if err != nil {
		return nil, err
	}
	columns := dao.GetRowMapper().ColumnsList(collectionName)
	builder := &cosmosdbSelectBuilder{
		SelectBuilder: godalsql.NewSelectBuilder().WithFlavor(dao.GetSqlFlavor()).WithColumns(columns...).
			WithTables(collectionName).WithFilter(f).WithSorting(o).WithLimit(numRows, fromOffset),
	}
	dbRows, err := dao.SqlSelectEx(ctx, builder, tx, collectionName, columns, f, o, fromOffset, numRows)
	if dbRows != nil {
		defer func() { _ = dbRows.Close() }()
	}
	if err != nil {
		return nil, err
	}
	return dao.FetchAll(collectionName, dbRows)
}

// cosmosdbInsertBuilder is CosmosDB variant of InsertBuilder.
type cosmosdbInsertBuilder struct {
	*godalsql.InsertBuilder
	pkValue  interface{}
	isUpsert bool
}

// Build implements ISqlBuilder.Build
func (b *cosmosdbInsertBuilder) Build(opts ...interface{}) (string, []interface{}) {
	sql, values := b.InsertBuilder.Build(opts...)
	values = append(values, b.pkValue)
	if b.isUpsert {
		sql = regexp.MustCompile(`(?i)^INSERT\s+`).ReplaceAllString(sql, "UPSERT ")
	}
	return sql, values
}

// GdaoCreate implements godal.IGenericDao.GdaoCreate.
func (dao *GenericDaoCosmosdb) GdaoCreate(collectionName string, bo godal.IGenericBo) (int, error) {
	return dao.GdaoCreateWithTx(nil, nil, collectionName, bo)
}

// GdaoCreateWithTx is database/sql variant of GdaoCreate.
func (dao *GenericDaoCosmosdb) GdaoCreateWithTx(ctx context.Context, tx *gosql.Tx, collectionName string, bo godal.IGenericBo) (int, error) {
	if row, err := dao.GetRowMapper().ToRow(collectionName, bo); err != nil {
		return 0, err
	} else if colsAndVals, err := reddo.ToMap(row, typeMap); err != nil {
		return 0, err
	} else {
		builder := &cosmosdbInsertBuilder{
			pkValue:       dao.CosmosGetPk(collectionName, bo),
			InsertBuilder: godalsql.NewInsertBuilder().WithFlavor(dao.GetSqlFlavor()).WithTable(collectionName).WithValues(colsAndVals.(map[string]interface{})),
		}
		result, err := dao.SqlInsertEx(ctx, builder, tx, collectionName, colsAndVals.(map[string]interface{}))
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

// GdaoSave implements godal.IGenericDao.GdaoSave.
func (dao *GenericDaoCosmosdb) GdaoSave(collectionName string, bo godal.IGenericBo) (int, error) {
	return dao.GdaoSaveWithTx(nil, nil, collectionName, bo)
}

// GdaoSaveWithTx is extended-implementation of godal.IGenericDao.GdaoSave.
func (dao *GenericDaoCosmosdb) GdaoSaveWithTx(ctx context.Context, tx *gosql.Tx, collectionName string, bo godal.IGenericBo) (int, error) {
	if row, err := dao.GetRowMapper().ToRow(collectionName, bo); err != nil {
		return 0, err
	} else if colsAndVals, err := reddo.ToMap(row, typeMap); err != nil {
		return 0, err
	} else {
		builder := &cosmosdbInsertBuilder{
			isUpsert:      true,
			pkValue:       dao.CosmosGetPk(collectionName, bo),
			InsertBuilder: godalsql.NewInsertBuilder().WithFlavor(dao.GetSqlFlavor()).WithTable(collectionName).WithValues(colsAndVals.(map[string]interface{})),
		}
		result, err := dao.SqlInsertEx(ctx, builder, tx, collectionName, colsAndVals.(map[string]interface{}))
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

// cosmosdbUpdateBuilder is CosmosDB variant of UpdateBuilder.
type cosmosdbUpdateBuilder struct {
	*godalsql.UpdateBuilder
	pkValue interface{}
}

// Build implements ISqlBuilder.Build
func (b *cosmosdbUpdateBuilder) Build(opts ...interface{}) (string, []interface{}) {
	sql, values := b.UpdateBuilder.Build(opts...)
	values = append(values, b.pkValue)
	return sql, values
}

// GdaoUpdate implements godal.IGenericDao.GdaoUpdate.
func (dao *GenericDaoCosmosdb) GdaoUpdate(collectionName string, bo godal.IGenericBo) (int, error) {
	return dao.GdaoUpdateWithTx(nil, nil, collectionName, bo)
}

// GdaoUpdateWithTx is database/sql variant of GdaoUpdate.
func (dao *GenericDaoCosmosdb) GdaoUpdateWithTx(ctx context.Context, tx *gosql.Tx, collectionName string, bo godal.IGenericBo) (int, error) {
	f, err := dao.BuildFilter(collectionName, dao.GdaoCreateFilter(collectionName, bo))
	if err != nil {
		return 0, err
	}
	row, err := dao.GetRowMapper().ToRow(collectionName, bo)
	if err != nil {
		return 0, err
	}
	colsAndVals, err := reddo.ToMap(row, typeMap)
	if err != nil {
		return 0, err
	}
	builder := &cosmosdbUpdateBuilder{
		pkValue: dao.CosmosGetPk(collectionName, bo),
		UpdateBuilder: godalsql.NewUpdateBuilder().WithFlavor(dao.GetSqlFlavor()).WithTable(collectionName).
			WithValues(colsAndVals.(map[string]interface{})).WithFilter(f),
	}
	result, err := dao.SqlUpdateEx(ctx, builder, tx, collectionName, colsAndVals.(map[string]interface{}), f)
	if err != nil {
		if dao.IsErrorDuplicatedEntry(err) {
			return 0, godal.ErrGdaoDuplicatedEntry
		}
		return 0, err
	}
	numRows, err := result.RowsAffected()
	return int(numRows), err
}
