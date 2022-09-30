/*
Package mongo provides a generic MongoDB implementation of godal.IGenericDao.

General guideline:

	- Dao must implement IGenericDao.GdaoCreateFilter(string, IGenericBo) FilterOpt.

Guideline: Use GenericDaoMongo (and godal.IGenericBo) directly

	- Define a dao struct that implements IGenericDao.GdaoCreateFilter(string, IGenericBo) FilterOpt.
	- Optionally, create a helper function to create dao instances.

	import (
		"github.com/btnguyen2k/consu/reddo"
		"github.com/btnguyen2k/godal"
		"github.com/btnguyen2k/godal/mongo"
		"github.com/btnguyen2k/prom"
	)

	type myGenericDaoMongo struct {
		*mongodrv.GenericDaoMongo
	}

	// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter.
	func (dao *myGenericDaoMongo) GdaoCreateFilter(storageId string, bo godal.IGenericBo) godal.FilterOpt {
		id := bo.GboGetAttrUnsafe(fieldId, reddo.TypeString)
		return &godal.FilterOptFieldOpValue{FieldName: fieldId, Operator: godal.FilterOpEqual, Value: id}
	}

	// newGenericDaoMongo is convenient method to create myGenericDaoMongo instances.
	func newGenericDaoMongo(mc *mongo.MongoConnect, txModeOnWrite bool) godal.IGenericDao {
		dao := &myGenericDaoMongo{}
		dao.GenericDaoMongo = mongodrv.NewGenericDaoMongo(mc, godal.NewAbstractGenericDao(dao))
		dao.SetTxModeOnWrite(txModeOnWrite)
		return dao
	}

	Since MongoDB is schema-less, GenericRowMapperMongo should be sufficient. NewGenericDaoMongo(...) creates a *GenericDaoMongo that uses GenericRowMapperMongo under-the-hood.

Guideline: Implement custom MongoDB business dao and bo

	- Define and implement the business dao (Note: dao must implement IGenericDao.GdaoCreateFilter(string, IGenericBo) FilterOpt).
	- Optionally, create a helper function to create dao instances.
	- Define functions to transform godal.IGenericBo to business bo and vice versa.

	import (
		"github.com/btnguyen2k/consu/reddo"
		"github.com/btnguyen2k/godal"
		"github.com/btnguyen2k/godal/mongo"
		"github.com/btnguyen2k/prom"
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

	// DaoAppMongodb is MongoDB-implementation of business dao
	type DaoAppMongodb struct {
		*mongodrv.GenericDaoMongo
		collectionName string
	}

	// NewDaoAppMongodb is convenient method to create DaoAppMongodb instances.
	func NewDaoAppMongodb(mc *mongo.MongoConnect, collectionName string, txModeOnWrite bool) *DaoAppMongodb {
		dao := &DaoAppMongodb{collectionName: collectionName}
		dao.GenericDaoMongo = mongodrv.NewGenericDaoMongo(mc, godal.NewAbstractGenericDao(dao))
		dao.SetTxModeOnWrite(txModeOnWrite)
		return dao
	}

	Since MongoDB is schema-less, GenericRowMapperMongo should be sufficient. NewGenericDaoMongo(...) creates a *GenericDaoMongo that uses GenericRowMapperMongo under-the-hood.

See more examples in 'examples' directory on project's GitHub: https://github.com/btnguyen2k/godal/tree/master/examples

To create mongo.MongoConnect, see package github.com/btnguyen2k/prom
*/
package mongo

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"regexp"

	"github.com/btnguyen2k/consu/reddo"
	"go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/prom/mongo"
)

// GenericRowMapperMongo is a generic implementation of godal.IRowMapper for MongoDB.
//
// Implementation rules:
//   - ToRow        : transform godal.IGenericBo "as-is" to map[string]interface{}.
//   - ToBo         : expect input is a map[string]interface{}, or JSON data (string or array/slice of bytes), transforms input to godal.IGenericBo via JSON unmarshalling.
//   - ColumnsList  : return []string{"*"} (MongoDB is schema-free, hence column-list is not used).
//   - ToDbColName  : return the input field name "as-is".
//   - ToBoFieldName: return the input column name "as-is".
//
// Available: since v0.0.2.
type GenericRowMapperMongo struct {
}

// ToRow implements godal.IRowMapper.ToRow.
//
// This function transforms godal.IGenericBo to map[string]interface{}. Field names are kept intact.
func (mapper *GenericRowMapperMongo) ToRow(collectionName string, bo godal.IGenericBo) (interface{}, error) {
	if bo == nil {
		return nil, nil
	}
	result := make(map[string]interface{})
	return result, bo.GboTransferViaJson(&result)
}

// ToBo implements godal.IRowMapper.ToBo.
//
// This function expects input to be a map[string]interface{}, or JSON data (string or array/slice of bytes), transforms it to godal.IGenericBo via JSON unmarshalling. Field names are kept intact.
func (mapper *GenericRowMapperMongo) ToBo(collectionName string, row interface{}) (godal.IGenericBo, error) {
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
// This function returns []string{"*"} since MongoDB is schema-free (hence column-list is not used).
func (mapper *GenericRowMapperMongo) ColumnsList(_ string) []string {
	return []string{"*"}
}

// ToDbColName implements godal.IRowMapper.ToDbColName.
//
// This function returns the input field name "as-is".
func (mapper *GenericRowMapperMongo) ToDbColName(_, fieldName string) string {
	return fieldName
}

// ToBoFieldName implements godal.IRowMapper.ToBoFieldName.
//
// This function returns the input column name "as-is".
func (mapper *GenericRowMapperMongo) ToBoFieldName(_, colName string) string {
	return colName
}

var (
	// GenericRowMapperMongoInstance is a pre-created instance of GenericRowMapperMongo that is ready to use.
	GenericRowMapperMongoInstance godal.IRowMapper = &GenericRowMapperMongo{}
)

/*--------------------------------------------------------------------------------*/

// NewGenericDaoMongo constructs a new MongoDB implementation of godal.IGenericDao with 'txModeOnWrite=false'.
func NewGenericDaoMongo(mongoConnect *mongo.MongoConnect, agdao *godal.AbstractGenericDao) *GenericDaoMongo {
	dao := &GenericDaoMongo{AbstractGenericDao: agdao, mongoConnect: mongoConnect, txModeOnWrite: false}
	if dao.GetRowMapper() == nil {
		dao.SetRowMapper(GenericRowMapperMongoInstance)
	}
	return dao
}

// GenericDaoMongo is MongoDB implementation of godal.IGenericDao.
//
// Function implementations (n = No, y = Yes, i = inherited):
//   - (n) GdaoCreateFilter(storageId string, bo godal.IGenericBo) godal.FilterOpt
// 	 - (y) GdaoDelete(storageId string, bo godal.IGenericBo) (int, error)
// 	 - (y) GdaoDeleteMany(storageId string, filter godal.FilterOpt) (int, error)
// 	 - (y) GdaoFetchOne(storageId string, filter godal.FilterOpt) (godal.IGenericBo, error)
// 	 - (y) GdaoFetchMany(storageId string, filter godal.FilterOpt, sorting *godal.SortingOpt, startOffset, numItems int) ([]godal.IGenericBo, error)
// 	 - (y) GdaoCreate(storageId string, bo godal.IGenericBo) (int, error)
// 	 - (y) GdaoUpdate(storageId string, bo godal.IGenericBo) (int, error)
// 	 - (y) GdaoSave(storageId string, bo godal.IGenericBo) (int, error)
type GenericDaoMongo struct {
	*godal.AbstractGenericDao
	mongoConnect  *mongo.MongoConnect
	txModeOnWrite bool
}

// GetMongoConnect returns the '*mongo.MongoConnect' instance attached to this DAO.
func (dao *GenericDaoMongo) GetMongoConnect() *mongo.MongoConnect {
	return dao.mongoConnect
}

// SetMongoConnect attaches a '*mongo.MongoConnect' instance to this DAO.
//
// Available since v0.0.2
func (dao *GenericDaoMongo) SetMongoConnect(mc *mongo.MongoConnect) *GenericDaoMongo {
	dao.mongoConnect = mc
	return dao
}

// GetTxModeOnWrite returns 'true' if transaction mode is enabled on write operations, 'false' otherwise.
//
// MongoDB's implementation of GdaoCreate is "get/check and write". It can be done either in transaction (txModeOnWrite=true) or non-transaction (txModeOnWrite=false) mode.
//
// Available: since v0.1.0
func (dao *GenericDaoMongo) GetTxModeOnWrite() bool {
	return dao.txModeOnWrite
}

// SetTxModeOnWrite enables/disables transaction mode on write operations.
//
// MongoDB's implementation of GdaoCreate is "get/check and write". It can be done either in transaction (txModeOnWrite=true) or non-transaction (txModeOnWrite=false) mode.
// As of MongoDB 4.0, transactions are available for replica set deployments only. Since MongoDB 4.2, transactions are also available for sharded cluster.
// By default, GenericDaoMongo is created with 'txModeOnWrite=false'. However, it is recommended to set 'txModeOnWrite=true' whenever possible.
//
// Available: since v0.1.0
func (dao *GenericDaoMongo) SetTxModeOnWrite(enabled bool) *GenericDaoMongo {
	dao.txModeOnWrite = enabled
	return dao
}

// GetMongoCollection returns the MongoDB collection object specified by 'collectionName'.
func (dao *GenericDaoMongo) GetMongoCollection(collectionName string, opts ...*options.CollectionOptions) *mongodrv.Collection {
	return dao.mongoConnect.GetCollection(collectionName, opts...)
}

func translateOperator(op godal.FilterOperator) (string, error) {
	switch op {
	case godal.FilterOpEqual:
		return "$eq", nil
	case godal.FilterOpNotEqual:
		return "$ne", nil
	case godal.FilterOpGreater:
		return "$gt", nil
	case godal.FilterOpGreaterOrEqual:
		return "$gte", nil
	case godal.FilterOpLess:
		return "$lt", nil
	case godal.FilterOpLessOrEqual:
		return "$lte", nil
	}
	return "", fmt.Errorf("cannot translate operator \"%#v\"", op)
}

// BuildFilter transforms a godal.FilterOpt to MongoDB-compatible filter map.
//
// See MongoDB query selector (https://docs.mongodb.com/manual/reference/operator/query/#query-selectors).
//
// Available since v0.5.0
func (dao *GenericDaoMongo) BuildFilter(collectionName string, filter godal.FilterOpt) (bson.M, error) {
	if filter == nil {
		return nil, nil
	}
	rm := dao.GetRowMapper()
	if rm == nil {
		return nil, errors.New("row-mapper is required to build filter")
	}

	switch filter.(type) {
	case godal.FilterOptFieldOpValue:
		f := filter.(godal.FilterOptFieldOpValue)
		return dao.BuildFilter(collectionName, &f)
	case *godal.FilterOptFieldOpValue:
		f := filter.(*godal.FilterOptFieldOpValue)
		opStr, err := translateOperator(f.Operator)
		result := bson.M{rm.ToDbColName(collectionName, f.FieldName): bson.M{opStr: f.Value}}
		return result, err
	case godal.FilterOptFieldIsNull:
		f := filter.(godal.FilterOptFieldIsNull)
		return dao.BuildFilter(collectionName, &f)
	case *godal.FilterOptFieldIsNull:
		f := filter.(*godal.FilterOptFieldIsNull)
		result := bson.M{rm.ToDbColName(collectionName, f.FieldName): bson.M{"$eq": nil}}
		return result, nil
	case godal.FilterOptFieldIsNotNull:
		f := filter.(godal.FilterOptFieldIsNotNull)
		return dao.BuildFilter(collectionName, &f)
	case *godal.FilterOptFieldIsNotNull:
		f := filter.(*godal.FilterOptFieldIsNotNull)
		result := bson.M{rm.ToDbColName(collectionName, f.FieldName): bson.M{"$ne": nil}}
		return result, nil
	case godal.FilterOptAnd:
		f := filter.(godal.FilterOptAnd)
		return dao.BuildFilter(collectionName, &f)
	case *godal.FilterOptAnd:
		f := filter.(*godal.FilterOptAnd)
		inner := bson.A{}
		for _, innerF := range f.Filters {
			innerResult, err := dao.BuildFilter(collectionName, innerF)
			if err != nil {
				return nil, err
			}
			inner = append(inner, innerResult)
		}
		return bson.M{"$and": inner}, nil
	case godal.FilterOptOr:
		f := filter.(godal.FilterOptOr)
		return dao.BuildFilter(collectionName, &f)
	case *godal.FilterOptOr:
		f := filter.(*godal.FilterOptOr)
		inner := bson.A{}
		for _, innerF := range f.Filters {
			innerResult, err := dao.BuildFilter(collectionName, innerF)
			if err != nil {
				return nil, err
			}
			inner = append(inner, innerResult)
		}
		return bson.M{"$or": inner}, nil
	}
	return nil, fmt.Errorf("cannot build filter from %T", filter)
}

// MongoDeleteMany performs a MongoDB's delete-many command on the specified collection.
//   - ctx: can be used to pass a transaction down to the operation.
//   - filter: see MongoDB query selector (https://docs.mongodb.com/manual/reference/operator/query/#query-selectors).
func (dao *GenericDaoMongo) MongoDeleteMany(ctx context.Context, collectionName string, filter godal.FilterOpt) (*mongodrv.DeleteResult, error) {
	f, err := dao.BuildFilter(collectionName, filter)
	if err != nil {
		return nil, err
	}
	return dao.GetMongoCollection(collectionName).DeleteMany(ctx, f)
}

// MongoFetchOne performs a MongoDB's find-one command on the specified collection.
//   - ctx: can be used to pass a transaction down to the operation.
//   - filter: see MongoDB query selector (https://docs.mongodb.com/manual/reference/operator/query/#query-selectors).
func (dao *GenericDaoMongo) MongoFetchOne(ctx context.Context, collectionName string, filter godal.FilterOpt) *mongodrv.SingleResult {
	f, err := dao.BuildFilter(collectionName, filter)
	if err != nil {
		return nil
	}
	return dao.GetMongoCollection(collectionName).FindOne(ctx, f)
}

// MongoFetchMany performs a MongoDB's find command on the specified collection.
//   - ctx: can be used to pass a transaction down to the operation.
//   - filter: see MongoDB query selector (https://docs.mongodb.com/manual/reference/operator/query/#query-selectors).
//   - sorting: see MongoDB ascending/descending sort (https://docs.mongodb.com/manual/reference/method/cursor.sort/index.html#sort-asc-desc).
func (dao *GenericDaoMongo) MongoFetchMany(ctx context.Context, collectionName string, filter godal.FilterOpt, sorting *godal.SortingOpt, startOffset, numItems int) (*mongodrv.Cursor, error) {
	f, err := dao.BuildFilter(collectionName, filter)
	if err != nil {
		return nil, err
	}

	opt := &options.FindOptions{}
	if sorting != nil && len(sorting.Fields) > 0 {
		sortingInfo := bson.D{}
		for _, field := range sorting.Fields {
			if field.Descending {
				sortingInfo = append(sortingInfo, bson.E{Key: field.FieldName, Value: -1})
			} else {
				sortingInfo = append(sortingInfo, bson.E{Key: field.FieldName, Value: 1})
			}
		}
		opt.SetSort(sortingInfo)
	}
	if numItems > 0 {
		opt.SetLimit(int64(numItems))
	}
	if startOffset > 0 {
		opt.SetSkip(int64(startOffset))
	}

	return dao.GetMongoCollection(collectionName).Find(ctx, f, opt)
}

// MongoInsertOne performs a MongoDB's insert-one command on the specified collection.
//   - ctx: can be used to pass a transaction down to the operation.
func (dao *GenericDaoMongo) MongoInsertOne(ctx context.Context, collectionName string, doc interface{}) (*mongodrv.InsertOneResult, error) {
	return dao.GetMongoCollection(collectionName).InsertOne(ctx, doc)
}

// MongoUpdateOne performs a MongoDB's find-one-and-replace command with 'upsert=false' on the specified collection.
//   - ctx: can be used to pass a transaction down to the operation.
//   - filter: see MongoDB query selector (https://docs.mongodb.com/manual/reference/operator/query/#query-selectors).
func (dao *GenericDaoMongo) MongoUpdateOne(ctx context.Context, collectionName string, filter godal.FilterOpt, doc interface{}) *mongodrv.SingleResult {
	f, err := dao.BuildFilter(collectionName, filter)
	if err != nil {
		return nil
	}
	upsert := false
	opt := options.FindOneAndReplaceOptions{Upsert: &upsert}
	return dao.GetMongoCollection(collectionName).FindOneAndReplace(ctx, f, doc, &opt)
}

// MongoSaveOne performs a MongoDB's find-one-and-replace command with 'upsert=true' on the specified collection.
//   - ctx: can be used to pass a transaction down to the operation.
//   - filter: see MongoDB query selector (https://docs.mongodb.com/manual/reference/operator/query/#query-selectors).
func (dao *GenericDaoMongo) MongoSaveOne(ctx context.Context, collectionName string, filter godal.FilterOpt, doc interface{}) *mongodrv.SingleResult {
	f, err := dao.BuildFilter(collectionName, filter)
	if err != nil {
		return nil
	}
	upsert := true
	opt := options.FindOneAndReplaceOptions{Upsert: &upsert}
	return dao.GetMongoCollection(collectionName).FindOneAndReplace(ctx, f, doc, &opt)
}

/*----------------------------------------------------------------------*/

// GdaoDelete implements godal.IGenericDao.GdaoDelete.
//
// Available: since v0.1.0
func (dao *GenericDaoMongo) GdaoDelete(collectionName string, bo godal.IGenericBo) (int, error) {
	return dao.GdaoDeleteWithContext(nil, collectionName, bo)
}

// GdaoDeleteWithContext is is MongoDB variant of GdaoDelete.
//
// Available: since v0.1.0
func (dao *GenericDaoMongo) GdaoDeleteWithContext(ctx context.Context, collectionName string, bo godal.IGenericBo) (int, error) {
	filter := dao.GdaoCreateFilter(collectionName, bo)
	return dao.GdaoDeleteManyWithContext(ctx, collectionName, filter)
}

// GdaoDeleteMany implements godal.IGenericDao.GdaoDeleteMany.
func (dao *GenericDaoMongo) GdaoDeleteMany(collectionName string, filter godal.FilterOpt) (int, error) {
	return dao.GdaoDeleteManyWithContext(nil, collectionName, filter)
}

// GdaoDeleteManyWithContext is is MongoDB variant of GdaoDeleteMany.
//
// Available: since v0.1.0
func (dao *GenericDaoMongo) GdaoDeleteManyWithContext(ctx context.Context, collectionName string, filter godal.FilterOpt) (int, error) {
	dbResult, err := dao.MongoDeleteMany(dao.mongoConnect.NewContextIfNil(ctx), collectionName, filter)
	if err != nil {
		return 0, err
	}
	return int(dbResult.DeletedCount), nil
}

// GdaoFetchOne implements godal.IGenericDao.GdaoFetchOne.
func (dao *GenericDaoMongo) GdaoFetchOne(collectionName string, filter godal.FilterOpt) (godal.IGenericBo, error) {
	return dao.GdaoFetchOneWithContext(nil, collectionName, filter)
}

// GdaoFetchOneWithContext is is MongoDB variant of GdaoFetchOne.
//
// Available: since v0.1.0
func (dao *GenericDaoMongo) GdaoFetchOneWithContext(ctx context.Context, collectionName string, filter godal.FilterOpt) (godal.IGenericBo, error) {
	row := dao.MongoFetchOne(dao.mongoConnect.NewContextIfNil(ctx), collectionName, filter)
	if row == nil {
		return nil, errors.New("nil result from MongoFetchOne")
	}
	jsData, err := dao.mongoConnect.DecodeSingleResultRaw(row)
	if err != nil || jsData == nil {
		return nil, err
	}
	return dao.GetRowMapper().ToBo(collectionName, jsData)
}

// GdaoFetchMany implements godal.IGenericDao.GdaoFetchMany.
//   - nil filter means "match all".
func (dao *GenericDaoMongo) GdaoFetchMany(collectionName string, filter godal.FilterOpt, sorting *godal.SortingOpt, startOffset, numItems int) ([]godal.IGenericBo, error) {
	return dao.GdaoFetchManyWithContext(nil, collectionName, filter, sorting, startOffset, numItems)
}

// GdaoFetchManyWithContext is is MongoDB variant of GdaoFetchMany.
//
// Available: since v0.1.0
func (dao *GenericDaoMongo) GdaoFetchManyWithContext(ctx context.Context, collectionName string, filter godal.FilterOpt, sorting *godal.SortingOpt, startOffset, numItems int) ([]godal.IGenericBo, error) {
	ctx = dao.mongoConnect.NewContextIfNil(ctx)
	cursor, err := dao.MongoFetchMany(ctx, collectionName, filter, sorting, startOffset, numItems)
	if cursor != nil {
		defer func() { _ = cursor.Close(ctx) }()
	}
	if err != nil {
		return nil, err
	}
	resultBoList := make([]godal.IGenericBo, 0)
	var resultError error = nil
	dao.mongoConnect.DecodeResultCallbackRaw(ctx, cursor, func(docNum int, doc []byte, err error) bool {
		if err != nil {
			resultError = err
			return false
		}
		bo, e := dao.GetRowMapper().ToBo(collectionName, doc)
		if e != nil {
			resultError = e
			return false
		}
		resultBoList = append(resultBoList, bo)
		return true
	})
	return resultBoList, resultError
}

func isErrorDuplicatedKey(err error) bool {
	if err == nil {
		return false
	}
	return err == godal.ErrGdaoDuplicatedEntry || // already duplicated key error
		regexp.MustCompile(`\WE11000\W`).FindString(err.Error()) != "" || // MongoDB's duplicated key error
		regexp.MustCompile(`\WConflictingOperationInProgress\W`).FindString(err.Error()) != "" // CosmosDB's MongoDB API duplicated key error
}

func (dao *GenericDaoMongo) insertIfNotExist(ctx context.Context, collectionName string, bo godal.IGenericBo) (bool, error) {
	// first fetch existing document from storage
	filter := dao.GdaoCreateFilter(collectionName, bo)
	row := dao.MongoFetchOne(ctx, collectionName, filter)
	if row == nil {
		return false, errors.New("nil result from MongoFetchOne")
	}
	if jsData, err := dao.mongoConnect.DecodeSingleResultRaw(row); err != nil || jsData != nil {
		if err != nil {
			return false, err
		}
		return false, godal.ErrGdaoDuplicatedEntry
	}

	// insert new document
	if doc, err := dao.GetRowMapper().ToRow(collectionName, bo); err != nil {
		return false, err
	} else if _, err = dao.MongoInsertOne(ctx, collectionName, doc); err != nil {
		return false, err
	}
	return true, nil
}

// WrapTransaction wraps a function inside a transaction.
//   - txFunc: the function to wrap. If the function returns error, the transaction will be aborted, otherwise transaction is committed.
//
// Available: since v0.0.4
func (dao *GenericDaoMongo) WrapTransaction(ctx context.Context, txFunc func(sctx mongodrv.SessionContext) error) error {
	// UseSession will close open session and pending transaction
	return dao.mongoConnect.GetMongoClient().UseSession(dao.mongoConnect.NewContextIfNil(ctx), func(sctx mongodrv.SessionContext) error {
		if err := sctx.StartTransaction(options.Transaction().
			SetReadConcern(readconcern.Snapshot()).
			SetWriteConcern(writeconcern.New(writeconcern.WMajority()))); err != nil {
			return err
		}
		if err := txFunc(sctx); err != nil {
			sctx.AbortTransaction(sctx)
			return err
		}
		return sctx.CommitTransaction(sctx)
	})
}

// GdaoCreate implements godal.IGenericDao.GdaoCreate.
func (dao *GenericDaoMongo) GdaoCreate(collectionName string, bo godal.IGenericBo) (int, error) {
	return dao.GdaoCreateWithContext(nil, collectionName, bo)
}

// GdaoCreateWithContext is is MongoDB variant of GdaoCreate.
//
// Available: since v0.1.0
func (dao *GenericDaoMongo) GdaoCreateWithContext(ctx context.Context, collectionName string, bo godal.IGenericBo) (int, error) {
	ctx = dao.mongoConnect.NewContextIfNil(ctx)
	if dao.txModeOnWrite {
		numRows := 0
		err := dao.WrapTransaction(ctx, func(sctx mongodrv.SessionContext) error {
			if result, err := dao.insertIfNotExist(sctx, collectionName, bo); err != nil {
				return err
			} else if result {
				numRows = 1
			}
			return nil
		})
		if isErrorDuplicatedKey(err) {
			return 0, godal.ErrGdaoDuplicatedEntry
		}
		return numRows, err
	}
	if result, err := dao.insertIfNotExist(ctx, collectionName, bo); err != nil {
		if isErrorDuplicatedKey(err) {
			return 0, godal.ErrGdaoDuplicatedEntry
		}
		return 0, err
	} else if result {
		return 1, nil
	}
	return 0, nil
}

// GdaoUpdate implements godal.IGenericDao.GdaoUpdate.
func (dao *GenericDaoMongo) GdaoUpdate(collectionName string, bo godal.IGenericBo) (int, error) {
	return dao.GdaoUpdateWithContext(nil, collectionName, bo)
}

// GdaoUpdateWithContext is is MongoDB variant of GdaoUpdate.
//
// Available: since v0.1.0
func (dao *GenericDaoMongo) GdaoUpdateWithContext(ctx context.Context, collectionName string, bo godal.IGenericBo) (int, error) {
	doc, err := dao.GetRowMapper().ToRow(collectionName, bo)
	if err != nil {
		return 0, err
	}
	filter := dao.GdaoCreateFilter(collectionName, bo)
	result := dao.MongoUpdateOne(dao.mongoConnect.NewContextIfNil(ctx), collectionName, filter, doc)
	if _, err := result.DecodeBytes(); err == mongodrv.ErrNoDocuments {
		return 0, nil
	} else if isErrorDuplicatedKey(err) {
		return 0, godal.ErrGdaoDuplicatedEntry
	} else {
		return 1, err
	}
}

// GdaoSave implements godal.IGenericDao.GdaoSave.
func (dao *GenericDaoMongo) GdaoSave(collectionName string, bo godal.IGenericBo) (int, error) {
	return dao.GdaoSaveWithContext(nil, collectionName, bo)
}

// GdaoSaveWithContext is is MongoDB variant of GdaoSave.
//
// Available: since v0.1.0
func (dao *GenericDaoMongo) GdaoSaveWithContext(ctx context.Context, collectionName string, bo godal.IGenericBo) (int, error) {
	doc, err := dao.GetRowMapper().ToRow(collectionName, bo)
	if err != nil {
		return 0, err
	}
	filter := dao.GdaoCreateFilter(collectionName, bo)
	result := dao.MongoSaveOne(dao.mongoConnect.NewContextIfNil(ctx), collectionName, filter, doc)
	if err = result.Err(); err == nil || err == mongodrv.ErrNoDocuments {
		return 1, nil
	}
	if isErrorDuplicatedKey(err) {
		return 0, godal.ErrGdaoDuplicatedEntry
	}
	return 1, err
}
