/*
Package mongo provides a generic implementation of godal.IGenericDao.

General guideline:

	- Dao must implement IGenericDao.GdaoCreateFilter(string, IGenericBo) interface{}.

Guideline: Use GenericDaoMongo (and godal.IGenericBo) directly

	- Defined a dao struct that implements IGenericDao.GdaoCreateFilter(string, IGenericBo) interface{}.
	- Optionally, create a helper function to create dao instances.

	import (
		"github.com/btnguyen2k/godal"
		"github.com/btnguyen2k/godal/mongo"
		"github.com/btnguyen2k/prom"
	)

	type myGenericDaoMongo struct {
		*mongo.GenericDaoMongo
	}

	// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter.
	func (dao *myGenericDaoMongo) GdaoCreateFilter(storageId string, bo godal.IGenericBo) interface{} {
		id := bo.GboGetAttrUnsafe(fieldId, reddo.TypeString)
		return map[string]interface{}{fieldId: id}
	}

	// newGenericDaoMongo is convenient method to create myGenericDaoMongo instances.
	func newGenericDaoMongo(mc *prom.MongoConnect, txModeOnWrite bool) godal.IGenericDao {
		dao := &myGenericDaoMongo{}
		dao.GenericDaoMongo = mongo.NewGenericDaoMongo(mc, godal.NewAbstractGenericDao(dao))
		dao.SetTxModeOnWrite(txModeOnWrite)
		return dao
	}

	Since MongoDB is schema-less, GenericRowMapperMongo should be sufficient. NewGenericDaoMongo(...) creates a *GenericDaoMongo that uses GenericRowMapperMongo under-the-hood.

Guideline: Implement custom MongoDB business dao and bo

	- Declare / implement the business dao (Note: dao must implement IGenericDao.GdaoCreateFilter(string, IGenericBo) interface{}.)
	- Optionally, create a helper function to create dao instances.
	- Define functions to transform godal.IGenericBo to business bo and vice versa.

	import (
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
		*mongo.GenericDaoMongo
		collectionName string
	}

	// NewDaoAppMongodb is convenient method to create DaoAppMongodb instances.
	func NewDaoAppMongodb(mc *prom.MongoConnect, collectionName string, txModeOnWrite bool) *DaoAppMongodb {
		dao := &DaoAppMongodb{collectionName: collectionName}
		dao.GenericDaoMongo = mongo.NewGenericDaoMongo(mc, godal.NewAbstractGenericDao(dao))
		dao.SetTxModeOnWrite(txModeOnWrite)
		return dao
	}

	Since MongoDB is schema-less, GenericRowMapperMongo should be sufficient. NewGenericDaoMongo(...) creates a *GenericDaoMongo that uses GenericRowMapperMongo under-the-hood.

See more examples in 'examples' directory on project's GitHub: https://github.com/btnguyen2k/godal/tree/master/examples
*/
package mongo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/prom"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"reflect"
	"regexp"
)

/*
GenericRowMapperMongo is a generic implementation of godal.IRowMapper for MongoDB.

Implementation rules:

	- ToRow: transform godal.IGenericBo "as-is" to map[string]interface{}.
	- ToBo: expects input is a map[string]interface{}, or JSON data (string or array/slice of bytes), transforms input to godal.IGenericBo via JSON unmarshalling.
	- ColumnsList: return []string{"*"} (MongoDB is schema-free, hence column-list is not used).

Available: since v0.0.2.
*/
type GenericRowMapperMongo struct {
}

/*
ToRow implements godal.IRowMapper.ToRow.
This function transforms godal.IGenericBo to map[string]interface{}. Field names are kept intact.
*/
func (mapper *GenericRowMapperMongo) ToRow(collectionName string, bo godal.IGenericBo) (interface{}, error) {
	if bo == nil {
		return nil, nil
	}
	result := make(map[string]interface{})
	return result, bo.GboTransferViaJson(&result)
}

/*
ToBo implements godal.IRowMapper.ToBo.
This function expects input to be a map[string]interface{}, or JSON data (string or array/slice of bytes), transforms it to godal.IGenericBo via JSON unmarshalling. Field names are kept intact.
*/
func (mapper *GenericRowMapperMongo) ToBo(collectionName string, row interface{}) (godal.IGenericBo, error) {
	if row == nil {
		return nil, nil
	}
	switch row.(type) {
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
		bo := godal.NewGenericBo()
		return bo, bo.GboFromJson([]byte(*row.(*string)))
	case []byte:
		bo := godal.NewGenericBo()
		return bo, bo.GboFromJson(row.([]byte))
	case *[]byte:
		bo := godal.NewGenericBo()
		return bo, bo.GboFromJson(*row.(*[]byte))
	}

	v := reflect.ValueOf(row)
	for ; v.Kind() == reflect.Ptr; v = v.Elem() {
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
			if err != nil {
				return nil, err
			}
			bo := godal.NewGenericBo()
			return bo, bo.GboFromJson(arr.([]byte))
		}
	}
	return nil, errors.New(fmt.Sprintf("cannot construct godal.IGenericBo from input %v", row))
}

/*
ColumnsList implements godal.IRowMapper.ColumnsList.
This function returns []string{"*"} since MongoDB is schema-free (hence column-list is not used).
*/
func (mapper *GenericRowMapperMongo) ColumnsList(collectionName string) []string {
	return []string{"*"}
}

var (
	/*
		GenericRowMapperMongoInstance is a pre-created instance of GenericRowMapperMongo that is ready to use.
	*/
	GenericRowMapperMongoInstance godal.IRowMapper = &GenericRowMapperMongo{}
)

/*--------------------------------------------------------------------------------*/

/*
NewGenericDaoMongo constructs a new MongoDB implementation of 'godal.IGenericDao' with 'txModeOnWrite=false'.
*/
func NewGenericDaoMongo(mongoConnect *prom.MongoConnect, agdao *godal.AbstractGenericDao) *GenericDaoMongo {
	dao := &GenericDaoMongo{AbstractGenericDao: agdao, mongoConnect: mongoConnect, txModeOnWrite: false}
	if dao.GetRowMapper() == nil {
		dao.SetRowMapper(GenericRowMapperMongoInstance)
	}
	return dao
}

/*
GenericDaoMongo is MongoDB implementation of godal.IGenericDao.

Function implementations (n = No, y = Yes, i = inherited):

	(n) GdaoCreateFilter(storageId string, bo godal.IGenericBo) interface{}
	(y) GdaoDelete(storageId string, bo godal.IGenericBo) (int, error)
	(y) GdaoDeleteMany(storageId string, filter interface{}) (int, error)
	(y) GdaoFetchOne(storageId string, filter interface{}) (godal.IGenericBo, error)
	(y) GdaoFetchMany(storageId string, filter interface{}, sorting interface{}, startOffset, numItems int) ([]godal.IGenericBo, error)
	(y) GdaoCreate(storageId string, bo godal.IGenericBo) (int, error)
	(y) GdaoUpdate(storageId string, bo godal.IGenericBo) (int, error)
	(y) GdaoSave(storageId string, bo godal.IGenericBo) (int, error)
*/
type GenericDaoMongo struct {
	*godal.AbstractGenericDao
	mongoConnect  *prom.MongoConnect
	txModeOnWrite bool
}

/*
GetMongoConnect returns the '*prom.MongoConnect' instance attached to this DAO.
*/
func (dao *GenericDaoMongo) GetMongoConnect() *prom.MongoConnect {
	return dao.mongoConnect
}

/*
SetMongoConnect attaches a '*prom.MongoConnect' instance to this DAO.

Available since v0.0.2
*/
func (dao *GenericDaoMongo) SetMongoConnect(mc *prom.MongoConnect) *GenericDaoMongo {
	dao.mongoConnect = mc
	return dao
}

/*
GetTxModeOnWrite returns 'true' if transaction mode is enabled on write operations, 'false' otherwise.

MongoDB's implementation of GdaoCreate is "get/check and write". It can be done either in transaction (txModeOnWrite=true) or non-transaction (txModeOnWrite=false) mode.

Available: since v0.1.0
*/
func (dao *GenericDaoMongo) GetTxModeOnWrite() bool {
	return dao.txModeOnWrite
}

/*
SetTxModeOnWrite enables/disables transaction mode on write operations.

MongoDB's implementation of GdaoCreate is "get/check and write". It can be done either in transaction (txModeOnWrite=true) or non-transaction (txModeOnWrite=false) mode.
As of MongoDB 4.0, transactions are available for replica set deployments only. Since MongoDB 4.2, transactions are also available for sharded cluster.
By default, GenericDaoMongo is created with 'txModeOnWrite=false'. However, it is recommended to set 'txModeOnWrite=true' whenever possible.

Available: since v0.1.0
*/
func (dao *GenericDaoMongo) SetTxModeOnWrite(enabled bool) *GenericDaoMongo {
	dao.txModeOnWrite = enabled
	return dao
}

/*
GetTransactionMode returns 'true' if transaction mode is enabled, 'false' otherwise.

Deprecated: since v0.1.0 use GetTxModeOnWrite instead.
*/
func (dao *GenericDaoMongo) GetTransactionMode() bool {
	return dao.GetTxModeOnWrite()
}

/*
SetTransactionMode enables/disables transaction mode.

Deprecated: since v0.1.0 use SetTxModeOnWrite instead.
*/
func (dao *GenericDaoMongo) SetTransactionMode(enabled bool) *GenericDaoMongo {
	return dao.SetTxModeOnWrite(enabled)
}

/*
GetMongoCollection returns the MongoDB collection object specified by 'collectionName'.
*/
func (dao *GenericDaoMongo) GetMongoCollection(collectionName string, opts ...*options.CollectionOptions) *mongo.Collection {
	return dao.mongoConnect.GetCollection(collectionName, opts...)
}

/*
MongoDeleteMany performs a MongoDB's delete-many command on the specified collection.

	- ctx: can be used to pass a transaction down to the operation
	- filter: see MongoDB query selector (https://docs.mongodb.com/manual/reference/operator/query/#query-selectors)
*/
func (dao *GenericDaoMongo) MongoDeleteMany(ctx context.Context, collectionName string, filter map[string]interface{}) (*mongo.DeleteResult, error) {
	return dao.GetMongoCollection(collectionName).DeleteMany(ctx, filter)
}

/*
MongoFetchOne performs a MongoDB's find-one command on the specified collection.

	- ctx: can be used to pass a transaction down to the operation
	- filter: see MongoDB query selector (https://docs.mongodb.com/manual/reference/operator/query/#query-selectors)
*/
func (dao *GenericDaoMongo) MongoFetchOne(ctx context.Context, collectionName string, filter map[string]interface{}) *mongo.SingleResult {
	return dao.GetMongoCollection(collectionName).FindOne(ctx, filter)
}

/*
MongoFetchMany performs a MongoDB's find command on the specified collection.

	- ctx: can be used to pass a transaction down to the operation
	- filter: see MongoDB query selector (https://docs.mongodb.com/manual/reference/operator/query/#query-selectors)
	- sorting: see MongoDB ascending/descending sort (https://docs.mongodb.com/manual/reference/method/cursor.sort/index.html#sort-asc-desc)
*/
func (dao *GenericDaoMongo) MongoFetchMany(ctx context.Context, collectionName string, filter map[string]interface{}, sorting map[string]int, startOffset, numItems int) (*mongo.Cursor, error) {
	opt := &options.FindOptions{}
	if sorting != nil && len(sorting) > 0 {
		opt.SetSort(sorting)
	}
	if numItems > 0 {
		opt.SetLimit(int64(numItems))
	}
	if startOffset > 0 {
		opt.SetSkip(int64(startOffset))
	}
	return dao.GetMongoCollection(collectionName).Find(ctx, filter, opt)
}

/*
MongoInsertOne performs a MongoDB's insert-one command on the specified collection.

	- ctx: can be used to pass a transaction down to the operation
*/
func (dao *GenericDaoMongo) MongoInsertOne(ctx context.Context, collectionName string, doc interface{}) (*mongo.InsertOneResult, error) {
	return dao.GetMongoCollection(collectionName).InsertOne(ctx, doc)
}

/*
MongoUpdateOne performs a MongoDB's find-one-and-replace command with 'upsert=false' on the specified collection.

	- ctx: can be used to pass a transaction down to the operation
	- filter: see MongoDB query selector (https://docs.mongodb.com/manual/reference/operator/query/#query-selectors)
*/
func (dao *GenericDaoMongo) MongoUpdateOne(ctx context.Context, collectionName string, filter map[string]interface{}, doc interface{}) *mongo.SingleResult {
	upsert := false
	opt := options.FindOneAndReplaceOptions{Upsert: &upsert}
	return dao.GetMongoCollection(collectionName).FindOneAndReplace(ctx, filter, doc, &opt)
}

/*
MongoSaveOne performs a MongoDB's find-one-and-replace command with 'upsert=true' on the specified collection.

	- ctx: can be used to pass a transaction down to the operation
	- filter: see MongoDB query selector (https://docs.mongodb.com/manual/reference/operator/query/#query-selectors)
*/
func (dao *GenericDaoMongo) MongoSaveOne(ctx context.Context, collectionName string, filter map[string]interface{}, doc interface{}) *mongo.SingleResult {
	upsert := true
	opt := options.FindOneAndReplaceOptions{Upsert: &upsert}
	return dao.GetMongoCollection(collectionName).FindOneAndReplace(ctx, filter, doc, &opt)

}

/*----------------------------------------------------------------------*/

func toMap(input interface{}) (map[string]interface{}, error) {
	if input == nil {
		return nil, nil
	}
	v := reflect.ValueOf(input)
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.String:
		// expect input to be a map in JSON
		result := make(map[string]interface{})
		err := json.Unmarshal([]byte(v.Interface().(string)), &result)
		return result, err
	case reflect.Array, reflect.Slice:
		// expect input to be a map in JSON
		t, err := reddo.ToSlice(v.Interface(), reflect.TypeOf(byte(0)))
		if err != nil {
			return nil, err
		}
		result := make(map[string]interface{})
		err = json.Unmarshal(t.([]byte), &result)
		return result, err
	case reflect.Map:
		t := make(map[string]interface{})
		result, err := reddo.ToMap(v.Interface(), reflect.TypeOf(t))
		return result.(map[string]interface{}), err

	}
	return nil, errors.New(fmt.Sprintf("cannot convert %v to map[string]interface{}", input))
}

func toSortingMap(input interface{}) (map[string]int, error) {
	if input == nil {
		return nil, nil
	}
	v := reflect.ValueOf(input)
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.String:
		// expect input to be a map in JSON
		result := make(map[string]int)
		err := json.Unmarshal([]byte(v.Interface().(string)), &result)
		return result, err
	case reflect.Array, reflect.Slice:
		// expect input to be a map in JSON
		t, err := reddo.ToSlice(v.Interface(), reflect.TypeOf(byte(0)))
		if err != nil {
			return nil, err
		}
		result := make(map[string]int)
		err = json.Unmarshal(t.([]byte), &result)
		return result, err
	case reflect.Map:
		t := make(map[string]int)
		result, err := reddo.ToMap(v.Interface(), reflect.TypeOf(t))
		return result.(map[string]int), err

	}
	return nil, errors.New(fmt.Sprintf("cannot convert %v to map[string]int", input))
}

/*
GdaoDelete implements godal.IGenericDao.GdaoDelete.

Available: since v0.1.0
*/
func (dao *GenericDaoMongo) GdaoDelete(collectionName string, bo godal.IGenericBo) (int, error) {
	return dao.GdaoDeleteWithContext(nil, collectionName, bo)
}

/*
GdaoDeleteWithContext is extended-implementation of godal.IGenericDao.GdaoDelete.

	- ctx: can be used to pass a transaction down to the operation

Available: since v0.1.0
*/
func (dao *GenericDaoMongo) GdaoDeleteWithContext(ctx context.Context, collectionName string, bo godal.IGenericBo) (int, error) {
	filter := dao.GdaoCreateFilter(collectionName, bo)
	return dao.GdaoDeleteManyWithContext(ctx, collectionName, filter)
}

/*
GdaoDeleteMany implements godal.IGenericDao.GdaoDeleteMany.

	- filter should be a map[string]interface{}, or it can be a string/[]byte representing map[string]interface{} in JSON, then it is unmarshalled to map[string]interface{}
	- see MongoDB query selector (https://docs.mongodb.com/manual/reference/operator/query/#query-selectors)
*/
func (dao *GenericDaoMongo) GdaoDeleteMany(collectionName string, filter interface{}) (int, error) {
	return dao.GdaoDeleteManyWithContext(nil, collectionName, filter)
}

/*
GdaoDeleteManyWithContext is extended-implementation of godal.IGenericDao.GdaoDeleteMany.

	- ctx: can be used to pass a transaction down to the operation
	- filter should be a map[string]interface{}, or it can be a string/[]byte representing map[string]interface{} in JSON, then it is unmarshalled to map[string]interface{}
	- see MongoDB query selector (https://docs.mongodb.com/manual/reference/operator/query/#query-selectors)

Available: since v0.1.0
*/
func (dao *GenericDaoMongo) GdaoDeleteManyWithContext(ctx context.Context, collectionName string, filter interface{}) (int, error) {
	if f, err := toMap(filter); err != nil {
		return 0, err
	} else {
		if ctx == nil {
			ctx, _ = dao.mongoConnect.NewContext()
		}
		if dbResult, err := dao.MongoDeleteMany(ctx, collectionName, f); err != nil {
			return 0, err
		} else {
			return int(dbResult.DeletedCount), nil
		}
	}
}

/*
GdaoFetchOne implements godal.IGenericDao.GdaoFetchOne.

	- filter should be a map[string]interface{}, or it can be a string/[]byte representing map[string]interface{} in JSON, then it is unmarshalled to map[string]interface{}
	- see MongoDB query selector (https://docs.mongodb.com/manual/reference/operator/query/#query-selectors)
*/
func (dao *GenericDaoMongo) GdaoFetchOne(collectionName string, filter interface{}) (godal.IGenericBo, error) {
	return dao.GdaoFetchOneWithContext(nil, collectionName, filter)
}

/*
GdaoFetchOneWithContext is extended-implementation of godal.IGenericDao.GdaoFetchOne.

	- ctx: can be used to pass a transaction down to the operation
	- filter should be a map[string]interface{}, or it can be a string/[]byte representing map[string]interface{} in JSON, then it is unmarshalled to map[string]interface{}
	- see MongoDB query selector (https://docs.mongodb.com/manual/reference/operator/query/#query-selectors)

Available: since v0.1.0
*/
func (dao *GenericDaoMongo) GdaoFetchOneWithContext(ctx context.Context, collectionName string, filter interface{}) (godal.IGenericBo, error) {
	if f, err := toMap(filter); err != nil {
		return nil, err
	} else {
		if ctx == nil {
			ctx, _ = dao.mongoConnect.NewContext()
		}
		dbResult := dao.MongoFetchOne(ctx, collectionName, f)
		if jsData, err := dao.mongoConnect.DecodeSingleResultRaw(dbResult); err != nil || jsData == nil {
			return nil, err
		} else {
			return dao.GetRowMapper().ToBo(collectionName, jsData)
		}
	}
}

/*
GdaoFetchMany implements godal.IGenericDao.GdaoFetchMany.

	- filter should be a map[string]interface{}, or it can be a string/[]byte representing map[string]interface{} in JSON, then it is unmarshalled to map[string]interface{}
	- nil filter means "match all"
	- see MongoDB query selector (https://docs.mongodb.com/manual/reference/operator/query/#query-selectors)
	- sorting should be a map[string]int, or it can be a string/[]byte representing map[string]int in JSON, then it is unmarshalled to map[string]int
	- see MongoDB ascending/descending sort (https://docs.mongodb.com/manual/reference/method/cursor.sort/index.html#sort-asc-desc)
*/
func (dao *GenericDaoMongo) GdaoFetchMany(collectionName string, filter interface{}, sorting interface{}, startOffset, numItems int) ([]godal.IGenericBo, error) {
	return dao.GdaoFetchManyWithContext(nil, collectionName, filter, sorting, startOffset, numItems)
}

/*
GdaoFetchManyWithContext is extended-implementation of godal.IGenericDao.GdaoFetchMany.

	- ctx: can be used to pass a transaction down to the operation
	- filter should be a map[string]interface{}, or it can be a string/[]byte representing map[string]interface{} in JSON, then it is unmarshalled to map[string]interface{}
	- nil filter means "match all"
	- see MongoDB query selector (https://docs.mongodb.com/manual/reference/operator/query/#query-selectors)
	- sorting should be a map[string]int, or it can be a string/[]byte representing map[string]int in JSON, then it is unmarshalled to map[string]int
	- see MongoDB ascending/descending sort (https://docs.mongodb.com/manual/reference/method/cursor.sort/index.html#sort-asc-desc)

Available: since v0.1.0
*/
func (dao *GenericDaoMongo) GdaoFetchManyWithContext(ctx context.Context, collectionName string, filter interface{}, sorting interface{}, startOffset, numItems int) ([]godal.IGenericBo, error) {
	f, err := toMap(filter)
	if err != nil {
		return nil, err
	}
	s, err := toSortingMap(sorting)
	if err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx, _ = dao.mongoConnect.NewContext()
	}
	cursor, err := dao.MongoFetchMany(ctx, collectionName, f, s, startOffset, numItems)
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
		} else {
			bo, e := dao.GetRowMapper().ToBo(collectionName, doc)
			if e != nil {
				resultError = e
				return false
			} else {
				resultBoList = append(resultBoList, bo)
			}
		}
		return true
	})
	return resultBoList, resultError
}

func isErrorDuplicatedKey(err error) bool {
	if err == nil {
		return false
	}
	return regexp.MustCompile(`\WE11000\W`).FindString(err.Error()) != ""
}

func (dao *GenericDaoMongo) insertIfNotExist(ctx context.Context, collectionName string, bo godal.IGenericBo) (bool, error) {
	// first fetch existing document from storage
	filter, err := toMap(dao.GdaoCreateFilter(collectionName, bo))
	if err != nil {
		return false, err
	}
	row := dao.MongoFetchOne(ctx, collectionName, filter)
	if jsData, err := dao.mongoConnect.DecodeSingleResultRaw(row); err != nil || jsData != nil {
		if err != nil {
			return false, err
		} else {
			return false, godal.GdaoErrorDuplicatedEntry
		}
	}

	// insert new document
	if doc, err := dao.GetRowMapper().ToRow(collectionName, bo); err != nil {
		return false, err
	} else if _, err = dao.MongoInsertOne(ctx, collectionName, doc); err != nil {
		return false, err
	}
	return true, nil
}

/*
WrapTransaction wraps a function inside a transaction.

	- txFunc: the function to wrap. If the function returns error, the transaction will be aborted, otherwise transaction is committed.

Available: since v0.0.4
*/
func (dao *GenericDaoMongo) WrapTransaction(ctx context.Context, txFunc func(sctx mongo.SessionContext) error) error {
	if ctx == nil {
		ctx, _ = dao.mongoConnect.NewContext()
	}
	// UseSession will close open session and pending transaction
	return dao.mongoConnect.GetMongoClient().UseSession(ctx, func(sctx mongo.SessionContext) error {
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

/*
GdaoCreate implements godal.IGenericDao.GdaoCreate.
*/
func (dao *GenericDaoMongo) GdaoCreate(collectionName string, bo godal.IGenericBo) (int, error) {
	return dao.GdaoCreateWithContext(nil, collectionName, bo)
}

/*
GdaoCreateWithContext is extended-implementation of godal.IGenericDao.GdaoCreate.

	- ctx: can be used to pass a transaction down to the operation

Available: since v0.1.0
*/
func (dao *GenericDaoMongo) GdaoCreateWithContext(ctx context.Context, collectionName string, bo godal.IGenericBo) (int, error) {
	if ctx == nil {
		ctx, _ = dao.mongoConnect.NewContext()
	}
	if dao.txModeOnWrite {
		numRows := 0
		err := dao.WrapTransaction(ctx, func(sctx mongo.SessionContext) error {
			if result, err := dao.insertIfNotExist(sctx, collectionName, bo); err != nil {
				return err
			} else if result {
				numRows = 1
			}
			return nil
		})
		if isErrorDuplicatedKey(err) {
			return 0, godal.GdaoErrorDuplicatedEntry
		}
		return numRows, err
	} else {
		if result, err := dao.insertIfNotExist(ctx, collectionName, bo); err != nil {
			if isErrorDuplicatedKey(err) {
				return 0, godal.GdaoErrorDuplicatedEntry
			}
			return 0, err
		} else if result {
			return 1, nil
		}
		return 0, nil
	}
}

/*
GdaoUpdate implements godal.IGenericDao.GdaoUpdate.
*/
func (dao *GenericDaoMongo) GdaoUpdate(collectionName string, bo godal.IGenericBo) (int, error) {
	return dao.GdaoUpdateWithContext(nil, collectionName, bo)
}

/*
GdaoUpdateWithContext is extended-implementation of godal.IGenericDao.GdaoUpdate.

	- ctx: can be used to pass a transaction down to the operation

Available: since v0.1.0
*/
func (dao *GenericDaoMongo) GdaoUpdateWithContext(ctx context.Context, collectionName string, bo godal.IGenericBo) (int, error) {
	if ctx == nil {
		ctx, _ = dao.mongoConnect.NewContext()
	}
	doc, err := dao.GetRowMapper().ToRow(collectionName, bo)
	if err != nil {
		return 0, err
	}
	filter, err := toMap(dao.GdaoCreateFilter(collectionName, bo))
	if err != nil {
		return 0, err
	}
	result := dao.MongoUpdateOne(ctx, collectionName, filter, doc)
	if _, err := result.DecodeBytes(); err == mongo.ErrNoDocuments {
		return 0, nil
	} else if isErrorDuplicatedKey(err) {
		return 0, godal.GdaoErrorDuplicatedEntry
	} else {
		return 1, err
	}
}

/*
GdaoSave implements godal.IGenericDao.GdaoSave.
*/
func (dao *GenericDaoMongo) GdaoSave(collectionName string, bo godal.IGenericBo) (int, error) {
	return dao.GdaoSaveWithContext(nil, collectionName, bo)
}

/*
GdaoSaveWithContext is extended-implementation of godal.IGenericDao.GdaoSave.

	- ctx: can be used to pass a transaction down to the operation

Available: since v0.1.0
*/
func (dao *GenericDaoMongo) GdaoSaveWithContext(ctx context.Context, collectionName string, bo godal.IGenericBo) (int, error) {
	if ctx == nil {
		ctx, _ = dao.mongoConnect.NewContext()
	}
	doc, err := dao.GetRowMapper().ToRow(collectionName, bo)
	if err != nil {
		return 0, err
	}
	filter, err := toMap(dao.GdaoCreateFilter(collectionName, bo))
	if err != nil {
		return 0, err
	}
	result := dao.MongoSaveOne(ctx, collectionName, filter, doc)
	if err = result.Err(); err == nil || err == mongo.ErrNoDocuments {
		return 1, nil
	} else {
		if isErrorDuplicatedKey(err) {
			return 0, godal.GdaoErrorDuplicatedEntry
		}
		return 1, err
	}
}
