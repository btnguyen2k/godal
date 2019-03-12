package mongo

import (
	"context"
	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/prom"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

/*
NewGenericDaoMongo constructs a new MongoDB implementation of 'godal.IGenericDao'
*/
func NewGenericDaoMongo(mongoConnect *prom.MongoConnect, agdao *godal.AbstractGenericDao) *GenericDaoMongo {
	return &GenericDaoMongo{AbstractGenericDao: agdao, mongoConnect: mongoConnect}
}

/*
GenericDaoMongo is MongoDB implementation of godal.IGenericDao.

Function implementations (n = No, y = Yes, i = inherited):

	(n) GdaoCreateFilter(storageId string, bo godal.IGenericBo) interface{}
	(i) GdaoDelete(storageId string, bo godal.IGenericBo) (int, error)
	(y) GdaoDeleteMany(storageId string, filter interface{}) (int, error)
	(y) GdaoFetchOne(storageId string, filter interface{}) (godal.IGenericBo, error)
	(y) GdaoFetchMany(storageId string, filter interface{}) ([]godal.IGenericBo, error)
	(y) GdaoCreate(storageId string, bo godal.IGenericBo) (int, error)
	(y) GdaoUpdate(storageId string, bo godal.IGenericBo) (int, error)
	(y) GdaoSave(storageId string, bo godal.IGenericBo) (int, error)
*/
type GenericDaoMongo struct {
	*godal.AbstractGenericDao
	mongoConnect *prom.MongoConnect
	txMode       bool
}

/*
GetMongoConnect returns the '*prom.MongoConnect' instance attached to this DAO.
*/
func (dao *GenericDaoMongo) GetMongoConnect() *prom.MongoConnect {
	return dao.mongoConnect
}

/*
GetTransactionMode returns 'true' if transaction mode is enabled, 'false' otherwise.
*/
func (dao *GenericDaoMongo) GetTransactionMode() bool {
	return dao.txMode
}

/*
SetTransactionMode enables/disables transaction mode.
*/
func (dao *GenericDaoMongo) SetTransactionMode(enabled bool) *GenericDaoMongo {
	dao.txMode = enabled
	return dao
}

/*
GetMongoCollection returns the MongoDB collection object specified by 'collectionName'.
*/
func (dao *GenericDaoMongo) GetMongoCollection(collectionName string, opts ...*options.CollectionOptions) *mongo.Collection {
	return dao.mongoConnect.GetCollection(collectionName, opts...)
}

/*
MongoDeleteMany performs a MongoDB's delete-many command on the specified collection.
*/
func (dao *GenericDaoMongo) MongoDeleteMany(ctx context.Context, collectionName string, filter interface{}) (*mongo.DeleteResult, error) {
	return dao.GetMongoCollection(collectionName).DeleteMany(ctx, filter)
}

/*
MongoFetchOne performs a MongoDB's find-one command on the specified collection.
*/
func (dao *GenericDaoMongo) MongoFetchOne(ctx context.Context, collectionName string, filter interface{}) *mongo.SingleResult {
	return dao.GetMongoCollection(collectionName).FindOne(ctx, filter)
}

/*
MongoFetchMany performs a MongoDB's find command on the specified collection.
*/
func (dao *GenericDaoMongo) MongoFetchMany(ctx context.Context, collectionName string, filter interface{}) (*mongo.Cursor, error) {
	return dao.GetMongoCollection(collectionName).Find(ctx, filter)
}

/*
MongoInsertOne performs a MongoDB's insert-one command on the specified collection.
*/
func (dao *GenericDaoMongo) MongoInsertOne(ctx context.Context, collectionName string, doc interface{}) (*mongo.InsertOneResult, error) {
	return dao.GetMongoCollection(collectionName).InsertOne(ctx, doc)
}

/*
MongoUpdateOne performs a MongoDB's find-one-and-replace command with 'upsert=false' on the specified collection.
*/
func (dao *GenericDaoMongo) MongoUpdateOne(ctx context.Context, collectionName string, filter interface{}, doc interface{}) *mongo.SingleResult {
	upsert := false
	opt := options.FindOneAndReplaceOptions{Upsert: &upsert}
	return dao.GetMongoCollection(collectionName).FindOneAndReplace(ctx, filter, doc, &opt)
}

/*
MongoSaveOne performs a MongoDB's find-one-and-replace command with 'upsert=true' on the specified collection.
*/
func (dao *GenericDaoMongo) MongoSaveOne(ctx context.Context, collectionName string, filter interface{}, doc interface{}) *mongo.SingleResult {
	upsert := true
	opt := options.FindOneAndReplaceOptions{Upsert: &upsert}
	return dao.GetMongoCollection(collectionName).FindOneAndReplace(ctx, filter, doc, &opt)

}

/*----------------------------------------------------------------------*/
/*
GdaoDeleteMany implements godal.IGenericDao.GdaoDeleteMany.
*/
func (dao *GenericDaoMongo) GdaoDeleteMany(storageId string, filter interface{}) (int, error) {
	ctx, _ := dao.mongoConnect.NewBackgroundContext()
	dbResult, err := dao.MongoDeleteMany(ctx, storageId, filter)
	if err != nil {
		return 0, err
	}
	return int(dbResult.DeletedCount), nil
}

/*
GdaoFetchOne implements godal.IGenericDao.GdaoFetchOne.
*/
func (dao *GenericDaoMongo) GdaoFetchOne(storageId string, filter interface{}) (godal.IGenericBo, error) {
	ctx, _ := dao.mongoConnect.NewBackgroundContext()
	dbResult := dao.MongoFetchOne(ctx, storageId, filter)
	jsData, err := dao.mongoConnect.DecodeSingleResultRaw(dbResult)
	if err != nil || jsData == nil {
		return nil, err
	}
	bo := godal.NewGenericBo()
	err = bo.GboFromJson([]byte(jsData))
	return bo, err
}

/*
GdaoFetchMany implements godal.IGenericDao.GdaoFetchMany.
*/
func (dao *GenericDaoMongo) GdaoFetchMany(storageId string, filter interface{}, ordering interface{}) ([]godal.IGenericBo, error) {
	ctx, _ := dao.mongoConnect.NewBackgroundContext()
	cursor, err := dao.MongoFetchMany(ctx, storageId, filter)
	if cursor != nil {
		defer cursor.Close(ctx)
	}
	if err != nil {
		return nil, err
	}

	var resultBoList []godal.IGenericBo
	var resultError error = nil
	dao.mongoConnect.DecodeResultCallbackRaw(ctx, cursor, func(docNum int, doc []byte, err error) bool {
		if err != nil {
			resultError = err
			return false
		} else {
			bo := godal.NewGenericBo()
			e := bo.GboFromJson(doc)
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

func (dao *GenericDaoMongo) insertIfNotExist(ctx context.Context, storageId string, bo godal.IGenericBo) (bool, error) {
	// first fetch existing document from storage
	filter := dao.GdaoCreateFilter(storageId, bo)
	row := dao.MongoFetchOne(ctx, storageId, filter)
	jsData, err := dao.mongoConnect.DecodeSingleResultRaw(row)
	if err != nil || jsData != nil {
		// error or document already existed
		return false, err
	}

	// insert new document
	var doc bson.M
	err = bo.GboTransferViaJson(&doc)
	if err != nil {
		return false, err
	}
	_, err = dao.MongoInsertOne(ctx, storageId, doc)
	if err != nil {
		return false, err
	}
	return true, nil
}

/*
GdaoCreate implements godal.IGenericDao.GdaoCreate.
*/
func (dao *GenericDaoMongo) GdaoCreate(storageId string, bo godal.IGenericBo) (int, error) {
	ctx, _ := dao.mongoConnect.NewBackgroundContext()
	if dao.txMode {
		numRows := 0
		err := dao.mongoConnect.GetMongoClient().UseSession(ctx, func(sctx mongo.SessionContext) error {
			err := sctx.StartTransaction(options.Transaction().
				SetReadConcern(readconcern.Snapshot()).
				SetWriteConcern(writeconcern.New(writeconcern.WMajority())))
			if err != nil {
				return err
			}
			result, err := dao.insertIfNotExist(sctx, storageId, bo)
			if err != nil {
				return err
			}
			if result {
				numRows = 1
			}
			return sctx.CommitTransaction(sctx)
		})
		return numRows, err
	} else {
		result, err := dao.insertIfNotExist(ctx, storageId, bo)
		if err != nil {
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
func (dao *GenericDaoMongo) GdaoUpdate(storageId string, bo godal.IGenericBo) (int, error) {
	ctx, _ := dao.mongoConnect.NewBackgroundContext()
	var doc bson.M
	err := bo.GboTransferViaJson(&doc)
	if err != nil {
		return 0, err
	}
	filter := dao.GdaoCreateFilter(storageId, bo)
	result := dao.MongoUpdateOne(ctx, storageId, filter, doc)
	return 1, result.Err()
}

/*
GdaoSave implements godal.IGenericDao.GdaoSave.
*/
func (dao *GenericDaoMongo) GdaoSave(storageId string, bo godal.IGenericBo) (int, error) {
	ctx, _ := dao.mongoConnect.NewBackgroundContext()
	var doc bson.M
	err := bo.GboTransferViaJson(&doc)
	if err != nil {
		return 0, err
	}
	filter := dao.GdaoCreateFilter(storageId, bo)
	result := dao.MongoSaveOne(ctx, storageId, filter, doc)
	return 1, result.Err()
}
