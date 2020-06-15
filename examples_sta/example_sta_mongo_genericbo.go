package main

import (
	"fmt"
	"time"

	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/prom"

	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/godal/mongo"
)

// convenient function to create prom.MongoConnect instance
func createMongoConnect(url, db string) *prom.MongoConnect {
	timeoutMs := 10000
	mgc, err := prom.NewMongoConnect(url, db, timeoutMs)
	if err != nil {
		panic(err)
	}
	return mgc
}

// convenient function to create MyGenericDaoMongo instance
func createMyGenericDaoMongo(mgc *prom.MongoConnect, rowMapper godal.IRowMapper) godal.IGenericDao {
	dao := &MyGenericDaoMongo{}
	dao.GenericDaoMongo = mongo.NewGenericDaoMongo(mgc, godal.NewAbstractGenericDao(dao))
	dao.SetRowMapper(rowMapper)
	return dao
}

const (
	collectionUser    = "test_user"
	fieldUserId       = "_id"
	fieldUserUsername = "username"
	fieldUserName     = "name"
	fieldUserVersion  = "version"
	fieldUserActived  = "actived"
)

type MyGenericDaoMongo struct {
	*mongo.GenericDaoMongo
}

// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter
func (dao *MyGenericDaoMongo) GdaoCreateFilter(collectionName string, bo godal.IGenericBo) interface{} {
	if collectionName == collectionUser {
		// should match row id or unique index
		return map[string]interface{}{
			fieldUserId: bo.GboGetAttrUnsafe(fieldUserId, reddo.TypeString),
		}
	}

	// id/unique index filtering for other collections
	// ...

	return nil
}

func main() {
	// create new prom.MongoConnect
	mgc := createMongoConnect("mongodb://test:test@localhost:27017/test", "test")

	rowMapper := mongo.GenericRowMapperMongoInstance

	// create new MyGenericDaoMongo
	myDao := createMyGenericDaoMongo(mgc, rowMapper)

	bo := godal.NewGenericBo()
	bo.GboSetAttr(fieldUserId, "1")
	bo.GboSetAttr(fieldUserUsername, "btnguyen2k")
	bo.GboSetAttr(fieldUserName, "Nguyễn Bá Thành")
	bo.GboSetAttr(fieldUserVersion, time.Now().Unix())
	bo.GboSetAttr(fieldUserActived, true)

	{
		// CREATE
		_, err := myDao.GdaoCreate(collectionUser, bo)
		fmt.Printf("Creating user [%s]...: %e\n", bo.GboToJsonUnsafe(), err)
	}

	{
		// READ
		filterBo := godal.NewGenericBo()
		filterBo.GboSetAttr(fieldUserId, "1")
		myBo, err := myDao.GdaoFetchOne(collectionUser, myDao.GdaoCreateFilter(collectionUser, filterBo))
		fmt.Printf("Fetched user [%s]: %e\n", myBo.GboToJsonUnsafe(), err)
	}

	{
		// UPDATE
		bo.GboSetAttr(fieldUserVersion, nil)
		bo.GboSetAttr("new_field", "a value")
		bo.GboSetAttr(fieldUserActived, false)
		_, err := myDao.GdaoUpdate(collectionUser, bo)
		fmt.Printf("Updated user [%s]: %e\n", bo.GboToJsonUnsafe(), err)

		_, err = myDao.GdaoSave(collectionUser, bo)
		fmt.Printf("Saved user [%s]: %e\n", bo.GboToJsonUnsafe(), err)
	}

	{
		// DELETE
		_, err := myDao.GdaoDelete(collectionUser, bo)
		fmt.Printf("Deleted user [%s]: %e\n", bo.GboToJsonUnsafe(), err)
	}
}
