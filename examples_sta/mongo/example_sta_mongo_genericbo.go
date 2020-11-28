package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/prom"

	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/godal/mongo"
)

// convenient function to create prom.MongoConnect instance
func createMongoConnectGeneric() *prom.MongoConnect {
	mongoUrl := strings.ReplaceAll(os.Getenv("MONGO_URL"), `"`, "")
	mongoDb := strings.ReplaceAll(os.Getenv("MONGO_DB"), `"`, "")
	if mongoUrl == "" || mongoDb == "" {
		panic("Please define env MONGO_URL, MONGO_DB")
	}
	mc, err := prom.NewMongoConnect(mongoUrl, mongoDb, 10000)
	if err != nil {
		panic(err)
	}

	// HACK to force database creation
	mc.CreateCollection("__prom")

	return mc
}

// convenient function to create MyGenericDaoMongo instance
func createMyGenericDaoMongo(mgc *prom.MongoConnect, rowMapper godal.IRowMapper) godal.IGenericDao {
	dao := &MyGenericDaoMongo{}
	dao.GenericDaoMongo = mongo.NewGenericDaoMongo(mgc, godal.NewAbstractGenericDao(dao))
	dao.SetRowMapper(rowMapper)
	return dao
}

const (
	collectionUserGeneric    = "test_user"
	fieldUserIdGeneric       = "_id"
	fieldUserUsernameGeneric = "username"
	fieldUserNameGeneric     = "name"
	fieldUserVersionGeneric  = "version"
	fieldUserActivedGeneric  = "actived"
)

type MyGenericDaoMongo struct {
	*mongo.GenericDaoMongo
}

// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter
func (dao *MyGenericDaoMongo) GdaoCreateFilter(collectionName string, bo godal.IGenericBo) interface{} {
	if collectionName == collectionUserGeneric {
		// should match row id or unique index
		return map[string]interface{}{
			fieldUserIdGeneric: bo.GboGetAttrUnsafe(fieldUserIdGeneric, reddo.TypeString),
		}
	}

	// id/unique index filtering for other collections
	// ...

	return nil
}

func main() {
	mc := createMongoConnectGeneric()
	mc.GetCollection(collectionUserGeneric).Drop(nil)

	rowMapper := mongo.GenericRowMapperMongoInstance

	// create new MyGenericDaoMongo
	myDao := createMyGenericDaoMongo(mc, rowMapper)

	bo := godal.NewGenericBo()
	bo.GboSetAttr(fieldUserIdGeneric, "1")
	bo.GboSetAttr(fieldUserUsernameGeneric, "btnguyen2k")
	bo.GboSetAttr(fieldUserNameGeneric, "Nguyễn Bá Thành")
	bo.GboSetAttr(fieldUserVersionGeneric, time.Now().Unix())
	bo.GboSetAttr(fieldUserActivedGeneric, true)

	{
		// CREATE
		_, err := myDao.GdaoCreate(collectionUserGeneric, bo)
		fmt.Printf("Creating user [%s]...: %e\n", bo.GboToJsonUnsafe(), err)
	}

	{
		// READ
		filterBo := godal.NewGenericBo()
		filterBo.GboSetAttr(fieldUserIdGeneric, "1")
		myBo, err := myDao.GdaoFetchOne(collectionUserGeneric, myDao.GdaoCreateFilter(collectionUserGeneric, filterBo))
		fmt.Printf("Fetched user [%s]: %e\n", myBo.GboToJsonUnsafe(), err)
	}

	{
		// UPDATE
		bo.GboSetAttr(fieldUserVersionGeneric, nil)
		bo.GboSetAttr("new_field", "a value")
		bo.GboSetAttr(fieldUserActivedGeneric, false)
		_, err := myDao.GdaoUpdate(collectionUserGeneric, bo)
		fmt.Printf("Updated user [%s]: %e\n", bo.GboToJsonUnsafe(), err)

		_, err = myDao.GdaoSave(collectionUserGeneric, bo)
		fmt.Printf("Saved user [%s]: %e\n", bo.GboToJsonUnsafe(), err)
	}

	{
		// DELETE
		_, err := myDao.GdaoDelete(collectionUserGeneric, bo)
		fmt.Printf("Deleted user [%s]: %e\n", bo.GboToJsonUnsafe(), err)
	}
}
