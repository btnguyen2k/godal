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

// convenient function to create UserDaoMongo instance
func createUserDaoMongo(mgc *prom.MongoConnect, collectionName string, rowMapper godal.IRowMapper) IUserDao {
	dao := &UserDaoMongo{collectionName: collectionName}
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

// UserBo is a custom BO that encapsulates an application user object
type UserBo struct {
	Id       string `json:"_id"`
	Username string `json:"username"`
	Name     string `json:"name"`
	Version  int    `json:"version"`
	Actived  bool   `json:"actived"`
}

// IUserDao defines interface to access user storage
type IUserDao interface {
	Create(bo *UserBo) (bool, error)
	Get(id string) (*UserBo, error)
	Update(bo *UserBo) (bool, error)
	Save(bo *UserBo) (bool, error)
	Delete(bo *UserBo) (bool, error)
}

// UserDaoMongo is MongoDB implementation of IUserDao
type UserDaoMongo struct {
	*mongo.GenericDaoMongo
	collectionName string
}

// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter
func (dao *UserDaoMongo) GdaoCreateFilter(_ string, bo godal.IGenericBo) interface{} {
	return map[string]interface{}{
		fieldUserId: bo.GboGetAttrUnsafe(fieldUserId, reddo.TypeString),
	}
}

// toGbo converts a UserBo to godal.IGenericBo
// (*) it is recommended that DAO provides method to convert BO to IGenericBo
func (dao *UserDaoMongo) toGbo(bo *UserBo) godal.IGenericBo {
	if bo == nil {
		return nil
	}
	gbo := godal.NewGenericBo()

	gbo.GboSetAttr(fieldUserId, bo.Id)
	gbo.GboSetAttr(fieldUserUsername, bo.Username)
	gbo.GboSetAttr(fieldUserName, bo.Name)
	gbo.GboSetAttr(fieldUserVersion, bo.Version)
	gbo.GboSetAttr(fieldUserActived, bo.Actived)

	// another way
	// js, _ := json.Marshal(bo)
	// gbo.GboFromJson(js)

	return gbo
}

// toBo converts a godal.IGenericBo to UserBo
// (*) it is recommended that DAO provides method to convert IGenericBo to BO
func (dao *UserDaoMongo) toBo(gbo godal.IGenericBo) *UserBo {
	if gbo == nil {
		return nil
	}
	bo := UserBo{}

	bo.Id = gbo.GboGetAttrUnsafe(fieldUserId, reddo.TypeString).(string)             // assume field "_id" is not nil
	bo.Username = gbo.GboGetAttrUnsafe(fieldUserUsername, reddo.TypeString).(string) // assume field "username" is not nil
	bo.Name = gbo.GboGetAttrUnsafe(fieldUserName, reddo.TypeString).(string)         // assume field "name" is not nil
	bo.Version = int(gbo.GboGetAttrUnsafe(fieldUserVersion, reddo.TypeInt).(int64))  // assume field "version" is not nil
	bo.Actived = gbo.GboGetAttrUnsafe(fieldUserActived, reddo.TypeBool).(bool)       // assume field "actived" is not nil

	// another way
	// if err := json.Unmarshal(gbo.GboToJsonUnsafe(), &bo); err != nil {
	// 	panic(err)
	// }

	return &bo
}

// Create implements IUserDao.Create
func (dao *UserDaoMongo) Create(bo *UserBo) (bool, error) {
	numRows, err := dao.GdaoCreate(dao.collectionName, dao.toGbo(bo))
	return numRows > 0, err
}

// Get implements IUserDao.Get
func (dao *UserDaoMongo) Get(id string) (*UserBo, error) {
	filterGbo := godal.NewGenericBo()
	filterGbo.GboSetAttr(fieldUserId, id)
	gbo, err := dao.GdaoFetchOne(dao.collectionName, dao.GdaoCreateFilter(dao.collectionName, filterGbo))
	return dao.toBo(gbo), err
}

// Update implements IUserDao.Update
func (dao *UserDaoMongo) Update(bo *UserBo) (bool, error) {
	numRows, err := dao.GdaoUpdate(dao.collectionName, dao.toGbo(bo))
	return numRows > 0, err
}

// Save implements IUserDao.Save
func (dao *UserDaoMongo) Save(bo *UserBo) (bool, error) {
	numRows, err := dao.GdaoSave(dao.collectionName, dao.toGbo(bo))
	return numRows > 0, err
}

// Delete implements IUserDao.Delete
func (dao *UserDaoMongo) Delete(bo *UserBo) (bool, error) {
	numRows, err := dao.GdaoDelete(dao.collectionName, dao.toGbo(bo))
	return numRows > 0, err
}

func main() {
	// create new prom.MongoConnect
	mgc := createMongoConnect("mongodb://test:test@localhost:27017/test", "test")

	rowMapper := mongo.GenericRowMapperMongoInstance

	// create new UserDaoMongo
	daoUser := createUserDaoMongo(mgc, collectionUser, rowMapper)

	bo := &UserBo{
		Id:       "1",
		Username: "btnguyen2k",
		Name:     "Thành Nguyễn",
		Version:  int(time.Now().Unix()),
		Actived:  true,
	}

	{
		// CREATE
		_, err := daoUser.Create(bo)
		fmt.Printf("Creating user [%v]...: %e\n", bo, err)
	}

	{
		// READ
		user, err := daoUser.Get("1")
		fmt.Printf("Fetched user [%v]: %e\n", user, err)
	}

	{
		// UPDATE
		bo.Version = 103
		bo.Actived = false
		_, err := daoUser.Update(bo)
		fmt.Printf("Updated user [%v]: %e\n", bo, err)

		bo.Id = "1"
		bo.Version = 301
		bo.Username = "thanhn"
		_, err = daoUser.Save(bo)
		fmt.Printf("Saved user [%v]: %e\n", bo, err)
	}

	{
		// DELETE
		_, err := daoUser.Delete(bo)
		fmt.Printf("Deleted user [%v]: %e\n", bo, err)
	}
}
