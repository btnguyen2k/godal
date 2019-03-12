/*
MongoDB Dao example.

MongoDB Dao implementation guideline:

	- Must implement method godal.IGenericDao.GdaoCreateFilter(storageId string, bo godal.IGenericBo) interface{}
	- If application uses its own BOs instead of godal.IGenericBo, it is recommended to implement a utility method
	  to transform godal.IGenericBo to application's BO and vice versa.
*/
package main

import (
	"encoding/json"
	"fmt"
	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/godal/mongo"
	"github.com/btnguyen2k/prom"
	"go.mongodb.org/mongo-driver/bson"
)

// BoApp defines business object app
type BoApp struct {
	Id          string   `json:"id"`
	Description string   `json:"desc"`
	Tags        []string `json:"tags"`
}

func (app *BoApp) toJson() []byte {
	js, _ := json.Marshal(app)
	return js
}

// IDaoApp defines DAO APIs for apps
type IDaoApp interface {
	Delete(bo *BoApp) (bool, error)
	Create(bo *BoApp) (bool, error)
	Get(id string) (*BoApp, error)
	GetAll() ([]*BoApp, error)
	Update(bo *BoApp) (bool, error)
	Upsert(bo *BoApp) (bool, error)
}

type DaoAppMongodb struct {
	*mongo.GenericDaoMongo
	collectionName string
}

func NewDaoAppMongodb(mc *prom.MongoConnect, collectionName string) IDaoApp {
	dao := &DaoAppMongodb{collectionName: collectionName}
	dao.GenericDaoMongo = mongo.NewGenericDaoMongo(mc, godal.NewAbstractGenericDao(dao))
	return dao
}

/*----------------------------------------------------------------------*/

// toGenericBo transforms BoApp to godal.IGenericBo
func (dao *DaoAppMongodb) toGenericBo(bo *BoApp) (godal.IGenericBo, error) {
	if bo == nil {
		return nil, nil
	}
	gbo := godal.NewGenericBo()
	js, err := json.Marshal(bo)
	if err != nil {
		return nil, err
	}
	err = gbo.GboFromJson(js)
	return gbo, err
}

// toBoApp transforms godal.IGenericBo to BoApp
func (dao *DaoAppMongodb) toBoApp(gbo godal.IGenericBo) (*BoApp, error) {
	if gbo == nil {
		return nil, nil
	}
	bo := BoApp{}
	err := gbo.GboTransferViaJson(&bo)
	return &bo, err
}

/*----------------------------------------------------------------------*/
// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter.
func (dao *DaoAppMongodb) GdaoCreateFilter(storageId string, bo godal.IGenericBo) interface{} {
	id, _ := bo.GboGetAttr("id", reddo.TypeString)
	return bson.M{"id": id}
}

/*----------------------------------------------------------------------*/

// Delete deletes an application from database.
func (dao *DaoAppMongodb) Delete(bo *BoApp) (bool, error) {
	gbo, err := dao.toGenericBo(bo)
	if err != nil {
		return false, err
	}
	numRows, err := dao.GdaoDelete(dao.collectionName, gbo)
	return numRows > 0, err
}

// Get saves new application to the database
// If the application already existed in database, this function returns (false, nil)
func (dao *DaoAppMongodb) Create(bo *BoApp) (bool, error) {
	gbo, err := dao.toGenericBo(bo)
	if err != nil {
		return false, err
	}
	numRows, err := dao.GdaoCreate(dao.collectionName, gbo)
	return numRows > 0, err
}

// Get finds an application by id & fetches it from storage
func (dao *DaoAppMongodb) Get(id string) (*BoApp, error) {
	filter := bson.M{"id": id}
	gbo, err := dao.GdaoFetchOne(dao.collectionName, filter)
	if err != nil || gbo == nil {
		return nil, err
	}
	return dao.toBoApp(gbo)
}

// GetAll retrieves all available applications from storage
func (dao *DaoAppMongodb) GetAll() ([]*BoApp, error) {
	filter := bson.M{}
	rows, err := dao.GdaoFetchMany(dao.collectionName, filter, nil)
	if err != nil {
		return nil, err
	}
	var result []*BoApp
	for _, e := range rows {
		bo, err := dao.toBoApp(e)
		if err != nil {
			return nil, err
		}
		result = append(result, bo)
	}
	return result, nil
}

// Update updates data of an existing application.
// If the application does not exist in database, this function returns (false, nil)
func (dao *DaoAppMongodb) Update(bo *BoApp) (bool, error) {
	gbo, err := dao.toGenericBo(bo)
	if err != nil {
		return false, err
	}
	numRows, err := dao.GdaoUpdate(dao.collectionName, gbo)
	return numRows > 0, err
}

// Upsert updates data of an existing application or inserts a new one if not exist.
func (dao *DaoAppMongodb) Upsert(bo *BoApp) (bool, error) {
	gbo, err := dao.toGenericBo(bo)
	if err != nil {
		return false, err
	}
	numRows, err := dao.GdaoSave(dao.collectionName, gbo)
	return numRows > 0, err
}

/*----------------------------------------------------------------------*/

func main() {
	url := "mongodb://test:test@localhost:27017/test"
	db := "test"
	collection := "apps"
	mc, err := prom.NewMongoConnect(url, db, 10000)
	if err != nil {
		panic(err)
	}
	dao := NewDaoAppMongodb(mc, collection)

	{
		// cleanup existing data
		mc.GetCollection(collection).Drop(nil)
	}

	{
		// insert a document
		bo := BoApp{Id: "log", Description: "Logging application", Tags: []string{"system", "utility"}}
		fmt.Println("Creating bo:", string(bo.toJson()))
		result, err := dao.Create(&bo)
		if err != nil {
			fmt.Printf("\tError: %e\n", err)
		} else {
			fmt.Printf("\tResult: %v\n", result)
		}

		// insert another document
		bo = BoApp{Id: "login", Description: "Authentication application", Tags: []string{"system", "security"}}
		fmt.Println("Creating bo:", string(bo.toJson()))
		result, err = dao.Create(&bo)
		if err != nil {
			fmt.Printf("\tError: %e\n", err)
		} else {
			fmt.Printf("\tResult: %v\n", result)
		}

		fmt.Println("==================================================")
	}

	{
		// get an Application by id
		id := "login"
		bo, err := dao.Get(id)
		if err != nil {
			fmt.Printf("Error while fetching app [%s]: %e\n", id, err)
		} else if bo != nil {
			fmt.Printf("App [%s] info: %v\n", bo.Id, string(bo.toJson()))
		} else {
			fmt.Printf("App [%s] does not exist\n", id)
		}

		// get an Application by id
		id = "loggin"
		bo, err = dao.Get(id)
		if err != nil {
			fmt.Printf("Error while fetching app [%s]: %e\n", id, err)
		} else if bo != nil {
			fmt.Printf("App [%s] info: %v\n", bo.Id, string(bo.toJson()))
		} else {
			fmt.Printf("App [%s] does not exist\n", id)
		}

		fmt.Println("==================================================")
	}

	{
		// get all applications
		boList, err := dao.GetAll()
		if err != nil {
			fmt.Printf("Error while fetching apps: %e\n", err)
		} else {
			for _, bo := range boList {
				fmt.Printf("App [%s] info: %v\n", bo.Id, string(bo.toJson()))
			}
		}

		fmt.Println("==================================================")
	}

	{
		// get an Application by id
		id := "login"
		bo, err := dao.Get(id)
		if err != nil {
			fmt.Printf("Error while fetching app [%s]: %e\n", id, err)
		} else if bo == nil {
			fmt.Printf("App [%s] does not exist\n", id)
		} else {
			// delete an existing application
			fmt.Println("Deleting bo:", string(bo.toJson()))
			result, err := dao.Delete(bo)
			if err != nil {
				fmt.Printf("\tError: %e\n", err)
			} else {
				fmt.Printf("\tResult: %v\n", result)
			}
		}

		fmt.Println("==================================================")
	}

	{
		// upsert a document
		bo := BoApp{Id: "log", Description: "Logging application (upsert)", Tags: []string{"utility"}}
		fmt.Println("Upserting bo:", string(bo.toJson()))
		result, err := dao.Upsert(&bo)
		if err != nil {
			fmt.Printf("\tError: %e\n", err)
		} else {
			fmt.Printf("\tResult: %v\n", result)
		}

		// upsert another document
		bo = BoApp{Id: "login", Description: "Authentication application (upsert)", Tags: []string{"security"}}
		fmt.Println("Upserting bo:", string(bo.toJson()))
		result, err = dao.Create(&bo)
		if err != nil {
			fmt.Printf("\tError: %e\n", err)
		} else {
			fmt.Printf("\tResult: %v\n", result)
		}

		fmt.Println("==================================================")
	}
}
