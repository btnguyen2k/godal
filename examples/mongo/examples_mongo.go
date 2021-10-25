/*
MongoDB Dao example. Run with command:

$ go run examples_mongo.go

MongoDB Dao implementation guideline:

	- Must implement method godal.IGenericDao.GdaoCreateFilter(storageId string, bo godal.IGenericBo) godal.FilterOpt
	- If application uses its own BOs instead of godal.IGenericBo, it is recommended to implement a utility method
	  to transform godal.IGenericBo to application's BO and vice versa.
*/
package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godal/examples/common"
	"github.com/btnguyen2k/prom"

	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/godal/mongo"
)

const mongoFieldId = "_id"

// DaoAppMongodb is MongoDB-implementation of IDaoApp.
type DaoAppMongodb struct {
	*mongo.GenericDaoMongo
	collectionName string
}

// NewDaoAppMongodb is helper function to create MongoDB-implementation of IDaoApp.
func NewDaoAppMongodb(mc *prom.MongoConnect, collectionName string) common.IDaoApp {
	dao := &DaoAppMongodb{collectionName: collectionName}
	dao.GenericDaoMongo = mongo.NewGenericDaoMongo(mc, godal.NewAbstractGenericDao(dao))
	return dao
}

// toGenericBo transforms BoApp to godal.IGenericBo
func (dao *DaoAppMongodb) toGenericBo(bo *common.BoApp) (godal.IGenericBo, error) {
	if bo == nil {
		return nil, nil
	}
	gbo := bo.ToGenericBo()
	gbo.GboSetAttr("id", nil)
	gbo.GboSetAttr(mongoFieldId, bo.Id)
	return gbo, nil
}

// toBoApp transforms godal.IGenericBo to BoApp
func (dao *DaoAppMongodb) toBoApp(gbo godal.IGenericBo) (*common.BoApp, error) {
	if gbo == nil {
		return nil, nil
	}
	bo := common.BoApp{}
	bo.FromGenericBo(gbo)
	bo.Id = gbo.GboGetAttrUnsafe(mongoFieldId, reddo.TypeString).(string)
	return &bo, nil
}

/*----------------------------------------------------------------------*/

// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter.
func (dao *DaoAppMongodb) GdaoCreateFilter(storageId string, bo godal.IGenericBo) godal.FilterOpt {
	id, _ := bo.GboGetAttr(mongoFieldId, reddo.TypeString)
	return godal.MakeFilter(map[string]interface{}{mongoFieldId: id})
}

// EnableTxMode implements IDaoApp.EnableTxMode
func (dao *DaoAppMongodb) EnableTxMode(txMode bool) {
	dao.SetTxModeOnWrite(txMode)
}

// Delete implements IDaoApp.Delete
func (dao *DaoAppMongodb) Delete(bo *common.BoApp) (bool, error) {
	gbo, err := dao.toGenericBo(bo)
	if err != nil {
		return false, err
	}
	numRows, err := dao.GdaoDelete(dao.collectionName, gbo)
	return numRows > 0, err
}

// Create implements IDaoApp.Create
func (dao *DaoAppMongodb) Create(bo *common.BoApp) (bool, error) {
	gbo, err := dao.toGenericBo(bo)
	if err != nil {
		return false, err
	}
	numRows, err := dao.GdaoCreate(dao.collectionName, gbo)
	return numRows > 0, err
}

// Get implements IDaoApp.Get
func (dao *DaoAppMongodb) Get(id string) (*common.BoApp, error) {
	filter := godal.MakeFilter(map[string]interface{}{mongoFieldId: id})
	gbo, err := dao.GdaoFetchOne(dao.collectionName, filter)
	if err != nil || gbo == nil {
		return nil, err
	}
	return dao.toBoApp(gbo)
}

// GetAll implements IDaoApp.GetAll
func (dao *DaoAppMongodb) GetAll() ([]*common.BoApp, error) {
	sorting := (&godal.SortingField{FieldName: "val_time"}).ToSortingOpt()
	rows, err := dao.GdaoFetchMany(dao.collectionName, nil, sorting, 0, 0)
	if err != nil {
		return nil, err
	}
	var result []*common.BoApp
	for _, e := range rows {
		bo, err := dao.toBoApp(e)
		if err != nil {
			return nil, err
		}
		result = append(result, bo)
	}
	return result, nil
}

// Update implements IDaoApp.Update
func (dao *DaoAppMongodb) Update(bo *common.BoApp) (bool, error) {
	gbo, err := dao.toGenericBo(bo)
	if err != nil {
		return false, err
	}
	numRows, err := dao.GdaoUpdate(dao.collectionName, gbo)
	return numRows > 0, err
}

// Upsert implements IDaoApp.Upsert
func (dao *DaoAppMongodb) Upsert(bo *common.BoApp) (bool, error) {
	gbo, err := dao.toGenericBo(bo)
	if err != nil {
		return false, err
	}
	numRows, err := dao.GdaoSave(dao.collectionName, gbo)
	return numRows > 0, err
}

// GetN demonstrates fetching documents with paging (result is sorted by "val_time")
func (dao *DaoAppMongodb) GetN(startOffset, numRows int) ([]*common.BoApp, error) {
	sorting := (&godal.SortingField{FieldName: "id"}).ToSortingOpt()
	rows, err := dao.GdaoFetchMany(dao.collectionName, nil, sorting, startOffset, numRows)
	if err != nil {
		return nil, err
	}
	var result []*common.BoApp
	for _, e := range rows {
		bo, err := dao.toBoApp(e)
		if err != nil {
			return nil, err
		}
		result = append(result, bo)
	}
	return result, nil
}

/*----------------------------------------------------------------------*/
func createMongoConnect() *prom.MongoConnect {
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
	mc.CreateCollection("__godal")

	return mc
}

func initDataMongo(mc *prom.MongoConnect, collection string) {
	err := mc.GetCollection(collection).Drop(nil)
	if err != nil {
		panic(err)
	}
	_, err = mc.CreateCollection(collection)
	if err != nil {
		panic(err)
	}
}

func demoMongoInsertDocs(loc *time.Location, collection string, txMode bool) {
	mc := createMongoConnect()
	dao := NewDaoAppMongodb(mc, collection)
	initDataMongo(mc, collection)
	dao.EnableTxMode(txMode)

	fmt.Printf("-== Insert documents to collection (TxMode=%v) ==-\n", txMode)

	// insert a document
	t := time.Unix(int64(rand.Int31()), rand.Int63()%1000000000).In(loc)
	bo := common.BoApp{
		Id:            "log",
		Description:   t.String(),
		ValBool:       rand.Int31()%2 == 0,
		ValInt:        rand.Int(),
		ValFloat:      rand.Float64(),
		ValString:     fmt.Sprintf("Logging application (TxMode=%v)", txMode),
		ValTime:       t,
		ValTimeZ:      t,
		ValDate:       t,
		ValDateZ:      t,
		ValDatetime:   t,
		ValDatetimeZ:  t,
		ValTimestamp:  t,
		ValTimestampZ: t,
		ValList:       []interface{}{true, 0, "1", 2.3, "system", "utility"},
		ValMap:        map[string]interface{}{"tags": []string{"system", "utility"}, "age": 103, "active": true},
	}
	fmt.Println("\tCreating bo:", string(bo.ToJson()))
	result, err := dao.Create(&bo)
	if err != nil {
		fmt.Printf("\t\tError: %s\n", err)
	} else {
		fmt.Printf("\t\tResult: %v\n", result)
	}

	// insert another document
	t = time.Unix(int64(rand.Int31()), rand.Int63()%1000000000).In(loc)
	bo = common.BoApp{
		Id:            "login",
		Description:   t.String(),
		ValBool:       rand.Int31()%2 == 0,
		ValInt:        rand.Int(),
		ValFloat:      rand.Float64(),
		ValString:     fmt.Sprintf("Authentication application (TxMode=%v)", txMode),
		ValTime:       t,
		ValTimeZ:      t,
		ValDate:       t,
		ValDateZ:      t,
		ValDatetime:   t,
		ValDatetimeZ:  t,
		ValTimestamp:  t,
		ValTimestampZ: t,
		ValList:       []interface{}{false, 9.8, "7", 6, "system", "security"},
		ValMap:        map[string]interface{}{"tags": []string{"system", "security"}, "age": 81, "active": false},
	}
	fmt.Println("\tCreating bo:", string(bo.ToJson()))
	result, err = dao.Create(&bo)
	if err != nil {
		fmt.Printf("\t\tError: %s\n", err)
	} else {
		fmt.Printf("\t\tResult: %v\n", result)
	}

	// insert another document with duplicated id
	bo = common.BoApp{Id: "login", ValString: "Authentication application (TxMode=true)(again)", ValList: []interface{}{"duplicated"}}
	fmt.Println("\tCreating bo:", string(bo.ToJson()))
	result, err = dao.Create(&bo)
	if err != nil {
		fmt.Printf("\t\tError: %s\n", err)
	} else {
		fmt.Printf("\t\tResult: %v\n", result)
	}

	fmt.Println(common.SEP)
}

func demoMongoFetchDocById(collection string, docIds ...string) {
	mc := createMongoConnect()
	dao := NewDaoAppMongodb(mc, collection)
	dao.EnableTxMode(false)

	fmt.Printf("-== Fetch documents by id ==-\n")
	for _, id := range docIds {
		bo, err := dao.Get(id)
		if err != nil {
			fmt.Printf("\tError while fetching app [%s]: %s\n", id, err)
		} else if bo != nil {
			common.PrintApp(bo)
		} else {
			fmt.Printf("\tApp [%s] does not exist\n", id)
		}
	}

	fmt.Println(common.SEP)
}

func demoMongoFetchAllDocs(collection string) {
	mc := createMongoConnect()
	dao := NewDaoAppMongodb(mc, collection)
	dao.EnableTxMode(false)

	fmt.Println("-== Fetch all documents in collection ==-")
	boList, err := dao.GetAll()
	if err != nil {
		fmt.Printf("\tError while fetching apps: %s\n", err)
	} else {
		for _, bo := range boList {
			common.PrintApp(bo)
		}
	}
	fmt.Println(common.SEP)
}

func demoMongoDeleteDocs(collection string, docIds ...string) {
	mc := createMongoConnect()
	dao := NewDaoAppMongodb(mc, collection)
	dao.EnableTxMode(false)

	fmt.Println("-== Delete documents from collection ==-")
	for _, id := range docIds {
		bo, err := dao.Get(id)
		if err != nil {
			fmt.Printf("\tError while fetching app [%s]: %s\n", id, err)
		} else if bo == nil {
			fmt.Printf("\tApp [%s] does not exist, no need to delete\n", id)
		} else {
			fmt.Println("\tDeleting bo:", string(bo.ToJson()))
			result, err := dao.Delete(bo)
			if err != nil {
				fmt.Printf("\t\tError: %s\n", err)
			} else {
				fmt.Printf("\t\tResult: %v\n", result)
			}
			app, err := dao.Get(id)
			if err != nil {
				fmt.Printf("\t\tError while fetching app [%s]: %s\n", id, err)
			} else if app != nil {
				fmt.Printf("\t\tApp [%s] info: %v\n", app.Id, string(app.ToJson()))
			} else {
				fmt.Printf("\t\tApp [%s] no longer exist\n", id)
				result, err = dao.Delete(bo)
				fmt.Printf("\t\tDeleting app [%s] again: %v / %s\n", id, result, err)
			}
		}

	}
	fmt.Println(common.SEP)
}

func demoMongoUpdateDocs(loc *time.Location, collection string, docIds ...string) {
	mc := createMongoConnect()
	dao := NewDaoAppMongodb(mc, collection)
	dao.EnableTxMode(false)

	fmt.Println("-== Update documents from collection ==-")
	for _, id := range docIds {
		t := time.Unix(int64(rand.Int31()), rand.Int63()%1000000000).In(loc)
		bo, err := dao.Get(id)
		if err != nil {
			fmt.Printf("\tError while fetching app [%s]: %s\n", id, err)
		} else if bo == nil {
			fmt.Printf("\tApp [%s] does not exist\n", id)
			bo = &common.BoApp{
				Id:            id,
				Description:   t.String(),
				ValString:     "(updated)",
				ValTime:       t,
				ValTimeZ:      t,
				ValDate:       t,
				ValDateZ:      t,
				ValDatetime:   t,
				ValDatetimeZ:  t,
				ValTimestamp:  t,
				ValTimestampZ: t,
			}
		} else {
			fmt.Println("\tExisting bo:", string(bo.ToJson()))
			bo.Description = t.String()
			bo.ValString += "(updated)"
			bo.ValTime = t
			bo.ValTimeZ = t
			bo.ValDate = t
			bo.ValDateZ = t
			bo.ValDatetime = t
			bo.ValDatetimeZ = t
			bo.ValTimestamp = t
			bo.ValTimestampZ = t
		}
		fmt.Println("\t\tUpdating bo:", string(bo.ToJson()))
		result, err := dao.Update(bo)
		if err != nil {
			fmt.Printf("\t\tError while updating app [%s]: %s\n", id, err)
		} else {
			fmt.Printf("\t\tResult: %v\n", result)
			bo, err = dao.Get(id)
			if err != nil {
				fmt.Printf("\t\tError while fetching app [%s]: %s\n", id, err)
			} else if bo != nil {
				fmt.Printf("\t\tApp [%s] info: %v\n", bo.Id, string(bo.ToJson()))
			} else {
				fmt.Printf("\t\tApp [%s] does not exist\n", id)
			}
		}
	}
	fmt.Println(common.SEP)
}

func demoMongoUpsertDocs(loc *time.Location, collection string, txMode bool, docIds ...string) {
	mc := createMongoConnect()
	dao := NewDaoAppMongodb(mc, collection)
	dao.EnableTxMode(txMode)

	fmt.Printf("-== Upsert documents to collection (TxMode=%v) ==-\n", txMode)
	for _, id := range docIds {
		t := time.Unix(int64(rand.Int31()), rand.Int63()%1000000000).In(loc)
		bo, err := dao.Get(id)
		if err != nil {
			fmt.Printf("\tError while fetching app [%s]: %s\n", id, err)
		} else if bo == nil {
			fmt.Printf("\tApp [%s] does not exist\n", id)
			bo = &common.BoApp{
				Id:            id,
				Description:   t.String(),
				ValString:     fmt.Sprintf("(upsert,txmode=%v)", txMode),
				ValTime:       t,
				ValTimeZ:      t,
				ValDate:       t,
				ValDateZ:      t,
				ValDatetime:   t,
				ValDatetimeZ:  t,
				ValTimestamp:  t,
				ValTimestampZ: t,
			}
		} else {
			fmt.Println("\tExisting bo:", string(bo.ToJson()))
			bo.Description = t.String()
			bo.ValString += fmt.Sprintf("(upsert,txmode=%v)", txMode)
			bo.ValTime = t
			bo.ValTimeZ = t
			bo.ValDate = t
			bo.ValDateZ = t
			bo.ValDatetime = t
			bo.ValDatetimeZ = t
			bo.ValTimestamp = t
			bo.ValTimestampZ = t
		}
		fmt.Println("\t\tUpserting bo:", string(bo.ToJson()))
		result, err := dao.Upsert(bo)
		if err != nil {
			fmt.Printf("\t\tError while upserting app [%s]: %s\n", id, err)
		} else {
			fmt.Printf("\t\tResult: %v\n", result)
			bo, err = dao.Get(id)
			if err != nil {
				fmt.Printf("\t\tError while fetching app [%s]: %s\n", id, err)
			} else if bo != nil {
				fmt.Printf("\t\tApp [%s] info: %v\n", bo.Id, string(bo.ToJson()))
			} else {
				fmt.Printf("\t\tApp [%s] does not exist\n", id)
			}
		}
	}
	fmt.Println(common.SEP)
}

func demoMongoSelectSortingAndLimit(loc *time.Location, collection string) {
	mc := createMongoConnect()
	initDataMongo(mc, collection)
	dao := NewDaoAppMongodb(mc, collection)
	dao.EnableTxMode(false)

	fmt.Println("-== Fetch documents from collection with sorting and limit ==-")
	n := 100
	fmt.Printf("\tInserting %d docs...\n", n)
	for i := 0; i < n; i++ {
		id := strconv.Itoa(i)
		for len(id) < 3 {
			id = "0" + id
		}
		t := time.Unix(int64(rand.Int31()), rand.Int63()%1000000000).In(loc)
		bo := common.BoApp{
			Id:            id,
			Description:   t.String(),
			ValBool:       rand.Int31()%2 == 0,
			ValInt:        rand.Int(),
			ValFloat:      rand.Float64(),
			ValString:     id + " (sorting and limit)",
			ValTime:       t,
			ValTimeZ:      t,
			ValDate:       t,
			ValDateZ:      t,
			ValDatetime:   t,
			ValDatetimeZ:  t,
			ValTimestamp:  t,
			ValTimestampZ: t,
			ValList:       []interface{}{rand.Int31()%2 == 0, i, id},
			ValMap:        map[string]interface{}{"tags": []interface{}{id, i}},
		}
		_, err := dao.Create(&bo)
		if err != nil {
			panic(err)
		}
	}
	startOffset := rand.Intn(n)
	numRows := rand.Intn(10) + 1
	fmt.Printf("\tFetching %d docs, starting from offset %d...\n", numRows, startOffset)
	boList, err := dao.GetN(startOffset, numRows)
	if err != nil {
		fmt.Printf("\t\tError while fetching apps: %s\n", err)
	} else {
		for _, bo := range boList {
			fmt.Printf("\t\tApp [%s] info: %v\n", bo.Id, string(bo.ToJson()))
		}
	}
	fmt.Println(common.SEP)
}

// func demoMongoFetchDocNotExists(collection string, docId string) {
// 	mc := createMongoConnect()
// 	dao := NewDaoAppMongodb(mc, collection)
// 	initDataMongo(mc, collection)
// 	dao.EnableTxMode(false)
//
// 	fmt.Printf("-== (not-exists) ==-\n")
// 	t := time.Unix(int64(rand.Int31()), rand.Int63()%1000000000)
// 	bo := common.BoApp{
// 		Id:            "log",
// 		Description:   t.String(),
// 		ValBool:       rand.Int31()%2 == 0,
// 		ValInt:        rand.Int(),
// 		ValFloat:      rand.Float64(),
// 		ValString:     fmt.Sprintf("Logging application"),
// 		ValTime:       t,
// 		ValTimeZ:      t,
// 		ValDate:       t,
// 		ValDateZ:      t,
// 		ValDatetime:   t,
// 		ValDatetimeZ:  t,
// 		ValTimestamp:  t,
// 		ValTimestampZ: t,
// 		ValList:       []interface{}{true, 0, "1", 2.3, "system", "utility"},
// 		ValMap:        map[string]interface{}{"tags": []string{"system", "utility"}, "age": 103, "active": true},
// 	}
// 	result, err := dao.Create(&bo)
// 	// bo, err := dao.Get(docId)
// 	fmt.Println(result, err)
//
// 	fmt.Println(common.SEP)
// }

func main() {
	rand.Seed(time.Now().UnixNano())
	timeZone := strings.ReplaceAll(os.Getenv("TIMEZONE"), `"`, "")
	loc, _ := time.LoadLocation(timeZone)

	collection := "apps"
	demoMongoInsertDocs(loc, collection, true)
	demoMongoInsertDocs(loc, collection, false)
	demoMongoFetchDocById(collection, "login", "loggin")
	demoMongoFetchAllDocs(collection)
	demoMongoDeleteDocs(collection, "login", "loggin")
	demoMongoUpdateDocs(loc, collection, "log", "logging")
	demoMongoUpsertDocs(loc, collection, true, "log", "logging")
	demoMongoUpsertDocs(loc, collection, false, "log", "loggging")
	demoMongoSelectSortingAndLimit(loc, collection)
	demoMongoFetchDocById(collection, "not-exists")
	// demoMongoFetchDocNotExists(collection, "not-exists")
}
