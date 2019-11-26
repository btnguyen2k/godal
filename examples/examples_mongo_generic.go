/*
Generic MongoDB Dao example. Run with command:

$ go run examples_mongo_generic.go
*/
package main

import (
	"fmt"
	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/godal/mongo"
	"github.com/btnguyen2k/prom"
	"math/rand"
	"strconv"
	"time"
)

const (
	timeZone = "Asia/Ho_Chi_Minh"
	sep      = "================================================================================"
	fieldId  = "_id"
)

func createMongoConnect() *prom.MongoConnect {
	url := "mongodb://test:test@localhost:27017/test"
	db := "test"
	mc, err := prom.NewMongoConnect(url, db, 10000)
	if err != nil {
		panic(err)
	}
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

type myGenericDaoMongo struct {
	*mongo.GenericDaoMongo
}

// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter.
func (dao *myGenericDaoMongo) GdaoCreateFilter(storageId string, bo godal.IGenericBo) interface{} {
	id := bo.GboGetAttrUnsafe(fieldId, reddo.TypeString)
	return map[string]interface{}{fieldId: id}
}

func newGenericDaoMongo(mc *prom.MongoConnect, txMode bool) godal.IGenericDao {
	dao := &myGenericDaoMongo{}
	dao.GenericDaoMongo = mongo.NewGenericDaoMongo(mc, godal.NewAbstractGenericDao(dao))
	dao.SetTxModeOnWrite(txMode)
	return dao
}

func demoMongoInsertDocs(loc *time.Location, collection string, txMode bool) {
	mc := createMongoConnect()
	initDataMongo(mc, collection)
	dao := newGenericDaoMongo(mc, txMode)

	fmt.Printf("-== Insert documents to collection (TxMode=%v) ==-\n", txMode)

	// insert a document
	t := time.Unix(int64(rand.Int31()), rand.Int63()%1000000000).In(loc)
	bo := godal.NewGenericBo()
	bo.GboSetAttr(fieldId, "log")
	bo.GboSetAttr("desc", t.String())
	bo.GboSetAttr("val_bool", rand.Int31()%2 == 0)
	bo.GboSetAttr("val_int", rand.Int())
	bo.GboSetAttr("val_float", rand.Float64())
	bo.GboSetAttr("val_string", fmt.Sprintf("Logging application (TxMode=%v)", txMode))
	bo.GboSetAttr("val_time", t)
	bo.GboSetAttr("val_list", []interface{}{true, 0, "1", 2.3, "system", "utility"})
	bo.GboSetAttr("val_map", map[string]interface{}{"tags": []string{"system", "utility"}, "age": 103, "active": true})
	fmt.Println("\tCreating bo:", string(bo.GboToJsonUnsafe()))
	result, err := dao.GdaoCreate(collection, bo)
	if err != nil {
		fmt.Printf("\t\tError: %s\n", err)
	} else {
		fmt.Printf("\t\tResult: %v\n", result)
	}

	// insert another document
	t = time.Unix(int64(rand.Int31()), rand.Int63()%1000000000).In(loc)
	bo = godal.NewGenericBo()
	bo.GboSetAttr(fieldId, "login")
	bo.GboSetAttr("desc", t.String())
	bo.GboSetAttr("val_bool", rand.Int31()%2 == 0)
	bo.GboSetAttr("val_int", rand.Int())
	bo.GboSetAttr("val_float", rand.Float64())
	bo.GboSetAttr("val_string", fmt.Sprintf("Authentication application (TxMode=%v)", txMode))
	bo.GboSetAttr("val_time", t)
	bo.GboSetAttr("val_list", []interface{}{false, 9.8, "7", 6, "system", "security"})
	bo.GboSetAttr("val_map", map[string]interface{}{"tags": []string{"system", "security"}, "age": 81, "active": false})
	fmt.Println("\tCreating bo:", string(bo.GboToJsonUnsafe()))
	result, err = dao.GdaoCreate(collection, bo)
	if err != nil {
		fmt.Printf("\t\tError: %s\n", err)
	} else {
		fmt.Printf("\t\tResult: %v\n", result)
	}

	// insert another document with duplicated id
	bo = godal.NewGenericBo()
	bo.GboSetAttr(fieldId, "login")
	bo.GboSetAttr("val_string", "Authentication application (TxMode=true)(again)")
	bo.GboSetAttr("val_list", []interface{}{"duplicated"})
	fmt.Println("\tCreating bo:", string(bo.GboToJsonUnsafe()))
	result, err = dao.GdaoCreate(collection, bo)
	if err != nil {
		fmt.Printf("\t\tError: %s\n", err)
	} else {
		fmt.Printf("\t\tResult: %v\n", result)
	}

	fmt.Println(sep)
}

func demoMongoFetchDocById(collection string, docIds ...string) {
	mc := createMongoConnect()
	dao := newGenericDaoMongo(mc, false)

	fmt.Printf("-== Fetch documents by id ==-\n")
	for _, id := range docIds {
		bo, err := dao.GdaoFetchOne(collection, map[string]interface{}{fieldId: id})
		if err != nil {
			fmt.Printf("\tError while fetching app [%s]: %s\n", id, err)
		} else if bo != nil {
			fmt.Println("\tFetched bo:", string(bo.GboToJsonUnsafe()))
		} else {
			fmt.Printf("\tApp [%s] does not exist\n", id)
		}
	}

	fmt.Println(sep)
}

func demoMongoFetchAllDocs(collection string) {
	mc := createMongoConnect()
	dao := newGenericDaoMongo(mc, false)

	fmt.Println("-== Fetch all documents in collection ==-")
	boList, err := dao.GdaoFetchMany(collection, nil, nil, 0, 0)
	if err != nil {
		fmt.Printf("\tError while fetching apps: %s\n", err)
	} else {
		for _, bo := range boList {
			fmt.Println("\tFetched bo:", string(bo.GboToJsonUnsafe()))
		}
	}
	fmt.Println(sep)
}

func demoMongoDeleteDocs(collection string, docIds ...string) {
	mc := createMongoConnect()
	dao := newGenericDaoMongo(mc, false)

	fmt.Println("-== Delete documents from collection ==-")
	for _, id := range docIds {
		bo, err := dao.GdaoFetchOne(collection, map[string]interface{}{fieldId: id})
		if err != nil {
			fmt.Printf("\tError while fetching app [%s]: %s\n", id, err)
		} else if bo == nil {
			fmt.Printf("\tApp [%s] does not exist, no need to delete\n", id)
		} else {
			fmt.Println("\tDeleting bo:", string(bo.GboToJsonUnsafe()))
			result, err := dao.GdaoDelete(collection, bo)
			if err != nil {
				fmt.Printf("\t\tError: %s\n", err)
			} else {
				fmt.Printf("\t\tResult: %v\n", result)
			}
			bo1, err := dao.GdaoFetchOne(collection, map[string]interface{}{fieldId: id})
			if err != nil {
				fmt.Printf("\t\tError while fetching app [%s]: %s\n", id, err)
			} else if bo1 != nil {
				fmt.Printf("\t\tApp info: %v\n", string(bo.GboToJsonUnsafe()))
			} else {
				fmt.Printf("\t\tApp [%s] no longer exist\n", id)
				result, err := dao.GdaoDelete(collection, bo)
				fmt.Printf("\t\tDeleting app [%s] again: %v / %s\n", id, result, err)
			}
		}

	}
	fmt.Println(sep)
}

func demoMongoUpdateDocs(loc *time.Location, collection string, docIds ...string) {
	mc := createMongoConnect()
	dao := newGenericDaoMongo(mc, false)

	fmt.Println("-== Update documents from collection ==-")
	for _, id := range docIds {
		t := time.Unix(int64(rand.Int31()), rand.Int63()%1000000000).In(loc)
		bo, err := dao.GdaoFetchOne(collection, map[string]interface{}{fieldId: id})
		if err != nil {
			fmt.Printf("\tError while fetching app [%s]: %s\n", id, err)
		} else if bo == nil {
			fmt.Printf("\tApp [%s] does not exist\n", id)
			bo = godal.NewGenericBo()
			bo.GboSetAttr(fieldId, id)
			bo.GboSetAttr("desc", t.String())
			bo.GboSetAttr("val_string", "(updated)")
			bo.GboSetAttr("val_time", t)
		} else {
			fmt.Println("\tExisting bo:", string(bo.GboToJsonUnsafe()))
			bo.GboSetAttr("desc", t.String())
			bo.GboSetAttr("val_string", bo.GboGetAttrUnsafe("val_string", reddo.TypeString).(string)+"(updated)")
			bo.GboSetAttr("val_time", t)
		}
		fmt.Println("\t\tUpdating bo:", string(bo.GboToJsonUnsafe()))
		result, err := dao.GdaoUpdate(collection, bo)
		if err != nil {
			fmt.Printf("\t\tError while updating app [%s]: %s\n", id, err)
		} else {
			fmt.Printf("\t\tResult: %v\n", result)
			bo, err := dao.GdaoFetchOne(collection, map[string]interface{}{fieldId: id})
			if err != nil {
				fmt.Printf("\t\tError while fetching app [%s]: %s\n", id, err)
			} else if bo != nil {
				fmt.Printf("\t\tApp info: %v\n", string(bo.GboToJsonUnsafe()))
			} else {
				fmt.Printf("\t\tApp [%s] does not exist\n", id)
			}
		}
	}
	fmt.Println(sep)
}

func demoMongoUpsertDocs(loc *time.Location, collection string, txMode bool, docIds ...string) {
	mc := createMongoConnect()
	dao := newGenericDaoMongo(mc, txMode)

	fmt.Printf("-== Upsert documents to collection (TxMode=%v) ==-", txMode)
	for _, id := range docIds {
		t := time.Unix(int64(rand.Int31()), rand.Int63()%1000000000).In(loc)
		bo, err := dao.GdaoFetchOne(collection, map[string]interface{}{fieldId: id})
		if err != nil {
			fmt.Printf("\tError while fetching app [%s]: %s\n", id, err)
		} else if bo == nil {
			fmt.Printf("\tApp [%s] does not exist\n", id)
			bo = godal.NewGenericBo()
			bo.GboSetAttr(fieldId, id)
			bo.GboSetAttr("desc", t.String())
			bo.GboSetAttr("val_string", fmt.Sprintf("(upsert,txmode=%v)", txMode))
			bo.GboSetAttr("val_time", t)
		} else {
			fmt.Println("\tExisting bo:", string(bo.GboToJsonUnsafe()))
			bo.GboSetAttr("desc", t.String())
			bo.GboSetAttr("val_string", bo.GboGetAttrUnsafe("val_string", reddo.TypeString).(string)+fmt.Sprintf("(upsert,txmode=%v)", txMode))
			bo.GboSetAttr("val_time", t)
		}
		fmt.Println("\t\tUpserting bo:", string(bo.GboToJsonUnsafe()))
		result, err := dao.GdaoSave(collection, bo)
		if err != nil {
			fmt.Printf("\t\tError while upserting app [%s]: %s\n", id, err)
		} else {
			fmt.Printf("\t\tResult: %v\n", result)
			bo, err := dao.GdaoFetchOne(collection, map[string]interface{}{fieldId: id})
			if err != nil {
				fmt.Printf("\t\tError while fetching app [%s]: %s\n", id, err)
			} else if bo != nil {
				fmt.Printf("\t\tApp info: %v\n", string(bo.GboToJsonUnsafe()))
			} else {
				fmt.Printf("\t\tApp [%s] does not exist\n", id)
			}
		}
	}
	fmt.Println(sep)
}

func demoMongoSelectSortingAndLimit(loc *time.Location, collection string) {
	mc := createMongoConnect()
	initDataMongo(mc, collection)
	dao := newGenericDaoMongo(mc, false)

	fmt.Println("-== Fetch documents from collection with sorting and limit ==-")
	n := 100
	fmt.Printf("\tInserting %d docs...\n", n)
	for i := 0; i < n; i++ {
		id := strconv.Itoa(i)
		for len(id) < 3 {
			id = "0" + id
		}
		t := time.Unix(int64(rand.Int31()), rand.Int63()%1000000000).In(loc)
		bo := godal.NewGenericBo()
		bo.GboSetAttr(fieldId, id)
		bo.GboSetAttr("desc", t.String())
		bo.GboSetAttr("val_bool", rand.Int31()%2 == 0)
		bo.GboSetAttr("val_int", rand.Int())
		bo.GboSetAttr("val_float", rand.Float64())
		bo.GboSetAttr("val_string", id+" (sorting and limit)")
		bo.GboSetAttr("val_time", t)
		bo.GboSetAttr("val_list", []interface{}{rand.Int31()%2 == 0, i, id})
		bo.GboSetAttr("val_map", map[string]interface{}{"tags": []interface{}{id, i}})
		_, err := dao.GdaoCreate(collection, bo)
		if err != nil {
			panic(err)
		}
	}
	startOffset := rand.Intn(n)
	numRows := rand.Intn(10) + 1
	fmt.Printf("\tFetching %d docs, starting from offset %d...\n", numRows, startOffset)
	sorting := map[string]int{fieldId: 1} // sort by "id" attribute, ascending
	boList, err := dao.GdaoFetchMany(collection, nil, sorting, startOffset, numRows)
	if err != nil {
		fmt.Printf("\t\tError while fetching apps: %s\n", err)
	} else {
		for _, bo := range boList {
			fmt.Printf("\t\tApp info: %v\n", string(bo.GboToJsonUnsafe()))
		}
	}
	fmt.Println(sep)
}

func main() {
	rand.Seed(time.Now().UnixNano())
	loc, _ := time.LoadLocation(timeZone)
	fmt.Println("Timezone:", loc)
	collection := "apps"
	fmt.Println("Collection:", collection)

	demoMongoInsertDocs(loc, collection, true)
	demoMongoInsertDocs(loc, collection, false)
	demoMongoFetchDocById(collection, "login", "loggin")
	demoMongoFetchAllDocs(collection)
	demoMongoDeleteDocs(collection, "login", "loggin")
	demoMongoUpdateDocs(loc, collection, "log", "logging")
	demoMongoUpsertDocs(loc, collection, true, "log", "logging")
	demoMongoUpsertDocs(loc, collection, false, "log", "loggging")
	demoMongoSelectSortingAndLimit(loc, collection)
}
