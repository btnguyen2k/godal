/*
Generic MongoDB Dao example. Run with command:

$ go run examples_mongo_generic.go
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

const (
	// mongoGenericTz      = "Asia/Ho_Chi_Minh"
	// mongoGenericSep     = "================================================================================"
	mongoGenericFieldId = "_id"
)

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
	mc.CreateCollection("__godal")

	return mc
}

func initDataMongoGeneric(mc *prom.MongoConnect, collection string) {
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
func (dao *myGenericDaoMongo) GdaoCreateFilter(storageId string, bo godal.IGenericBo) godal.FilterOpt {
	id, _ := bo.GboGetAttr(mongoGenericFieldId, reddo.TypeString)
	return godal.MakeFilter(map[string]interface{}{mongoGenericFieldId: id})
}

func newGenericDaoMongo(mc *prom.MongoConnect, txMode bool) godal.IGenericDao {
	dao := &myGenericDaoMongo{}
	dao.GenericDaoMongo = mongo.NewGenericDaoMongo(mc, godal.NewAbstractGenericDao(dao))
	dao.SetTxModeOnWrite(txMode)
	return dao
}

func demoMongoInsertDocsGeneric(loc *time.Location, collection string, txMode bool) {
	mc := createMongoConnectGeneric()
	initDataMongoGeneric(mc, collection)
	dao := newGenericDaoMongo(mc, txMode)

	fmt.Printf("-== Insert documents to collection (TxMode=%v) ==-\n", txMode)

	// insert a document
	t := time.Unix(int64(rand.Int31()), rand.Int63()%1000000000).In(loc)
	bo := godal.NewGenericBo()
	bo.GboSetAttr(mongoGenericFieldId, "log")
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
	bo.GboSetAttr(mongoGenericFieldId, "login")
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
	bo.GboSetAttr(mongoGenericFieldId, "login")
	bo.GboSetAttr("val_string", "Authentication application (TxMode=true)(again)")
	bo.GboSetAttr("val_list", []interface{}{"duplicated"})
	fmt.Println("\tCreating bo:", string(bo.GboToJsonUnsafe()))
	result, err = dao.GdaoCreate(collection, bo)
	if err != nil {
		fmt.Printf("\t\tError: %s\n", err)
	} else {
		fmt.Printf("\t\tResult: %v\n", result)
	}

	fmt.Println(common.SEP)
}

func demoMongoFetchDocByIdGeneric(collection string, docIds ...string) {
	mc := createMongoConnectGeneric()
	dao := newGenericDaoMongo(mc, false)

	fmt.Printf("-== Fetch documents by id ==-\n")
	for _, id := range docIds {
		bo, err := dao.GdaoFetchOne(collection, godal.MakeFilter(map[string]interface{}{mongoGenericFieldId: id}))
		if err != nil {
			fmt.Printf("\tError while fetching app [%s]: %s\n", id, err)
		} else if bo != nil {
			fmt.Println("\tFetched bo:", string(bo.GboToJsonUnsafe()))
		} else {
			fmt.Printf("\tApp [%s] does not exist\n", id)
		}
	}

	fmt.Println(common.SEP)
}

func demoMongoFetchAllDocsGeneric(collection string) {
	mc := createMongoConnectGeneric()
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
	fmt.Println(common.SEP)
}

func demoMongoDeleteDocsGeneric(collection string, docIds ...string) {
	mc := createMongoConnectGeneric()
	dao := newGenericDaoMongo(mc, false)

	fmt.Println("-== Delete documents from collection ==-")
	for _, id := range docIds {
		bo, err := dao.GdaoFetchOne(collection, godal.MakeFilter(map[string]interface{}{mongoGenericFieldId: id}))
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
			bo1, err := dao.GdaoFetchOne(collection, godal.MakeFilter(map[string]interface{}{mongoGenericFieldId: id}))
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
	fmt.Println(common.SEP)
}

func demoMongoUpdateDocsGeneric(loc *time.Location, collection string, docIds ...string) {
	mc := createMongoConnectGeneric()
	dao := newGenericDaoMongo(mc, false)

	fmt.Println("-== Update documents from collection ==-")
	for _, id := range docIds {
		t := time.Unix(int64(rand.Int31()), rand.Int63()%1000000000).In(loc)
		bo, err := dao.GdaoFetchOne(collection, godal.MakeFilter(map[string]interface{}{mongoGenericFieldId: id}))
		if err != nil {
			fmt.Printf("\tError while fetching app [%s]: %s\n", id, err)
			continue
		} else if bo == nil {
			fmt.Printf("\tApp [%s] does not exist\n", id)
			bo = godal.NewGenericBo()
			bo.GboSetAttr(mongoGenericFieldId, id)
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
			bo, err := dao.GdaoFetchOne(collection, godal.MakeFilter(map[string]interface{}{mongoGenericFieldId: id}))
			if err != nil {
				fmt.Printf("\t\tError while fetching app [%s]: %s\n", id, err)
			} else if bo != nil {
				fmt.Printf("\t\tApp info: %v\n", string(bo.GboToJsonUnsafe()))
			} else {
				fmt.Printf("\t\tApp [%s] does not exist\n", id)
			}
		}
	}
	fmt.Println(common.SEP)
}

func demoMongoUpsertDocsGeneric(loc *time.Location, collection string, txMode bool, docIds ...string) {
	mc := createMongoConnectGeneric()
	dao := newGenericDaoMongo(mc, txMode)

	fmt.Printf("-== Upsert documents to collection (TxMode=%v) ==-", txMode)
	for _, id := range docIds {
		t := time.Unix(int64(rand.Int31()), rand.Int63()%1000000000).In(loc)
		bo, err := dao.GdaoFetchOne(collection, godal.MakeFilter(map[string]interface{}{mongoGenericFieldId: id}))
		if err != nil {
			fmt.Printf("\tError while fetching app [%s]: %s\n", id, err)
			continue
		} else if bo == nil {
			fmt.Printf("\tApp [%s] does not exist\n", id)
			bo = godal.NewGenericBo()
			bo.GboSetAttr(mongoGenericFieldId, id)
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
			bo, err := dao.GdaoFetchOne(collection, godal.MakeFilter(map[string]interface{}{mongoGenericFieldId: id}))
			if err != nil {
				fmt.Printf("\t\tError while fetching app [%s]: %s\n", id, err)
			} else if bo != nil {
				fmt.Printf("\t\tApp info: %v\n", string(bo.GboToJsonUnsafe()))
			} else {
				fmt.Printf("\t\tApp [%s] does not exist\n", id)
			}
		}
	}
	fmt.Println(common.SEP)
}

func demoMongoSelectSortingAndLimitGeneric(loc *time.Location, collection string) {
	mc := createMongoConnectGeneric()
	initDataMongoGeneric(mc, collection)
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
		bo.GboSetAttr(mongoGenericFieldId, id)
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
	sorting := (&godal.SortingField{FieldName: mongoGenericFieldId}).ToSortingOpt()
	boList, err := dao.GdaoFetchMany(collection, nil, sorting, startOffset, numRows)
	if err != nil {
		fmt.Printf("\t\tError while fetching apps: %s\n", err)
	} else {
		for _, bo := range boList {
			fmt.Printf("\t\tApp info: %v\n", string(bo.GboToJsonUnsafe()))
		}
	}
	fmt.Println(common.SEP)
}

func main() {
	rand.Seed(time.Now().UnixNano())
	loc, _ := time.LoadLocation(common.TIMEZONE)
	fmt.Println("mongoGenericTz:", loc)
	collection := "apps"
	fmt.Println("Collection:", collection)

	demoMongoInsertDocsGeneric(loc, collection, true)
	demoMongoInsertDocsGeneric(loc, collection, false)
	demoMongoFetchDocByIdGeneric(collection, "login", "loggin")
	demoMongoFetchAllDocsGeneric(collection)
	demoMongoDeleteDocsGeneric(collection, "login", "loggin")
	demoMongoUpdateDocsGeneric(loc, collection, "log", "logging")
	demoMongoUpsertDocsGeneric(loc, collection, true, "log", "logging")
	demoMongoUpsertDocsGeneric(loc, collection, false, "log", "loggging")
	demoMongoSelectSortingAndLimitGeneric(loc, collection)
}
