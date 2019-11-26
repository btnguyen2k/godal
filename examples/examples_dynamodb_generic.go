/*
Generic AWS DynamoDB Dao example. Run with command:

$ go run examples_dynamodb_generic.go
*/
package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godal"
	gdaodynamodb "github.com/btnguyen2k/godal/dynamodb"
	"github.com/btnguyen2k/prom"
	"math/rand"
	"time"
)

const (
	timeZone      = "Asia/Ho_Chi_Minh"
	sep           = "================================================================================"
	fieldId       = "id"
	fieldUsername = "username"
	fieldVersion  = "version"
	fieldActived  = "actived"
)

func createAwsDynamodbConnect() *prom.AwsDynamodbConnect {
	region := "ap-southeast-1"
	cfg := &aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewEnvCredentials(),
	}
	adc, err := prom.NewAwsDynamodbConnect(cfg, nil, nil, 10000)
	if err != nil {
		panic(err)
	}
	return adc
}

func initDataDynamodb(adc *prom.AwsDynamodbConnect, tableName, indexName string) {
	var schema, key []prom.AwsDynamodbNameAndType

	if ok, err := adc.HasTable(nil, tableName); err != nil {
		panic(err)
	} else if !ok {
		schema = []prom.AwsDynamodbNameAndType{
			{fieldId, prom.AwsAttrTypeString},
		}
		key = []prom.AwsDynamodbNameAndType{
			{fieldId, prom.AwsKeyTypePartition},
		}
		if err := adc.CreateTable(nil, tableName, 2, 2, schema, key); err != nil {
			panic(err)
		}
		time.Sleep(1 * time.Second)
		for status, err := adc.GetTableStatus(nil, tableName); status != "ACTIVE" && err == nil; {
			fmt.Printf("    Table [%s] status: %v - %e\n", tableName, status, err)
			time.Sleep(1 * time.Second)
			status, err = adc.GetTableStatus(nil, tableName)
		}
	}

	if status, err := adc.GetGlobalSecondaryIndexStatus(nil, tableName, indexName); err != nil {
		panic(err)
	} else if status == "" {
		schema = []prom.AwsDynamodbNameAndType{
			{fieldActived, prom.AwsAttrTypeNumber},
			{fieldVersion, prom.AwsAttrTypeNumber},
		}
		key = []prom.AwsDynamodbNameAndType{
			{fieldActived, prom.AwsKeyTypePartition},
			{fieldVersion, prom.AwsKeyTypeSort},
		}
		if err := adc.CreateGlobalSecondaryIndex(nil, tableName, indexName, 1, 1, schema, key); err != nil {
			panic(err)
		}
		time.Sleep(5 * time.Second)
		for status, err := adc.GetGlobalSecondaryIndexStatus(nil, tableName, indexName); status != "ACTIVE" && err == nil; {
			fmt.Printf("    GSI [%s] on table [%s] status: %v - %e\n", indexName, tableName, status, err)
			time.Sleep(5 * time.Second)
			status, err = adc.GetGlobalSecondaryIndexStatus(nil, tableName, indexName)
		}
	}

	// delete all items
	pkAttrs := []string{fieldId}
	adc.ScanItemsWithCallback(nil, tableName, nil, prom.AwsDynamodbNoIndex, nil, func(item prom.AwsDynamodbItem, lastEvaluatedKey map[string]*dynamodb.AttributeValue) (b bool, e error) {
		keyFilter := make(map[string]interface{})
		for _, v := range pkAttrs {
			keyFilter[v] = item[v]
		}
		_, err := adc.DeleteItem(nil, tableName, keyFilter, nil)
		if err != nil {
			fmt.Printf("    Delete item from table [%s] with key %s: %e\n", tableName, keyFilter, err)
		}
		// fmt.Printf("    Delete item from table [%s] with key %s: %e\n", table, keyFilter, err)
		return true, nil
	})
}

type myGenericDaoDynamodb struct {
	*gdaodynamodb.GenericDaoDynamodb
}

// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter.
func (dao *myGenericDaoDynamodb) GdaoCreateFilter(table string, bo godal.IGenericBo) interface{} {
	return map[string]interface{}{fieldId: bo.GboGetAttrUnsafe(fieldId, reddo.TypeString)}
}

func newGenericDaoDynamodb(adc *prom.AwsDynamodbConnect, tableName string) godal.IGenericDao {
	dao := &myGenericDaoDynamodb{}
	dao.GenericDaoDynamodb = gdaodynamodb.NewGenericDaoDynamodb(adc, godal.NewAbstractGenericDao(dao))
	dao.SetRowMapper(&gdaodynamodb.GenericRowMapperDynamodb{ColumnsListMap: map[string][]string{tableName: {fieldId}}})
	return dao
}

func demoDynamodbInsertItem(loc *time.Location, tableName, indexName string) {
	adc := createAwsDynamodbConnect()
	initDataDynamodb(adc, tableName, indexName)
	dao := newGenericDaoDynamodb(adc, tableName)

	fmt.Printf("-== Insert items to table ==-\n")

	// insert an item
	t := time.Unix(int64(rand.Int31()), rand.Int63()%1000000000).In(loc)
	bo := godal.NewGenericBo()
	bo.GboSetAttr(fieldId, "log")
	bo.GboSetAttr(fieldUsername, t.String())
	bo.GboSetAttr(fieldVersion, rand.Int31n(123456))
	bo.GboSetAttr(fieldActived, 1)
	bo.GboSetAttr("val_bool", rand.Int31()%2 == 0)
	bo.GboSetAttr("val_int", rand.Int())
	bo.GboSetAttr("val_float", rand.Float64())
	bo.GboSetAttr("val_string", fmt.Sprintf("Logging application"))
	bo.GboSetAttr("val_time", t)
	bo.GboSetAttr("val_list", []interface{}{true, 0, "1", 2.3, "system", "utility"})
	bo.GboSetAttr("val_map", map[string]interface{}{"tags": []string{"system", "utility"}, "age": 103, "active": true})
	fmt.Println("\tCreating bo:", string(bo.GboToJsonUnsafe()))
	result, err := dao.GdaoCreate(tableName, bo)
	if err != nil {
		fmt.Printf("\t\tError: %s\n", err)
	} else {
		fmt.Printf("\t\tResult: %v\n", result)
	}

	// insert another item
	t = time.Unix(int64(rand.Int31()), rand.Int63()%1000000000).In(loc)
	bo = godal.NewGenericBo()
	bo.GboSetAttr(fieldId, "login")
	bo.GboSetAttr(fieldUsername, t.String())
	bo.GboSetAttr(fieldVersion, rand.Int31n(123456))
	bo.GboSetAttr(fieldActived, 1)
	bo.GboSetAttr("val_bool", rand.Int31()%2 == 0)
	bo.GboSetAttr("val_int", rand.Int())
	bo.GboSetAttr("val_float", rand.Float64())
	bo.GboSetAttr("val_string", fmt.Sprintf("Authentication application"))
	bo.GboSetAttr("val_time", t)
	bo.GboSetAttr("val_list", []interface{}{false, 9.8, "7", 6, "system", "security"})
	bo.GboSetAttr("val_map", map[string]interface{}{"tags": []string{"system", "security"}, "age": 81, "active": false})
	fmt.Println("\tCreating bo:", string(bo.GboToJsonUnsafe()))
	result, err = dao.GdaoCreate(tableName, bo)
	if err != nil {
		fmt.Printf("\t\tError: %s\n", err)
	} else {
		fmt.Printf("\t\tResult: %v\n", result)
	}

	// insert another document with duplicated id
	bo = godal.NewGenericBo()
	bo.GboSetAttr(fieldId, "login")
	bo.GboSetAttr("val_string", "Authentication application (again)")
	bo.GboSetAttr("val_list", []interface{}{"duplicated"})
	fmt.Println("\tCreating bo:", string(bo.GboToJsonUnsafe()))
	result, err = dao.GdaoCreate(tableName, bo)
	if err != nil {
		fmt.Printf("\t\tError: %s\n", err)
	} else {
		fmt.Printf("\t\tResult: %v\n", result)
	}

	fmt.Println(sep)
}

func demoDynamodbFetchItemById(tableName string, itemIds ...string) {
	adc := createAwsDynamodbConnect()
	dao := newGenericDaoDynamodb(adc, tableName)

	fmt.Printf("-== Fetch items by id ==-\n")
	for _, id := range itemIds {
		bo, err := dao.GdaoFetchOne(tableName, map[string]interface{}{fieldId: id})
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

func demoDynamodbFetchAllItems(tableName, indexName string) {
	adc := createAwsDynamodbConnect()
	dao := newGenericDaoDynamodb(adc, tableName)

	table := tableName
	if indexName != "" {
		table += ":" + indexName + ":true"
	}

	fmt.Printf("-== Fetch all items from table (%s) ==-\n", table)
	boList, err := dao.GdaoFetchMany(table, nil, nil, 0, 0)
	if err != nil {
		fmt.Printf("\tError while fetching apps: %s\n", err)
	} else {
		for _, bo := range boList {
			fmt.Println("\tFetched bo:", string(bo.GboToJsonUnsafe()))
		}
	}
	fmt.Println(sep)
}

func demoDynamodbDeleteItems(tableName string, itemIds ...string) {
	adc := createAwsDynamodbConnect()
	dao := newGenericDaoDynamodb(adc, tableName)

	fmt.Println("-== Delete items from table ==-")
	for _, id := range itemIds {
		bo, err := dao.GdaoFetchOne(tableName, map[string]interface{}{fieldId: id})
		if err != nil {
			fmt.Printf("\tError while fetching app [%s]: %s\n", id, err)
		} else if bo == nil {
			fmt.Printf("\tApp [%s] does not exist, no need to delete\n", id)
		} else {
			fmt.Println("\tDeleting bo:", string(bo.GboToJsonUnsafe()))
			result, err := dao.GdaoDelete(tableName, bo)
			if err != nil {
				fmt.Printf("\t\tError: %s\n", err)
			} else {
				fmt.Printf("\t\tResult: %v\n", result)
			}
			bo1, err := dao.GdaoFetchOne(tableName, map[string]interface{}{fieldId: id})
			if err != nil {
				fmt.Printf("\t\tError while fetching app [%s]: %s\n", id, err)
			} else if bo1 != nil {
				fmt.Printf("\t\tApp info: %v\n", string(bo.GboToJsonUnsafe()))
			} else {
				fmt.Printf("\t\tApp [%s] no longer exist\n", id)
				result, err := dao.GdaoDelete(tableName, bo)
				fmt.Printf("\t\tDeleting app [%s] again: %v / %s\n", id, result, err)
			}
		}

	}
	fmt.Println(sep)
}

func demoDynamodbUpdateItems(loc *time.Location, tableName string, itemIds ...string) {
	adc := createAwsDynamodbConnect()
	dao := newGenericDaoDynamodb(adc, tableName)

	fmt.Println("-== Update items from table ==-")
	for _, id := range itemIds {
		t := time.Unix(int64(rand.Int31()), rand.Int63()%1000000000).In(loc)
		bo, err := dao.GdaoFetchOne(tableName, map[string]interface{}{fieldId: id})
		if err != nil {
			fmt.Printf("\tError while fetching app [%s]: %s\n", id, err)
		} else if bo == nil {
			fmt.Printf("\tApp [%s] does not exist\n", id)
			bo = godal.NewGenericBo()
			bo.GboSetAttr(fieldId, id)
			bo.GboSetAttr(fieldUsername, t.String())
			bo.GboSetAttr(fieldVersion, rand.Int31n(123456))
			bo.GboSetAttr(fieldActived, 1)
			bo.GboSetAttr("val_string", "(updated)")
			bo.GboSetAttr("val_time", t)
		} else {
			fmt.Println("\tExisting bo:", string(bo.GboToJsonUnsafe()))
			bo.GboSetAttr("val_string", bo.GboGetAttrUnsafe("val_string", reddo.TypeString).(string)+"(updated)")
			bo.GboSetAttr("val_time", t)
		}
		fmt.Println("\t\tUpdating bo:", string(bo.GboToJsonUnsafe()))
		result, err := dao.GdaoUpdate(tableName, bo)
		if err != nil {
			fmt.Printf("\t\tError while updating app [%s]: %s\n", id, err)
		} else {
			fmt.Printf("\t\tResult: %v\n", result)
			bo, err := dao.GdaoFetchOne(tableName, map[string]interface{}{fieldId: id})
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

func demoDynamodbUpsertItem(loc *time.Location, tableName string, itemIds ...string) {
	adc := createAwsDynamodbConnect()
	dao := newGenericDaoDynamodb(adc, tableName)

	fmt.Printf("-== Upsert items to collection ==-\n")
	for _, id := range itemIds {
		t := time.Unix(int64(rand.Int31()), rand.Int63()%1000000000).In(loc)
		bo, err := dao.GdaoFetchOne(tableName, map[string]interface{}{fieldId: id})
		if err != nil {
			fmt.Printf("\tError while fetching app [%s]: %s\n", id, err)
		} else if bo == nil {
			fmt.Printf("\tApp [%s] does not exist\n", id)
			bo = godal.NewGenericBo()
			bo.GboSetAttr(fieldId, id)
			bo.GboSetAttr(fieldUsername, t.String())
			bo.GboSetAttr(fieldVersion, rand.Int31n(123456))
			bo.GboSetAttr(fieldActived, 1)
			bo.GboSetAttr("val_string", fmt.Sprintf("(upsert)"))
			bo.GboSetAttr("val_time", t)
		} else {
			fmt.Println("\tExisting bo:", string(bo.GboToJsonUnsafe()))
			bo.GboSetAttr("val_string", bo.GboGetAttrUnsafe("val_string", reddo.TypeString).(string)+fmt.Sprintf("(upsert)"))
			bo.GboSetAttr("val_time", t)
		}
		fmt.Println("\t\tUpserting bo:", string(bo.GboToJsonUnsafe()))
		result, err := dao.GdaoSave(tableName, bo)
		if err != nil {
			fmt.Printf("\t\tError while upserting app [%s]: %s\n", id, err)
		} else {
			fmt.Printf("\t\tResult: %v\n", result)
			bo, err := dao.GdaoFetchOne(tableName, map[string]interface{}{fieldId: id})
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

func demoDynamodbSelectSortingAndLimit(loc *time.Location, tableName, indexNameInit, indexNameFetch string) {
	adc := createAwsDynamodbConnect()
	initDataDynamodb(adc, tableName, indexNameInit)
	dao := newGenericDaoDynamodb(adc, tableName)

	table := tableName
	if indexNameFetch != "" {
		table += ":" + indexNameFetch + ":true"
	}

	fmt.Println("-== Fetch items from collection with sorting and limit ==-")
	n := 100
	fmt.Printf("\tInserting %d items...\n", n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("%03d", i)
		t := time.Unix(int64(rand.Int31()), rand.Int63()%1000000000).In(loc)
		bo := godal.NewGenericBo()
		bo.GboSetAttr(fieldId, id)
		bo.GboSetAttr(fieldUsername, t.String())
		bo.GboSetAttr(fieldVersion, i)
		bo.GboSetAttr(fieldActived, 1)
		bo.GboSetAttr("val_bool", rand.Int31()%2 == 0)
		bo.GboSetAttr("val_int", rand.Int())
		bo.GboSetAttr("val_float", rand.Float64())
		bo.GboSetAttr("val_string", id+" (sorting and limit)")
		bo.GboSetAttr("val_time", t)
		bo.GboSetAttr("val_list", []interface{}{rand.Int31()%2 == 0, i, id})
		bo.GboSetAttr("val_map", map[string]interface{}{"tags": []interface{}{id, i}})
		_, err := dao.GdaoCreate(tableName, bo)
		if err != nil {
			panic(err)
		}
	}
	startOffset := rand.Intn(n)
	numRows := rand.Intn(10) + 1
	fmt.Printf("\tFetching %d docs, starting from offset %d...\n", numRows, startOffset)
	boList, err := dao.GdaoFetchMany(table, nil, nil, startOffset, numRows)
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
	tableName := "test"
	indexName := "idx_sorted"
	fmt.Println("Table & Index:", tableName, indexName)

	demoDynamodbInsertItem(loc, tableName, indexName)
	demoDynamodbFetchItemById(tableName, "login", "loggin")
	demoDynamodbFetchAllItems(tableName, "")
	demoDynamodbFetchAllItems(tableName, indexName)
	demoDynamodbDeleteItems(tableName, "login", "loggin")
	demoDynamodbUpdateItems(loc, tableName, "log", "logging")
	demoDynamodbUpsertItem(loc, tableName, "log", "logging")
	demoDynamodbSelectSortingAndLimit(loc, tableName, indexName, "")
	demoDynamodbSelectSortingAndLimit(loc, tableName, indexName, indexName)
}
