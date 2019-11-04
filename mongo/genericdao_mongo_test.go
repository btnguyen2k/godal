package mongo

import (
	"encoding/json"
	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/prom"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strconv"
	"sync"
	"testing"
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
	if err := mc.GetCollection(collection).Drop(nil); err != nil {
		panic(err)
	}
	if _, err := mc.CreateCollection(collection); err != nil {
		panic(err)
	}
	indexName := "uidx_username"
	isUnique := true
	indexes := []interface{}{
		mongo.IndexModel{
			Keys: map[string]interface{}{
				fieldUsername: 1,
			},
			Options: &options.IndexOptions{
				Name:   &indexName,
				Unique: &isUnique,
			},
		},
	}
	if _, err := mc.CreateCollectionIndexes(collection, indexes); err != nil {
		panic(err)
	}
}

func createDaoMongo(mc *prom.MongoConnect, collectionName string) *MyDaoMongo {
	dao := &MyDaoMongo{collectionName: collectionName}
	dao.GenericDaoMongo = NewGenericDaoMongo(mc, godal.NewAbstractGenericDao(dao))
	return dao
}

type MyBo struct {
	Id       string `json:"_id"`
	Username string `json:"username"`
	Name     string `json:"name"`
	Version  int    `json:"version"`
}

func (bo *MyBo) ToGbo() godal.IGenericBo {
	gbo := godal.NewGenericBo()
	if err := gbo.GboImportViaJson(bo); err != nil {
		panic(err)
	}
	return gbo
}

func fromGbo(gbo godal.IGenericBo) *MyBo {
	bo := MyBo{}
	gbo.GboTransferViaJson(&bo)
	return &bo
}

const (
	collectionName = "test"
	fieldId        = "_id"
	fieldUsername  = "username"
)

type MyDaoMongo struct {
	*GenericDaoMongo
	collectionName string
}

// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter.
func (dao *MyDaoMongo) GdaoCreateFilter(storageId string, bo godal.IGenericBo) interface{} {
	return map[string]interface{}{fieldId: bo.GboGetAttrUnsafe(fieldId, reddo.TypeString)}
}

/*----------------------------------------------------------------------*/
func initDao() *MyDaoMongo {
	mc := createMongoConnect()
	initDataMongo(mc, collectionName)
	return createDaoMongo(mc, collectionName)
}

func TestGenericDaoMongo_Empty(t *testing.T) {
	name := "TestGenericDaoMongo_Empty"
	dao := initDao()

	boList, err := dao.GdaoFetchMany(dao.collectionName, nil, nil, 0, 0)
	if err != nil {
		t.Fatalf("%s failed, has error: %e", name, err)
	}
	if boList == nil {
		t.Fatalf("%s failed, nil result", name)
	}
	if len(boList) != 0 {
		t.Fatalf("%s failed, non-empty result: %v", name, boList)
	}

	bo, err := dao.GdaoFetchOne(dao.collectionName, map[string]interface{}{fieldId: "any"})
	if err != nil {
		t.Fatalf("%s failed, has error: %e", name, err)
	}
	if bo != nil {
		t.Fatalf("%s failed, should have nill result, but received: %v", name, bo)
	}
}

func TestGenericDaoMongo_GdaoCreateDuplicated_TxModeOff(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoCreateDuplicated_TxModeOff"
	dao := initDao()
	dao.SetTxModeOnWrite(false)
	bo1 := &MyBo{
		Id:       "1",
		Username: "1",
		Name:     "BO - 1",
		Version:  1,
	}
	if numRows, err := dao.GdaoCreate(dao.collectionName, bo1.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
	bo2 := &MyBo{
		Id:       "2",
		Username: "1",
		Name:     "BO - 2",
		Version:  2,
	}
	if numRows, err := dao.GdaoCreate(dao.collectionName, bo2.ToGbo()); err != godal.GdaoErrorDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
}

func TestGenericDaoMongo_GdaoCreateDuplicated_TxModeOn(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoCreateDuplicated_TxModeOn"
	dao := initDao()
	dao.SetTxModeOnWrite(true)
	bo1 := &MyBo{
		Id:       "1",
		Username: "1",
		Name:     "BO - 1",
		Version:  1,
	}
	if numRows, err := dao.GdaoCreate(dao.collectionName, bo1.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
	bo2 := &MyBo{
		Id:       "2",
		Username: "1",
		Name:     "BO - 2",
		Version:  2,
	}
	if numRows, err := dao.GdaoCreate(dao.collectionName, bo2.ToGbo()); err != godal.GdaoErrorDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
}

func TestGenericDaoMongo_GdaoCreateGet(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoCreate"
	dao := initDao()
	bo := &MyBo{
		Id:       "1",
		Username: "2",
		Name:     "BO - 3",
		Version:  4,
	}
	if numRows, err := dao.GdaoCreate(dao.collectionName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	gbo, err := dao.GdaoFetchOne(dao.collectionName, map[string]interface{}{fieldId: "1"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != bo.Id || myBo.Username != bo.Username || myBo.Name != bo.Name || myBo.Version != bo.Version {
		t.Fatalf("%s failed - Expected: %v / Received: %v", name, bo, myBo)
	}
}

func TestGenericDaoMongo_GdaoCreateTwiceGet_TxModeOff(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoCreateTwiceGet_TxModeOff"
	dao := initDao()
	dao.SetTxModeOnWrite(false)
	bo := &MyBo{
		Id:       "1",
		Username: "2",
		Name:     "BO - 3",
		Version:  4,
	}
	if numRows, err := dao.GdaoCreate(dao.collectionName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
	bo.Version = bo.Version + 1
	if numRows, err := dao.GdaoCreate(dao.collectionName, bo.ToGbo()); err != nil || numRows != 0 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	bo.Version = bo.Version - 1
	gbo, err := dao.GdaoFetchOne(dao.collectionName, map[string]interface{}{fieldId: "1"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != bo.Id || myBo.Username != bo.Username || myBo.Name != bo.Name || myBo.Version != bo.Version {
		t.Fatalf("%s failed - Expected: %v / Received: %v", name, bo, myBo)
	}
}

func TestGenericDaoMongo_GdaoCreateTwiceGet_TxModeOn(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoCreateTwiceGet_TxModeOn"
	dao := initDao()
	dao.SetTxModeOnWrite(true)
	bo := &MyBo{
		Id:       "1",
		Username: "2",
		Name:     "BO - 3",
		Version:  4,
	}
	if numRows, err := dao.GdaoCreate(dao.collectionName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
	bo.Version = bo.Version + 1
	if numRows, err := dao.GdaoCreate(dao.collectionName, bo.ToGbo()); err != nil || numRows != 0 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	bo.Version = bo.Version - 1
	gbo, err := dao.GdaoFetchOne(dao.collectionName, map[string]interface{}{fieldId: "1"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != bo.Id || myBo.Username != bo.Username || myBo.Name != bo.Name || myBo.Version != bo.Version {
		t.Fatalf("%s failed - Expected: %v / Received: %v", name, bo, myBo)
	}
}

func TestGenericDaoMongo_GdaoCreateMultiThreadsGet_TxModeOff(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoCreateMultiThreadsGet_TxModeOff"
	dao := initDao()
	dao.SetTxModeOnWrite(false)
	numThreads := 8
	numLoopsPerThread := 10
	var wg sync.WaitGroup
	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go func(threadNum int, bo *MyBo) {
			for j := 0; j < numLoopsPerThread; j++ {
				if _, err := dao.GdaoCreate(dao.collectionName, bo.ToGbo()); err != nil {
					t.Fatalf("%s failed - Thread: %v / Error: %e", name, threadNum, err)
				}
				bo.Version = bo.Version + 1
			}
			wg.Done()
		}(i, &MyBo{
			Id:       "1",
			Username: "2",
			Name:     "BO - " + strconv.Itoa(i),
			Version:  3,
		})
	}
	wg.Wait()

	gbo, err := dao.GdaoFetchOne(dao.collectionName, map[string]interface{}{fieldId: "1"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != "1" || myBo.Username != "2" || myBo.Version != 3 {
		t.Fatalf("%s failed - Received: %v", name, myBo)
	}
}

func TestGenericDaoMongo_GdaoCreateMultiThreadsGet_TxModeOn(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoCreateMultiThreadsGet_TxModeOn"
	dao := initDao()
	dao.SetTxModeOnWrite(true)
	numThreads := 8
	numLoopsPerThread := 10
	var wg sync.WaitGroup
	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go func(threadNum int, bo *MyBo) {
			for j := 0; j < numLoopsPerThread; j++ {
				if _, err := dao.GdaoCreate(dao.collectionName, bo.ToGbo()); err != nil {
					t.Fatalf("%s failed - Thread: %v / Error: %e", name, threadNum, err)
				}
				bo.Version = bo.Version + 1
			}
			wg.Done()
		}(i, &MyBo{
			Id:       "1",
			Username: "2",
			Name:     "BO - " + strconv.Itoa(i),
			Version:  3,
		})
	}
	wg.Wait()

	gbo, err := dao.GdaoFetchOne(dao.collectionName, map[string]interface{}{fieldId: "1"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != "1" || myBo.Username != "2" || myBo.Version != 3 {
		t.Fatalf("%s failed - Received: %v", name, myBo)
	}
}

func TestGenericDaoMongo_GdaoCreateDelete(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoCreateDelete"
	dao := initDao()
	bo := &MyBo{
		Id:       "1",
		Username: "2",
		Name:     "BO - 3",
		Version:  4,
	}
	if numRows, err := dao.GdaoCreate(dao.collectionName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	gbo, err := dao.GdaoFetchOne(dao.collectionName, map[string]interface{}{fieldId: "1"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != bo.Id || myBo.Name != bo.Name || myBo.Version != bo.Version {
		t.Fatalf("%s failed - Expected: %v / Received: %v", name, bo, myBo)
	}

	if numRows, err := dao.GdaoDelete(dao.collectionName, gbo); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	gbo, err = dao.GdaoFetchOne(dao.collectionName, map[string]interface{}{fieldId: "1"})
	if err != nil {
		t.Fatalf("%s failed, has error: %e", name, err)
	}
	if gbo != nil {
		t.Fatalf("%s failed, should have nill result, but received: %v", name, gbo)
	}
}

func TestGenericDaoMongo_GdaoCreateDeleteAll(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoCreateDeleteAll"
	dao := initDao()
	bo := &MyBo{
		Id:       "1",
		Username: "11",
		Name:     "BO - 1",
		Version:  111,
	}
	if numRows, err := dao.GdaoCreate(dao.collectionName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
	bo = &MyBo{
		Id:       "2",
		Username: "22",
		Name:     "BO - 2",
		Version:  222,
	}
	if numRows, err := dao.GdaoCreate(dao.collectionName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	if numRows, err := dao.GdaoDeleteMany(dao.collectionName, nil); err != nil || numRows != 2 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	gbo, err := dao.GdaoFetchOne(dao.collectionName, map[string]interface{}{fieldId: "1"})
	if err != nil {
		t.Fatalf("%s failed, has error: %e", name, err)
	}
	if gbo != nil {
		t.Fatalf("%s failed, should have nill result, but received: %v", name, gbo)
	}
}

func TestGenericDaoMongo_GdaoCreateDeleteMany(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoCreateDeleteMany"
	dao := initDao()
	totalRows := 10
	for i := 0; i < totalRows; i++ {
		bo := &MyBo{
			Id:       strconv.Itoa(i),
			Username: strconv.Itoa(i + 1),
			Name:     "BO - " + strconv.Itoa(i+2),
			Version:  i + 3,
		}
		if numRows, err := dao.GdaoCreate(dao.collectionName, bo.ToGbo()); err != nil || numRows != 1 {
			t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
		}
	}

	js := `{"$and":[{"version":{"$gte":4}},{"version":{"$lte":11}}]}`
	filter := make(map[string]interface{})
	json.Unmarshal([]byte(js), &filter)
	if numRows, err := dao.GdaoDeleteMany(dao.collectionName, filter); err != nil || numRows != totalRows-2 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	for i := 0; i < totalRows; i++ {
		gbo, err := dao.GdaoFetchOne(dao.collectionName, map[string]interface{}{fieldId: strconv.Itoa(i)})
		if i == 0 || i == totalRows-1 {
			if err != nil || gbo == nil {
				t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
			}
		} else {
			if err != nil {
				t.Fatalf("%s failed, has error: %e", name, err)
			}
			if gbo != nil {
				t.Fatalf("%s failed, should have nill result, but received: %v", name, gbo)
			}
		}
	}
}

func TestGenericDaoMongo_GdaoFetchAllWithSorting(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoFetchAllWithSorting"
	dao := initDao()
	numItems := 100
	for i := 0; i < numItems; i++ {
		bo := &MyBo{
			Id:       strconv.Itoa(i),
			Username: strconv.Itoa(i),
			Name:     "BO - " + strconv.Itoa(i),
			Version:  i,
		}
		if numRows, err := dao.GdaoCreate(dao.collectionName, bo.ToGbo()); err != nil || numRows != 1 {
			t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
		}
	}

	gboList, err := dao.GdaoFetchMany(dao.collectionName, nil, map[string]int{"version": -1}, 0, 0)
	if err != nil || gboList == nil || len(gboList) != 100 {
		t.Fatalf("%s failed - NumItems: %v / Error: %e", name, len(gboList), err)
	}

	for i, gbo := range gboList {
		if bo := fromGbo(gbo); bo.Id != strconv.Itoa(numItems-i-1) {
			t.Fatalf("%s failed - Expected: %v / Received: %v", name, numItems-i-1, bo)
		}
	}
}

func TestGenericDaoMongo_GdaoFetchManyWithPaging(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoFetchManyWithPaging"
	dao := initDao()
	numItems := 100
	for i := 0; i < numItems; i++ {
		bo := &MyBo{
			Id:       strconv.Itoa(i),
			Username: strconv.Itoa(i),
			Name:     "BO - " + strconv.Itoa(i),
			Version:  i,
		}
		if numRows, err := dao.GdaoCreate(dao.collectionName, bo.ToGbo()); err != nil || numRows != 1 {
			t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
		}
	}

	js := `{"version":{"$gte": 80}}`
	filter := make(map[string]interface{})
	json.Unmarshal([]byte(js), &filter)
	gboList, err := dao.GdaoFetchMany(dao.collectionName, filter, map[string]int{"version": 1}, 5, 20)
	if err != nil || gboList == nil || len(gboList) != 15 {
		t.Fatalf("%s failed - NumItems: %v / Error: %e", name, len(gboList), err)
	}

	for i, gbo := range gboList {
		if bo := fromGbo(gbo); bo.Id != strconv.Itoa(80+i+5) {
			t.Fatalf("%s failed - Expected: %v / Received: %v", name, 80+i+5, bo)
		}
	}
}

func TestGenericDaoMongo_GdaoUpdateNotExist(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoUpdateNotExist"
	dao := initDao()
	bo := &MyBo{
		Id:       "1",
		Username: "1",
		Name:     "BO - 1",
		Version:  1,
	}
	if numRows, err := dao.GdaoUpdate(dao.collectionName, bo.ToGbo()); err != nil || numRows != 0 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
}

func TestGenericDaoMongo_GdaoUpdateDuplicated(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoUpdateDuplicated"
	dao := initDao()
	for i := 0; i < 2; i++ {
		bo := &MyBo{
			Id:       strconv.Itoa(i),
			Username: strconv.Itoa(i),
			Name:     "BO - " + strconv.Itoa(i),
			Version:  1,
		}
		if numRows, err := dao.GdaoCreate(dao.collectionName, bo.ToGbo()); err != nil || numRows != 1 {
			t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
		}
	}
	gbo, err := dao.GdaoFetchOne(dao.collectionName, map[string]interface{}{fieldId: "0"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	myBo := fromGbo(gbo)
	myBo.Username = "1"
	if numRows, err := dao.GdaoUpdate(dao.collectionName, myBo.ToGbo()); err != godal.GdaoErrorDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
}

func TestGenericDaoMongo_GdaoUpdate(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoUpdate"
	dao := initDao()
	for i := 0; i < 3; i++ {
		bo := &MyBo{
			Id:       strconv.Itoa(i),
			Username: strconv.Itoa(i),
			Name:     "BO - " + strconv.Itoa(i),
			Version:  i,
		}
		if numRows, err := dao.GdaoCreate(dao.collectionName, bo.ToGbo()); err != nil || numRows != 1 {
			t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
		}
	}

	bo := &MyBo{
		Id:       "0",
		Username: strconv.Itoa(100),
		Name:     "BO",
		Version:  100,
	}
	if numRows, err := dao.GdaoUpdate(dao.collectionName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	for i := 0; i < 3; i++ {
		gbo, err := dao.GdaoFetchOne(dao.collectionName, map[string]interface{}{fieldId: strconv.Itoa(i)})
		if err != nil || gbo == nil {
			t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
		}
		if myBo := fromGbo(gbo); myBo == nil {
			t.Fatalf("%s failed - not found: %v", name, i)
		} else if i == 0 && (myBo.Id != bo.Id || myBo.Name != bo.Name || myBo.Version != bo.Version) {
			t.Fatalf("%s failed - Expected: %v / Received: %v", name, bo, myBo)
		} else if i != 0 && myBo.Version != i {
			t.Fatalf("%s failed - Expected: %v / Received: %v", name, i, myBo.Version)
		}
	}
}

func TestGenericDaoMongo_GdaoSaveDuplicated(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoSaveDuplicated"
	dao := initDao()
	for i := 1; i <= 3; i++ {
		bo := &MyBo{
			Id:       strconv.Itoa(i),
			Username: strconv.Itoa(i),
			Name:     "BO - " + strconv.Itoa(i),
			Version:  i,
		}
		if numRows, err := dao.GdaoSave(dao.collectionName, bo.ToGbo()); err != nil || numRows != 1 {
			t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
		}
		gbo, err := dao.GdaoFetchOne(dao.collectionName, map[string]interface{}{fieldId: strconv.Itoa(i)})
		if err != nil || gbo == nil {
			t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
		}
		if myBo := fromGbo(gbo); myBo == nil || myBo.Id != bo.Id || myBo.Name != bo.Name || myBo.Version != bo.Version {
			t.Fatalf("%s failed - Expected: %v / Received: %v", name, bo, myBo)
		}
	}

	// save new one with duplicated key
	bo := &MyBo{
		Id:       strconv.Itoa(0),
		Username: strconv.Itoa(1),
		Name:     "BO - " + strconv.Itoa(0),
		Version:  0,
	}
	if numRows, err := dao.GdaoSave(dao.collectionName, bo.ToGbo()); err != godal.GdaoErrorDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	// save existing one with duplicated key
	bo = &MyBo{
		Id:       strconv.Itoa(1),
		Username: strconv.Itoa(2),
		Name:     "BO - " + strconv.Itoa(1),
		Version:  1,
	}
	if numRows, err := dao.GdaoSave(dao.collectionName, bo.ToGbo()); err != godal.GdaoErrorDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
}

func TestGenericDaoMongo_GdaoSave(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoSave"
	dao := initDao()

	bo := &MyBo{
		Id:       "1",
		Username: "1",
		Name:     "BO - 1",
		Version:  1,
	}
	if numRows, err := dao.GdaoSave(dao.collectionName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
	gbo, err := dao.GdaoFetchOne(dao.collectionName, map[string]interface{}{fieldId: "1"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != bo.Id || myBo.Name != bo.Name || myBo.Version != bo.Version {
		t.Fatalf("%s failed - Expected: %v / Received: %v", name, bo, myBo)
	}

	bo.Name = "BO"
	bo.Version = 10
	if numRows, err := dao.GdaoSave(dao.collectionName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
	gbo, err = dao.GdaoFetchOne(dao.collectionName, map[string]interface{}{fieldId: "1"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != bo.Id || myBo.Name != bo.Name || myBo.Version != bo.Version {
		t.Fatalf("%s failed - Expected: %v / Received: %v", name, bo, myBo)
	}
}
