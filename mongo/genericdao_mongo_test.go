package mongo

import (
	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/prom"
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
}

func createDaoMongo(mc *prom.MongoConnect, collectionName string) *MyDaoMongo {
	dao := &MyDaoMongo{collectionName: collectionName}
	dao.GenericDaoMongo = NewGenericDaoMongo(mc, godal.NewAbstractGenericDao(dao))
	return dao
}

type MyBo struct {
	Id      string `json:"_id"`
	Name    string `json:"name"`
	Version int    `json:"version"`
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

func TestGenericDaoMongo_GdaoCreateGet(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoCreate"
	dao := initDao()
	bo := &MyBo{
		Id:      "1",
		Name:    "BO - 1",
		Version: 0,
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
}

func TestGenericDaoMongo_GdaoCreateTwiceGet_TxModeOff(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoCreateTwiceGet_TxModeOff"
	dao := initDao()
	dao.SetTransactionMode(false)
	bo := &MyBo{
		Id:      "1",
		Name:    "BO - 1",
		Version: 0,
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
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != bo.Id || myBo.Name != bo.Name || myBo.Version != bo.Version {
		t.Fatalf("%s failed - Expected: %v / Received: %v", name, bo, myBo)
	}
}

func TestGenericDaoMongo_GdaoCreateTwiceGet_TxModeOn(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoCreateTwiceGet_TxModeOn"
	dao := initDao()
	dao.SetTransactionMode(true)
	bo := &MyBo{
		Id:      "1",
		Name:    "BO - 1",
		Version: 0,
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
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != bo.Id || myBo.Name != bo.Name || myBo.Version != bo.Version {
		t.Fatalf("%s failed - Expected: %v / Received: %v", name, bo, myBo)
	}
}

func TestGenericDaoMongo_GdaoCreateMultiThreadsGet_TxModeOff(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoCreateMultiThreadsGet_TxModeOff"
	dao := initDao()
	dao.SetTransactionMode(false)
	numThreads := 4
	numLoopsPerThread := 10
	var wg sync.WaitGroup
	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go func(bo *MyBo) {
			for j := 0; j < numLoopsPerThread; j++ {
				if _, err := dao.GdaoCreate(dao.collectionName, bo.ToGbo()); err != nil {
					t.Fatalf("%s failed - Thread: %v / Error: %e", name, i, err)
				}
				bo.Version = bo.Version + 1
			}
			wg.Done()
		}(&MyBo{
			Id:      "0",
			Name:    "BO - " + strconv.Itoa(i),
			Version: 0,
		})
	}
	wg.Wait()

	gbo, err := dao.GdaoFetchOne(dao.collectionName, map[string]interface{}{fieldId: "0"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != "0" || myBo.Version != 0 {
		t.Fatalf("%s failed - Received: %v", name, myBo)
	}
}

func TestGenericDaoMongo_GdaoCreateMultiThreadsGet_TxModeOn(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoCreateMultiThreadsGet_TxModeOn"
	dao := initDao()
	dao.SetTransactionMode(true)
	numThreads := 4
	numLoopsPerThread := 10
	var wg sync.WaitGroup
	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go func(bo *MyBo) {
			for j := 0; j < numLoopsPerThread; j++ {
				if _, err := dao.GdaoCreate(dao.collectionName, bo.ToGbo()); err != nil {
					t.Fatalf("%s failed - Thread: %v / Error: %e", name, i, err)
				}
				bo.Version = bo.Version + 1
			}
			wg.Done()
		}(&MyBo{
			Id:      "0",
			Name:    "BO - " + strconv.Itoa(i),
			Version: 0,
		})
	}
	wg.Wait()

	gbo, err := dao.GdaoFetchOne(dao.collectionName, map[string]interface{}{fieldId: "0"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != "0" || myBo.Version != 0 {
		t.Fatalf("%s failed - Received: %v", name, myBo)
	}
}

func TestGenericDaoMongo_GdaoCreateDelete(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoCreateDelete"
	dao := initDao()
	bo := &MyBo{
		Id:      "1",
		Name:    "BO - 1",
		Version: 0,
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
		Id:      "1",
		Name:    "BO - 1",
		Version: 0,
	}
	if numRows, err := dao.GdaoCreate(dao.collectionName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
	bo = &MyBo{
		Id:      "2",
		Name:    "BO - 2",
		Version: 1,
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
	for i := 0; i < 3; i++ {
		bo := &MyBo{
			Id:      strconv.Itoa(i),
			Name:    "BO - " + strconv.Itoa(i),
			Version: i,
		}
		if numRows, err := dao.GdaoCreate(dao.collectionName, bo.ToGbo()); err != nil || numRows != 1 {
			t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
		}
	}

	gbo, err := dao.GdaoFetchOne(dao.collectionName, map[string]interface{}{fieldId: "1"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	filter := dao.GdaoCreateFilter(collectionName, gbo)
	if numRows, err := dao.GdaoDeleteMany(dao.collectionName, filter); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	gbo, err = dao.GdaoFetchOne(dao.collectionName, map[string]interface{}{fieldId: "1"})
	if err != nil {
		t.Fatalf("%s failed, has error: %e", name, err)
	}
	if gbo != nil {
		t.Fatalf("%s failed, should have nill result, but received: %v", name, gbo)
	}

	gbo, err = dao.GdaoFetchOne(dao.collectionName, map[string]interface{}{fieldId: "0"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	gbo, err = dao.GdaoFetchOne(dao.collectionName, map[string]interface{}{fieldId: "2"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
}

func TestGenericDaoMongo_GdaoFetchAllWithSorting(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoCreateDeleteMany"
	dao := initDao()
	numItems := 100
	for i := 0; i < numItems; i++ {
		bo := &MyBo{
			Id:      strconv.Itoa(i),
			Name:    "BO - " + strconv.Itoa(i),
			Version: i,
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
			Id:      strconv.Itoa(i),
			Name:    "BO - " + strconv.Itoa(i),
			Version: i,
		}
		if numRows, err := dao.GdaoCreate(dao.collectionName, bo.ToGbo()); err != nil || numRows != 1 {
			t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
		}
	}

	gboList, err := dao.GdaoFetchMany(dao.collectionName, map[string]interface{}{"version": map[string]interface{}{"$gte": 80}}, map[string]int{"version": 1}, 5, 20)
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
		Id:      "1",
		Name:    "BO - 1",
		Version: 0,
	}
	if numRows, err := dao.GdaoUpdate(dao.collectionName, bo.ToGbo()); err != nil || numRows != 0 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
}

func TestGenericDaoMongo_GdaoUpdate(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoUpdate"
	dao := initDao()
	for i := 0; i < 3; i++ {
		bo := &MyBo{
			Id:      strconv.Itoa(i),
			Name:    "BO - " + strconv.Itoa(i),
			Version: i,
		}
		if numRows, err := dao.GdaoCreate(dao.collectionName, bo.ToGbo()); err != nil || numRows != 1 {
			t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
		}
	}

	bo := &MyBo{
		Id:      "0",
		Name:    "BO",
		Version: 100,
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

func TestGenericDaoMongo_GdaoSave(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoSave"
	dao := initDao()

	bo := &MyBo{
		Id:      "1",
		Name:    "BO - 1",
		Version: 0,
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
