package sql

import (
	"database/sql"
	"fmt"
	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/prom"
	_ "github.com/lib/pq"
	"strconv"
	"sync"
	"testing"
	"time"
)

func createPgsqlConnect() *prom.SqlConnect {
	driver := "postgres"
	dsn := "postgres://test:test@localhost:5432/test?sslmode=disable&client_encoding=UTF-8&application_name=godal"
	sqlConnect, err := prom.NewSqlConnect(driver, dsn, 10000, nil)
	if sqlConnect == nil || err != nil {
		if err != nil {
			fmt.Println("Error:", err)
		}
		if sqlConnect == nil {
			panic("error creating [prom.SqlConnect] instance")
		}
	}
	loc, _ := time.LoadLocation(timeZone)
	sqlConnect.SetLocation(loc)
	return sqlConnect
}

func initDataPgsql(sqlc *prom.SqlConnect, table string) {
	sql := fmt.Sprintf("DROP TABLE IF EXISTS %s", table)
	if _, err := sqlc.GetDB().Exec(sql); err != nil {
		panic(err)
	}
	sql = fmt.Sprintf("CREATE TABLE %s (id VARCHAR(64), data JSONB, PRIMARY KEY (id))", table)
	if _, err := sqlc.GetDB().Exec(sql); err != nil {
		panic(err)
	}
	sql = fmt.Sprintf("CREATE INDEX idx_%s_data ON %s USING gin(data)", table, table)
	if _, err := sqlc.GetDB().Exec(sql); err != nil {
		panic(err)
	}
}

func createDaoPgsql(sqlc *prom.SqlConnect, tableName string) *MyDaoPgsql {
	dao := &MyDaoPgsql{tableName: tableName}
	dao.GenericDaoSql = NewGenericDaoSql(sqlc, godal.NewAbstractGenericDao(dao))
	dao.SetSqlFlavor(prom.FlavorPgSql).SetRowMapper(&MyRowMapperSql{})
	return dao
}

type MyDaoPgsql struct {
	*GenericDaoSql
	tableName string
}

// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter.
func (dao *MyDaoPgsql) GdaoCreateFilter(storageId string, bo godal.IGenericBo) interface{} {
	return map[string]interface{}{colId: bo.GboGetAttrUnsafe(fieldGboId, reddo.TypeString)}
}

// /*----------------------------------------------------------------------*/
func initDaoPgsql() *MyDaoPgsql {
	sqlc := createPgsqlConnect()
	initDataPgsql(sqlc, tableName)
	return createDaoPgsql(sqlc, tableName)
}

func TestGenericDaoPgsql_Empty(t *testing.T) {
	name := "TestGenericDaoPgsql_Empty"
	dao := initDaoPgsql()

	boList, err := dao.GdaoFetchMany(dao.tableName, nil, nil, 0, 0)
	if err != nil {
		t.Fatalf("%s failed, has error: %e", name, err)
	}
	if boList == nil {
		t.Fatalf("%s failed, nil result", name)
	}
	if len(boList) != 0 {
		t.Fatalf("%s failed, non-empty result: %v", name, boList)
	}

	bo, err := dao.GdaoFetchOne(dao.tableName, map[string]interface{}{colId: "any"})
	if err != nil {
		t.Fatalf("%s failed, has error: %e", name, err)
	}
	if bo != nil {
		t.Fatalf("%s failed, should have nill result, but received: %v", name, bo)
	}
}

func TestGenericDaoPgsql_GdaoCreateGet(t *testing.T) {
	name := "TestGenericDaoPgsql_GdaoCreateGet"
	dao := initDaoPgsql()
	bo := &MyBo{
		Id:      "1",
		Name:    "BO - 1",
		Version: 1,
	}
	if numRows, err := dao.GdaoCreate(dao.tableName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	gbo, err := dao.GdaoFetchOne(dao.tableName, map[string]interface{}{colId: "1"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != bo.Id || myBo.Name != bo.Name || myBo.Version != bo.Version {
		t.Fatalf("%s failed - Expected: %v / Received: %v", name, bo, myBo)
	}
}

func TestGenericDaoPgsql_GdaoCreateTwiceGet_TxModeOff(t *testing.T) {
	name := "TestGenericDaoPgsql_GdaoCreateTwiceGet_TxModeOff"
	dao := initDaoPgsql()
	dao.SetTransactionMode(false, sql.LevelDefault)
	bo := &MyBo{
		Id:      "1",
		Name:    "BO - 1",
		Version: 1,
	}
	if numRows, err := dao.GdaoCreate(dao.tableName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
	bo.Version = bo.Version + 1
	if numRows, err := dao.GdaoCreate(dao.tableName, bo.ToGbo()); err != nil || numRows != 0 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	bo.Version = bo.Version - 1
	gbo, err := dao.GdaoFetchOne(dao.tableName, map[string]interface{}{colId: "1"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != bo.Id || myBo.Name != bo.Name || myBo.Version != bo.Version {
		t.Fatalf("%s failed - Expected: %v / Received: %v", name, bo, myBo)
	}
}

func TestGenericDaoPgsql_GdaoCreateTwiceGet_TxModeOn(t *testing.T) {
	name := "TestGenericDaoPgsql_GdaoCreateTwiceGet_TxModeOn"
	dao := initDaoPgsql()
	dao.SetTransactionMode(true, sql.LevelDefault)
	bo := &MyBo{
		Id:      "1",
		Name:    "BO - 1",
		Version: 1,
	}
	if numRows, err := dao.GdaoCreate(dao.tableName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
	bo.Version = bo.Version + 1
	if numRows, err := dao.GdaoCreate(dao.tableName, bo.ToGbo()); err != nil || numRows != 0 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	bo.Version = bo.Version - 1
	gbo, err := dao.GdaoFetchOne(dao.tableName, map[string]interface{}{colId: "1"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != bo.Id || myBo.Name != bo.Name || myBo.Version != bo.Version {
		t.Fatalf("%s failed - Expected: %v / Received: %v", name, bo, myBo)
	}
}

func TestGenericDaoPgsql_GdaoCreateMultiThreadsGet_TxModeOff(t *testing.T) {
	name := "TestGenericDaoPgsql_GdaoCreateMultiThreadsGet_TxModeOff"
	dao := initDaoPgsql()
	dao.SetTransactionMode(false, sql.LevelDefault)
	numThreads := 4
	numLoopsPerThread := 10
	var wg sync.WaitGroup
	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go func(threadNum int, bo *MyBo) {
			defer wg.Done()
			for j := 0; j < numLoopsPerThread; j++ {
				if _, err := dao.GdaoCreate(dao.tableName, bo.ToGbo()); err != nil {
					t.Fatalf("%s failed - Thread: %v / Error: %e", name, threadNum, err)
				}
				bo.Version = bo.Version + 1
			}
		}(i, &MyBo{
			Id:      "1",
			Name:    "BO - " + strconv.Itoa(i+1),
			Version: 1,
		})
	}
	wg.Wait()

	gbo, err := dao.GdaoFetchOne(dao.tableName, map[string]interface{}{colId: "1"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != "1" || myBo.Version != 1 {
		t.Fatalf("%s failed - Received: %v", name, myBo)
	}
}

func TestGenericDaoPgsql_GdaoCreateMultiThreadsGet_TxModeOn(t *testing.T) {
	name := "TestGenericDaoPgsql_GdaoCreateMultiThreadsGet_TxModeOn"
	dao := initDaoPgsql()
	dao.SetTransactionMode(true, sql.LevelRepeatableRead)
	numThreads := 8
	numLoopsPerThread := 10
	var wg sync.WaitGroup
	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go func(threadNum int, bo *MyBo) {
			defer wg.Done()
			for j := 0; j < numLoopsPerThread; j++ {
				if _, err := dao.GdaoCreate(dao.tableName, bo.ToGbo()); err != nil {
					t.Fatalf("%s failed - Thread: %v / Error: %e", name, threadNum, err)
				}
				bo.Version = bo.Version + 1
			}
		}(i, &MyBo{
			Id:      "1",
			Name:    "BO - " + strconv.Itoa(i+1),
			Version: 1,
		})
	}
	wg.Wait()

	gbo, err := dao.GdaoFetchOne(dao.tableName, map[string]interface{}{colId: "1"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != "1" || myBo.Version != 1 {
		t.Fatalf("%s failed - Received: %v", name, myBo)
	}
}

func TestGenericDaoPgsql_GdaoCreateDelete(t *testing.T) {
	name := "TestGenericDaoPgsql_GdaoCreateDelete"
	dao := initDaoPgsql()
	bo := &MyBo{
		Id:      "1",
		Name:    "BO - 1",
		Version: 1,
	}
	if numRows, err := dao.GdaoCreate(dao.tableName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	gbo, err := dao.GdaoFetchOne(dao.tableName, map[string]interface{}{colId: "1"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != bo.Id || myBo.Name != bo.Name || myBo.Version != bo.Version {
		t.Fatalf("%s failed - Expected: %v / Received: %v", name, bo, myBo)
	}

	if numRows, err := dao.GdaoDelete(dao.tableName, gbo); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	gbo, err = dao.GdaoFetchOne(dao.tableName, map[string]interface{}{colId: "1"})
	if err != nil {
		t.Fatalf("%s failed, has error: %e", name, err)
	}
	if gbo != nil {
		t.Fatalf("%s failed, should have nill result, but received: %v", name, gbo)
	}
}

func TestGenericDaoPgsql_GdaoCreateDeleteAll(t *testing.T) {
	name := "TestGenericDaoPgsql_GdaoCreateDeleteAll"
	dao := initDaoPgsql()
	bo := &MyBo{
		Id:      "1",
		Name:    "BO - 1",
		Version: 1,
	}
	if numRows, err := dao.GdaoCreate(dao.tableName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
	bo = &MyBo{
		Id:      "2",
		Name:    "BO - 2",
		Version: 2,
	}
	if numRows, err := dao.GdaoCreate(dao.tableName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	if numRows, err := dao.GdaoDeleteMany(dao.tableName, nil); err != nil || numRows != 2 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	gbo, err := dao.GdaoFetchOne(dao.tableName, map[string]interface{}{colId: "1"})
	if err != nil {
		t.Fatalf("%s failed, has error: %e", name, err)
	}
	if gbo != nil {
		t.Fatalf("%s failed, should have nill result, but received: %v", name, gbo)
	}
}

func TestGenericDaoPgsql_GdaoCreateDeleteMany(t *testing.T) {
	name := "TestGenericDaoPgsql_GdaoCreateDeleteMany"
	dao := initDaoPgsql()
	for i := 1; i <= 3; i++ {
		bo := &MyBo{
			Id:      strconv.Itoa(i),
			Name:    "BO - " + strconv.Itoa(i),
			Version: i,
		}
		if numRows, err := dao.GdaoCreate(dao.tableName, bo.ToGbo()); err != nil || numRows != 1 {
			t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
		}
	}

	gbo, err := dao.GdaoFetchOne(dao.tableName, map[string]interface{}{colId: "2"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	filter := dao.GdaoCreateFilter(tableName, gbo)
	if numRows, err := dao.GdaoDeleteMany(dao.tableName, filter); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	gbo, err = dao.GdaoFetchOne(dao.tableName, map[string]interface{}{colId: "2"})
	if err != nil {
		t.Fatalf("%s failed, has error: %e", name, err)
	}
	if gbo != nil {
		t.Fatalf("%s failed, should have nill result, but received: %v", name, gbo)
	}

	gbo, err = dao.GdaoFetchOne(dao.tableName, map[string]interface{}{colId: "1"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	gbo, err = dao.GdaoFetchOne(dao.tableName, map[string]interface{}{colId: "3"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
}

func TestGenericDaoPgsql_GdaoFetchAllWithSorting(t *testing.T) {
	name := "TestGenericDaoPgsql_GdaoFetchAllWithSorting"
	dao := initDaoPgsql()
	numItems := 100
	for i := 1; i <= numItems; i++ {
		bo := &MyBo{
			Id:      fmt.Sprintf("%03d", i),
			Name:    "BO - " + strconv.Itoa(i),
			Version: i,
		}
		if numRows, err := dao.GdaoCreate(dao.tableName, bo.ToGbo()); err != nil || numRows != 1 {
			t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
		}
	}

	gboList, err := dao.GdaoFetchMany(dao.tableName, nil, map[string]int{colId: -1}, 0, 0)
	if err != nil || gboList == nil || len(gboList) != 100 {
		t.Fatalf("%s failed - NumItems: %v / Error: %e", name, len(gboList), err)
	}

	for i, gbo := range gboList {
		if bo := fromGbo(gbo); bo.Id != fmt.Sprintf("%03d", numItems-i) {
			t.Fatalf("%s failed - Expected: %v / Received: %v", name, numItems-i, bo)
		}
	}
}

func TestGenericDaoPgsql_GdaoFetchManyWithPaging(t *testing.T) {
	name := "TestGenericDaoPgsql_GdaoFetchManyWithPaging"
	dao := initDaoPgsql()
	numItems := 100
	for i := 0; i < numItems; i++ {
		bo := &MyBo{
			Id:      fmt.Sprintf("%03d", i),
			Name:    "BO - " + strconv.Itoa(i),
			Version: i,
		}
		if numRows, err := dao.GdaoCreate(dao.tableName, bo.ToGbo()); err != nil || numRows != 1 {
			t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
		}
	}

	gboList, err := dao.GdaoFetchMany(dao.tableName, &FilterFieldValue{Field: colId, Operation: ">=", Value: "080"}, map[string]int{colId: 1}, 5, 20)
	if err != nil || gboList == nil || len(gboList) != 15 {
		t.Fatalf("%s failed - NumItems: %v / Error: %e", name, len(gboList), err)
	}

	for i, gbo := range gboList {
		if bo := fromGbo(gbo); bo.Id != fmt.Sprintf("%03d", 80+i+5) {
			t.Fatalf("%s failed - Expected: %v / Received: %v", name, 80+i+5, bo)
		}
	}
}

func TestGenericDaoPgsql_GdaoUpdateNotExist(t *testing.T) {
	name := "TestGenericDaoPgsql_GdaoUpdateNotExist"
	dao := initDaoPgsql()
	bo := &MyBo{
		Id:      "1",
		Name:    "BO - 1",
		Version: 1,
	}
	if numRows, err := dao.GdaoUpdate(dao.tableName, bo.ToGbo()); err != nil || numRows != 0 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
}

func TestGenericDaoPgsql_GdaoUpdate(t *testing.T) {
	name := "TestGenericDaoPgsql_GdaoUpdate"
	dao := initDaoPgsql()
	for i := 0; i < 3; i++ {
		bo := &MyBo{
			Id:      strconv.Itoa(i),
			Name:    "BO - " + strconv.Itoa(i),
			Version: i,
		}
		if numRows, err := dao.GdaoCreate(dao.tableName, bo.ToGbo()); err != nil || numRows != 1 {
			t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
		}
	}

	bo := &MyBo{
		Id:      "0",
		Name:    "BO",
		Version: 100,
	}
	if numRows, err := dao.GdaoUpdate(dao.tableName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	for i := 0; i < 3; i++ {
		gbo, err := dao.GdaoFetchOne(dao.tableName, map[string]interface{}{colId: strconv.Itoa(i)})
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

func TestGenericDaoPgsql_GdaoSave(t *testing.T) {
	name := "TestGenericDaoPgsql_GdaoSave"
	dao := initDaoPgsql()

	bo := &MyBo{
		Id:      "1",
		Name:    "BO - 1",
		Version: 1,
	}
	if numRows, err := dao.GdaoSave(dao.tableName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
	gbo, err := dao.GdaoFetchOne(dao.tableName, map[string]interface{}{colId: "1"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != bo.Id || myBo.Name != bo.Name || myBo.Version != bo.Version {
		t.Fatalf("%s failed - Expected: %v / Received: %v", name, bo, myBo)
	}

	bo.Name = "BO"
	bo.Version = 10
	if numRows, err := dao.GdaoSave(dao.tableName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
	gbo, err = dao.GdaoFetchOne(dao.tableName, map[string]interface{}{colId: "1"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != bo.Id || myBo.Name != bo.Name || myBo.Version != bo.Version {
		t.Fatalf("%s failed - Expected: %v / Received: %v", name, bo, myBo)
	}
}
