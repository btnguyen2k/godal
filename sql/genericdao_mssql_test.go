package sql

import (
	"database/sql"
	"fmt"
	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/prom"
	_ "github.com/denisenkom/go-mssqldb"
	"strconv"
	"sync"
	"testing"
	"time"
)

func createMssqlConnect() *prom.SqlConnect {
	driver := "sqlserver"
	dsn := "sqlserver://test:Test1Test1@localhost:1433?database=tempdb"
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

func initDataMssql(sqlc *prom.SqlConnect, table string) {
	sql := fmt.Sprintf("DROP TABLE IF EXISTS %s", table)
	if _, err := sqlc.GetDB().Exec(sql); err != nil {
		panic(err)
	}
	sql = fmt.Sprintf("CREATE TABLE %s (id NVARCHAR(64), data NTEXT, PRIMARY KEY (id))", table)
	if _, err := sqlc.GetDB().Exec(sql); err != nil {
		panic(err)
	}
}

func createDaoMssql(sqlc *prom.SqlConnect, tableName string) *MyDaoMssql {
	dao := &MyDaoMssql{tableName: tableName}
	dao.GenericDaoSql = NewGenericDaoSql(sqlc, godal.NewAbstractGenericDao(dao))
	dao.SetSqlFlavor(prom.FlavorMsSql).SetRowMapper(&MyRowMapperSql{})
	return dao
}

type MyDaoMssql struct {
	*GenericDaoSql
	tableName string
}

// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter.
func (dao *MyDaoMssql) GdaoCreateFilter(storageId string, bo godal.IGenericBo) interface{} {
	return map[string]interface{}{colId: bo.GboGetAttrUnsafe(fieldGboId, reddo.TypeString)}
}

// /*----------------------------------------------------------------------*/
func initDaoMssql() *MyDaoMssql {
	sqlc := createMssqlConnect()
	initDataMssql(sqlc, tableName)
	return createDaoMssql(sqlc, tableName)
}

func TestGenericDaoMssql_Empty(t *testing.T) {
	name := "TestGenericDaoMssql_Empty"
	dao := initDaoMssql()

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

func TestGenericDaoMssql_GdaoCreateGet(t *testing.T) {
	name := "TestGenericDaoMssql_GdaoCreateGet"
	dao := initDaoMssql()
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

func TestGenericDaoMssql_GdaoCreateTwiceGet_TxModeOff(t *testing.T) {
	name := "TestGenericDaoMssql_GdaoCreateTwiceGet_TxModeOff"
	dao := initDaoMssql()
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

func TestGenericDaoMssql_GdaoCreateTwiceGet_TxModeOn(t *testing.T) {
	name := "TestGenericDaoMssql_GdaoCreateTwiceGet_TxModeOn"
	dao := initDaoMssql()
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

func TestGenericDaoMssql_GdaoCreateMultiThreadsGet_TxModeOff(t *testing.T) {
	name := "TestGenericDaoMssql_GdaoCreateMultiThreadsGet_TxModeOff"
	dao := initDaoMssql()
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

func TestGenericDaoMssql_GdaoCreateMultiThreadsGet_TxModeOn(t *testing.T) {
	name := "TestGenericDaoMssql_GdaoCreateMultiThreadsGet_TxModeOn"
	dao := initDaoMssql()
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

func TestGenericDaoMssql_GdaoCreateDelete(t *testing.T) {
	name := "TestGenericDaoMssql_GdaoCreateDelete"
	dao := initDaoMssql()
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

func TestGenericDaoMssql_GdaoCreateDeleteAll(t *testing.T) {
	name := "TestGenericDaoMssql_GdaoCreateDeleteAll"
	dao := initDaoMssql()
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

func TestGenericDaoMssql_GdaoCreateDeleteMany(t *testing.T) {
	name := "TestGenericDaoMssql_GdaoCreateDeleteMany"
	dao := initDaoMssql()
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

func TestGenericDaoMssql_GdaoFetchAllWithSorting(t *testing.T) {
	name := "TestGenericDaoMssql_GdaoFetchAllWithSorting"
	dao := initDaoMssql()
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

func TestGenericDaoMssql_GdaoFetchManyWithPaging(t *testing.T) {
	name := "TestGenericDaoMssql_GdaoFetchManyWithPaging"
	dao := initDaoMssql()
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

func TestGenericDaoMssql_GdaoUpdateNotExist(t *testing.T) {
	name := "TestGenericDaoMssql_GdaoUpdateNotExist"
	dao := initDaoMssql()
	bo := &MyBo{
		Id:      "1",
		Name:    "BO - 1",
		Version: 1,
	}
	if numRows, err := dao.GdaoUpdate(dao.tableName, bo.ToGbo()); err != nil || numRows != 0 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
}

func TestGenericDaoMssql_GdaoUpdate(t *testing.T) {
	name := "TestGenericDaoMssql_GdaoUpdate"
	dao := initDaoMssql()
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

func TestGenericDaoMssql_GdaoSave(t *testing.T) {
	name := "TestGenericDaoMssql_GdaoSave"
	dao := initDaoMssql()

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
