package sql

import (
	"database/sql"
	"fmt"
	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/prom"
	_ "github.com/denisenkom/go-mssqldb"
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
	sql = fmt.Sprintf("CREATE TABLE %s (id NVARCHAR(64), username NVARCHAR(64), data NTEXT, PRIMARY KEY (id))", table)
	if _, err := sqlc.GetDB().Exec(sql); err != nil {
		panic(err)
	}
	sql = fmt.Sprintf("CREATE UNIQUE INDEX uidx_%s_username ON %s(username)", table, table)
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
	dao := initDaoMssql()
	testGenericDao_Empty(dao, dao.tableName, t)
}

func TestGenericDaoMssql_GdaoCreateDuplicated(t *testing.T) {
	dao := initDaoMssql()
	testGenericDao_GdaoCreateDuplicated(dao, dao.tableName, t)
}

func TestGenericDaoMssql_GdaoCreateGet(t *testing.T) {
	dao := initDaoMssql()
	testGenericDao_GdaoCreateGet(dao, dao.tableName, t)
}

func TestGenericDaoMssql_GdaoCreateTwiceGet(t *testing.T) {
	dao := initDaoMssql()
	testGenericDao_GdaoCreateTwiceGet(dao, dao.tableName, t)
}

func TestGenericDaoMssql_GdaoCreateMultiThreadsGet(t *testing.T) {
	dao := initDaoMssql()
	testGenericDao_GdaoCreateMultiThreadsGet(dao, dao.tableName, t)
}

func TestGenericDaoMssql_GdaoCreateDelete(t *testing.T) {
	dao := initDaoMssql()
	testGenericDao_GdaoCreateDelete(dao, dao.tableName, t)
}

func TestGenericDaoMssql_GdaoCreateDeleteAll(t *testing.T) {
	dao := initDaoMssql()
	testGenericDao_GdaoCreateDeleteAll(dao, dao.tableName, t)
}

func TestGenericDaoMssql_GdaoCreateDeleteMany(t *testing.T) {
	dao := initDaoMssql()
	testGenericDao_GdaoCreateDeleteMany(dao, dao.tableName, t)
}

func TestGenericDaoMssql_GdaoFetchAllWithSorting(t *testing.T) {
	dao := initDaoMssql()
	testGenericDao_GdaoFetchAllWithSorting(dao, dao.tableName, t)
}

func TestGenericDaoMssql_GdaoFetchManyWithPaging(t *testing.T) {
	dao := initDaoMssql()
	testGenericDao_GdaoFetchManyWithPaging(dao, dao.tableName, t)
}

func TestGenericDaoMssql_GdaoUpdateNotExist(t *testing.T) {
	dao := initDaoMssql()
	testGenericDao_GdaoUpdateNotExist(dao, dao.tableName, t)
}

func TestGenericDaoMssql_GdaoUpdateDuplicated(t *testing.T) {
	dao := initDaoMssql()
	testGenericDao_GdaoUpdateDuplicated(dao, dao.tableName, t)
}

func TestGenericDaoMssql_GdaoUpdate(t *testing.T) {
	dao := initDaoMssql()
	testGenericDao_GdaoUpdate(dao, dao.tableName, t)
}

func TestGenericDaoMssql_GdaoSaveDuplicated_TxModeOff(t *testing.T) {
	dao := initDaoMssql()
	dao.SetTxModeOnWrite(false).SetTxIsolationLevel(sql.LevelDefault)
	testGenericDao_GdaoSaveDuplicated_TxModeOff(dao, dao.tableName, t)
}

func TestGenericDaoMssql_GdaoSaveDuplicated_TxModeOn(t *testing.T) {
	dao := initDaoMssql()
	dao.SetTxModeOnWrite(true).SetTxIsolationLevel(sql.LevelDefault)
	testGenericDao_GdaoSaveDuplicated_TxModeOn(dao, dao.tableName, t)
}

func TestGenericDaoMssql_GdaoSave_TxModeOff(t *testing.T) {
	dao := initDaoMssql()
	dao.SetTxModeOnWrite(false).SetTxIsolationLevel(sql.LevelDefault)
	testGenericDao_GdaoSave_TxModeOff(dao, dao.tableName, t)
}

func TestGenericDaoMssql_GdaoSave_TxModeOn(t *testing.T) {
	dao := initDaoMssql()
	dao.SetTxModeOnWrite(true).SetTxIsolationLevel(sql.LevelDefault)
	testGenericDao_GdaoSave_TxModeOn(dao, dao.tableName, t)
}
