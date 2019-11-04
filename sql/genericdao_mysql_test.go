package sql

import (
	"database/sql"
	"fmt"
	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/prom"
	_ "github.com/go-sql-driver/mysql"
	"strings"
	"testing"
	"time"
)

func createMysqlConnect() *prom.SqlConnect {
	driver := "mysql"
	dsn := "test:test@tcp(localhost:3306)/test?charset=utf8mb4,utf8&parseTime=true&loc="
	dsn = dsn + strings.Replace(timeZone, "/", "%2f", -1)
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

func initDataMysql(sqlc *prom.SqlConnect, table string) {
	sql := fmt.Sprintf("DROP TABLE IF EXISTS %s", table)
	if _, err := sqlc.GetDB().Exec(sql); err != nil {
		panic(err)
	}
	sql = fmt.Sprintf("CREATE TABLE %s (id VARCHAR(64), username VARCHAR(64), UNIQUE INDEX (username), data TEXT, PRIMARY KEY (id)) ENGINE InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci", table)
	if _, err := sqlc.GetDB().Exec(sql); err != nil {
		panic(err)
	}
}

func createDaoMysql(sqlc *prom.SqlConnect, tableName string) *MyDaoMysql {
	dao := &MyDaoMysql{tableName: tableName}
	dao.GenericDaoSql = NewGenericDaoSql(sqlc, godal.NewAbstractGenericDao(dao))
	dao.SetSqlFlavor(prom.FlavorMySql).SetRowMapper(&MyRowMapperSql{})
	return dao
}

type MyDaoMysql struct {
	*GenericDaoSql
	tableName string
}

// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter.
func (dao *MyDaoMysql) GdaoCreateFilter(storageId string, bo godal.IGenericBo) interface{} {
	return map[string]interface{}{colId: bo.GboGetAttrUnsafe(fieldGboId, reddo.TypeString)}
}

/*----------------------------------------------------------------------*/
func initDaoMysql() *MyDaoMysql {
	sqlc := createMysqlConnect()
	initDataMysql(sqlc, tableName)
	return createDaoMysql(sqlc, tableName)
}

func TestGenericDaoMysql_Empty(t *testing.T) {
	dao := initDaoMysql()
	testGenericDao_Empty(dao, dao.tableName, t)
}

func TestGenericDaoMysql_GdaoCreateDuplicated(t *testing.T) {
	dao := initDaoMysql()
	testGenericDao_GdaoCreateDuplicated(dao, dao.tableName, t)
}

func TestGenericDaoMysql_GdaoCreateGet(t *testing.T) {
	dao := initDaoMysql()
	testGenericDao_GdaoCreateGet(dao, dao.tableName, t)
}

func TestGenericDaoMysql_GdaoCreateTwiceGet(t *testing.T) {
	dao := initDaoMysql()
	testGenericDao_GdaoCreateTwiceGet(dao, dao.tableName, t)
}

func TestGenericDaoMysql_GdaoCreateMultiThreadsGet(t *testing.T) {
	dao := initDaoMysql()
	testGenericDao_GdaoCreateMultiThreadsGet(dao, dao.tableName, t)
}

func TestGenericDaoMysql_GdaoCreateDelete(t *testing.T) {
	dao := initDaoMysql()
	testGenericDao_GdaoCreateDelete(dao, dao.tableName, t)
}

func TestGenericDaoMysql_GdaoCreateDeleteAll(t *testing.T) {
	dao := initDaoMysql()
	testGenericDao_GdaoCreateDeleteAll(dao, dao.tableName, t)
}

func TestGenericDaoMysql_GdaoCreateDeleteMany(t *testing.T) {
	dao := initDaoMysql()
	testGenericDao_GdaoCreateDeleteMany(dao, dao.tableName, t)
}

func TestGenericDaoMysql_GdaoFetchAllWithSorting(t *testing.T) {
	dao := initDaoMysql()
	testGenericDao_GdaoFetchAllWithSorting(dao, dao.tableName, t)
}

func TestGenericDaoMysql_GdaoFetchManyWithPaging(t *testing.T) {
	dao := initDaoMysql()
	testGenericDao_GdaoFetchManyWithPaging(dao, dao.tableName, t)
}

func TestGenericDaoMysql_GdaoUpdateNotExist(t *testing.T) {
	dao := initDaoMysql()
	testGenericDao_GdaoUpdateNotExist(dao, dao.tableName, t)
}

func TestGenericDaoMysql_GdaoUpdateDuplicated(t *testing.T) {
	dao := initDaoMysql()
	testGenericDao_GdaoUpdateDuplicated(dao, dao.tableName, t)
}

func TestGenericDaoMysql_GdaoUpdate(t *testing.T) {
	dao := initDaoMysql()
	testGenericDao_GdaoUpdate(dao, dao.tableName, t)
}

func TestGenericDaoMysql_GdaoSaveDuplicated_TxModeOff(t *testing.T) {
	dao := initDaoMysql()
	dao.SetTxModeOnWrite(false).SetTxIsolationLevel(sql.LevelDefault)
	testGenericDao_GdaoSaveDuplicated_TxModeOff(dao, dao.tableName, t)
}

func TestGenericDaoMysql_GdaoSaveDuplicated_TxModeOn(t *testing.T) {
	dao := initDaoMysql()
	dao.SetTxModeOnWrite(true).SetTxIsolationLevel(sql.LevelDefault)
	testGenericDao_GdaoSaveDuplicated_TxModeOn(dao, dao.tableName, t)
}

func TestGenericDaoMysql_GdaoSave_TxModeOff(t *testing.T) {
	dao := initDaoMysql()
	dao.SetTxModeOnWrite(false).SetTxIsolationLevel(sql.LevelDefault)
	testGenericDao_GdaoSave_TxModeOff(dao, dao.tableName, t)
}

func TestGenericDaoMysql_GdaoSave_TxModeOn(t *testing.T) {
	dao := initDaoMysql()
	dao.SetTxModeOnWrite(true).SetTxIsolationLevel(sql.LevelDefault)
	testGenericDao_GdaoSave_TxModeOn(dao, dao.tableName, t)
}
