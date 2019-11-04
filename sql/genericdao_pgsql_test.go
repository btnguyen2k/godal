package sql

import (
	"database/sql"
	"fmt"
	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/prom"
	_ "github.com/lib/pq"
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
	sql = fmt.Sprintf("CREATE TABLE %s (id VARCHAR(64), username VARCHAR(64), data JSONB, PRIMARY KEY (id))", table)
	if _, err := sqlc.GetDB().Exec(sql); err != nil {
		panic(err)
	}
	sql = fmt.Sprintf("CREATE UNIQUE INDEX uidx_%s_username ON %s(username)", table, table)
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

/*----------------------------------------------------------------------*/
func initDaoPgsql() *MyDaoPgsql {
	sqlc := createPgsqlConnect()
	initDataPgsql(sqlc, tableName)
	return createDaoPgsql(sqlc, tableName)
}

func TestGenericDaoPgsql_Empty(t *testing.T) {
	dao := initDaoPgsql()
	testGenericDao_Empty(dao, dao.tableName, t)
}

func TestGenericDaoPgsql_GdaoCreateDuplicated(t *testing.T) {
	dao := initDaoPgsql()
	testGenericDao_GdaoCreateDuplicated(dao, dao.tableName, t)
}

func TestGenericDaoPgsql_GdaoCreateGet(t *testing.T) {
	dao := initDaoPgsql()
	testGenericDao_GdaoCreateGet(dao, dao.tableName, t)
}

func TestGenericDaoPgsql_GdaoCreateTwiceGet(t *testing.T) {
	dao := initDaoPgsql()
	testGenericDao_GdaoCreateTwiceGet(dao, dao.tableName, t)
}

func TestGenericDaoPgsql_GdaoCreateMultiThreadsGet(t *testing.T) {
	dao := initDaoPgsql()
	testGenericDao_GdaoCreateMultiThreadsGet(dao, dao.tableName, t)
}

func TestGenericDaoPgsql_GdaoCreateDelete(t *testing.T) {
	dao := initDaoPgsql()
	testGenericDao_GdaoCreateDelete(dao, dao.tableName, t)
}

func TestGenericDaoPgsql_GdaoCreateDeleteAll(t *testing.T) {
	dao := initDaoPgsql()
	testGenericDao_GdaoCreateDeleteAll(dao, dao.tableName, t)
}

func TestGenericDaoPgsql_GdaoCreateDeleteMany(t *testing.T) {
	dao := initDaoPgsql()
	testGenericDao_GdaoCreateDeleteMany(dao, dao.tableName, t)
}

func TestGenericDaoPgsql_GdaoFetchAllWithSorting(t *testing.T) {
	dao := initDaoPgsql()
	testGenericDao_GdaoFetchAllWithSorting(dao, dao.tableName, t)
}

func TestGenericDaoPgsql_GdaoFetchManyWithPaging(t *testing.T) {
	dao := initDaoPgsql()
	testGenericDao_GdaoFetchManyWithPaging(dao, dao.tableName, t)
}

func TestGenericDaoPgsql_GdaoUpdateNotExist(t *testing.T) {
	dao := initDaoPgsql()
	testGenericDao_GdaoUpdateNotExist(dao, dao.tableName, t)
}

func TestGenericDaoPgsql_GdaoUpdateDuplicated(t *testing.T) {
	dao := initDaoPgsql()
	testGenericDao_GdaoUpdateDuplicated(dao, dao.tableName, t)
}

func TestGenericDaoPgsql_GdaoUpdate(t *testing.T) {
	dao := initDaoPgsql()
	testGenericDao_GdaoUpdate(dao, dao.tableName, t)
}

func TestGenericDaoPgsql_GdaoSaveDuplicated_TxModeOff(t *testing.T) {
	dao := initDaoPgsql()
	dao.SetTxModeOnWrite(false).SetTxIsolationLevel(sql.LevelDefault)
	testGenericDao_GdaoSaveDuplicated_TxModeOff(dao, dao.tableName, t)
}

func TestGenericDaoPgsql_GdaoSaveDuplicated_TxModeOn(t *testing.T) {
	dao := initDaoPgsql()
	dao.SetTxModeOnWrite(true).SetTxIsolationLevel(sql.LevelDefault)
	testGenericDao_GdaoSaveDuplicated_TxModeOn(dao, dao.tableName, t)
}

func TestGenericDaoPgsql_GdaoSave_TxModeOff(t *testing.T) {
	dao := initDaoPgsql()
	dao.SetTxModeOnWrite(false).SetTxIsolationLevel(sql.LevelDefault)
	testGenericDao_GdaoSave_TxModeOff(dao, dao.tableName, t)
}

func TestGenericDaoPgsql_GdaoSave_TxModeOn(t *testing.T) {
	dao := initDaoPgsql()
	dao.SetTxModeOnWrite(true).SetTxIsolationLevel(sql.LevelDefault)
	testGenericDao_GdaoSave_TxModeOn(dao, dao.tableName, t)
}
