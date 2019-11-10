package sql

import (
	"database/sql"
	"fmt"
	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/prom"
	_ "gopkg.in/goracle.v2"
	"strings"
	"testing"
	"time"
)

func createOracleConnect() *prom.SqlConnect {
	driver := "goracle"
	dsn := "test/Test1@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=localhost)(PORT=1521)))(CONNECT_DATA=(SID=ORCLCDB)))"
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
	sql := "ALTER SESSION SET TIME_ZONE='" + timeZone + "'"
	_, err = sqlConnect.GetDB().Exec(sql)
	if err != nil {
		panic(err)
	}
	return sqlConnect
}

func initDataOracle(sqlc *prom.SqlConnect, table string) {
	sql := fmt.Sprintf("DROP TABLE %s", table)
	if _, err := sqlc.GetDB().Exec(sql); err != nil {
		if !strings.Contains(err.Error(), "ORA-00942") {
			panic(err)
		}
	}
	sql = fmt.Sprintf("CREATE TABLE %s (id NVARCHAR2(64), username NVARCHAR2(64), data CLOB, PRIMARY KEY (id))", table)
	if _, err := sqlc.GetDB().Exec(sql); err != nil {
		panic(err)
	}
	sql = fmt.Sprintf("CREATE UNIQUE INDEX uidx_%s_username ON %s(username)", table, table)
	if _, err := sqlc.GetDB().Exec(sql); err != nil {
		panic(err)
	}
}

func createDaoOracle(sqlc *prom.SqlConnect, tableName string) *MyDaoOracle {
	dao := &MyDaoOracle{tableName: tableName}
	dao.GenericDaoSql = NewGenericDaoSql(sqlc, godal.NewAbstractGenericDao(dao))
	dao.SetSqlFlavor(prom.FlavorOracle).SetRowMapper(&GenericRowMapperSql{NameTransformation: NameTransfLowerCase, ColumnsListMap: nil})
	return dao
}

type MyDaoOracle struct {
	*GenericDaoSql
	tableName string
}

// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter.
func (dao *MyDaoOracle) GdaoCreateFilter(storageId string, bo godal.IGenericBo) interface{} {
	return map[string]interface{}{colId: bo.GboGetAttrUnsafe(fieldGboId, reddo.TypeString)}
}

// /*----------------------------------------------------------------------*/
func initDaoOracle() *MyDaoOracle {
	sqlc := createOracleConnect()
	initDataOracle(sqlc, tableName)
	return createDaoOracle(sqlc, tableName)
}

func TestGenericDaoOracle_Empty(t *testing.T) {
	dao := initDaoOracle()
	testGenericDao_Empty(dao, dao.tableName, t)
}

func TestGenericDaoOracle_GdaoCreateDuplicated(t *testing.T) {
	dao := initDaoOracle()
	testGenericDao_GdaoCreateDuplicated(dao, dao.tableName, t)
}

func TestGenericDaoOracle_GdaoCreateGet(t *testing.T) {
	dao := initDaoOracle()
	testGenericDao_GdaoCreateGet(dao, dao.tableName, t)
}

func TestGenericDaoOracle_GdaoCreateTwiceGet(t *testing.T) {
	dao := initDaoOracle()
	testGenericDao_GdaoCreateTwiceGet(dao, dao.tableName, t)
}

func TestGenericDaoOracle_GdaoCreateMultiThreadsGet(t *testing.T) {
	dao := initDaoOracle()
	testGenericDao_GdaoCreateMultiThreadsGet(dao, dao.tableName, t)
}

func TestGenericDaoOracle_GdaoCreateDelete(t *testing.T) {
	dao := initDaoOracle()
	testGenericDao_GdaoCreateDelete(dao, dao.tableName, t)
}

func TestGenericDaoOracle_GdaoCreateDeleteAll(t *testing.T) {
	dao := initDaoOracle()
	testGenericDao_GdaoCreateDeleteAll(dao, dao.tableName, t)
}

func TestGenericDaoOracle_GdaoCreateDeleteMany(t *testing.T) {
	dao := initDaoOracle()
	testGenericDao_GdaoCreateDeleteMany(dao, dao.tableName, t)
}

func TestGenericDaoOracle_GdaoFetchAllWithSorting(t *testing.T) {
	dao := initDaoOracle()
	testGenericDao_GdaoFetchAllWithSorting(dao, dao.tableName, t)
}

func TestGenericDaoOracle_GdaoFetchManyWithPaging(t *testing.T) {
	dao := initDaoOracle()
	testGenericDao_GdaoFetchManyWithPaging(dao, dao.tableName, t)
}

func TestGenericDaoOracle_GdaoUpdateNotExist(t *testing.T) {
	dao := initDaoOracle()
	testGenericDao_GdaoUpdateNotExist(dao, dao.tableName, t)
}

func TestGenericDaoOracle_GdaoUpdateDuplicated(t *testing.T) {
	dao := initDaoOracle()
	testGenericDao_GdaoUpdateDuplicated(dao, dao.tableName, t)
}

func TestGenericDaoOracle_GdaoUpdate(t *testing.T) {
	dao := initDaoOracle()
	testGenericDao_GdaoUpdate(dao, dao.tableName, t)
}

func TestGenericDaoOracle_GdaoSaveDuplicated_TxModeOff(t *testing.T) {
	dao := initDaoOracle()
	dao.SetTxModeOnWrite(false).SetTxIsolationLevel(sql.LevelDefault)
	testGenericDao_GdaoSaveDuplicated_TxModeOff(dao, dao.tableName, t)
}

func TestGenericDaoOracle_GdaoSaveDuplicated_TxModeOn(t *testing.T) {
	dao := initDaoOracle()
	dao.SetTxModeOnWrite(true).SetTxIsolationLevel(sql.LevelDefault)
	testGenericDao_GdaoSaveDuplicated_TxModeOn(dao, dao.tableName, t)
}

func TestGenericDaoOracle_GdaoSave_TxModeOff(t *testing.T) {
	dao := initDaoOracle()
	dao.SetTxModeOnWrite(false).SetTxIsolationLevel(sql.LevelDefault)
	testGenericDao_GdaoSave_TxModeOff(dao, dao.tableName, t)
}

func TestGenericDaoOracle_GdaoSave_TxModeOn(t *testing.T) {
	dao := initDaoOracle()
	dao.SetTxModeOnWrite(true).SetTxIsolationLevel(sql.LevelDefault)
	testGenericDao_GdaoSave_TxModeOn(dao, dao.tableName, t)
}
