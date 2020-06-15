package sql

import (
	"fmt"
	"os"
	"testing"

	"github.com/btnguyen2k/prom"
	_ "github.com/godror/godror"
)

func prepareTableOracle(sqlc *prom.SqlConnect, table string) error {
	sql := fmt.Sprintf("DROP TABLE %s", table)
	sqlc.GetDB().Exec(sql)
	sql = fmt.Sprintf("CREATE TABLE %s (%s NVARCHAR2(64), %s NVARCHAR2(64), %s CLOB, PRIMARY KEY (%s))", table, colSqlId, colSqlUsername, colSqlData, colSqlId)
	if _, err := sqlc.GetDB().Exec(sql); err != nil {
		fmt.Println(sql, err)
		return err
	}
	sql = fmt.Sprintf("CREATE UNIQUE INDEX uidx_%s_username ON %s(%s)", table, table, colSqlUsername)
	if _, err := sqlc.GetDB().Exec(sql); err != nil {
		fmt.Println(sql, err)
		return err
	}
	return nil
}

/*---------------------------------------------------------------*/

const (
	envOracleDriver = "Oracle_DRIVER"
	envOracleUrl    = "Oracle_URL"
)

func TestGenericDaoOracle_SetGetSqlConnect(t *testing.T) {
	if os.Getenv(envOracleDriver) == "" || os.Getenv(envOracleUrl) == "" {
		return
	}
	name := "TestGenericDaoOracle_SetGetSqlConnect"
	dao := initDao(os.Getenv(envOracleDriver), os.Getenv(envOracleUrl), testTableName, prom.FlavorOracle)

	sqlc, _ := newSqlConnect(os.Getenv(envOracleDriver), os.Getenv(envOracleUrl), testTimeZone, prom.FlavorOracle)
	if sqlc == dao.GetSqlConnect() {
		t.Fatalf("%s failed: should not equal", name)
	}
	dao.SetSqlConnect(sqlc)
	if sqlc != dao.GetSqlConnect() {
		t.Fatalf("%s failed: should equal", name)
	}
}

func TestGenericDaoOracle_GdaoDelete(t *testing.T) {
	if os.Getenv(envOracleDriver) == "" || os.Getenv(envOracleUrl) == "" {
		return
	}
	name := "TestGenericDaoSql_GdaoDelete"
	dao := initDao(os.Getenv(envOracleDriver), os.Getenv(envOracleUrl), testTableName, prom.FlavorOracle)
	err := prepareTableOracle(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableOracle", err)
	}
	dotestGenericDaoSql_GdaoDelete(t, name, dao)
}

func TestGenericDaoOracle_GdaoDeleteMany(t *testing.T) {
	if os.Getenv(envOracleDriver) == "" || os.Getenv(envOracleUrl) == "" {
		return
	}
	name := "TestGenericDaoOracle_GdaoDeleteMany"
	dao := initDao(os.Getenv(envOracleDriver), os.Getenv(envOracleUrl), testTableName, prom.FlavorOracle)
	err := prepareTableOracle(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableOracle", err)
	}
	dotestGenericDaoSql_GdaoDeleteMany(t, name, dao)
}

func TestGenericDaoOracle_GdaoFetchOne(t *testing.T) {
	if os.Getenv(envOracleDriver) == "" || os.Getenv(envOracleUrl) == "" {
		return
	}
	name := "TestGenericDaoOracle_GdaoDeleteMany"
	dao := initDao(os.Getenv(envOracleDriver), os.Getenv(envOracleUrl), testTableName, prom.FlavorOracle)
	err := prepareTableOracle(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableOracle", err)
	}
	dotestGenericDaoSql_GdaoFetchOne(t, name, dao)
}

func TestGenericDaoOracle_GdaoFetchMany(t *testing.T) {
	if os.Getenv(envOracleDriver) == "" || os.Getenv(envOracleUrl) == "" {
		return
	}
	name := "TestGenericDaoOracle_GdaoFetchMany"
	dao := initDao(os.Getenv(envOracleDriver), os.Getenv(envOracleUrl), testTableName, prom.FlavorOracle)
	err := prepareTableOracle(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableOracle", err)
	}
	dotestGenericDaoSql_GdaoFetchMany(t, name, dao)
}

func TestGenericDaoOracle_GdaoCreate(t *testing.T) {
	if os.Getenv(envOracleDriver) == "" || os.Getenv(envOracleUrl) == "" {
		return
	}
	name := "TestGenericDaoOracle_GdaoCreate"
	dao := initDao(os.Getenv(envOracleDriver), os.Getenv(envOracleUrl), testTableName, prom.FlavorOracle)
	err := prepareTableOracle(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableOracle", err)
	}
	dotestGenericDaoSql_GdaoCreate(t, name, dao)
}

func TestGenericDaoOracle_GdaoUpdate(t *testing.T) {
	if os.Getenv(envOracleDriver) == "" || os.Getenv(envOracleUrl) == "" {
		return
	}
	name := "TestGenericDaoOracle_GdaoUpdate"
	dao := initDao(os.Getenv(envOracleDriver), os.Getenv(envOracleUrl), testTableName, prom.FlavorOracle)
	err := prepareTableOracle(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableOracle", err)
	}
	dotestGenericDaoSql_GdaoUpdate(t, name, dao)
}

func TestGenericDaoOracle_GdaoSave(t *testing.T) {
	if os.Getenv(envOracleDriver) == "" || os.Getenv(envOracleUrl) == "" {
		return
	}
	name := "TestGenericDaoOracle_GdaoSave"
	dao := initDao(os.Getenv(envOracleDriver), os.Getenv(envOracleUrl), testTableName, prom.FlavorOracle)
	err := prepareTableOracle(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableOracle", err)
	}
	dotestGenericDaoSql_GdaoSave(t, name, dao)
}

func TestGenericDaoOracle_GdaoSaveTxModeOnWrite(t *testing.T) {
	if os.Getenv(envOracleDriver) == "" || os.Getenv(envOracleUrl) == "" {
		return
	}
	name := "TestGenericDaoOracle_GdaoSaveTxModeOnWrite"
	dao := initDao(os.Getenv(envOracleDriver), os.Getenv(envOracleUrl), testTableName, prom.FlavorOracle)
	err := prepareTableOracle(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableOracle", err)
	}
	dao.SetTxModeOnWrite(true)
	dotestGenericDaoSql_GdaoSave(t, name, dao)
}
