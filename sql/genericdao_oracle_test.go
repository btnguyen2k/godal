package sql

import (
	"fmt"
	"os"
	"testing"

	"github.com/btnguyen2k/prom/sql"
	_ "github.com/godror/godror"
)

func prepareTableOracle(sqlc *sql.SqlConnect, table string) error {
	sql := fmt.Sprintf("DROP TABLE %s", table)
	sqlc.GetDB().Exec(sql)
	sql = fmt.Sprintf("CREATE TABLE %s (%s NVARCHAR2(64), %s NVARCHAR2(64), %s CLOB, %s INT, %s DECIMAL(32,3), %s NVARCHAR2(64), %s TIMESTAMP(0), PRIMARY KEY (%s))",
		table, colSqlId, colSqlUsername, colSqlData, colSqlValPInt, colSqlValPFloat, colSqlValPString, colSqlValPTime,
		colSqlId)
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
	envOracleDriver = "ORACLE_DRIVER"
	envOracleUrl    = "ORACLE_URL"
)

func TestGenericDaoOracle_SetGetSqlConnect(t *testing.T) {
	testName := "TestGenericDaoOracle_SetGetSqlConnect"
	dao := _initDao(os.Getenv(envOracleDriver), os.Getenv(envOracleUrl), testTableName, sql.FlavorOracle)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	sqlc, _ := _newSqlConnect(os.Getenv(envOracleDriver), os.Getenv(envOracleUrl), testTimeZone, sql.FlavorOracle)
	defer sqlc.Close()
	if sqlc == dao.GetSqlConnect() {
		t.Fatalf("%s failed: should not equal", testName)
	}
	dao.SetSqlConnect(sqlc)
	if sqlc != dao.GetSqlConnect() {
		t.Fatalf("%s failed: should equal", testName)
	}
}

func TestGenericDaoOracle_StartTx(t *testing.T) {
	testName := "TestGenericDaoOracle_StartTx"
	dao := _initDao(os.Getenv(envOracleDriver), os.Getenv(envOracleUrl), testTableName, sql.FlavorOracle)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	if tx, err := dao.StartTx(nil); tx == nil || err != nil {
		t.Fatalf("%s failed: %#v / %#v", testName, tx, err)
	}
}

func TestGenericDaoOracle_GdaoDelete(t *testing.T) {
	testName := "TestGenericDaoSql_GdaoDelete"
	dao := _initDao(os.Getenv(envOracleDriver), os.Getenv(envOracleUrl), testTableName, sql.FlavorOracle)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableOracle(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableOracle", err)
	}
	dotestGenericDaoSqlGdaoDelete(t, testName, dao)
}

func TestGenericDaoOracle_GdaoDeleteMany(t *testing.T) {
	testName := "TestGenericDaoOracle_GdaoDeleteMany"
	dao := _initDao(os.Getenv(envOracleDriver), os.Getenv(envOracleUrl), testTableName, sql.FlavorOracle)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableOracle(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableOracle", err)
	}
	dotestGenericDaoSqlGdaoDeleteMany(t, testName, dao)
}

func TestGenericDaoOracle_GdaoFetchOne(t *testing.T) {
	testName := "TestGenericDaoOracle_GdaoDeleteMany"
	dao := _initDao(os.Getenv(envOracleDriver), os.Getenv(envOracleUrl), testTableName, sql.FlavorOracle)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableOracle(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableOracle", err)
	}
	dotestGenericDaoSqlGdaoFetchOne(t, testName, dao)
}

func TestGenericDaoOracle_GdaoFetchMany(t *testing.T) {
	testName := "TestGenericDaoOracle_GdaoFetchMany"
	dao := _initDao(os.Getenv(envOracleDriver), os.Getenv(envOracleUrl), testTableName, sql.FlavorOracle)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableOracle(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableOracle", err)
	}
	dotestGenericDaoSqlGdaoFetchMany(t, testName, dao)
}

func TestGenericDaoOracle_GdaoCreate(t *testing.T) {
	testName := "TestGenericDaoOracle_GdaoCreate"
	dao := _initDao(os.Getenv(envOracleDriver), os.Getenv(envOracleUrl), testTableName, sql.FlavorOracle)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableOracle(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableOracle", err)
	}
	dotestGenericDaoSqlGdaoCreate(t, testName, dao)
}

func TestGenericDaoOracle_GdaoUpdate(t *testing.T) {
	testName := "TestGenericDaoOracle_GdaoUpdate"
	dao := _initDao(os.Getenv(envOracleDriver), os.Getenv(envOracleUrl), testTableName, sql.FlavorOracle)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableOracle(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableOracle", err)
	}
	dotestGenericDaoSqlGdaoUpdate(t, testName, dao)
}

func TestGenericDaoOracle_GdaoSave(t *testing.T) {
	testName := "TestGenericDaoOracle_GdaoSave"
	dao := _initDao(os.Getenv(envOracleDriver), os.Getenv(envOracleUrl), testTableName, sql.FlavorOracle)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableOracle(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableOracle", err)
	}
	dotestGenericDaoSqlGdaoSave(t, testName, dao)
}

func TestGenericDaoOracle_GdaoSaveTxModeOnWrite(t *testing.T) {
	testName := "TestGenericDaoOracle_GdaoSaveTxModeOnWrite"
	dao := _initDao(os.Getenv(envOracleDriver), os.Getenv(envOracleUrl), testTableName, sql.FlavorOracle)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableOracle(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableOracle", err)
	}
	dao.SetTxModeOnWrite(true)
	dotestGenericDaoSqlGdaoSave(t, testName, dao)
}

func TestGenericDaoOracle_Tx(t *testing.T) {
	testName := "TestGenericDaoOracle_Tx"
	dao := _initDao(os.Getenv(envOracleDriver), os.Getenv(envOracleUrl), testTableName, sql.FlavorOracle)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableOracle(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableOracle", err)
	}
	dotestGenericDaoSqlTx(t, testName, dao)
}

func TestGenericDaoOracle_FilterNull(t *testing.T) {
	testName := "TestGenericDaoOracle_FilterNull"
	dao := _initDao(os.Getenv(envOracleDriver), os.Getenv(envOracleUrl), testTableName, sql.FlavorOracle)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableOracle(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableOracle", err)
	}
	dotestGenericDaoSqlGdaoFilterNull(t, testName, dao)
}

func TestGenericDaoOracle_FilterNotNull(t *testing.T) {
	testName := "TestGenericDaoOracle_FilterNotNull"
	dao := _initDao(os.Getenv(envOracleDriver), os.Getenv(envOracleUrl), testTableName, sql.FlavorOracle)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()
	
	err := prepareTableOracle(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableOracle", err)
	}
	dotestGenericDaoSqlGdaoFilterNotNull(t, testName, dao)
}
