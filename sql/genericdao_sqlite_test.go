package sql

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/btnguyen2k/prom/sql"
	_ "github.com/mattn/go-sqlite3"
)

func prepareTableSqlite(sqlc *sql.SqlConnect, table string) error {
	driver := strings.Trim(os.Getenv(envSqliteUrl), "\"")
	os.MkdirAll(filepath.Dir(driver), 0711)
	os.Remove(driver)

	sql := fmt.Sprintf("DROP TABLE IF EXISTS %s", table)
	if _, err := sqlc.GetDB().Exec(sql); err != nil {
		return err
	}
	sql = fmt.Sprintf("CREATE TABLE %s (%s VARCHAR(64), %s VARCHAR(64), %s TEXT, %s INT, %s REAL, %s VARCHAR(64), %s DATETIME, PRIMARY KEY (%s))",
		table, colSqlId, colSqlUsername, colSqlData, colSqlValPInt, colSqlValPFloat, colSqlValPString, colSqlValPTime,
		colSqlId)
	if _, err := sqlc.GetDB().Exec(sql); err != nil {
		return err
	}
	sql = fmt.Sprintf("CREATE UNIQUE INDEX uidx_%s_username ON %s(%s)", table, table, colSqlUsername)
	if _, err := sqlc.GetDB().Exec(sql); err != nil {
		return err
	}
	return nil
}

/*---------------------------------------------------------------*/

const (
	envSqliteDriver = "SQLITE_DRIVER"
	envSqliteUrl    = "SQLITE_URL"
)

func TestGenericDaoSqlite_SetGetSqlConnect(t *testing.T) {
	testName := "TestGenericDaoSqlite_SetGetSqlConnect"
	dao := _initDao(os.Getenv(envSqliteDriver), os.Getenv(envSqliteUrl), testTableName, sql.FlavorSqlite)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	sqlc, _ := _newSqlConnect(os.Getenv(envSqliteDriver), os.Getenv(envSqliteUrl), testTimeZone, sql.FlavorSqlite)
	defer sqlc.Close()
	if sqlc == dao.GetSqlConnect() {
		t.Fatalf("%s failed: should not equal", testName)
	}
	dao.SetSqlConnect(sqlc)
	if sqlc != dao.GetSqlConnect() {
		t.Fatalf("%s failed: should equal", testName)
	}
}

func TestGenericDaoSqlite_StartTx(t *testing.T) {
	testName := "TestGenericDaoSqlite_StartTx"
	dao := _initDao(os.Getenv(envSqliteDriver), os.Getenv(envSqliteUrl), testTableName, sql.FlavorSqlite)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	if err := prepareTableSqlite(dao.GetSqlConnect(), dao.tableName); err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableSqlite", err)
	}
	if tx, err := dao.StartTx(nil); tx == nil || err != nil {
		t.Fatalf("%s failed: %#v / %#v", testName, tx, err)
	}
}

func TestGenericDaoSqlite_GdaoDelete(t *testing.T) {
	testName := "TestGenericDaoSqlite_GdaoDelete"
	dao := _initDao(os.Getenv(envSqliteDriver), os.Getenv(envSqliteUrl), testTableName, sql.FlavorSqlite)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableSqlite(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableSqlite", err)
	}
	dotestGenericDaoSqlGdaoDelete(t, testName, dao)
}

func TestGenericDaoSqlite_GdaoDeleteMany(t *testing.T) {
	testName := "TestGenericDaoSqlite_GdaoDeleteMany"
	dao := _initDao(os.Getenv(envSqliteDriver), os.Getenv(envSqliteUrl), testTableName, sql.FlavorSqlite)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableSqlite(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableSqlite", err)
	}
	dotestGenericDaoSqlGdaoDeleteMany(t, testName, dao)
}

func TestGenericDaoSqlite_GdaoFetchOne(t *testing.T) {
	testName := "TestGenericDaoSqlite_GdaoDeleteMany"
	dao := _initDao(os.Getenv(envSqliteDriver), os.Getenv(envSqliteUrl), testTableName, sql.FlavorSqlite)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableSqlite(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableSqlite", err)
	}
	dotestGenericDaoSqlGdaoFetchOne(t, testName, dao)
}

func TestGenericDaoSqlite_GdaoFetchMany(t *testing.T) {
	testName := "TestGenericDaoSqlite_GdaoFetchMany"
	dao := _initDao(os.Getenv(envSqliteDriver), os.Getenv(envSqliteUrl), testTableName, sql.FlavorSqlite)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableSqlite(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableSqlite", err)
	}
	dotestGenericDaoSqlGdaoFetchMany(t, testName, dao)
}

func TestGenericDaoSqlite_GdaoCreate(t *testing.T) {
	testName := "TestGenericDaoSqlite_GdaoCreate"
	dao := _initDao(os.Getenv(envSqliteDriver), os.Getenv(envSqliteUrl), testTableName, sql.FlavorSqlite)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableSqlite(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableSqlite", err)
	}
	dotestGenericDaoSqlGdaoCreate(t, testName, dao)
}

func TestGenericDaoSqlite_GdaoUpdate(t *testing.T) {
	testName := "TestGenericDaoSqlite_GdaoUpdate"
	dao := _initDao(os.Getenv(envSqliteDriver), os.Getenv(envSqliteUrl), testTableName, sql.FlavorSqlite)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableSqlite(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableSqlite", err)
	}
	dotestGenericDaoSqlGdaoUpdate(t, testName, dao)
}

func TestGenericDaoSqlite_GdaoSave(t *testing.T) {
	testName := "TestGenericDaoSqlite_GdaoSave"
	dao := _initDao(os.Getenv(envSqliteDriver), os.Getenv(envSqliteUrl), testTableName, sql.FlavorSqlite)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableSqlite(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableSqlite", err)
	}
	dotestGenericDaoSqlGdaoSave(t, testName, dao)
}

func TestGenericDaoSqlite_GdaoSaveTxModeOnWrite(t *testing.T) {
	testName := "TestGenericDaoSqlite_GdaoSaveTxModeOnWrite"
	dao := _initDao(os.Getenv(envSqliteDriver), os.Getenv(envSqliteUrl), testTableName, sql.FlavorSqlite)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableSqlite(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableSqlite", err)
	}
	dao.SetTxModeOnWrite(true)
	dotestGenericDaoSqlGdaoSave(t, testName, dao)
}

func TestGenericDaoSqlite_Tx(t *testing.T) {
	testName := "TestGenericDaoSqlite_Tx"
	dao := _initDao(os.Getenv(envSqliteDriver), os.Getenv(envSqliteUrl), testTableName, sql.FlavorSqlite)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableSqlite(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableSqlite", err)
	}
	dotestGenericDaoSqlTx(t, testName, dao)
}

func TestGenericDaoSqlite_FilterNull(t *testing.T) {
	testName := "TestGenericDaoSqlite_FilterNull"
	dao := _initDao(os.Getenv(envSqliteDriver), os.Getenv(envSqliteUrl), testTableName, sql.FlavorSqlite)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableSqlite(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableSqlite", err)
	}
	dotestGenericDaoSqlGdaoFilterNull(t, testName, dao)
}

func TestGenericDaoSqlite_FilterNotNull(t *testing.T) {
	testName := "TestGenericDaoSqlite_FilterNotNull"
	dao := _initDao(os.Getenv(envSqliteDriver), os.Getenv(envSqliteUrl), testTableName, sql.FlavorSqlite)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableSqlite(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableSqlite", err)
	}
	dotestGenericDaoSqlGdaoFilterNotNull(t, testName, dao)
}
