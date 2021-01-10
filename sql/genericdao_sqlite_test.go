package sql

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/btnguyen2k/prom"
	_ "github.com/mattn/go-sqlite3"
)

func prepareTableSqlite(sqlc *prom.SqlConnect, table string) error {
	driver := strings.Trim(os.Getenv(envSqliteUrl), "\"")
	os.MkdirAll(filepath.Dir(driver), 0711)
	os.Remove(driver)

	sql := fmt.Sprintf("DROP TABLE IF EXISTS %s", table)
	if _, err := sqlc.GetDB().Exec(sql); err != nil {
		return err
	}
	sql = fmt.Sprintf("CREATE TABLE %s (%s VARCHAR(64), %s VARCHAR(64), %s TEXT, PRIMARY KEY (%s))", table, colSqlId, colSqlUsername, colSqlData, colSqlId)
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
	name := "TestGenericDaoSqlite_SetGetSqlConnect"
	dao := initDao(t, name, os.Getenv(envSqliteDriver), os.Getenv(envSqliteUrl), testTableName, prom.FlavorSqlite)
	sqlc, _ := newSqlConnect(t, name, os.Getenv(envSqliteDriver), os.Getenv(envSqliteUrl), testTimeZone, prom.FlavorSqlite)
	if sqlc == dao.GetSqlConnect() {
		t.Fatalf("%s failed: should not equal", name)
	}
	dao.SetSqlConnect(sqlc)
	if sqlc != dao.GetSqlConnect() {
		t.Fatalf("%s failed: should equal", name)
	}
}

func TestGenericDaoSqlite_StartTx(t *testing.T) {
	name := "TestGenericDaoSqlite_StartTx"
	dao := initDao(t, name, os.Getenv(envSqliteDriver), os.Getenv(envSqliteUrl), testTableName, prom.FlavorSqlite)
	if tx, err := dao.StartTx(nil); tx == nil || err != nil {
		t.Fatalf("%s failed: %#v / %#v", name, tx, err)
	}
}

func TestGenericDaoSqlite_GdaoDelete(t *testing.T) {
	name := "TestGenericDaoSqlite_GdaoDelete"
	dao := initDao(t, name, os.Getenv(envSqliteDriver), os.Getenv(envSqliteUrl), testTableName, prom.FlavorSqlite)
	err := prepareTableSqlite(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableSqlite", err)
	}
	dotestGenericDaoSqlGdaoDelete(t, name, dao)
}

func TestGenericDaoSqlite_GdaoDeleteMany(t *testing.T) {
	name := "TestGenericDaoSqlite_GdaoDeleteMany"
	dao := initDao(t, name, os.Getenv(envSqliteDriver), os.Getenv(envSqliteUrl), testTableName, prom.FlavorSqlite)
	err := prepareTableSqlite(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableSqlite", err)
	}
	dotestGenericDaoSqlGdaoDeleteMany(t, name, dao)
}

func TestGenericDaoSqlite_GdaoFetchOne(t *testing.T) {
	name := "TestGenericDaoSqlite_GdaoDeleteMany"
	dao := initDao(t, name, os.Getenv(envSqliteDriver), os.Getenv(envSqliteUrl), testTableName, prom.FlavorSqlite)
	err := prepareTableSqlite(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableSqlite", err)
	}
	dotestGenericDaoSqlGdaoFetchOne(t, name, dao)
}

func TestGenericDaoSqlite_GdaoFetchMany(t *testing.T) {
	name := "TestGenericDaoSqlite_GdaoFetchMany"
	dao := initDao(t, name, os.Getenv(envSqliteDriver), os.Getenv(envSqliteUrl), testTableName, prom.FlavorSqlite)
	err := prepareTableSqlite(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableSqlite", err)
	}
	dotestGenericDaoSqlGdaoFetchMany(t, name, dao)
}

func TestGenericDaoSqlite_GdaoCreate(t *testing.T) {
	name := "TestGenericDaoSqlite_GdaoCreate"
	dao := initDao(t, name, os.Getenv(envSqliteDriver), os.Getenv(envSqliteUrl), testTableName, prom.FlavorSqlite)
	err := prepareTableSqlite(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableSqlite", err)
	}
	dotestGenericDaoSqlGdaoCreate(t, name, dao)
}

func TestGenericDaoSqlite_GdaoUpdate(t *testing.T) {
	name := "TestGenericDaoSqlite_GdaoUpdate"
	dao := initDao(t, name, os.Getenv(envSqliteDriver), os.Getenv(envSqliteUrl), testTableName, prom.FlavorSqlite)
	err := prepareTableSqlite(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableSqlite", err)
	}
	dotestGenericDaoSqlGdaoUpdate(t, name, dao)
}

func TestGenericDaoSqlite_GdaoSave(t *testing.T) {
	name := "TestGenericDaoSqlite_GdaoSave"
	dao := initDao(t, name, os.Getenv(envSqliteDriver), os.Getenv(envSqliteUrl), testTableName, prom.FlavorSqlite)
	err := prepareTableSqlite(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableSqlite", err)
	}
	dotestGenericDaoSqlGdaoSave(t, name, dao)
}

func TestGenericDaoSqlite_GdaoSaveTxModeOnWrite(t *testing.T) {
	name := "TestGenericDaoSqlite_GdaoSaveTxModeOnWrite"
	dao := initDao(t, name, os.Getenv(envSqliteDriver), os.Getenv(envSqliteUrl), testTableName, prom.FlavorSqlite)
	err := prepareTableSqlite(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableSqlite", err)
	}
	dao.SetTxModeOnWrite(true)
	dotestGenericDaoSqlGdaoSave(t, name, dao)
}

func TestGenericDaoSqlite_Tx(t *testing.T) {
	name := "TestGenericDaoSqlite_Tx"
	dao := initDao(t, name, os.Getenv(envSqliteDriver), os.Getenv(envSqliteUrl), testTableName, prom.FlavorSqlite)
	err := prepareTableSqlite(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableSqlite", err)
	}
	dotestGenericDaoSql_Tx(t, name, dao)
}
