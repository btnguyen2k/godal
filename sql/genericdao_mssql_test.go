package sql

import (
	"fmt"
	"os"
	"testing"

	"github.com/btnguyen2k/prom"
	_ "github.com/denisenkom/go-mssqldb"
)

func prepareTableMssql(sqlc *prom.SqlConnect, table string) error {
	sql := fmt.Sprintf("DROP TABLE IF EXISTS %s", table)
	if _, err := sqlc.GetDB().Exec(sql); err != nil {
		return err
	}
	sql = fmt.Sprintf("CREATE TABLE %s (%s NVARCHAR(64), %s NVARCHAR(64), %s NTEXT, %s INT, %s REAL, %s NVARCHAR(64), %s DATETIME, PRIMARY KEY (%s))",
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
	envMssqlDriver = "MSSQL_DRIVER"
	envMssqlUrl    = "MSSQL_URL"
)

func TestGenericDaoMssql_SetGetSqlConnect(t *testing.T) {
	name := "TestGenericDaoMssql_SetGetSqlConnect"
	dao := initDao(t, name, os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, prom.FlavorMsSql)
	sqlc, _ := newSqlConnect(t, name, os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTimeZone, prom.FlavorMsSql)
	if sqlc == dao.GetSqlConnect() {
		t.Fatalf("%s failed: should not equal", name)
	}
	dao.SetSqlConnect(sqlc)
	if sqlc != dao.GetSqlConnect() {
		t.Fatalf("%s failed: should equal", name)
	}
}

func TestGenericDaoMssql_StartTx(t *testing.T) {
	name := "TestGenericDaoMssql_StartTx"
	dao := initDao(t, name, os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, prom.FlavorMsSql)
	defer dao.sqlConnect.Close()
	if tx, err := dao.StartTx(nil); tx == nil || err != nil {
		t.Fatalf("%s failed: %#v / %#v", name, tx, err)
	}
}

func TestGenericDaoMssql_GdaoDelete(t *testing.T) {
	name := "TestGenericDaoSql_GdaoDelete"
	dao := initDao(t, name, os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, prom.FlavorMsSql)
	defer dao.sqlConnect.Close()
	err := prepareTableMssql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableMssql", err)
	}
	dotestGenericDaoSqlGdaoDelete(t, name, dao)
}

func TestGenericDaoMssql_GdaoDeleteMany(t *testing.T) {
	name := "TestGenericDaoMssql_GdaoDeleteMany"
	dao := initDao(t, name, os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, prom.FlavorMsSql)
	defer dao.sqlConnect.Close()
	err := prepareTableMssql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableMssql", err)
	}
	dotestGenericDaoSqlGdaoDeleteMany(t, name, dao)
}

func TestGenericDaoMssql_GdaoFetchOne(t *testing.T) {
	name := "TestGenericDaoMssql_GdaoDeleteMany"
	dao := initDao(t, name, os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, prom.FlavorMsSql)
	defer dao.sqlConnect.Close()
	err := prepareTableMssql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableMssql", err)
	}
	dotestGenericDaoSqlGdaoFetchOne(t, name, dao)
}

func TestGenericDaoMssql_GdaoFetchMany(t *testing.T) {
	name := "TestGenericDaoMssql_GdaoFetchMany"
	dao := initDao(t, name, os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, prom.FlavorMsSql)
	defer dao.sqlConnect.Close()
	err := prepareTableMssql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableMssql", err)
	}
	dotestGenericDaoSqlGdaoFetchMany(t, name, dao)
}

func TestGenericDaoMssql_GdaoCreate(t *testing.T) {
	name := "TestGenericDaoMssql_GdaoCreate"
	dao := initDao(t, name, os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, prom.FlavorMsSql)
	defer dao.sqlConnect.Close()
	err := prepareTableMssql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableMssql", err)
	}
	dotestGenericDaoSqlGdaoCreate(t, name, dao)
}

func TestGenericDaoMssql_GdaoUpdate(t *testing.T) {
	name := "TestGenericDaoMssql_GdaoUpdate"
	dao := initDao(t, name, os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, prom.FlavorMsSql)
	defer dao.sqlConnect.Close()
	err := prepareTableMssql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableMssql", err)
	}
	dotestGenericDaoSqlGdaoUpdate(t, name, dao)
}

func TestGenericDaoMssql_GdaoSave(t *testing.T) {
	name := "TestGenericDaoMssql_GdaoSave"
	dao := initDao(t, name, os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, prom.FlavorMsSql)
	defer dao.sqlConnect.Close()
	err := prepareTableMssql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableMssql", err)
	}
	dotestGenericDaoSqlGdaoSave(t, name, dao)
}

func TestGenericDaoMssql_GdaoSaveTxModeOnWrite(t *testing.T) {
	name := "TestGenericDaoMssql_GdaoSaveTxModeOnWrite"
	dao := initDao(t, name, os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, prom.FlavorMsSql)
	defer dao.sqlConnect.Close()
	err := prepareTableMssql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableMssql", err)
	}
	dao.SetTxModeOnWrite(true)
	dotestGenericDaoSqlGdaoSave(t, name, dao)
}

func TestGenericDaoMssql_Tx(t *testing.T) {
	name := "TestGenericDaoMssql_Tx"
	dao := initDao(t, name, os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, prom.FlavorMsSql)
	defer dao.sqlConnect.Close()
	err := prepareTableMssql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableMssql", err)
	}
	dotestGenericDaoSqlTx(t, name, dao)
}

func TestGenericDaoMssql_FilterNull(t *testing.T) {
	name := "TestGenericDaoMssql_FilterNull"
	dao := initDao(t, name, os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, prom.FlavorMsSql)
	defer dao.sqlConnect.Close()
	err := prepareTableMssql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableMssql", err)
	}
	dotestGenericDaoSqlGdaoFilterNull(t, name, dao)
}

func TestGenericDaoMssql_FilterNotNull(t *testing.T) {
	name := "TestGenericDaoMssql_FilterNotNull"
	dao := initDao(t, name, os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, prom.FlavorMsSql)
	defer dao.sqlConnect.Close()
	err := prepareTableMssql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableMssql", err)
	}
	dotestGenericDaoSqlGdaoFilterNotNull(t, name, dao)
}
