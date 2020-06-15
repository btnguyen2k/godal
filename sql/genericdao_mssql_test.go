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
	sql = fmt.Sprintf("CREATE TABLE %s (%s NVARCHAR(64), %s NVARCHAR(64), %s NTEXT, PRIMARY KEY (%s))", table, colSqlId, colSqlUsername, colSqlData, colSqlId)
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
	if os.Getenv(envMssqlDriver) == "" || os.Getenv(envMssqlUrl) == "" {
		return
	}
	name := "TestGenericDaoMssql_SetGetSqlConnect"
	dao := initDao(os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, prom.FlavorMsSql)

	sqlc, _ := newSqlConnect(os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTimeZone, prom.FlavorMsSql)
	if sqlc == dao.GetSqlConnect() {
		t.Fatalf("%s failed: should not equal", name)
	}
	dao.SetSqlConnect(sqlc)
	if sqlc != dao.GetSqlConnect() {
		t.Fatalf("%s failed: should equal", name)
	}
}

func TestGenericDaoMssql_GdaoDelete(t *testing.T) {
	if os.Getenv(envMssqlDriver) == "" || os.Getenv(envMssqlUrl) == "" {
		return
	}
	name := "TestGenericDaoSql_GdaoDelete"
	dao := initDao(os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, prom.FlavorMsSql)
	err := prepareTableMssql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableMssql", err)
	}
	dotestGenericDaoSql_GdaoDelete(t, name, dao)
}

func TestGenericDaoMssql_GdaoDeleteMany(t *testing.T) {
	if os.Getenv(envMssqlDriver) == "" || os.Getenv(envMssqlUrl) == "" {
		return
	}
	name := "TestGenericDaoMssql_GdaoDeleteMany"
	dao := initDao(os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, prom.FlavorMsSql)
	err := prepareTableMssql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableMssql", err)
	}
	dotestGenericDaoSql_GdaoDeleteMany(t, name, dao)
}

func TestGenericDaoMssql_GdaoFetchOne(t *testing.T) {
	if os.Getenv(envMssqlDriver) == "" || os.Getenv(envMssqlUrl) == "" {
		return
	}
	name := "TestGenericDaoMssql_GdaoDeleteMany"
	dao := initDao(os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, prom.FlavorMsSql)
	err := prepareTableMssql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableMssql", err)
	}
	dotestGenericDaoSql_GdaoFetchOne(t, name, dao)
}

func TestGenericDaoMssql_GdaoFetchMany(t *testing.T) {
	if os.Getenv(envMssqlDriver) == "" || os.Getenv(envMssqlUrl) == "" {
		return
	}
	name := "TestGenericDaoMssql_GdaoFetchMany"
	dao := initDao(os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, prom.FlavorMsSql)
	err := prepareTableMssql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableMssql", err)
	}
	dotestGenericDaoSql_GdaoFetchMany(t, name, dao)
}

func TestGenericDaoMssql_GdaoCreate(t *testing.T) {
	if os.Getenv(envMssqlDriver) == "" || os.Getenv(envMssqlUrl) == "" {
		return
	}
	name := "TestGenericDaoMssql_GdaoCreate"
	dao := initDao(os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, prom.FlavorMsSql)
	err := prepareTableMssql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableMssql", err)
	}
	dotestGenericDaoSql_GdaoCreate(t, name, dao)
}

func TestGenericDaoMssql_GdaoUpdate(t *testing.T) {
	if os.Getenv(envMssqlDriver) == "" || os.Getenv(envMssqlUrl) == "" {
		return
	}
	name := "TestGenericDaoMssql_GdaoUpdate"
	dao := initDao(os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, prom.FlavorMsSql)
	err := prepareTableMssql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableMssql", err)
	}
	dotestGenericDaoSql_GdaoUpdate(t, name, dao)
}

func TestGenericDaoMssql_GdaoSave(t *testing.T) {
	if os.Getenv(envMssqlDriver) == "" || os.Getenv(envMssqlUrl) == "" {
		return
	}
	name := "TestGenericDaoMssql_GdaoSave"
	dao := initDao(os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, prom.FlavorMsSql)
	err := prepareTableMssql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableMssql", err)
	}
	dotestGenericDaoSql_GdaoSave(t, name, dao)
}

func TestGenericDaoMssql_GdaoSaveTxModeOnWrite(t *testing.T) {
	if os.Getenv(envMssqlDriver) == "" || os.Getenv(envMssqlUrl) == "" {
		return
	}
	name := "TestGenericDaoMssql_GdaoSaveTxModeOnWrite"
	dao := initDao(os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, prom.FlavorMsSql)
	err := prepareTableMssql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableMssql", err)
	}
	dao.SetTxModeOnWrite(true)
	dotestGenericDaoSql_GdaoSave(t, name, dao)
}
