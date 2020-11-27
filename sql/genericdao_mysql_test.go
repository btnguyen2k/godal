package sql

import (
	"fmt"
	"os"
	"testing"

	"github.com/btnguyen2k/prom"
	_ "github.com/go-sql-driver/mysql"
)

func prepareTableMysql(sqlc *prom.SqlConnect, table string) error {
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
	envMysqlDriver = "MYSQL_DRIVER"
	envMysqlUrl    = "MYSQL_URL"
)

func TestGenericDaoMysql_SetGetSqlConnect(t *testing.T) {
	name := "TestGenericDaoMysql_SetGetSqlConnect"
	dao := initDao(t, name, os.Getenv(envMysqlDriver), os.Getenv(envMysqlUrl), testTableName, prom.FlavorMySql)
	sqlc, _ := newSqlConnect(t, name, os.Getenv(envMysqlDriver), os.Getenv(envMysqlUrl), testTimeZone, prom.FlavorMySql)
	if sqlc == dao.GetSqlConnect() {
		t.Fatalf("%s failed: should not equal", name)
	}
	dao.SetSqlConnect(sqlc)
	if sqlc != dao.GetSqlConnect() {
		t.Fatalf("%s failed: should equal", name)
	}
}

func TestGenericDaoMysql_GdaoDelete(t *testing.T) {
	name := "TestGenericDaoSql_GdaoDelete"
	dao := initDao(t, name, os.Getenv(envMysqlDriver), os.Getenv(envMysqlUrl), testTableName, prom.FlavorMySql)
	err := prepareTableMysql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableMysql", err)
	}
	dotestGenericDaoSql_GdaoDelete(t, name, dao)
}

func TestGenericDaoMysql_GdaoDeleteMany(t *testing.T) {
	name := "TestGenericDaoMysql_GdaoDeleteMany"
	dao := initDao(t, name, os.Getenv(envMysqlDriver), os.Getenv(envMysqlUrl), testTableName, prom.FlavorMySql)
	err := prepareTableMysql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableMysql", err)
	}
	dotestGenericDaoSql_GdaoDeleteMany(t, name, dao)
}

func TestGenericDaoMysql_GdaoFetchOne(t *testing.T) {
	name := "TestGenericDaoMysql_GdaoDeleteMany"
	dao := initDao(t, name, os.Getenv(envMysqlDriver), os.Getenv(envMysqlUrl), testTableName, prom.FlavorMySql)
	err := prepareTableMysql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableMysql", err)
	}
	dotestGenericDaoSql_GdaoFetchOne(t, name, dao)
}

func TestGenericDaoMysql_GdaoFetchMany(t *testing.T) {
	name := "TestGenericDaoMysql_GdaoFetchMany"
	dao := initDao(t, name, os.Getenv(envMysqlDriver), os.Getenv(envMysqlUrl), testTableName, prom.FlavorMySql)
	err := prepareTableMysql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableMysql", err)
	}
	dotestGenericDaoSql_GdaoFetchMany(t, name, dao)
}

func TestGenericDaoMysql_GdaoCreate(t *testing.T) {
	name := "TestGenericDaoMysql_GdaoCreate"
	dao := initDao(t, name, os.Getenv(envMysqlDriver), os.Getenv(envMysqlUrl), testTableName, prom.FlavorMySql)
	err := prepareTableMysql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableMysql", err)
	}
	dotestGenericDaoSql_GdaoCreate(t, name, dao)
}

func TestGenericDaoMysql_GdaoUpdate(t *testing.T) {
	name := "TestGenericDaoMysql_GdaoUpdate"
	dao := initDao(t, name, os.Getenv(envMysqlDriver), os.Getenv(envMysqlUrl), testTableName, prom.FlavorMySql)
	err := prepareTableMysql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableMysql", err)
	}
	dotestGenericDaoSql_GdaoUpdate(t, name, dao)
}

func TestGenericDaoMysql_GdaoSave(t *testing.T) {
	name := "TestGenericDaoMysql_GdaoSave"
	dao := initDao(t, name, os.Getenv(envMysqlDriver), os.Getenv(envMysqlUrl), testTableName, prom.FlavorMySql)
	err := prepareTableMysql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableMysql", err)
	}
	dotestGenericDaoSql_GdaoSave(t, name, dao)
}

func TestGenericDaoMysql_GdaoSaveTxModeOnWrite(t *testing.T) {
	name := "TestGenericDaoMysql_GdaoSaveTxModeOnWrite"
	dao := initDao(t, name, os.Getenv(envMysqlDriver), os.Getenv(envMysqlUrl), testTableName, prom.FlavorMySql)
	err := prepareTableMysql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableMysql", err)
	}
	dao.SetTxModeOnWrite(true)
	dotestGenericDaoSql_GdaoSave(t, name, dao)
}
