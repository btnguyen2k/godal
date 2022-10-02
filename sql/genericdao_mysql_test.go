package sql

import (
	"fmt"
	"os"
	"testing"

	"github.com/btnguyen2k/prom/sql"
	_ "github.com/go-sql-driver/mysql"
)

func prepareTableMysql(sqlc *sql.SqlConnect, table string) error {
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
	envMysqlDriver = "MYSQL_DRIVER"
	envMysqlUrl    = "MYSQL_URL"
)

func TestGenericDaoMysql_SetGetSqlConnect(t *testing.T) {
	testName := "TestGenericDaoMysql_SetGetSqlConnect"
	dao := _initDao(os.Getenv(envMysqlDriver), os.Getenv(envMysqlUrl), testTableName, sql.FlavorMySql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	sqlc, _ := _newSqlConnect(os.Getenv(envMysqlDriver), os.Getenv(envMysqlUrl), testTimeZone, sql.FlavorMySql)
	defer sqlc.Close()
	if sqlc == dao.GetSqlConnect() {
		t.Fatalf("%s failed: should not equal", testName)
	}
	dao.SetSqlConnect(sqlc)
	if sqlc != dao.GetSqlConnect() {
		t.Fatalf("%s failed: should equal", testName)
	}
}

func TestGenericDaoMysql_StartTx(t *testing.T) {
	testName := "TestGenericDaoMysql_StartTx"
	dao := _initDao(os.Getenv(envMysqlDriver), os.Getenv(envMysqlUrl), testTableName, sql.FlavorMySql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	if tx, err := dao.StartTx(nil); tx == nil || err != nil {
		t.Fatalf("%s failed: %#v / %#v", testName, tx, err)
	}
}

func TestGenericDaoMysql_GdaoDelete(t *testing.T) {
	testName := "TestGenericDaoSql_GdaoDelete"
	dao := _initDao(os.Getenv(envMysqlDriver), os.Getenv(envMysqlUrl), testTableName, sql.FlavorMySql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableMysql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableMysql", err)
	}
	dotestGenericDaoSqlGdaoDelete(t, testName, dao)
}

func TestGenericDaoMysql_GdaoDeleteMany(t *testing.T) {
	testName := "TestGenericDaoMysql_GdaoDeleteMany"
	dao := _initDao(os.Getenv(envMysqlDriver), os.Getenv(envMysqlUrl), testTableName, sql.FlavorMySql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableMysql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableMysql", err)
	}
	dotestGenericDaoSqlGdaoDeleteMany(t, testName, dao)
}

func TestGenericDaoMysql_GdaoFetchOne(t *testing.T) {
	testName := "TestGenericDaoMysql_GdaoDeleteMany"
	dao := _initDao(os.Getenv(envMysqlDriver), os.Getenv(envMysqlUrl), testTableName, sql.FlavorMySql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableMysql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableMysql", err)
	}
	dotestGenericDaoSqlGdaoFetchOne(t, testName, dao)
}

func TestGenericDaoMysql_GdaoFetchMany(t *testing.T) {
	testName := "TestGenericDaoMysql_GdaoFetchMany"
	dao := _initDao(os.Getenv(envMysqlDriver), os.Getenv(envMysqlUrl), testTableName, sql.FlavorMySql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableMysql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableMysql", err)
	}
	dotestGenericDaoSqlGdaoFetchMany(t, testName, dao)
}

func TestGenericDaoMysql_GdaoCreate(t *testing.T) {
	testName := "TestGenericDaoMysql_GdaoCreate"
	dao := _initDao(os.Getenv(envMysqlDriver), os.Getenv(envMysqlUrl), testTableName, sql.FlavorMySql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableMysql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableMysql", err)
	}
	dotestGenericDaoSqlGdaoCreate(t, testName, dao)
}

func TestGenericDaoMysql_GdaoUpdate(t *testing.T) {
	testName := "TestGenericDaoMysql_GdaoUpdate"
	dao := _initDao(os.Getenv(envMysqlDriver), os.Getenv(envMysqlUrl), testTableName, sql.FlavorMySql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableMysql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableMysql", err)
	}
	dotestGenericDaoSqlGdaoUpdate(t, testName, dao)
}

func TestGenericDaoMysql_GdaoSave(t *testing.T) {
	testName := "TestGenericDaoMysql_GdaoSave"
	dao := _initDao(os.Getenv(envMysqlDriver), os.Getenv(envMysqlUrl), testTableName, sql.FlavorMySql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableMysql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableMysql", err)
	}
	dotestGenericDaoSqlGdaoSave(t, testName, dao)
}

func TestGenericDaoMysql_GdaoSaveTxModeOnWrite(t *testing.T) {
	testName := "TestGenericDaoMysql_GdaoSaveTxModeOnWrite"
	dao := _initDao(os.Getenv(envMysqlDriver), os.Getenv(envMysqlUrl), testTableName, sql.FlavorMySql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableMysql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableMysql", err)
	}
	dao.SetTxModeOnWrite(true)
	dotestGenericDaoSqlGdaoSave(t, testName, dao)
}

func TestGenericDaoMysql_Tx(t *testing.T) {
	testName := "TestGenericDaoMysql_Tx"
	dao := _initDao(os.Getenv(envMysqlDriver), os.Getenv(envMysqlUrl), testTableName, sql.FlavorMySql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableMysql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableMysql", err)
	}
	dotestGenericDaoSqlTx(t, testName, dao)
}

func TestGenericDaoMysql_FilterNull(t *testing.T) {
	testName := "TestGenericDaoMysql_FilterNull"
	dao := _initDao(os.Getenv(envMysqlDriver), os.Getenv(envMysqlUrl), testTableName, sql.FlavorMySql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableMysql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableMysql", err)
	}
	dotestGenericDaoSqlGdaoFilterNull(t, testName, dao)
}

func TestGenericDaoMysql_FilterNotNull(t *testing.T) {
	testName := "TestGenericDaoMysql_FilterNotNull"
	dao := _initDao(os.Getenv(envMysqlDriver), os.Getenv(envMysqlUrl), testTableName, sql.FlavorMySql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()
	
	err := prepareTableMysql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableMysql", err)
	}
	dotestGenericDaoSqlGdaoFilterNotNull(t, testName, dao)
}
