package sql

import (
	"fmt"
	"os"
	"testing"

	"github.com/btnguyen2k/prom/sql"
	_ "github.com/denisenkom/go-mssqldb"
)

func prepareTableMssql(sqlc *sql.SqlConnect, table string) error {
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
	testName := "TestGenericDaoMssql_SetGetSqlConnect"
	dao := _initDao(os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, sql.FlavorMsSql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	sqlc, _ := _newSqlConnect(os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTimeZone, sql.FlavorMsSql)
	defer sqlc.Close()
	if sqlc == dao.GetSqlConnect() {
		t.Fatalf("%s failed: should not equal", testName)
	}
	dao.SetSqlConnect(sqlc)
	if sqlc != dao.GetSqlConnect() {
		t.Fatalf("%s failed: should equal", testName)
	}
}

func TestGenericDaoMssql_StartTx(t *testing.T) {
	testName := "TestGenericDaoMssql_StartTx"
	dao := _initDao(os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, sql.FlavorMsSql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	if tx, err := dao.StartTx(nil); tx == nil || err != nil {
		t.Fatalf("%s failed: %#v / %#v", testName, tx, err)
	}
}

func TestGenericDaoMssql_GdaoDelete(t *testing.T) {
	testName := "TestGenericDaoSql_GdaoDelete"
	dao := _initDao(os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, sql.FlavorMsSql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableMssql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableMssql", err)
	}
	dotestGenericDaoSqlGdaoDelete(t, testName, dao)
}

func TestGenericDaoMssql_GdaoDeleteMany(t *testing.T) {
	testName := "TestGenericDaoMssql_GdaoDeleteMany"
	dao := _initDao(os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, sql.FlavorMsSql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableMssql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableMssql", err)
	}
	dotestGenericDaoSqlGdaoDeleteMany(t, testName, dao)
}

func TestGenericDaoMssql_GdaoFetchOne(t *testing.T) {
	testName := "TestGenericDaoMssql_GdaoDeleteMany"
	dao := _initDao(os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, sql.FlavorMsSql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableMssql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableMssql", err)
	}
	dotestGenericDaoSqlGdaoFetchOne(t, testName, dao)
}

func TestGenericDaoMssql_GdaoFetchMany(t *testing.T) {
	testName := "TestGenericDaoMssql_GdaoFetchMany"
	dao := _initDao(os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, sql.FlavorMsSql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableMssql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableMssql", err)
	}
	dotestGenericDaoSqlGdaoFetchMany(t, testName, dao)
}

func TestGenericDaoMssql_GdaoCreate(t *testing.T) {
	testName := "TestGenericDaoMssql_GdaoCreate"
	dao := _initDao(os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, sql.FlavorMsSql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableMssql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableMssql", err)
	}
	dotestGenericDaoSqlGdaoCreate(t, testName, dao)
}

func TestGenericDaoMssql_GdaoUpdate(t *testing.T) {
	testName := "TestGenericDaoMssql_GdaoUpdate"
	dao := _initDao(os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, sql.FlavorMsSql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableMssql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableMssql", err)
	}
	dotestGenericDaoSqlGdaoUpdate(t, testName, dao)
}

func TestGenericDaoMssql_GdaoSave(t *testing.T) {
	testName := "TestGenericDaoMssql_GdaoSave"
	dao := _initDao(os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, sql.FlavorMsSql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableMssql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableMssql", err)
	}
	dotestGenericDaoSqlGdaoSave(t, testName, dao)
}

func TestGenericDaoMssql_GdaoSaveTxModeOnWrite(t *testing.T) {
	testName := "TestGenericDaoMssql_GdaoSaveTxModeOnWrite"
	dao := _initDao(os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, sql.FlavorMsSql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableMssql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableMssql", err)
	}
	dao.SetTxModeOnWrite(true)
	dotestGenericDaoSqlGdaoSave(t, testName, dao)
}

func TestGenericDaoMssql_Tx(t *testing.T) {
	testName := "TestGenericDaoMssql_Tx"
	dao := _initDao(os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, sql.FlavorMsSql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableMssql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableMssql", err)
	}
	dotestGenericDaoSqlTx(t, testName, dao)
}

func TestGenericDaoMssql_FilterNull(t *testing.T) {
	testName := "TestGenericDaoMssql_FilterNull"
	dao := _initDao(os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, sql.FlavorMsSql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTableMssql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableMssql", err)
	}
	dotestGenericDaoSqlGdaoFilterNull(t, testName, dao)
}

func TestGenericDaoMssql_FilterNotNull(t *testing.T) {
	testName := "TestGenericDaoMssql_FilterNotNull"
	dao := _initDao(os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl), testTableName, sql.FlavorMsSql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()
	
	err := prepareTableMssql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableMssql", err)
	}
	dotestGenericDaoSqlGdaoFilterNotNull(t, testName, dao)
}
