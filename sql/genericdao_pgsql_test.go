package sql

import (
	"fmt"
	"os"
	"testing"

	"github.com/btnguyen2k/prom/sql"
	_ "github.com/jackc/pgx/v4/stdlib"
)

func prepareTablePgsql(sqlc *sql.SqlConnect, table string) error {
	sql := fmt.Sprintf("DROP TABLE IF EXISTS %s", table)
	if _, err := sqlc.GetDB().Exec(sql); err != nil {
		return err
	}
	sql = fmt.Sprintf("CREATE TABLE %s (%s VARCHAR(64), %s VARCHAR(64), %s JSONB, %s INT, %s FLOAT8, %s VARCHAR(64), %s TIMESTAMP(0), PRIMARY KEY (%s))",
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
	envPgsqlDriver = "PGSQL_DRIVER"
	envPgsqlUrl    = "PGSQL_URL"
)

func TestGenericDaoPgsql_SetGetSqlConnect(t *testing.T) {
	testName := "TestGenericDaoPgsql_SetGetSqlConnect"
	dao := _initDao(os.Getenv(envPgsqlDriver), os.Getenv(envPgsqlUrl), testTableName, sql.FlavorPgSql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	sqlc, _ := _newSqlConnect(os.Getenv(envPgsqlDriver), os.Getenv(envPgsqlUrl), testTimeZone, sql.FlavorPgSql)
	defer sqlc.Close()
	if sqlc == dao.GetSqlConnect() {
		t.Fatalf("%s failed: should not equal", testName)
	}
	dao.SetSqlConnect(sqlc)
	if sqlc != dao.GetSqlConnect() {
		t.Fatalf("%s failed: should equal", testName)
	}
}

func TestGenericDaoPgsql_StartTx(t *testing.T) {
	testName := "TestGenericDaoPgsql_StartTx"
	dao := _initDao(os.Getenv(envPgsqlDriver), os.Getenv(envPgsqlUrl), testTableName, sql.FlavorPgSql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	if tx, err := dao.StartTx(nil); tx == nil || err != nil {
		t.Fatalf("%s failed: %#v / %#v", testName, tx, err)
	}
}

func TestGenericDaoPgsql_GdaoDelete(t *testing.T) {
	testName := "TestGenericDaoSql_GdaoDelete"
	dao := _initDao(os.Getenv(envPgsqlDriver), os.Getenv(envPgsqlUrl), testTableName, sql.FlavorPgSql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTablePgsql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTablePgsql", err)
	}
	dotestGenericDaoSqlGdaoDelete(t, testName, dao)
}

func TestGenericDaoPgsql_GdaoDeleteMany(t *testing.T) {
	testName := "TestGenericDaoPgsql_GdaoDeleteMany"
	dao := _initDao(os.Getenv(envPgsqlDriver), os.Getenv(envPgsqlUrl), testTableName, sql.FlavorPgSql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTablePgsql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTablePgsql", err)
	}
	dotestGenericDaoSqlGdaoDeleteMany(t, testName, dao)
}

func TestGenericDaoPgsql_GdaoFetchOne(t *testing.T) {
	testName := "TestGenericDaoPgsql_GdaoDeleteMany"
	dao := _initDao(os.Getenv(envPgsqlDriver), os.Getenv(envPgsqlUrl), testTableName, sql.FlavorPgSql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTablePgsql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTablePgsql", err)
	}
	dotestGenericDaoSqlGdaoFetchOne(t, testName, dao)
}

func TestGenericDaoPgsql_GdaoFetchMany(t *testing.T) {
	testName := "TestGenericDaoPgsql_GdaoFetchMany"
	dao := _initDao(os.Getenv(envPgsqlDriver), os.Getenv(envPgsqlUrl), testTableName, sql.FlavorPgSql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTablePgsql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTablePgsql", err)
	}
	dotestGenericDaoSqlGdaoFetchMany(t, testName, dao)
}

func TestGenericDaoPgsql_GdaoCreate(t *testing.T) {
	testName := "TestGenericDaoPgsql_GdaoCreate"
	dao := _initDao(os.Getenv(envPgsqlDriver), os.Getenv(envPgsqlUrl), testTableName, sql.FlavorPgSql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTablePgsql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTablePgsql", err)
	}
	dotestGenericDaoSqlGdaoCreate(t, testName, dao)
}

func TestGenericDaoPgsql_GdaoUpdate(t *testing.T) {
	testName := "TestGenericDaoPgsql_GdaoUpdate"
	dao := _initDao(os.Getenv(envPgsqlDriver), os.Getenv(envPgsqlUrl), testTableName, sql.FlavorPgSql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTablePgsql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTablePgsql", err)
	}
	dotestGenericDaoSqlGdaoUpdate(t, testName, dao)
}

func TestGenericDaoPgsql_GdaoSave(t *testing.T) {
	testName := "TestGenericDaoPgsql_GdaoSave"
	dao := _initDao(os.Getenv(envPgsqlDriver), os.Getenv(envPgsqlUrl), testTableName, sql.FlavorPgSql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTablePgsql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTablePgsql", err)
	}
	dotestGenericDaoSqlGdaoSave(t, testName, dao)
}

func TestGenericDaoPgsql_GdaoSaveTxModeOnWrite(t *testing.T) {
	testName := "TestGenericDaoPgsql_GdaoSaveTxModeOnWrite"
	dao := _initDao(os.Getenv(envPgsqlDriver), os.Getenv(envPgsqlUrl), testTableName, sql.FlavorPgSql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTablePgsql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTablePgsql", err)
	}
	dao.SetTxModeOnWrite(true)
	dotestGenericDaoSqlGdaoSave(t, testName, dao)
}

func TestGenericDaoPgsql_Tx(t *testing.T) {
	testName := "TestGenericDaoPgsql_Tx"
	dao := _initDao(os.Getenv(envPgsqlDriver), os.Getenv(envPgsqlUrl), testTableName, sql.FlavorPgSql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTablePgsql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTablePgsql", err)
	}
	dotestGenericDaoSqlTx(t, testName, dao)
}

func TestGenericDaoPgsql_FilterNull(t *testing.T) {
	testName := "TestGenericDaoPgsql_FilterNull"
	dao := _initDao(os.Getenv(envPgsqlDriver), os.Getenv(envPgsqlUrl), testTableName, sql.FlavorPgSql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTablePgsql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTablePgsql", err)
	}
	dotestGenericDaoSqlGdaoFilterNull(t, testName, dao)
}

func TestGenericDaoPgsql_FilterNotNull(t *testing.T) {
	testName := "TestGenericDaoPgsql_FilterNotNull"
	dao := _initDao(os.Getenv(envPgsqlDriver), os.Getenv(envPgsqlUrl), testTableName, sql.FlavorPgSql)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.sqlConnect.Close()

	err := prepareTablePgsql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTablePgsql", err)
	}
	dotestGenericDaoSqlGdaoFilterNotNull(t, testName, dao)
}
