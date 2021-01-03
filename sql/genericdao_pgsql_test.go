package sql

import (
	"fmt"
	"os"
	"testing"

	"github.com/btnguyen2k/prom"
	_ "github.com/jackc/pgx/v4/stdlib"
)

func prepareTablePgsql(sqlc *prom.SqlConnect, table string) error {
	sql := fmt.Sprintf("DROP TABLE IF EXISTS %s", table)
	if _, err := sqlc.GetDB().Exec(sql); err != nil {
		return err
	}
	sql = fmt.Sprintf("CREATE TABLE %s (%s VARCHAR(64), %s VARCHAR(64), %s JSONB, PRIMARY KEY (%s))", table, colSqlId, colSqlUsername, colSqlData, colSqlId)
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
	name := "TestGenericDaoPgsql_SetGetSqlConnect"
	dao := initDao(t, name, os.Getenv(envPgsqlDriver), os.Getenv(envPgsqlUrl), testTableName, prom.FlavorPgSql)
	sqlc, _ := newSqlConnect(t, name, os.Getenv(envPgsqlDriver), os.Getenv(envPgsqlUrl), testTimeZone, prom.FlavorPgSql)
	if sqlc == dao.GetSqlConnect() {
		t.Fatalf("%s failed: should not equal", name)
	}
	dao.SetSqlConnect(sqlc)
	if sqlc != dao.GetSqlConnect() {
		t.Fatalf("%s failed: should equal", name)
	}
}

func TestGenericDaoPgsql_GdaoDelete(t *testing.T) {
	name := "TestGenericDaoSql_GdaoDelete"
	dao := initDao(t, name, os.Getenv(envPgsqlDriver), os.Getenv(envPgsqlUrl), testTableName, prom.FlavorPgSql)
	err := prepareTablePgsql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTablePgsql", err)
	}
	dotestGenericDaoSqlGdaoDelete(t, name, dao)
}

func TestGenericDaoPgsql_GdaoDeleteMany(t *testing.T) {
	name := "TestGenericDaoPgsql_GdaoDeleteMany"
	dao := initDao(t, name, os.Getenv(envPgsqlDriver), os.Getenv(envPgsqlUrl), testTableName, prom.FlavorPgSql)
	err := prepareTablePgsql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTablePgsql", err)
	}
	dotestGenericDaoSqlGdaoDeleteMany(t, name, dao)
}

func TestGenericDaoPgsql_GdaoFetchOne(t *testing.T) {
	name := "TestGenericDaoPgsql_GdaoDeleteMany"
	dao := initDao(t, name, os.Getenv(envPgsqlDriver), os.Getenv(envPgsqlUrl), testTableName, prom.FlavorPgSql)
	err := prepareTablePgsql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTablePgsql", err)
	}
	dotestGenericDaoSqlGdaoFetchOne(t, name, dao)
}

func TestGenericDaoPgsql_GdaoFetchMany(t *testing.T) {
	name := "TestGenericDaoPgsql_GdaoFetchMany"
	dao := initDao(t, name, os.Getenv(envPgsqlDriver), os.Getenv(envPgsqlUrl), testTableName, prom.FlavorPgSql)
	err := prepareTablePgsql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTablePgsql", err)
	}
	dotestGenericDaoSqlGdaoFetchMany(t, name, dao)
}

func TestGenericDaoPgsql_GdaoCreate(t *testing.T) {
	name := "TestGenericDaoPgsql_GdaoCreate"
	dao := initDao(t, name, os.Getenv(envPgsqlDriver), os.Getenv(envPgsqlUrl), testTableName, prom.FlavorPgSql)
	err := prepareTablePgsql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTablePgsql", err)
	}
	dotestGenericDaoSqlGdaoCreate(t, name, dao)
}

func TestGenericDaoPgsql_GdaoUpdate(t *testing.T) {
	name := "TestGenericDaoPgsql_GdaoUpdate"
	dao := initDao(t, name, os.Getenv(envPgsqlDriver), os.Getenv(envPgsqlUrl), testTableName, prom.FlavorPgSql)
	err := prepareTablePgsql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTablePgsql", err)
	}
	dotestGenericDaoSqlGdaoUpdate(t, name, dao)
}

func TestGenericDaoPgsql_GdaoSave(t *testing.T) {
	name := "TestGenericDaoPgsql_GdaoSave"
	dao := initDao(t, name, os.Getenv(envPgsqlDriver), os.Getenv(envPgsqlUrl), testTableName, prom.FlavorPgSql)
	err := prepareTablePgsql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTablePgsql", err)
	}
	dotestGenericDaoSqlGdaoSave(t, name, dao)
}

func TestGenericDaoPgsql_GdaoSaveTxModeOnWrite(t *testing.T) {
	name := "TestGenericDaoPgsql_GdaoSaveTxModeOnWrite"
	dao := initDao(t, name, os.Getenv(envPgsqlDriver), os.Getenv(envPgsqlUrl), testTableName, prom.FlavorPgSql)
	err := prepareTablePgsql(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTablePgsql", err)
	}
	dao.SetTxModeOnWrite(true)
	dotestGenericDaoSqlGdaoSave(t, name, dao)
}
