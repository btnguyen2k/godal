/*
$ go run example_sta_cosmosdbsql_genericbo.go
*/
package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/prom"

	_ "github.com/btnguyen2k/gocosmos"

	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/godal/cosmosdbsql"
	"github.com/btnguyen2k/godal/sql"
)

// convenient function to create prom.SqlConnect instance (for CosmosDB)
func createPgsqlConnect() *prom.SqlConnect {
	// driver := strings.ReplaceAll(os.Getenv("COSMOSDB_DRIVER"), `"`, "")
	driver := "gocosmos"
	dsn := strings.ReplaceAll(os.Getenv("COSMOSDB_URL"), `"`, "")
	if driver == "" || dsn == "" {
		panic("Please define env COSMOSDB_DRIVER, COSMOSDB_URL and optionally TIMEZONE")
	}
	timeZone := strings.ReplaceAll(os.Getenv("TIMEZONE"), `"`, "")
	if timeZone == "" {
		timeZone = "UTC"
	}
	urlTimezone := strings.ReplaceAll(timeZone, "/", "%2f")
	dsn = strings.ReplaceAll(dsn, "${loc}", urlTimezone)
	dsn = strings.ReplaceAll(dsn, "${tz}", urlTimezone)
	dsn = strings.ReplaceAll(dsn, "${timezone}", urlTimezone)

	dsn += ";Db=godal"

	sqlConnect, err := prom.NewSqlConnect(driver, dsn, 10000, nil)
	if sqlConnect == nil || err != nil {
		if err != nil {
			fmt.Println("Error:", err)
		}
		if sqlConnect == nil {
			panic("error creating [prom.SqlConnect] instance")
		}
	}
	loc, _ := time.LoadLocation(timeZone)
	sqlConnect.SetLocation(loc)
	sqlConnect.SetDbFlavor(prom.FlavorCosmosDb)

	sqlConnect.GetDB().Exec("CREATE DATABASE godal WITH maxru=10000")

	return sqlConnect
}

// convenient function to create MyGenericDaoSql instance
func createMyGenericDaoSql(sqlc *prom.SqlConnect, rowMapper godal.IRowMapper) godal.IGenericDao {
	_, err := sqlc.GetDB().Exec(fmt.Sprintf("DROP COLLECTION IF EXISTS %s", tableUserGeneric))
	fmt.Printf("[INFO] Dropped collection %s: %s\n", tableUserGeneric, err)
	_, err = sqlc.GetDB().Exec(fmt.Sprintf("CREATE COLLECTION %s WITH pk=/%s", tableUserGeneric, fieldUserIdGeneric))
	fmt.Printf("[INFO] Created collection %s: %s\n", tableUserGeneric, err)

	dao := &MyGenericDaoSql{}
	inner := cosmosdbsql.NewGenericDaoCosmosdb(sqlc, godal.NewAbstractGenericDao(dao))
	inner.CosmosSetIdGboMapPath(map[string]string{"*": fieldUserIdGeneric})
	dao.IGenericDaoSql = inner
	dao.SetRowMapper(rowMapper)
	dao.SetSqlFlavor(sqlc.GetDbFlavor())
	return dao
}

const (
	tableUserGeneric = "test_user"

	// table columns
	colUserIdGeneric       = "uid"
	colUserUsernameGeneric = "uusername"
	colUserNameGeneric     = "uname"
	colUserVersionGeneric  = "uversion"
	colUserActivedGeneric  = "uactived"

	// BO fields
	fieldUserIdGeneric       = "id"
	fieldUserUsernameGeneric = "username"
	fieldUserNameGeneric     = "name"
	fieldUserVersionGeneric  = "version"
	fieldUserActivedGeneric  = "actived"
)

// MyGenericDaoSql is SQL-based DAO implementation.
type MyGenericDaoSql struct {
	// *sql.GenericDaoSql
	sql.IGenericDaoSql
}

// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter
func (dao *MyGenericDaoSql) GdaoCreateFilter(tableName string, bo godal.IGenericBo) interface{} {
	if tableName == tableUserGeneric {
		// should match all primary keys
		return map[string]interface{}{
			colUserIdGeneric: bo.GboGetAttrUnsafe(fieldUserIdGeneric, reddo.TypeString),
		}
	}

	// primary key filtering for other database tables
	// ...

	return nil
}

func main() {
	// create new prom.SqlConnect
	sqlc := createPgsqlConnect()

	rowMapper := &sql.GenericRowMapperSql{
		// it is a good idea to normalize table column names and BO field names
		// in this case, we use "lower case transformation" rule to normalize table column and BO field names
		NameTransformation: sql.NameTransfLowerCase,
		GboFieldToColNameTranslator: map[string]map[string]interface{}{
			// {generic bo field -> database table column} mapping for tableUser
			tableUserGeneric: {
				fieldUserIdGeneric:       colUserIdGeneric,
				fieldUserUsernameGeneric: colUserUsernameGeneric,
				fieldUserNameGeneric:     colUserNameGeneric,
				fieldUserVersionGeneric:  colUserVersionGeneric,
				fieldUserActivedGeneric:  colUserActivedGeneric,
			},
			// mapping for other tables
		},
		ColNameToGboFieldTranslator: map[string]map[string]interface{}{
			// {database table column -> generic bo field} mapping for tableUser
			tableUserGeneric: {
				colUserIdGeneric:       fieldUserIdGeneric,
				colUserUsernameGeneric: fieldUserUsernameGeneric,
				colUserNameGeneric:     fieldUserNameGeneric,
				colUserVersionGeneric:  fieldUserVersionGeneric,
				colUserActivedGeneric:  fieldUserActivedGeneric,
			},
			// mapping for other tables
		},
		ColumnsListMap: map[string][]string{
			// all database table columns of tableUser
			tableUserGeneric: {colUserIdGeneric, colUserUsernameGeneric, colUserNameGeneric, colUserVersionGeneric, colUserActivedGeneric},

			// ...other tables
		},
	}

	// create new MyGenericDaoSql
	myDao := createMyGenericDaoSql(sqlc, rowMapper)

	bo := godal.NewGenericBo()
	bo.GboSetAttr(fieldUserIdGeneric, "1")
	bo.GboSetAttr(fieldUserUsernameGeneric, "btnguyen2k")
	bo.GboSetAttr(fieldUserNameGeneric, "Nguyễn Bá Thành")
	bo.GboSetAttr(fieldUserVersionGeneric, time.Now().Unix())
	bo.GboSetAttr(fieldUserActivedGeneric, 1) // convention: 1=true / 0=false

	{
		// CREATE
		_, err := myDao.GdaoCreate(tableUserGeneric, bo)
		fmt.Printf("Creating user [%s]...: %e\n", bo.GboToJsonUnsafe(), err)
	}

	{
		// READ
		filterBo := godal.NewGenericBo()
		filterBo.GboSetAttr(fieldUserIdGeneric, "1") // use "field" here for filtering
		myBo, err := myDao.GdaoFetchOne(tableUserGeneric, myDao.GdaoCreateFilter(tableUserGeneric, filterBo))
		fmt.Printf("Fetched user [%s]: %e\n", myBo.GboToJsonUnsafe(), err)
	}

	{
		// UPDATE
		bo.GboSetAttr(fieldUserVersionGeneric, godal.NilValue)
		// bo.GboSetAttr("new_field", "a value") // database table structure is pre-defined, adding new field will cause error!
		bo.GboSetAttr(fieldUserActivedGeneric, 0) // convention: 1=true / 0=false
		_, err := myDao.GdaoUpdate(tableUserGeneric, bo)
		fmt.Printf("Updated user [%s]: %e\n", bo.GboToJsonUnsafe(), err)

		// _, err = myDao.GdaoSave(tableUser, bo)
		// fmt.Printf("Saved user [%s]: %e\n", bo.GboToJsonUnsafe(), err)
	}

	{
		// DELETE
		_, err := myDao.GdaoDelete(tableUserGeneric, bo)
		fmt.Printf("Deleted user [%s]: %e\n", bo.GboToJsonUnsafe(), err)
	}
}
