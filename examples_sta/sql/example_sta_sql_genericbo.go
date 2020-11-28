package main

import (
	"fmt"
	"time"

	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/prom"

	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/godal/sql"

	_ "github.com/lib/pq"
)

// Sample database table for PostgreSQL
// CREATE TABLE test_user (
// 	uid			VARCHAR(32),
// 	uusername	VARCHAR(64),
// 	uname		VARCHAR(96),
// 	uversion	INT,
// 	uactived	INT,
// 	PRIMARY KEY (uid)
// )

// convenient function to create prom.SqlConnect instance (for PostgreSQL)
// it is highly recommended to provide a timezone setting (e.g. "Asia/Ho_Chi_Minh")
func createPgsqlConnect(url, timezone string) *prom.SqlConnect {
	timeoutMs := 10000
	var poolOptions *prom.SqlPoolOptions = nil
	driver := "postgres"
	sqlConnect, err := prom.NewSqlConnect(driver, url, timeoutMs, poolOptions)
	if err != nil {
		panic(err)
	}
	if timezone != "" {
		loc, err := time.LoadLocation(timezone)
		if loc != nil && err == nil {
			sqlConnect.SetLocation(loc)
		}
	}
	sqlConnect.SetDbFlavor(prom.FlavorPgSql)
	return sqlConnect
}

// convenient function to create MyGenericDaoSql instance
func createMyGenericDaoSql(sqlc *prom.SqlConnect, rowMapper godal.IRowMapper) godal.IGenericDao {
	dao := &MyGenericDaoSql{}
	dao.GenericDaoSql = sql.NewGenericDaoSql(sqlc, godal.NewAbstractGenericDao(dao))
	dao.SetRowMapper(rowMapper)
	dao.SetSqlFlavor(sqlc.GetDbFlavor())
	return dao
}

const (
	tableUser = "test_user"

	// table columns
	colUserId       = "uid"
	colUserUsername = "uusername"
	colUserName     = "uname"
	colUserVersion  = "uversion"
	colUserActived  = "uactived"

	// BO fields
	fieldUserId       = "id"
	fieldUserUsername = "username"
	fieldUserName     = "name"
	fieldUserVersion  = "version"
	fieldUserActived  = "actived"
)

type MyGenericDaoSql struct {
	*sql.GenericDaoSql
}

// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter
func (dao *MyGenericDaoSql) GdaoCreateFilter(tableName string, bo godal.IGenericBo) interface{} {
	if tableName == tableUser {
		// should match all primary keys
		return map[string]interface{}{
			colUserId: bo.GboGetAttrUnsafe(fieldUserId, reddo.TypeString),
		}
	}

	// primary key filtering for other database tables
	// ...

	return nil
}

func main() {
	// create new prom.SqlConnect
	url := "postgres://test:test@localhost:5432/test?sslmode=disable&client_encoding=UTF-8&application_name=godal"
	timezone := "Asia/Ho_Chi_Minh"
	sqlc := createPgsqlConnect(url, timezone)

	rowMapper := &sql.GenericRowMapperSql{
		// it is a good idea to normalize table column names and BO field names
		// in this case, we use "lower case transformation" rule to normalize table column and BO field names
		NameTransformation: sql.NameTransfLowerCase,
		GboFieldToColNameTranslator: map[string]map[string]interface{}{
			// {generic bo field -> database table column} mapping for tableUser
			tableUser: {
				fieldUserId:       colUserId,
				fieldUserUsername: colUserUsername,
				fieldUserName:     colUserName,
				fieldUserVersion:  colUserVersion,
				fieldUserActived:  colUserActived,
			},
			// mapping for other tables
		},
		ColNameToGboFieldTranslator: map[string]map[string]interface{}{
			// {database table column -> generic bo field} mapping for tableUser
			tableUser: {
				colUserId:       fieldUserId,
				colUserUsername: fieldUserUsername,
				colUserName:     fieldUserName,
				colUserVersion:  fieldUserVersion,
				colUserActived:  fieldUserActived,
			},
			// mapping for other tables
		},
		ColumnsListMap: map[string][]string{
			// all database table columns of tableUser
			tableUser: {colUserId, colUserUsername, colUserName, colUserVersion, colUserActived},

			// ...other tables
		},
	}

	// create new MyGenericDaoSql
	myDao := createMyGenericDaoSql(sqlc, rowMapper)

	bo := godal.NewGenericBo()
	bo.GboSetAttr(fieldUserId, "1")
	bo.GboSetAttr(fieldUserUsername, "btnguyen2k")
	bo.GboSetAttr(fieldUserName, "Nguyễn Bá Thành")
	bo.GboSetAttr(fieldUserVersion, time.Now().Unix())
	bo.GboSetAttr(fieldUserActived, 1) // convention: 1=true / 0=false

	{
		// CREATE
		_, err := myDao.GdaoCreate(tableUser, bo)
		fmt.Printf("Creating user [%s]...: %e\n", bo.GboToJsonUnsafe(), err)
	}

	{
		// READ
		filterBo := godal.NewGenericBo()
		filterBo.GboSetAttr(fieldUserId, "1") // use "field" here for filtering
		myBo, err := myDao.GdaoFetchOne(tableUser, myDao.GdaoCreateFilter(tableUser, filterBo))
		fmt.Printf("Fetched user [%s]: %e\n", myBo.GboToJsonUnsafe(), err)
	}

	{
		// UPDATE
		bo.GboSetAttr(fieldUserVersion, godal.NilValue)
		// bo.GboSetAttr("new_field", "a value") // database table structure is pre-defined, adding new field will cause error!
		bo.GboSetAttr(fieldUserActived, 0) // convention: 1=true / 0=false
		_, err := myDao.GdaoUpdate(tableUser, bo)
		fmt.Printf("Updated user [%s]: %e\n", bo.GboToJsonUnsafe(), err)

		// _, err = myDao.GdaoSave(tableUser, bo)
		// fmt.Printf("Saved user [%s]: %e\n", bo.GboToJsonUnsafe(), err)
	}

	// {
	// 	// DELETE
	// 	_, err := myDao.GdaoDelete(tableUser, bo)
	// 	fmt.Printf("Deleted user [%s]: %e\n", bo.GboToJsonUnsafe(), err)
	// }
}
