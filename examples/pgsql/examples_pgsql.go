/*
PostgreSQL Dao example.

$ go run examples_pgsql.go

PostgreSQL DAO implementation guideline:

	- Must implement method godal.IGenericDao.GdaoCreateFilter(storageId string, bo godal.IGenericBo) godal.FilterOpt
	  (already implemented by common.DaoAppSql)
	- If application uses its own BOs instead of godal.IGenericBo, it is recommended to implement a utility method
	  to transform godal.IGenericBo to application's BO and vice versa.
*/
package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/btnguyen2k/godal/examples/common"
	promsql "github.com/btnguyen2k/prom/sql"
	_ "github.com/jackc/pgx/v4/stdlib"

	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/godal/sql"
)

// DaoAppPgsql is PostgreSQL-implementation of IDaoApp.
type DaoAppPgsql struct {
	*common.DaoAppSql
}

// NewDaoAppPgsql is helper function to create PostgreSQL-implementation of IDaoApp.
func NewDaoAppPgsql(sqlC *promsql.SqlConnect, tableName string) common.IDaoApp {
	dao := &DaoAppPgsql{}
	dao.DaoAppSql = &common.DaoAppSql{TableName: tableName}
	dao.IGenericDaoSql = sql.NewGenericDaoSql(sqlC, godal.NewAbstractGenericDao(dao))
	dao.SetRowMapper(&sql.GenericRowMapperSql{
		NameTransformation: sql.NameTransfLowerCase,
		ColumnsListMap:     map[string][]string{tableName: common.ColsSql}})
	return dao
}

/*----------------------------------------------------------------------*/

func createSqlConnectForPgsql() *promsql.SqlConnect {
	driver := strings.ReplaceAll(os.Getenv("PGSQL_DRIVER"), `"`, "")
	dsn := strings.ReplaceAll(os.Getenv("PGSQL_URL"), `"`, "")
	if driver == "" || dsn == "" {
		panic("Please define env PGSQL_DRIVER, PGSQL_URL and optionally TIMEZONE")
	}
	timeZone := strings.ReplaceAll(os.Getenv("TIMEZONE"), `"`, "")
	if timeZone == "" {
		timeZone = "UTC"
	}
	urlTimezone := strings.ReplaceAll(timeZone, "/", "%2f")
	dsn = strings.ReplaceAll(dsn, "${loc}", urlTimezone)
	dsn = strings.ReplaceAll(dsn, "${tz}", urlTimezone)
	dsn = strings.ReplaceAll(dsn, "${timezone}", urlTimezone)
	sqlConnect, err := promsql.NewSqlConnectWithFlavor(driver, dsn, 10000, nil, promsql.FlavorPgSql)
	if sqlConnect == nil || err != nil {
		if err != nil {
			fmt.Println("Error:", err)
		}
		if sqlConnect == nil {
			panic("error creating [promsql.SqlConnect] instance")
		}
	}
	loc, _ := time.LoadLocation(timeZone)
	sqlConnect.SetLocation(loc)
	return sqlConnect
}

func initDataPgsql(sqlC *promsql.SqlConnect, table string) {
	sql := fmt.Sprintf("DROP TABLE IF EXISTS %s", table)
	_, err := sqlC.GetDB().Exec(sql)
	if err != nil {
		fmt.Printf("Error while executing query [%s]: %s\n", sql, err)
	}

	types := []string{"VARCHAR(16)", "VARCHAR(255)", "CHAR(1)", "BIGINT", "DOUBLE PRECISION", "VARCHAR(256)",
		"TIME", "TIME WITH TIME ZONE", "DATE", "DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE",
		"JSON", "JSON"}
	sql = fmt.Sprintf("CREATE TABLE %s (", table)
	for i := range common.ColsSql {
		sql += common.ColsSql[i] + " " + types[i] + ","
	}
	sql += "PRIMARY KEY(id))"
	fmt.Println("Query:", sql)
	_, err = sqlC.GetDB().Exec(sql)
	if err != nil {
		panic(err)
	}
}

func demoPgsqlInsertRows(loc *time.Location, table string, txMode bool) {
	sqlC := createSqlConnectForPgsql()
	defer sqlC.Close()
	initDataPgsql(sqlC, table)
	dao := NewDaoAppPgsql(sqlC, table)
	dao.EnableTxMode(txMode)

	fmt.Printf("-== Insert rows to table (TxMode=%v) ==-\n", txMode)

	// insert a row
	t := time.Unix(int64(rand.Int31()), rand.Int63()%1000000000).In(loc)
	bo := common.BoApp{
		Id:            "log",
		Description:   t.String(),
		ValBool:       rand.Int31()%2 == 0,
		ValInt:        rand.Int(),
		ValFloat:      rand.Float64(),
		ValString:     fmt.Sprintf("Logging application (TxMode=%v)", txMode),
		ValTime:       t,
		ValTimeZ:      t,
		ValDate:       t,
		ValDateZ:      t,
		ValDatetime:   t,
		ValDatetimeZ:  t,
		ValTimestamp:  t,
		ValTimestampZ: t,
		ValList:       []interface{}{true, 0, "1", 2.3, "system", "utility"},
		ValMap:        map[string]interface{}{"tags": []string{"system", "utility"}, "age": 103, "active": true},
	}
	fmt.Println("\tCreating bo:", string(bo.ToJson()))
	result, err := dao.Create(&bo)
	if err != nil {
		fmt.Printf("\t\tError: %s\n", err)
	} else {
		fmt.Printf("\t\tResult: %v\n", result)
	}

	// insert another row
	t = time.Unix(int64(rand.Int31()), rand.Int63()%1000000000).In(loc)
	bo = common.BoApp{
		Id:            "login",
		Description:   t.String(),
		ValBool:       rand.Int31()%2 == 0,
		ValInt:        rand.Int(),
		ValFloat:      rand.Float64(),
		ValString:     fmt.Sprintf("Authentication application (TxMode=%v)", txMode),
		ValTime:       t,
		ValTimeZ:      t,
		ValDate:       t,
		ValDateZ:      t,
		ValDatetime:   t,
		ValDatetimeZ:  t,
		ValTimestamp:  t,
		ValTimestampZ: t,
		ValList:       []interface{}{false, 9.8, "7", 6, "system", "security"},
		ValMap:        map[string]interface{}{"tags": []string{"system", "security"}, "age": 81, "active": false},
	}
	fmt.Println("\tCreating bo:", string(bo.ToJson()))
	result, err = dao.Create(&bo)
	if err != nil {
		fmt.Printf("\t\tError: %s\n", err)
	} else {
		fmt.Printf("\t\tResult: %v\n", result)
	}

	// insert another row with duplicated id
	bo = common.BoApp{Id: "login", ValString: "Authentication application (TxMode=true)(again)", ValList: []interface{}{"duplicated"}}
	fmt.Println("\tCreating bo:", string(bo.ToJson()))
	result, err = dao.Create(&bo)
	if err != nil {
		fmt.Printf("\t\tError: %s\n", err)
	} else {
		fmt.Printf("\t\tResult: %v\n", result)
	}

	fmt.Println(common.SEP)
}

func demoPgsqlFetchRowById(table string, ids ...string) {
	sqlC := createSqlConnectForPgsql()
	defer sqlC.Close()
	dao := NewDaoAppPgsql(sqlC, table)
	dao.EnableTxMode(false)

	fmt.Printf("-== Fetch rows by id ==-\n")
	for _, id := range ids {
		bo, err := dao.Get(id)
		if err != nil {
			fmt.Printf("\tError while fetching app [%s]: %s\n", id, err)
		} else if bo != nil {
			common.PrintApp(bo)
		} else {
			fmt.Printf("\tApp [%s] does not exist\n", id)
		}
	}

	fmt.Println(common.SEP)
}

func demoPgsqlFetchAllRows(table string) {
	sqlC := createSqlConnectForPgsql()
	defer sqlC.Close()
	dao := NewDaoAppPgsql(sqlC, table)
	dao.EnableTxMode(false)

	fmt.Println("-== Fetch all rows in table ==-")
	boList, err := dao.GetAll()
	if err != nil {
		fmt.Printf("\tError while fetching apps: %s\n", err)
	} else {
		for _, bo := range boList {
			common.PrintApp(bo)
		}
	}
	fmt.Println(common.SEP)
}

func demoPgsqlDeleteRow(table string, ids ...string) {
	sqlC := createSqlConnectForPgsql()
	defer sqlC.Close()
	dao := NewDaoAppPgsql(sqlC, table)
	dao.EnableTxMode(false)

	fmt.Println("-== Delete rows from table ==-")
	for _, id := range ids {
		bo, err := dao.Get(id)
		if err != nil {
			fmt.Printf("\tError while fetching app [%s]: %s\n", id, err)
		} else if bo == nil {
			fmt.Printf("\tApp [%s] does not exist, no need to delete\n", id)
		} else {
			fmt.Println("\tDeleting bo:", string(bo.ToJson()))
			result, err := dao.Delete(bo)
			if err != nil {
				fmt.Printf("\t\tError: %s\n", err)
			} else {
				fmt.Printf("\t\tResult: %v\n", result)
			}
			app, err := dao.Get(id)
			if err != nil {
				fmt.Printf("\t\tError while fetching app [%s]: %s\n", id, err)
			} else if app != nil {
				fmt.Printf("\t\tApp [%s] info: %v\n", app.Id, string(app.ToJson()))
			} else {
				fmt.Printf("\t\tApp [%s] no longer exist\n", id)
				result, err = dao.Delete(bo)
				fmt.Printf("\t\tDeleting app [%s] again: %v / %s\n", id, result, err)
			}
		}

	}
	fmt.Println(common.SEP)
}

func demoPgsqlUpdateRows(loc *time.Location, table string, ids ...string) {
	sqlC := createSqlConnectForPgsql()
	defer sqlC.Close()
	dao := NewDaoAppPgsql(sqlC, table)
	dao.EnableTxMode(false)

	fmt.Println("-== Update rows from table ==-")
	for _, id := range ids {
		t := time.Unix(int64(rand.Int31()), rand.Int63()%1000000000).In(loc)
		bo, err := dao.Get(id)
		if err != nil {
			fmt.Printf("\tError while fetching app [%s]: %s\n", id, err)
		} else if bo == nil {
			fmt.Printf("\tApp [%s] does not exist\n", id)
			bo = &common.BoApp{
				Id:            id,
				Description:   t.String(),
				ValString:     "(updated)",
				ValTime:       t,
				ValTimeZ:      t,
				ValDate:       t,
				ValDateZ:      t,
				ValDatetime:   t,
				ValDatetimeZ:  t,
				ValTimestamp:  t,
				ValTimestampZ: t,
			}
		} else {
			fmt.Println("\tExisting bo:", string(bo.ToJson()))
			bo.Description = t.String()
			bo.ValString += "(updated)"
			bo.ValTime = t
			bo.ValTimeZ = t
			bo.ValDate = t
			bo.ValDateZ = t
			bo.ValDatetime = t
			bo.ValDatetimeZ = t
			bo.ValTimestamp = t
			bo.ValTimestampZ = t
		}
		fmt.Println("\t\tUpdating bo:", string(bo.ToJson()))
		result, err := dao.Update(bo)
		if err != nil {
			fmt.Printf("\t\tError while updating app [%s]: %s\n", id, err)
		} else {
			fmt.Printf("\t\tResult: %v\n", result)
			bo, err = dao.Get(id)
			if err != nil {
				fmt.Printf("\t\tError while fetching app [%s]: %s\n", id, err)
			} else if bo != nil {
				fmt.Printf("\t\tApp [%s] info: %v\n", bo.Id, string(bo.ToJson()))
			} else {
				fmt.Printf("\t\tApp [%s] does not exist\n", id)
			}
		}
	}
	fmt.Println(common.SEP)
}

func demoPgsqlUpsertRows(loc *time.Location, table string, txMode bool, ids ...string) {
	sqlC := createSqlConnectForPgsql()
	defer sqlC.Close()
	dao := NewDaoAppPgsql(sqlC, table)
	dao.EnableTxMode(txMode)

	fmt.Printf("-== Upsert rows to table (TxMode=%v) ==-\n", txMode)
	for _, id := range ids {
		t := time.Unix(int64(rand.Int31()), rand.Int63()%1000000000).In(loc)
		bo, err := dao.Get(id)
		if err != nil {
			fmt.Printf("\tError while fetching app [%s]: %s\n", id, err)
		} else if bo == nil {
			fmt.Printf("\tApp [%s] does not exist\n", id)
			bo = &common.BoApp{
				Id:            id,
				Description:   t.String(),
				ValString:     fmt.Sprintf("(upsert,txmode=%v)", txMode),
				ValTime:       t,
				ValTimeZ:      t,
				ValDate:       t,
				ValDateZ:      t,
				ValDatetime:   t,
				ValDatetimeZ:  t,
				ValTimestamp:  t,
				ValTimestampZ: t,
			}
		} else {
			fmt.Println("\tExisting bo:", string(bo.ToJson()))
			bo.Description = t.String()
			bo.ValString += fmt.Sprintf("(upsert,txmode=%v)", txMode)
			bo.ValTime = t
			bo.ValTimeZ = t
			bo.ValDate = t
			bo.ValDateZ = t
			bo.ValDatetime = t
			bo.ValDatetimeZ = t
			bo.ValTimestamp = t
			bo.ValTimestampZ = t
		}
		fmt.Println("\t\tUpserting bo:", string(bo.ToJson()))
		result, err := dao.Upsert(bo)
		if err != nil {
			fmt.Printf("\t\tError while upserting app [%s]: %s\n", id, err)
		} else {
			fmt.Printf("\t\tResult: %v\n", result)
			bo, err = dao.Get(id)
			if err != nil {
				fmt.Printf("\t\tError while fetching app [%s]: %s\n", id, err)
			} else if bo != nil {
				fmt.Printf("\t\tApp [%s] info: %v\n", bo.Id, string(bo.ToJson()))
			} else {
				fmt.Printf("\t\tApp [%s] does not exist\n", id)
			}
		}
	}
	fmt.Println(common.SEP)
}

func demoPgsqlSelectSortingAndLimit(loc *time.Location, table string) {
	sqlC := createSqlConnectForPgsql()
	defer sqlC.Close()
	initDataPgsql(sqlC, table)
	dao := NewDaoAppPgsql(sqlC, table)
	dao.EnableTxMode(false)

	fmt.Println("-== Fetch rows from table with sorting and limit ==-")
	n := 100
	fmt.Printf("\tInserting %d rows...\n", n)
	for i := 0; i < n; i++ {
		id := strconv.Itoa(i)
		for len(id) < 3 {
			id = "0" + id
		}
		t := time.Unix(int64(rand.Int31()), rand.Int63()%1000000000).In(loc)
		bo := common.BoApp{
			Id:            id,
			Description:   t.String(),
			ValBool:       rand.Int31()%2 == 0,
			ValInt:        rand.Int(),
			ValFloat:      rand.Float64(),
			ValString:     id + " (sorting and limit)",
			ValTime:       t,
			ValTimeZ:      t,
			ValDate:       t,
			ValDateZ:      t,
			ValDatetime:   t,
			ValDatetimeZ:  t,
			ValTimestamp:  t,
			ValTimestampZ: t,
			ValList:       []interface{}{rand.Int31()%2 == 0, i, id},
			ValMap:        map[string]interface{}{"tags": []interface{}{id, i}},
		}
		_, err := dao.Create(&bo)
		if err != nil {
			panic(err)
		}
	}
	startOffset := rand.Intn(n)
	numRows := rand.Intn(10) + 1
	fmt.Printf("\tFetching %d rows, starting from offset %d...\n", numRows, startOffset)
	boList, err := dao.GetN(startOffset, numRows)
	if err != nil {
		fmt.Printf("\t\tError while fetching apps: %s\n", err)
	} else {
		for _, bo := range boList {
			fmt.Printf("\t\tApp [%s] info: %v\n", bo.Id, string(bo.ToJson()))
		}
	}
	fmt.Println(common.SEP)
}

func main() {
	rand.Seed(time.Now().UnixNano())
	timeZone := strings.ReplaceAll(os.Getenv("TIMEZONE"), `"`, "")
	loc, _ := time.LoadLocation(timeZone)

	table := "tbl_app"
	demoPgsqlInsertRows(loc, table, true)
	demoPgsqlInsertRows(loc, table, false)
	demoPgsqlFetchRowById(table, "login", "loggin")
	demoPgsqlFetchAllRows(table)
	demoPgsqlDeleteRow(table, "login", "loggin")
	demoPgsqlUpdateRows(loc, table, "log", "logging")
	demoPgsqlUpsertRows(loc, table, true, "log", "logging")
	demoPgsqlUpsertRows(loc, table, false, "log", "loggging")
	demoPgsqlSelectSortingAndLimit(loc, table)
}
