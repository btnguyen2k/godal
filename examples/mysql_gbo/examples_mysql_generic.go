/*
Generic MySQL Dao example. Run with command:

$ go run examples_mysql_generic.go

MySQL Dao implementation guideline:

	- Must implement method godal.IGenericDao.GdaoCreateFilter(storageId string, bo godal.IGenericBo) godal.FilterOpt
*/
package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godal/examples/common"
	"github.com/btnguyen2k/prom"
	_ "github.com/go-sql-driver/mysql"

	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/godal/sql"
)

const (
	fieldIdMysqlGeneric = "id"
)

var colsSqlMysqlGeneric = []string{"id", "val_desc", "val_bool", "val_int", "val_float", "val_string",
	"val_time", "val_list", "val_map"}

func createSqlConnectForMysqlGeneric() *prom.SqlConnect {
	driver := strings.ReplaceAll(os.Getenv("MYSQL_DRIVER"), `"`, "")
	dsn := strings.ReplaceAll(os.Getenv("MYSQL_URL"), `"`, "")
	if driver == "" || dsn == "" {
		panic("Please define env MYSQL_DRIVER, MYSQL_DRIVER and optionally TIMEZONE")
	}
	timeZone := strings.ReplaceAll(os.Getenv("TIMEZONE"), `"`, "")
	if timeZone == "" {
		timeZone = "UTC"
	}
	urlTimezone := strings.ReplaceAll(timeZone, "/", "%2f")
	dsn = strings.ReplaceAll(dsn, "${loc}", urlTimezone)
	dsn = strings.ReplaceAll(dsn, "${tz}", urlTimezone)
	dsn = strings.ReplaceAll(dsn, "${timezone}", urlTimezone)
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
	return sqlConnect
}

func initDataMysqlGeneric(sqlC *prom.SqlConnect, table string) {
	sql := fmt.Sprintf("DROP TABLE IF EXISTS %s", table)
	_, err := sqlC.GetDB().Exec(sql)
	if err != nil {
		fmt.Printf("Error while executing query [%s]: %s\n", sql, err)
	}

	types := []string{"VARCHAR(16)", "VARCHAR(255)", "CHAR(1)", "BIGINT", "DOUBLE", "VARCHAR(256)",
		"TIMESTAMP DEFAULT CURRENT_TIMESTAMP", "JSON", "JSON"}
	sql = fmt.Sprintf("CREATE TABLE %s (", table)
	for i := range colsSqlMysqlGeneric {
		sql += colsSqlMysqlGeneric[i] + " " + types[i] + ","
	}
	sql += "PRIMARY KEY(id))"
	fmt.Println("Query:", sql)
	_, err = sqlC.GetDB().Exec(sql)
	if err != nil {
		panic(err)
	}
}

type myGenericDaoMysql struct {
	*sql.GenericDaoSql
}

// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter.
func (dao *myGenericDaoMysql) GdaoCreateFilter(storageId string, bo godal.IGenericBo) godal.FilterOpt {
	id := bo.GboGetAttrUnsafe(fieldIdMysqlGeneric, reddo.TypeString)
	return godal.MakeFilter(map[string]interface{}{fieldIdMysqlGeneric: id})
}

// custom row mapper to transform 'val_list' and 'val_list' to Go objects
type myRowMapper struct {
	next godal.IRowMapper
}

func (m *myRowMapper) ToDbColName(storageId, fieldName string) string {
	return m.next.ToDbColName(storageId, fieldName)
}

func (m *myRowMapper) ToBoFieldName(storageId, colName string) string {
	panic("implement me")
}

func (m *myRowMapper) ToRow(storageId string, bo godal.IGenericBo) (interface{}, error) {
	row, err := m.next.ToRow(storageId, bo)
	return row, err
}

func (m *myRowMapper) ToBo(storageId string, row interface{}) (godal.IGenericBo, error) {
	gbo, err := m.next.ToBo(storageId, row)
	if v, e := gbo.GboGetAttrUnmarshalJson("val_list"); e == nil && v != nil {
		gbo.GboSetAttr("val_list", v)
	}
	if v, e := gbo.GboGetAttrUnmarshalJson("val_map"); e == nil && v != nil {
		gbo.GboSetAttr("val_map", v)
	}
	return gbo, err
}

func (m *myRowMapper) ColumnsList(storageId string) []string {
	return []string{"*"}
}

func newGenericDaoMysql(sqlc *prom.SqlConnect, txMode bool) godal.IGenericDao {
	dao := &myGenericDaoMysql{}
	dao.GenericDaoSql = sql.NewGenericDaoSql(sqlc, godal.NewAbstractGenericDao(dao))
	dao.SetTxModeOnWrite(txMode).SetSqlFlavor(prom.FlavorMySql)
	dao.SetRowMapper(&myRowMapper{&sql.GenericRowMapperSql{NameTransformation: sql.NameTransfLowerCase}})
	return dao
}

func demoMysqlInsertRowsGeneric(loc *time.Location, table string, txMode bool) {
	sqlC := createSqlConnectForMysqlGeneric()
	initDataMysqlGeneric(sqlC, table)
	dao := newGenericDaoMysql(sqlC, txMode)

	fmt.Printf("-== Insert rows to table (TxMode=%v) ==-\n", txMode)

	// insert a row
	t := time.Unix(int64(rand.Int31()), rand.Int63()%1000000000).In(loc)
	bo := godal.NewGenericBo()
	bo.GboSetAttr(fieldIdMysqlGeneric, "log")
	bo.GboSetAttr("val_desc", t.String())
	bo.GboSetAttr("val_bool", rand.Int31()%2 == 0)
	bo.GboSetAttr("val_int", rand.Int())
	bo.GboSetAttr("val_float", rand.Float64())
	bo.GboSetAttr("val_string", fmt.Sprintf("Logging application (TxMode=%v)", txMode))
	bo.GboSetAttr("val_time", t)
	bo.GboSetAttr("val_list", []interface{}{true, 0, "1", 2.3, "system", "utility"})
	bo.GboSetAttr("val_map", map[string]interface{}{"tags": []string{"system", "utility"}, "age": 103, "active": true})
	fmt.Println("\tCreating bo:", string(bo.GboToJsonUnsafe()))
	result, err := dao.GdaoCreate(table, bo)
	if err != nil {
		fmt.Printf("\t\tError: %s\n", err)
	} else {
		fmt.Printf("\t\tResult: %v\n", result)
	}

	// insert another row
	t = time.Unix(int64(rand.Int31()), rand.Int63()%1000000000).In(loc)
	bo = godal.NewGenericBo()
	bo.GboSetAttr(fieldIdMysqlGeneric, "login")
	bo.GboSetAttr("val_desc", t.String())
	bo.GboSetAttr("val_bool", rand.Int31()%2 == 0)
	bo.GboSetAttr("val_int", rand.Int())
	bo.GboSetAttr("val_float", rand.Float64())
	bo.GboSetAttr("val_string", fmt.Sprintf("Authentication application (TxMode=%v)", txMode))
	bo.GboSetAttr("val_time", t)
	bo.GboSetAttr("val_list", []interface{}{false, 9.8, "7", 6, "system", "security"})
	bo.GboSetAttr("val_map", map[string]interface{}{"tags": []string{"system", "security"}, "age": 81, "active": false})
	fmt.Println("\tCreating bo:", string(bo.GboToJsonUnsafe()))
	result, err = dao.GdaoCreate(table, bo)
	if err != nil {
		fmt.Printf("\t\tError: %s\n", err)
	} else {
		fmt.Printf("\t\tResult: %v\n", result)
	}

	// insert another row with duplicated id
	bo = godal.NewGenericBo()
	bo.GboSetAttr(fieldIdMysqlGeneric, "login")
	bo.GboSetAttr("val_string", "Authentication application (TxMode=true)(again)")
	bo.GboSetAttr("val_list", []interface{}{"duplicated"})
	fmt.Println("\tCreating bo:", string(bo.GboToJsonUnsafe()))
	result, err = dao.GdaoCreate(table, bo)
	if err != nil {
		fmt.Printf("\t\tError: %s\n", err)
	} else {
		fmt.Printf("\t\tResult: %v\n", result)
	}

	fmt.Println(common.SEP)
}

func demoMysqlFetchRowByIdGeneric(table string, ids ...string) {
	sqlC := createSqlConnectForMysqlGeneric()
	dao := newGenericDaoMysql(sqlC, false)

	fmt.Printf("-== Fetch rows by id ==-\n")
	for _, id := range ids {
		bo, err := dao.GdaoFetchOne(table, godal.MakeFilter(map[string]interface{}{fieldIdMysqlGeneric: id}))
		if err != nil {
			fmt.Printf("\tError while fetching app [%s]: %s\n", id, err)
		} else if bo != nil {
			fmt.Println("\tFetched bo:", string(bo.GboToJsonUnsafe()))
		} else {
			fmt.Printf("\tApp [%s] does not exist\n", id)
		}
	}

	fmt.Println(common.SEP)
}

func demoMysqlFetchAllRows(table string) {
	sqlC := createSqlConnectForMysqlGeneric()
	dao := newGenericDaoMysql(sqlC, false)

	fmt.Println("-== Fetch all rows in table ==-")
	boList, err := dao.GdaoFetchMany(table, nil, nil, 0, 0)
	if err != nil {
		fmt.Printf("\tError while fetching apps: %s\n", err)
	} else {
		for _, bo := range boList {
			fmt.Println("\tFetched bo:", string(bo.GboToJsonUnsafe()))
		}
	}
	fmt.Println(common.SEP)
}

func demoMysqlDeleteRowGeneric(table string, ids ...string) {
	sqlC := createSqlConnectForMysqlGeneric()
	dao := newGenericDaoMysql(sqlC, false)

	fmt.Println("-== Delete rows from table ==-")
	for _, id := range ids {
		bo, err := dao.GdaoFetchOne(table, godal.MakeFilter(map[string]interface{}{fieldIdMysqlGeneric: id}))
		if err != nil {
			fmt.Printf("\tError while fetching app [%s]: %s\n", id, err)
		} else if bo == nil {
			fmt.Printf("\tApp [%s] does not exist, no need to delete\n", id)
		} else {
			fmt.Println("\tDeleting bo:", string(bo.GboToJsonUnsafe()))
			result, err := dao.GdaoDelete(table, bo)
			if err != nil {
				fmt.Printf("\t\tError: %s\n", err)
			} else {
				fmt.Printf("\t\tResult: %v\n", result)
			}
			bo1, err := dao.GdaoFetchOne(table, godal.MakeFilter(map[string]interface{}{fieldIdMysqlGeneric: id}))
			if err != nil {
				fmt.Printf("\t\tError while fetching app [%s]: %s\n", id, err)
			} else if bo1 != nil {
				fmt.Printf("\t\tApp info: %v\n", string(bo.GboToJsonUnsafe()))
			} else {
				fmt.Printf("\t\tApp [%s] no longer exist\n", id)
				result, err := dao.GdaoDelete(table, bo)
				fmt.Printf("\t\tDeleting app [%s] again: %v / %s\n", id, result, err)
			}
		}

	}
	fmt.Println(common.SEP)
}

func demoMysqlUpdateRowsGeneric(loc *time.Location, table string, ids ...string) {
	sqlC := createSqlConnectForMysqlGeneric()
	dao := newGenericDaoMysql(sqlC, false)

	fmt.Println("-== Update rows from table ==-")
	for _, id := range ids {
		t := time.Unix(int64(rand.Int31()), rand.Int63()%1000000000).In(loc)
		bo, err := dao.GdaoFetchOne(table, godal.MakeFilter(map[string]interface{}{fieldIdMysqlGeneric: id}))
		if err != nil {
			fmt.Printf("\tError while fetching app [%s]: %s\n", id, err)
			continue
		} else if bo == nil {
			fmt.Printf("\tApp [%s] does not exist\n", id)
			bo = godal.NewGenericBo()
			bo.GboSetAttr(fieldIdMysqlGeneric, id)
			bo.GboSetAttr("val_desc", t.String())
			bo.GboSetAttr("val_string", "(updated)")
			bo.GboSetAttr("val_time", t)
		} else {
			fmt.Println("\tExisting bo:", string(bo.GboToJsonUnsafe()))
			bo.GboSetAttr("val_desc", t.String())
			bo.GboSetAttr("val_string", bo.GboGetAttrUnsafe("val_string", reddo.TypeString).(string)+"(updated)")
			bo.GboSetAttr("val_time", t)
		}
		fmt.Println("\t\tUpdating bo:", string(bo.GboToJsonUnsafe()))
		result, err := dao.GdaoUpdate(table, bo)
		if err != nil {
			fmt.Printf("\t\tError while updating app [%s]: %s\n", id, err)
		} else {
			fmt.Printf("\t\tResult: %v\n", result)
			bo, err := dao.GdaoFetchOne(table, godal.MakeFilter(map[string]interface{}{fieldIdMysqlGeneric: id}))
			if err != nil {
				fmt.Printf("\t\tError while fetching app [%s]: %s\n", id, err)
			} else if bo != nil {
				fmt.Printf("\t\tApp info: %v\n", string(bo.GboToJsonUnsafe()))
			} else {
				fmt.Printf("\t\tApp [%s] does not exist\n", id)
			}
		}
	}
	fmt.Println(common.SEP)
}

func demoMysqlUpsertRowsGeneric(loc *time.Location, table string, txMode bool, ids ...string) {
	sqlC := createSqlConnectForMysqlGeneric()
	dao := newGenericDaoMysql(sqlC, false)

	fmt.Printf("-== Upsert rows to table (TxMode=%v) ==-\n", txMode)
	for _, id := range ids {
		t := time.Unix(int64(rand.Int31()), rand.Int63()%1000000000).In(loc)
		bo, err := dao.GdaoFetchOne(table, godal.MakeFilter(map[string]interface{}{fieldIdMysqlGeneric: id}))
		if err != nil {
			fmt.Printf("\tError while fetching app [%s]: %s\n", id, err)
			continue
		} else if bo == nil {
			fmt.Printf("\tApp [%s] does not exist\n", id)
			bo = godal.NewGenericBo()
			bo.GboSetAttr(fieldIdMysqlGeneric, id)
			bo.GboSetAttr("val_desc", t.String())
			bo.GboSetAttr("val_string", fmt.Sprintf("(upsert,txmode=%v)", txMode))
			bo.GboSetAttr("val_time", t)
		} else {
			fmt.Println("\tExisting bo:", string(bo.GboToJsonUnsafe()))
			bo.GboSetAttr("val_desc", t.String())
			bo.GboSetAttr("val_string", bo.GboGetAttrUnsafe("val_string", reddo.TypeString).(string)+fmt.Sprintf("(upsert,txmode=%v)", txMode))
			bo.GboSetAttr("val_time", t)
		}
		fmt.Println("\t\tUpserting bo:", string(bo.GboToJsonUnsafe()))
		result, err := dao.GdaoSave(table, bo)
		if err != nil {
			fmt.Printf("\t\tError while upserting app [%s]: %s\n", id, err)
		} else {
			fmt.Printf("\t\tResult: %v\n", result)
			bo, err := dao.GdaoFetchOne(table, godal.MakeFilter(map[string]interface{}{fieldIdMysqlGeneric: id}))
			if err != nil {
				fmt.Printf("\t\tError while fetching app [%s]: %s\n", id, err)
			} else if bo != nil {
				fmt.Printf("\t\tApp info: %v\n", string(bo.GboToJsonUnsafe()))
			} else {
				fmt.Printf("\t\tApp [%s] does not exist\n", id)
			}
		}
	}
	fmt.Println(common.SEP)
}

func demoMysqlSelectSortingAndLimitGeneric(loc *time.Location, table string) {
	sqlC := createSqlConnectForMysqlGeneric()
	initDataMysqlGeneric(sqlC, table)
	dao := newGenericDaoMysql(sqlC, false)

	fmt.Println("-== Fetch rows from table with sorting and limit ==-")
	n := 100
	fmt.Printf("\tInserting %d rows...\n", n)
	for i := 0; i < n; i++ {
		id := strconv.Itoa(i)
		for len(id) < 3 {
			id = "0" + id
		}
		t := time.Unix(int64(rand.Int31()), rand.Int63()%1000000000).In(loc)
		bo := godal.NewGenericBo()
		bo.GboSetAttr(fieldIdMysqlGeneric, id)
		bo.GboSetAttr("val_desc", t.String())
		bo.GboSetAttr("val_bool", rand.Int31()%2 == 0)
		bo.GboSetAttr("val_int", rand.Int())
		bo.GboSetAttr("val_float", rand.Float64())
		bo.GboSetAttr("val_string", id+" (sorting and limit)")
		bo.GboSetAttr("val_time", t)
		bo.GboSetAttr("val_list", []interface{}{rand.Int31()%2 == 0, i, id})
		bo.GboSetAttr("val_map", map[string]interface{}{"tags": []interface{}{id, i}})
		_, err := dao.GdaoCreate(table, bo)
		if err != nil {
			panic(err)
		}
	}
	startOffset := rand.Intn(n)
	numRows := rand.Intn(10) + 1
	fmt.Printf("\tFetching %d rows, starting from offset %d...\n", numRows, startOffset)
	sorting := (&godal.SortingField{FieldName: fieldIdMysqlGeneric}).ToSortingOpt()
	boList, err := dao.GdaoFetchMany(table, nil, sorting, startOffset, numRows)
	if err != nil {
		fmt.Printf("\t\tError while fetching apps: %s\n", err)
	} else {
		for _, bo := range boList {
			fmt.Printf("\t\tApp info: %v\n", string(bo.GboToJsonUnsafe()))
		}
	}
	fmt.Println(common.SEP)
}

func main() {
	rand.Seed(time.Now().UnixNano())
	timeZone := strings.ReplaceAll(os.Getenv("TIMEZONE"), `"`, "")
	loc, _ := time.LoadLocation(timeZone)

	table := "tbl_app"
	demoMysqlInsertRowsGeneric(loc, table, true)
	demoMysqlInsertRowsGeneric(loc, table, false)
	demoMysqlFetchRowByIdGeneric(table, "login", "loggin")
	demoMysqlFetchAllRows(table)
	demoMysqlDeleteRowGeneric(table, "login", "loggin")
	demoMysqlUpdateRowsGeneric(loc, table, "log", "logging")
	demoMysqlUpsertRowsGeneric(loc, table, true, "log", "logging")
	demoMysqlUpsertRowsGeneric(loc, table, false, "log", "loggging")
	demoMysqlSelectSortingAndLimitGeneric(loc, table)
}
