package cosmosdbsql

import (
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/btnguyen2k/consu/reddo"
	_ "github.com/btnguyen2k/gocosmos"
	"github.com/btnguyen2k/prom/sql"

	"github.com/btnguyen2k/godal"
)

func TestGenericRowMapperCosmosdb_ColumnsList(t *testing.T) {
	name := "TestGenericRowMapperCosmosdb_ColumnsList"
	rm := &GenericRowMapperCosmosdb{}

	colsTest := rm.ColumnsList("test")
	if len(colsTest) != 1 || colsTest[0] != "*" {
		t.Fatalf("%s failed. Collection: %s / Column list: %#v", name, "test", colsTest)
	}
}

func testToBo(t *testing.T, name string, rowmapper godal.IRowMapper, table string, row interface{}) {
	colA, colB, colC, col1, col2 := "cola", "ColB", "colC", "Col1", "coL2"
	valA, valB, val1, val2 := "a", "B", int64(1), int64(2)

	bo, err := rowmapper.ToBo(table, row)
	if err != nil || bo == nil {
		t.Fatalf("%s failed: %e / %v", name, err, bo)
	}
	if bo.GboGetAttrUnsafe(colA, reddo.TypeString) != valA ||
		bo.GboGetAttrUnsafe(colB, reddo.TypeString) != valB ||
		bo.GboGetAttrUnsafe(colC, reddo.TypeString) != nil ||
		bo.GboGetAttrUnsafe(col1, reddo.TypeInt).(int64) != val1 ||
		bo.GboGetAttrUnsafe(col2, reddo.TypeInt).(int64) != val2 {
		t.Fatalf("%s failed, Row: %v - Bo: %v", name, row, bo)
	}
}

func TestGenericRowMapperCosmosdb_ToBo(t *testing.T) {
	name := "TestGenericRowMapperCosmosdb_ToBo"
	table := "table"
	colA, colB, col1, col2 := "cola", "ColB", "Col1", "coL2"
	valA, valB, val1, val2 := "a", "B", int64(1), int64(2)
	rowmapper := &GenericRowMapperCosmosdb{}

	{
		row := map[string]interface{}{colA: valA, colB: valB, col1: val1, col2: val2}
		testToBo(t, name, rowmapper, table, row)
		testToBo(t, name, rowmapper, table, &row)
		testToBo(t, name, rowmapper, table+"-not-exists", row)
		row2 := &row
		testToBo(t, name, rowmapper, table, &row2)
	}

	{
		row := fmt.Sprintf(`{"%s": "%v", "%s": "%v", "%s": %v, "%s": %v}`, colA, valA, colB, valB, col1, val1, col2, val2)
		testToBo(t, name, rowmapper, table, row)
		testToBo(t, name, rowmapper, table, &row)
		testToBo(t, name, rowmapper, table+"-not-exists", row)
		row2 := &row
		testToBo(t, name, rowmapper, table, &row2)
	}

	{
		row := []byte(fmt.Sprintf(`{"%s": "%v", "%s": "%v", "%s": %v, "%s": %v}`, colA, valA, colB, valB, col1, val1, col2, val2))
		testToBo(t, name, rowmapper, table, row)
		testToBo(t, name, rowmapper, table, &row)
		testToBo(t, name, rowmapper, table+"-not-exists", row)
		row2 := &row
		testToBo(t, name, rowmapper, table, &row2)
	}

	{
		var row interface{} = nil
		if bo, err := rowmapper.ToBo(table, row); err != nil || bo != nil {
			t.Fatalf("%s failed: %e / %v", name, err, bo)
		}
		if bo, err := rowmapper.ToBo(table, &row); err != nil || bo != nil {
			t.Fatalf("%s failed: %e / %v", name, err, bo)
		}
		row2 := &row
		if bo, err := rowmapper.ToBo(table, &row2); err != nil || bo != nil {
			t.Fatalf("%s failed: %e / %v", name, err, bo)
		}
	}

	{
		var row *string = nil
		if bo, err := rowmapper.ToBo(table, row); err != nil || bo != nil {
			t.Fatalf("%s failed: %e / %v", name, err, bo)
		}
		if bo, err := rowmapper.ToBo(table, &row); err != nil || bo != nil {
			t.Fatalf("%s failed: %e / %v", name, err, bo)
		}
		row2 := &row
		if bo, err := rowmapper.ToBo(table, &row2); err != nil || bo != nil {
			t.Fatalf("%s failed: %e / %v", name, err, bo)
		}
	}

	{
		var row []byte = nil
		if bo, err := rowmapper.ToBo(table, row); err != nil || bo != nil {
			t.Fatalf("%s failed: %e / %v", name, err, bo)
		}
		if bo, err := rowmapper.ToBo(table, &row); err != nil || bo != nil {
			t.Fatalf("%s failed: %e / %v", name, err, bo)
		}
		row2 := &row
		if bo, err := rowmapper.ToBo(table, &row2); err != nil || bo != nil {
			t.Fatalf("%s failed: %e / %v", name, err, bo)
		}
	}

	{
		var row *[]byte = nil
		if bo, err := rowmapper.ToBo(table, row); err != nil || bo != nil {
			t.Fatalf("%s failed: %e / %v", name, err, bo)
		}
		if bo, err := rowmapper.ToBo(table, &row); err != nil || bo != nil {
			t.Fatalf("%s failed: %e / %v", name, err, bo)
		}
		row2 := &row
		if bo, err := rowmapper.ToBo(table, &row2); err != nil || bo != nil {
			t.Fatalf("%s failed: %e / %v", name, err, bo)
		}
	}
}

func TestGenericRowMapperCosmosdb_ToBo_Invalid(t *testing.T) {
	name := "TestGenericRowMapperCosmosdb_ToBo_Invalid"
	rm := &GenericRowMapperCosmosdb{}
	gbo, err := rm.ToBo("", time.Time{})
	if err == nil || gbo != nil {
		t.Fatalf("%s failed: error: %#v", name, err)
	}
}

func TestGenericRowMapperCosmosdb_ToBo_Nil(t *testing.T) {
	name := "TestGenericRowMapperCosmosdb_ToBo_Nil"
	rm := &GenericRowMapperCosmosdb{}
	gbo, err := rm.ToBo("", nil)
	if err != nil || gbo != nil {
		t.Fatalf("%s failed: error: %#v", name, err)
	}
}

func TestGenericRowMapperCosmosdb_ToRow_Nil(t *testing.T) {
	name := "TestGenericRowMapperCosmosdb_ToRow_Nil"
	rm := &GenericRowMapperCosmosdb{}
	row, err := rm.ToRow("", nil)
	if err != nil || row != nil {
		t.Fatalf("%s failed: error: %#v", name, err)
	}
}

func TestGenericRowMapperCosmosdb_ToRow_Intact(t *testing.T) {
	name := "TestGenericRowMapperCosmosdb_ToRow_Intact"
	rm := &GenericRowMapperCosmosdb{}
	js := `{"ColA":1,"colb":"a string","COLC":2.3,"colD":true}`
	gbo := godal.NewGenericBo()
	gbo.GboFromJson([]byte(js))

	{
		row, err := rm.ToRow("", gbo)
		if err != nil || row == nil {
			t.Fatalf("%s failed: error: %#v", name, err)
		}
		m, ok := row.(map[string]interface{})
		if !ok || len(m) != 4 {
			t.Fatalf("%s failed: row: %#v", name, row)
		}
		if v, e := reddo.ToInt(m["ColA"]); e != nil || v != 1 {
			t.Fatalf("%s failed: expected data[%s] to be %#v but received %#v", name, "ColA", 1, m["ColA"])
		}
		if v, e := reddo.ToString(m["colb"]); e != nil || v != "a string" {
			t.Fatalf("%s failed: expected data[%s] to be %#v but received %#v", name, "colb", "a string", m["colb"])
		}
		if v, e := reddo.ToFloat(m["COLC"]); e != nil || v != 2.3 {
			t.Fatalf("%s failed: expected data[%s] to be %#v but received %#v", name, "COLC", 2.3, m["COLC"])
		}
		if v, e := reddo.ToBool(m["colD"]); e != nil || v != true {
			t.Fatalf("%s failed: expected data[%s] to be %#v but received %#v", name, "colD", true, m["colD"])
		}
	}
}

func TestGenericRowMapperCosmosdb_ToGbo_Intact(t *testing.T) {
	name := "TestGenericRowMapperCosmosdb_ToGbo_Intact"
	rm := &GenericRowMapperCosmosdb{}
	js := `{"ColA":1,"colb":"a string","COLC":2.3,"colD":true}`
	row := make(map[string]interface{})
	json.Unmarshal([]byte(js), &row)

	{
		gbo, err := rm.ToBo("", row)
		if err != nil || gbo == nil {
			t.Fatalf("%s failed: error: %#v", name, err)
		}
		if v, e := gbo.GboGetAttr("ColA", reddo.TypeInt); e != nil || v.(int64) != 1 {
			t.Fatalf("%s failed: expected attr[%s] to be %#v but received %#v", name, "ColA", 1, v)
		}
		if v, e := gbo.GboGetAttr("colb", reddo.TypeString); e != nil || v.(string) != "a string" {
			t.Fatalf("%s failed: expected attr[%s] to be %#v but received %#v", name, "colb", "a string", v)
		}
		if v, e := gbo.GboGetAttr("COLC", reddo.TypeFloat); e != nil || v.(float64) != 2.3 {
			t.Fatalf("%s failed: expected attr[%s] to be %#v but received %#v", name, "COLC", 2.3, v)
		}
		if v, e := gbo.GboGetAttr("colD", reddo.TypeBool); e != nil || v.(bool) != true {
			t.Fatalf("%s failed: expected attr[%s] to be %#v but received %#v", name, "colD", true, v)
		}
	}
}

func TestGenericRowMapperCosmosdb_ToDbColName_Intact(t *testing.T) {
	name := "TestGenericRowMapperCosmosdb_ToDbColName_Intact"
	cola := "field1"
	colb := "FIELD2"
	colc := "Field3"
	cold := "FielD4"
	rm := &GenericRowMapperCosmosdb{}

	if colName, expected := rm.ToDbColName("table", "field1"), cola; colName != expected {
		t.Fatalf("%s failed: expected %#v but received %#v", name, expected, colName)
	}
	if colName, expected := rm.ToDbColName("-", "FIELD2"), colb; colName != expected {
		t.Fatalf("%s failed: expected %#v but received %#v", name, expected, colName)
	}
	if colName, expected := rm.ToDbColName("*", "Field3"), colc; colName != expected {
		t.Fatalf("%s failed: expected %#v but received %#v", name, expected, colName)
	}
	if colName, expected := rm.ToDbColName("*", "FielD4"), cold; colName != expected {
		t.Fatalf("%s failed: expected %#v but received %#v", name, expected, colName)
	}
}

func TestGenericRowMapperCosmosdb_ToBoFieldName_Intact(t *testing.T) {
	name := "TestGenericRowMapperCosmosdb_ToBoFieldName_Intact"
	fielda := "col1"
	fieldb := "COL2"
	fieldc := "Col3"
	fieldd := "CoL4"
	rm := &GenericRowMapperCosmosdb{}

	if fieldName, expected := rm.ToBoFieldName("table", "col1"), fielda; fieldName != expected {
		t.Fatalf("%s failed: expected %#v but received %#v", name, expected, fieldName)
	}
	if fieldName, expected := rm.ToBoFieldName("-", "COL2"), fieldb; fieldName != expected {
		t.Fatalf("%s failed: expected %#v but received %#v", name, expected, fieldName)
	}
	if fieldName, expected := rm.ToBoFieldName("*", "Col3"), fieldc; fieldName != expected {
		t.Fatalf("%s failed: expected %#v but received %#v", name, expected, fieldName)
	}
	if fieldName, expected := rm.ToBoFieldName("*", "CoL4"), fieldd; fieldName != expected {
		t.Fatalf("%s failed: expected %#v but received %#v", name, expected, fieldName)
	}
}

/*--------------------------------------------------------------------------------*/

func prepareTableCosmosdb(sqlc *sql.SqlConnect, table string) error {
	sql := fmt.Sprintf("DROP COLLECTION IF EXISTS %s", table)
	if _, err := sqlc.GetDB().Exec(sql); err != nil {
		return err
	}
	sql = fmt.Sprintf("CREATE COLLECTION %s WITH pk=/%s WITH uk=/%s", table, fieldGboGroup, fieldGboUsername)
	if _, err := sqlc.GetDB().Exec(sql); err != nil {
		return err
	}
	return nil
}

func newSqlConnect(driver, url, timezone string, flavor sql.DbFlavor) (*sql.SqlConnect, error) {
	driver = strings.Trim(driver, "\"")
	url = strings.Trim(url, "\"")
	if driver == "" || url == "" {
		return nil, nil
	}

	dbre := regexp.MustCompile(`(?i);db=(\w+)`)
	db := "godal"
	if result := dbre.FindAllStringSubmatch(url, -1); result != nil {
		db = result[0][1]
	} else {
		url += ";Db=" + db
	}

	urlTimezone := strings.ReplaceAll(timezone, "/", "%2f")
	url = strings.ReplaceAll(url, "${loc}", urlTimezone)
	url = strings.ReplaceAll(url, "${tz}", urlTimezone)
	url = strings.ReplaceAll(url, "${timezone}", urlTimezone)
	sqlc, err := sql.NewSqlConnectWithFlavor(driver, url, 10000, nil, flavor)
	if err == nil && sqlc != nil {
		loc, _ := time.LoadLocation(timezone)
		sqlc.SetLocation(loc)
	}
	return sqlc, err

	sqlc.GetDB().Exec("CREATE DATABASE " + db + " WITH maxru=10000")

	return sqlc, err
}

func createDaoCosmosdb(sqlc *sql.SqlConnect, collectionName string) *UserDaoSql {
	dao := &UserDaoSql{collectionName: collectionName}
	dao.GenericDaoCosmosdb = NewGenericDaoCosmosdb(sqlc, godal.NewAbstractGenericDao(dao))
	dao.SetRowMapper(GenericRowMapperCosmosdbInstance)
	dao.SetTxModeOnWrite(false).SetTxIsolationLevel(gosql.LevelDefault)
	dao.CosmosSetPkGboMapPath(map[string]string{collectionName: fieldGboGroup})
	return dao
}

func initDaoCosmosdb(driver, url, tableName string, flavor sql.DbFlavor) *UserDaoSql {
	sqlc, err := newSqlConnect(driver, url, testTimeZone, flavor)
	if err != nil || sqlc == nil {
		return nil
	}
	return createDaoCosmosdb(sqlc, tableName)
}

const (
	testTableName = "test_user"

	colSqlId       = fieldGboId
	colSqlUsername = fieldGboUsername
	// colSqlData     = fieldGboData
	// colSqlGroup    = fieldGboGroup
	colSqlValPInt    = fieldGboValPInt
	colSqlValPFloat  = fieldGboValPFloat
	colSqlValPString = fieldGboValPString
	colSqlValPTime   = fieldGboValPTime

	fieldGboId         = "id"
	fieldGboUsername   = "username"
	fieldGboData       = "data"
	fieldGboGroup      = "group"
	fieldGboValPInt    = "pint"
	fieldGboValPFloat  = "pfloat"
	fieldGboValPString = "pstring"
	fieldGboValPTime   = "ptime"

	testTimeZone = "Asia/Ho_Chi_Minh"
)

type UserDaoSql struct {
	*GenericDaoCosmosdb
	collectionName string
}

// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter.
func (dao *UserDaoSql) GdaoCreateFilter(collectionName string, bo godal.IGenericBo) godal.FilterOpt {
	if collectionName == dao.collectionName {
		return godal.FilterOptFieldOpValue{FieldName: fieldGboId, Operator: godal.FilterOpEqual, Value: bo.GboGetAttrUnsafe(fieldGboId, reddo.TypeString)}
	}
	return false
}

func (dao *UserDaoSql) toGbo(u *UserBoSql) godal.IGenericBo {
	js, _ := json.Marshal(u)
	gbo := godal.NewGenericBo()
	if err := gbo.GboImportViaMap(map[string]interface{}{
		fieldGboGroup: u.Group,
		fieldGboId:    u.Id, fieldGboUsername: u.Username, fieldGboData: string(js),
		fieldGboValPInt: u.ValPInt, fieldGboValPFloat: u.ValPFloat, fieldGboValPString: u.ValPString, fieldGboValPTime: u.ValPTime,
	}); err != nil {
		return nil
	}
	return gbo
}

func (dao *UserDaoSql) toUser(gbo godal.IGenericBo) *UserBoSql {
	if gbo == nil {
		return nil
	}
	js := gbo.GboGetAttrUnsafe(fieldGboData, reddo.TypeString).(string)
	bo := UserBoSql{}
	if err := json.Unmarshal([]byte(js), &bo); err != nil {
		return nil
	}
	bo.Created = bo.Created.In(dao.GetSqlConnect().GetLocation())
	if bo.ValPTime != nil {
		t := bo.ValPTime.In(dao.GetSqlConnect().GetLocation())
		bo.ValPTime = &t
	}
	return &bo
}

type UserBoSql struct {
	Id         string     `json:"id"`
	Username   string     `json:"username"`
	Name       string     `json:"name"`
	Version    int        `json:"version"`
	Active     bool       `json:"active"`
	Created    time.Time  `json:"created"`
	Group      string     `json:"group"`
	ValPInt    *int64     `json:"pint"`
	ValPFloat  *float64   `json:"pfloat"`
	ValPString *string    `json:"pstring"`
	ValPTime   *time.Time `json:"ptime"`
}

/*---------------------------------------------------------------*/

func TestGenericDaoCosmosdb_CosmosSetGetIdGboMapPath(t *testing.T) {
	testName := "TestGenericDaoCosmosdb_CosmosSetGetIdGboMapPath"
	dao := initDaoCosmosdb(os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, sql.FlavorCosmosDb)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.GetSqlConnect().Close()

	idGboMapPath := map[string]string{"*": "myid"}
	dao.CosmosSetIdGboMapPath(idGboMapPath)
	idGboMapPath["*"] = "id"
	idGboMapPath2 := dao.CosmosGetIdGboMapPath()
	if idGboMapPath2 == nil || idGboMapPath2["*"] != "myid" {
		t.Fatalf("%s failed: %#v", testName, idGboMapPath2)
	}
}

func TestGenericDaoCosmosdb_CosmosSetGetPkGboMapPath(t *testing.T) {
	testName := "TestGenericDaoCosmosdb_CosmosSetGetPkGboMapPath"
	dao := initDaoCosmosdb(os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, sql.FlavorCosmosDb)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.GetSqlConnect().Close()

	pkGboMapPath := map[string]string{"*": "mypk"}
	dao.CosmosSetPkGboMapPath(pkGboMapPath)
	pkGboMapPath["*"] = "pk"
	pkGboMapPath2 := dao.CosmosGetPkGboMapPath()
	if pkGboMapPath2 == nil || pkGboMapPath2["*"] != "mypk" {
		t.Fatalf("%s failed: %#v", testName, pkGboMapPath2)
	}
}

func TestGenericDaoCosmosdb_CosmosSetGetPkRowMapPath(t *testing.T) {
	testName := "TestGenericDaoCosmosdb_CosmosSetGetPkRowMapPath"
	dao := initDaoCosmosdb(os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, sql.FlavorCosmosDb)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.GetSqlConnect().Close()

	pkRowMapPath := map[string]string{"*": "mypk"}
	dao.CosmosSetPkRowMapPath(pkRowMapPath)
	pkRowMapPath["*"] = "pk"
	pkRowMapPath2 := dao.CosmosGetPkRowMapPath()
	if pkRowMapPath2 == nil || pkRowMapPath2["*"] != "mypk" {
		t.Fatalf("%s failed: %#v", testName, pkRowMapPath2)
	}
}

func TestGenericDaoCosmosdb_CosmosGetId(t *testing.T) {
	testName := "TestGenericDaoCosmosdb_CosmosGetId"
	dao := initDaoCosmosdb(os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, sql.FlavorCosmosDb)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.GetSqlConnect().Close()

	gbo := godal.NewGenericBo()
	if id := dao.CosmosGetId("*", gbo); id != "" {
		t.Fatalf("%s failed: expected empty id-value but received %#v", testName, id)
	}

	gbo = godal.NewGenericBo()
	gbo.GboSetAttr("id", "myid")
	if id := dao.CosmosGetId("*", gbo); id != "myid" {
		t.Fatalf("%s failed: expected %#v but received %#v", testName, "myid", id)
	}

	dao.CosmosSetIdGboMapPath(map[string]string{"*": "uid"})
	if id := dao.CosmosGetId("*", gbo); id != "" {
		t.Fatalf("%s failed: expected empty id-value but received %#v", testName, id)
	}

	gbo = godal.NewGenericBo()
	gbo.GboSetAttr("uid", "myid")
	if id := dao.CosmosGetId("*", gbo); id != "myid" {
		t.Fatalf("%s failed: expected %#v but received %#v", testName, "myid", id)
	}
}

func TestGenericDaoCosmosdb_CosmosGetPk(t *testing.T) {
	testName := "TestGenericDaoCosmosdb_CosmosGetPk"
	dao := initDaoCosmosdb(os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, sql.FlavorCosmosDb)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.GetSqlConnect().Close()

	gbo := godal.NewGenericBo()
	if pk := dao.CosmosGetPk("*", gbo); pk != "" {
		t.Fatalf("%s failed: expected empty pk-value but received %#v", testName, pk)
	}

	gbo = godal.NewGenericBo()
	gbo.GboSetAttr("pk", "mypk")
	dao.CosmosSetPkRowMapPath(map[string]string{"*": "pk"})
	if id := dao.CosmosGetPk("*", gbo); id != "mypk" {
		t.Fatalf("%s failed: expected %#v but received %#v", testName, "mypk", id)
	}

	gbo = godal.NewGenericBo()
	dao.CosmosSetPkGboMapPath(map[string]string{"*": "upk"})
	if pk := dao.CosmosGetPk("*", gbo); pk != "" {
		t.Fatalf("%s failed: expected empty pk-value but received %#v", testName, pk)
	}

	gbo.GboSetAttr("upk", "myupk")
	if id := dao.CosmosGetPk("*", gbo); id != "myupk" {
		t.Fatalf("%s failed: expected %#v but received %#v", testName, "myupk", id)
	}
}

/*--------------------------------------------------------------------------------*/

func _compareUsers(t *testing.T, name string, expected, target *UserBoSql) {
	if target == nil {
		t.Fatalf("%s failed: target is nil", name)
	}
	if target.Id != expected.Id {
		t.Fatalf("%s failed: field [Id] mismatched - %#v / %#v", name, expected.Id, target.Id)
	}
	if target.Username != expected.Username {
		t.Fatalf("%s failed: field [Username] mismatched - %#v / %#v", name, expected.Username, target.Username)
	}
	if target.Name != expected.Name {
		t.Fatalf("%s failed: field [Name] mismatched - %#v / %#v", name, expected.Name, target.Name)
	}
	if target.Version != expected.Version {
		t.Fatalf("%s failed: field [Version] mismatched - %#v / %#v", name, expected.Version, target.Version)
	}
	if target.Active != expected.Active {
		t.Fatalf("%s failed: field [Active] mismatched - %#v / %#v", name, expected.Active, target.Active)
	}
	layout := time.RFC3339
	loc, _ := time.LoadLocation(testTimeZone)
	if target.Created.In(loc).Format(layout) != expected.Created.In(loc).Format(layout) {
		t.Fatalf("%s failed: field [Created] mismatched - %#v / %#v", name, expected.Created.Format(layout), target.Created.Format(layout))
	}

	if (expected.ValPInt != nil && (target.ValPInt == nil || *target.ValPInt != *expected.ValPInt)) || (expected.ValPInt == nil && target.ValPInt != nil) {
		t.Fatalf("%s failed: field [PInt] mismatched - %#v / %#v", name, expected.ValPInt, target.ValPInt)
	}
	if (expected.ValPFloat != nil && (target.ValPFloat == nil || *target.ValPFloat != *expected.ValPFloat)) || (expected.ValPFloat == nil && target.ValPFloat != nil) {
		t.Fatalf("%s failed: field [PFloat] mismatched - %#v / %#v", name, expected.ValPFloat, target.ValPFloat)
	}
	if (expected.ValPString != nil && (target.ValPString == nil || *target.ValPString != *expected.ValPString)) || (expected.ValPString == nil && target.ValPString != nil) {
		t.Fatalf("%s failed: field [PString] mismatched - %#v / %#v", name, expected.ValPString, target.ValPString)
	}
	if (expected.ValPTime != nil && (target.ValPTime == nil || target.ValPTime.In(loc).Format(layout) != expected.ValPTime.In(loc).Format(layout))) || (expected.ValPTime == nil && target.ValPTime != nil) {
		t.Fatalf("%s failed: field [PTime] mismatched - %#v / %#v", name, expected.ValPTime, target.ValPTime)
	}
}

func dotestGenericDaoSqlGdaoDelete(t *testing.T, name string, dao *UserDaoSql) {
	user := &UserBoSql{
		Id:       "1",
		Username: "btnguyen2k",
		Name:     "Thanh Nguyen",
		Version:  int(time.Now().Unix()),
		Active:   false,
		Created:  time.Now(),
		Group:    "Administrator",
	}
	_, err := dao.GdaoCreate(dao.collectionName, dao.toGbo(user))
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoCreate", err)
	}

	// GdaoDelete should be successful and number of affected rows should be 0
	clone := *user
	clone.Id = "2"
	if numRows, err := dao.GdaoDelete(dao.collectionName, dao.toGbo(&clone)); err != nil {
		t.Fatalf("%s failed: %e", name, err)
	} else if numRows != 0 {
		t.Fatalf("%s failed: expected %#v row(s) deleted but received %#v", name, 0, numRows)
	}

	// GdaoDelete should be successful and number of affected rows should be 1
	if numRows, err := dao.GdaoDelete(dao.collectionName, dao.toGbo(user)); err != nil {
		t.Fatalf("%s failed: %e", name, err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) deleted but received %#v", name, 1, numRows)
	}

	// GdaoFetchOne should be successful and the returned BO should be nil
	if u, err := dao.GdaoFetchOne(dao.collectionName, dao.GdaoCreateFilter(dao.collectionName, dao.toGbo(user))); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoFetchOne", err)
	} else if u != nil {
		t.Fatalf("%s failed: non-nil", name+"/GdaoFetchOne")
	}
}

func dotestGenericDaoSqlGdaoDeleteMany(t *testing.T, name string, dao *UserDaoSql) {
	// filter rows that has ID >= "8" OR ID < "3"
	filter := &godal.FilterOptOr{Filters: []godal.FilterOpt{
		&godal.FilterOptFieldOpValue{FieldName: fieldGboId, Operator: godal.FilterOpGreaterOrEqual, Value: "8"},
		&godal.FilterOptFieldOpValue{FieldName: fieldGboId, Operator: godal.FilterOpLess, Value: "3"},
	}}

	// GdaoDeleteMany should be successful and number of affected rows should be 0
	if numRows, err := dao.GdaoDeleteMany(dao.collectionName, filter); err != nil {
		t.Fatalf("%s failed: %e", name, err)
	} else if numRows != 0 {
		t.Fatalf("%s failed: expected %#v row(s) deleted but received %#v", name, 0, numRows)
	}

	for i := 0; i < 10; i++ {
		id := strconv.Itoa(i)
		user := &UserBoSql{
			Id:       id,
			Username: "user" + id,
			Name:     "Thanh " + id,
			Version:  int(time.Now().UnixNano()),
			Active:   i%3 == 0,
			Created:  time.Now(),
		}
		_, err := dao.GdaoCreate(dao.collectionName, dao.toGbo(user))
		if err != nil {
			t.Fatalf("%s failed: %e", name+"/GdaoCreate", err)
		}
	}

	// GdaoDeleteMany should be successful and number of affected rows should be 5 (removed rows "0", "1", "2", "8", "9")
	if numRows, err := dao.GdaoDeleteMany(dao.collectionName, filter); err != nil {
		t.Fatalf("%s failed: %e", name, err)
	} else if numRows != 5 {
		t.Fatalf("%s failed: expected %#v row(s) deleted but received %#v", name, 5, numRows)
	}
}

func dotestGenericDaoSqlGdaoFetchOne(t *testing.T, name string, dao *UserDaoSql) {
	filter := dao.GdaoCreateFilter(dao.collectionName, dao.toGbo(&UserBoSql{Id: "1"}))

	// GdaoFetchOne should be successful and the returned BO is nil
	if gbo, err := dao.GdaoFetchOne(dao.collectionName, filter); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoFetchOne", err)
	} else if gbo != nil {
		t.Fatalf("%s failed: non-nil", name+"/GdaoFetchOne")
	}

	user := &UserBoSql{
		Id:       "1",
		Username: "btnguyen2k",
		Name:     "Thanh Nguyen",
		Version:  int(time.Now().Unix()),
		Active:   false,
		Created:  time.Now().Round(time.Second),
	}
	_, err := dao.GdaoCreate(dao.collectionName, dao.toGbo(user))
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoCreate", err)
	}

	// GdaoFetchOne should be successful and the returned BO is equal to the original BO
	if gbo, err := dao.GdaoFetchOne(dao.collectionName, filter); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoFetchOne", err)
	} else if gbo == nil {
		t.Fatalf("%s failed: nil", name+"/GdaoFetchOne")
	} else {
		fetchedUser := dao.toUser(gbo)
		_compareUsers(t, name, user, fetchedUser)
	}
}

func dotestGenericDaoSqlGdaoFetchMany(t *testing.T, name string, dao *UserDaoSql) {
	// filter rows that has "3" < ID <= "8"
	filter := &godal.FilterOptAnd{Filters: []godal.FilterOpt{
		&godal.FilterOptFieldOpValue{FieldName: fieldGboId, Operator: godal.FilterOpLessOrEqual, Value: "8"},
		&godal.FilterOptFieldOpValue{FieldName: fieldGboId, Operator: godal.FilterOpGreater, Value: "3"},
	}}

	// GdaoFetchMany should be successful and number of affected rows is 0
	if dbRows, err := dao.GdaoFetchMany(dao.collectionName, filter, nil, 1, 3); err != nil {
		t.Fatalf("%s failed: %e", name, err)
	} else if dbRows == nil || len(dbRows) != 0 {
		t.Fatalf("%s failed: expected %#v row(s) but received %#v", name, 0, dbRows)
	}

	userMap := make(map[string]*UserBoSql)
	for i := 0; i < 10; i++ {
		id := strconv.Itoa(i)
		user := &UserBoSql{
			Id:       id,
			Username: "user" + id,
			Name:     "Thanh " + id,
			Version:  int(time.Now().UnixNano()),
			Active:   i%3 == 0,
			Created:  time.Now().Round(time.Second),
		}
		_, err := dao.GdaoCreate(dao.collectionName, dao.toGbo(user))
		if err != nil {
			t.Fatalf("%s failed: %e", name+"/GdaoCreate", err)
		}
		userMap[id] = user
	}

	fetchIdList := []string{"7", "6", "5"}
	sorting := (&godal.SortingOpt{}).Add(&godal.SortingField{FieldName: fieldGboUsername, Descending: true})
	if dbRows, err := dao.GdaoFetchMany(dao.collectionName, filter, sorting, 1, 3); err != nil {
		t.Fatalf("%s failed: %e", name, err)
	} else if dbRows == nil || len(dbRows) != 3 {
		t.Fatalf("%s failed: expected %#v row(s) but received %#v", name, 3, len(dbRows))
	} else {
		for i, row := range dbRows {
			fetchedUser := dao.toUser(row)
			_compareUsers(t, name, userMap[fetchIdList[i]], fetchedUser)
		}
	}
}

func dotestGenericDaoSqlGdaoCreate(t *testing.T, name string, dao *UserDaoSql) {
	user := &UserBoSql{
		Id:       "1",
		Username: "btnguyen2k",
		Name:     "Thanh Nguyen",
		Version:  int(time.Now().Unix()),
		Active:   false,
		Created:  time.Now().Round(time.Second),
	}
	if numRows, err := dao.GdaoCreate(dao.collectionName, dao.toGbo(user)); err != nil {
		t.Fatalf("%s failed: %e", name, err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name, 1, numRows)
	}

	clone := *user

	// duplicated id
	clone.Username = "thanhn"
	if numRows, err := dao.GdaoCreate(dao.collectionName, dao.toGbo(&clone)); err != godal.ErrGdaoDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed: num rows %#v / error: %e", name, numRows, err)
	}

	// duplicated username
	clone.Id = "2"
	clone.Username = "btnguyen2k"
	if numRows, err := dao.GdaoCreate(dao.collectionName, dao.toGbo(&clone)); err != godal.ErrGdaoDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed: num rows %#v / error: %e", name, numRows, err)
	}

	filter := dao.GdaoCreateFilter(dao.collectionName, dao.toGbo(&UserBoSql{Id: "1"}))
	if gbo, err := dao.GdaoFetchOne(dao.collectionName, filter); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoFetchOne", err)
	} else if gbo == nil {
		t.Fatalf("%s failed: nil", name+"/GdaoFetchOne")
	} else {
		fetchedUser := dao.toUser(gbo)
		_compareUsers(t, name, user, fetchedUser)
	}
}

func dotestGenericDaoSqlGdaoUpdate(t *testing.T, name string, dao *UserDaoSql) {
	user1 := &UserBoSql{
		Id:       "1",
		Username: "btnguyen2k",
		Name:     "Thanh Nguyen",
		Version:  int(time.Now().Unix()),
		Active:   false,
		Created:  time.Now().Round(time.Second),
		Group:    "users",
	}
	user2 := &UserBoSql{
		Id:       "2",
		Username: "nbthanh",
		Name:     "Thanh B. Nguyen",
		Version:  int(time.Now().Unix()),
		Active:   true,
		Created:  time.Now().Round(time.Second),
		Group:    "users",
	}

	// non-exist row
	if numRows, err := dao.GdaoUpdate(dao.collectionName, dao.toGbo(user1)); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoUpdate", err)
	} else if numRows != 0 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoUpdate", 0, numRows)
	}

	// insert a few rows
	if numRows, err := dao.GdaoCreate(dao.collectionName, dao.toGbo(user1)); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoCreate", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoCreate", 1, numRows)
	}
	if numRows, err := dao.GdaoCreate(dao.collectionName, dao.toGbo(user2)); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoCreate", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoCreate", 1, numRows)
	}

	user1.Username = "thanhn"
	if numRows, err := dao.GdaoUpdate(dao.collectionName, dao.toGbo(user1)); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoUpdate", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoUpdate", 1, numRows)
	}

	filter := dao.GdaoCreateFilter(dao.collectionName, dao.toGbo(&UserBoSql{Id: "1"}))
	if gbo, err := dao.GdaoFetchOne(dao.collectionName, filter); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoFetchOne", err)
	} else if gbo == nil {
		t.Fatalf("%s failed: nil", name+"/GdaoFetchOne")
	} else {
		fetchedUser := dao.toUser(gbo)
		_compareUsers(t, name, user1, fetchedUser)
	}

	// duplicated unique index
	user1.Username = user2.Username
	if numRows, err := dao.GdaoUpdate(dao.collectionName, dao.toGbo(user1)); err != godal.ErrGdaoDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed: expected 0/ErrGdaoDuplicatedEntry but received %#v/%#v", name+"/GdaoUpdate", numRows, err)
	}
}

func dotestGenericDaoSqlGdaoSave(t *testing.T, name string, dao *UserDaoSql) {
	user1 := &UserBoSql{
		Id:       "1",
		Username: "btnguyen2k",
		Name:     "Thanh Nguyen",
		Version:  int(time.Now().Unix()),
		Active:   false,
		Created:  time.Now().Round(time.Second),
		Group:    "users",
	}
	user2 := &UserBoSql{
		Id:       "2",
		Username: "nbthanh",
		Name:     "Thanh B. Nguyen",
		Version:  int(time.Now().Unix()),
		Active:   true,
		Created:  time.Now().Round(time.Second),
		Group:    "users",
	}

	if numRows, err := dao.GdaoSave(dao.collectionName, dao.toGbo(user1)); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoSave", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) saved but received %#v", name+"/GdaoSave", 1, numRows)
	}
	if numRows, err := dao.GdaoSave(dao.collectionName, dao.toGbo(user2)); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoSave", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) saved but received %#v", name+"/GdaoSave", 1, numRows)
	}

	// change username
	user1.Username = "thanhn"
	if numRows, err := dao.GdaoSave(dao.collectionName, dao.toGbo(user1)); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoSave", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoSave", 1, numRows)
	}

	filter := dao.GdaoCreateFilter(dao.collectionName, dao.toGbo(&UserBoSql{Id: "1"}))
	if gbo, err := dao.GdaoFetchOne(dao.collectionName, filter); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoFetchOne", err)
	} else if gbo == nil {
		t.Fatalf("%s failed: nil", name+"/GdaoFetchOne")
	} else {
		fetchedUser := dao.toUser(gbo)
		if !reflect.DeepEqual(user1, user1) {
			t.Fatalf("%s failed: expected %v but received %v", name+"/GdaoFetchOne", "thanhn", fetchedUser.Username)
		}
	}

	// duplicated username
	user1.Username = user2.Username
	if numRows, err := dao.GdaoSave(dao.collectionName, dao.toGbo(user1)); err != godal.ErrGdaoDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed: expected 0/ErrGdaoDuplicatedEntry but received %#v/%#v", name+"/GdaoUpdate", numRows, err)
	}
}

func dotestGenericDaoSqlGdaoFilterNull(t *testing.T, name string, dao *UserDaoSql) {
	rand.Seed(time.Now().UnixNano())
	var userList = make([]*UserBoSql, 0)
	for i := 0; i < 100; i++ {
		id := strconv.Itoa(i)
		user := &UserBoSql{
			Id:       id,
			Username: "user" + id,
			Name:     "Thanh " + id,
			Version:  int(time.Now().UnixNano()),
			Active:   i%3 == 0,
			Created:  time.Now().Round(time.Second).Add(time.Duration(rand.Intn(1024)) * time.Minute),
		}
		vInt := rand.Int63n(1024)
		vFloat := math.Round(rand.Float64()) * 1e3 / 1e3
		vString := fmt.Sprintf("%f", vFloat)
		vTime := time.Now().Add(time.Duration(rand.Intn(1024)) * time.Minute)
		if i%2 == 0 {
			user.ValPInt = &vInt
		}
		if i%3 == 0 {
			user.ValPFloat = &vFloat
		}
		if i%4 == 0 {
			user.ValPString = &vString
		}
		if i%5 == 0 {
			user.ValPTime = &vTime
		}
		_, err := dao.GdaoCreate(dao.collectionName, dao.toGbo(user))
		if err != nil {
			t.Fatalf("%s failed: %s", name+"/GdaoCreate", err)
		}
		userList = append(userList, user)
	}

	var filterInt godal.FilterOpt = &godal.FilterOptFieldIsNull{FieldName: fieldGboValPInt}
	var filterFloat godal.FilterOpt = &godal.FilterOptFieldIsNull{FieldName: fieldGboValPFloat}
	var filterString godal.FilterOpt = &godal.FilterOptFieldIsNull{FieldName: fieldGboValPString}
	var filterTime godal.FilterOpt = &godal.FilterOptFieldIsNull{FieldName: fieldGboValPTime}
	filerList := []godal.FilterOpt{filterInt, filterFloat, filterString, filterTime}
	for _, filter := range filerList {
		filter = (&godal.FilterOptAnd{}).Add(filter).
			Add(&godal.FilterOptFieldOpValue{FieldName: fieldGboId, Operator: godal.FilterOpGreater, Value: strconv.Itoa(rand.Intn(64))})
		gboList, err := dao.GdaoFetchMany(dao.collectionName, filter, nil, 0, 0)
		if err != nil {
			t.Fatalf("%s failed: %s", name+"/GdaoFetchMany", err)
		}
		if len(gboList) == 0 {
			t.Fatalf("%s failed: empty result list", name+"/GdaoFetchMany")
		}
		for _, gbo := range gboList {
			user := dao.toUser(gbo)
			id, _ := strconv.Atoi(user.Id)
			expected := userList[id]

			if filter == filterInt && user.ValPInt != nil {
				t.Fatalf("%s failed: field [PInt] should be nil, but %#v", name, *user.ValPInt)
			}
			if filter == filterFloat && user.ValPFloat != nil {
				t.Fatalf("%s failed: field [PFloat] should be nil, but %#v", name, *user.ValPFloat)
			}
			if filter == filterString && user.ValPString != nil {
				t.Fatalf("%s failed: field [PString] should be nil, but %#v", name, *user.ValPString)
			}
			if filter == filterTime && user.ValPTime != nil {
				t.Fatalf("%s failed: field [PTime] should be nil, but %#v", name, *user.ValPTime)
			}

			_compareUsers(t, name, expected, user)
			// _checkFilterNull(t, name, expected, user)
		}
	}
}

func dotestGenericDaoSqlGdaoFilterNotNull(t *testing.T, name string, dao *UserDaoSql) {
	rand.Seed(time.Now().UnixNano())
	var userList = make([]*UserBoSql, 0)
	for i := 0; i < 100; i++ {
		id := strconv.Itoa(i)
		user := &UserBoSql{
			Id:       id,
			Username: "user" + id,
			Name:     "Thanh " + id,
			Version:  int(time.Now().UnixNano()),
			Active:   i%3 == 0,
			Created:  time.Now().Round(time.Second).Add(time.Duration(rand.Intn(1024)) * time.Minute),
		}
		vInt := rand.Int63n(1024)
		vFloat := math.Round(rand.Float64()) * 1e3 / 1e3
		vString := fmt.Sprintf("%f", vFloat)
		vTime := time.Now().Add(time.Duration(rand.Intn(1024)) * time.Minute)
		if i%2 == 0 {
			user.ValPInt = &vInt
		}
		if i%3 == 0 {
			user.ValPFloat = &vFloat
		}
		if i%4 == 0 {
			user.ValPString = &vString
		}
		if i%5 == 0 {
			user.ValPTime = &vTime
		}
		_, err := dao.GdaoCreate(dao.collectionName, dao.toGbo(user))
		if err != nil {
			t.Fatalf("%s failed: %e", name+"/GdaoCreate", err)
		}
		userList = append(userList, user)
	}

	var filterInt godal.FilterOpt = &godal.FilterOptFieldIsNotNull{FieldName: fieldGboValPInt}
	var filterFloat godal.FilterOpt = &godal.FilterOptFieldIsNotNull{FieldName: fieldGboValPFloat}
	var filterString godal.FilterOpt = &godal.FilterOptFieldIsNotNull{FieldName: fieldGboValPString}
	var filterTime godal.FilterOpt = &godal.FilterOptFieldIsNotNull{FieldName: fieldGboValPTime}
	filerList := []godal.FilterOpt{filterInt, filterFloat, filterString, filterTime}
	for _, filter := range filerList {
		filter = (&godal.FilterOptAnd{}).Add(filter).
			Add(&godal.FilterOptFieldOpValue{FieldName: fieldGboId, Operator: godal.FilterOpGreater, Value: strconv.Itoa(rand.Intn(64))})
		gboList, err := dao.GdaoFetchMany(dao.collectionName, filter, nil, 0, 0)
		if err != nil {
			t.Fatalf("%s failed: %s", name+"/GdaoFetchMany/"+reflect.TypeOf(filter).String(), err)
		}
		if len(gboList) == 0 {
			t.Fatalf("%s failed: empty result list", name+"/GdaoFetchMany/"+reflect.TypeOf(filter).String())
		}
		for _, gbo := range gboList {
			user := dao.toUser(gbo)
			id, _ := strconv.Atoi(user.Id)
			expected := userList[id]

			if filter == filterInt && user.ValPInt == nil {
				t.Fatalf("%s failed: field [PInt] should not be nil, but %#v", name, *user.ValPInt)
			}
			if filter == filterFloat && user.ValPFloat == nil {
				t.Fatalf("%s failed: field [PFloat] should not be nil, but %#v", name, *user.ValPFloat)
			}
			if filter == filterString && user.ValPString == nil {
				t.Fatalf("%s failed: field [PString] should not be nil, but %#v", name, *user.ValPString)
			}
			if filter == filterTime && user.ValPTime == nil {
				t.Fatalf("%s failed: field [PTime] should not be nil, but %#v", name, *user.ValPTime)
			}

			_compareUsers(t, name, expected, user)
			// _checkFilterNull(t, name, expected, user)
		}
	}
}

/*---------------------------------------------------------------*/

const (
	envCosmosdbDriver = "COSMOSDB_DRIVER"
	envCosmosdbUrl    = "COSMOSDB_URL"
)

func TestGenericDaoCosmosdb_SetGetSqlConnect(t *testing.T) {
	testName := "TestGenericDaoCosmosdb_SetGetSqlConnect"
	dao := initDaoCosmosdb(os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, sql.FlavorCosmosDb)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.GetSqlConnect().Close()

	sqlc, _ := newSqlConnect(os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTimeZone, sql.FlavorCosmosDb)
	defer sqlc.Close()
	if sqlc == dao.GetSqlConnect() {
		t.Fatalf("%s failed: should not equal", testName)
	}
	dao.SetSqlConnect(sqlc)
	if sqlc != dao.GetSqlConnect() {
		t.Fatalf("%s failed: should equal", testName)
	}
}

func TestGenericDaoCosmosdb_GdaoDelete(t *testing.T) {
	testName := "TestGenericDaoCosmosdb_GdaoDelete"
	dao := initDaoCosmosdb(os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, sql.FlavorCosmosDb)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.GetSqlConnect().Close()

	err := prepareTableCosmosdb(dao.GetSqlConnect(), dao.collectionName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableCosmosdb", err)
	}
	dotestGenericDaoSqlGdaoDelete(t, testName, dao)
}

func TestGenericDaoCosmosdb_GdaoDeleteMany(t *testing.T) {
	testName := "TestGenericDaoCosmosdb_GdaoDeleteMany"
	dao := initDaoCosmosdb(os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, sql.FlavorCosmosDb)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.GetSqlConnect().Close()

	err := prepareTableCosmosdb(dao.GetSqlConnect(), dao.collectionName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableCosmosdb", err)
	}
	dotestGenericDaoSqlGdaoDeleteMany(t, testName, dao)
}

func TestGenericDaoCosmosdb_GdaoFetchOne(t *testing.T) {
	testName := "TestGenericDaoCosmosdb_GdaoDeleteMany"
	dao := initDaoCosmosdb(os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, sql.FlavorCosmosDb)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.GetSqlConnect().Close()

	err := prepareTableCosmosdb(dao.GetSqlConnect(), dao.collectionName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableCosmosdb", err)
	}
	dotestGenericDaoSqlGdaoFetchOne(t, testName, dao)
}

func TestGenericDaoCosmosdb_GdaoFetchMany(t *testing.T) {
	testName := "TestGenericDaoCosmosdb_GdaoFetchMany"
	dao := initDaoCosmosdb(os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, sql.FlavorCosmosDb)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.GetSqlConnect().Close()

	err := prepareTableCosmosdb(dao.GetSqlConnect(), dao.collectionName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableCosmosdb", err)
	}
	dotestGenericDaoSqlGdaoFetchMany(t, testName, dao)
}

func TestGenericDaoCosmosdb_GdaoCreate(t *testing.T) {
	testName := "TestGenericDaoCosmosdb_GdaoCreate"
	dao := initDaoCosmosdb(os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, sql.FlavorCosmosDb)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.GetSqlConnect().Close()

	err := prepareTableCosmosdb(dao.GetSqlConnect(), dao.collectionName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableCosmosdb", err)
	}
	dotestGenericDaoSqlGdaoCreate(t, testName, dao)
}

func TestGenericDaoCosmosdb_GdaoUpdate(t *testing.T) {
	testName := "TestGenericDaoCosmosdb_GdaoUpdate"
	dao := initDaoCosmosdb(os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, sql.FlavorCosmosDb)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.GetSqlConnect().Close()

	err := prepareTableCosmosdb(dao.GetSqlConnect(), dao.collectionName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableCosmosdb", err)
	}
	dotestGenericDaoSqlGdaoUpdate(t, testName, dao)
}

func TestGenericDaoCosmosdb_GdaoSave(t *testing.T) {
	testName := "TestGenericDaoCosmosdb_GdaoSave"
	dao := initDaoCosmosdb(os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, sql.FlavorCosmosDb)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.GetSqlConnect().Close()

	err := prepareTableCosmosdb(dao.GetSqlConnect(), dao.collectionName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableCosmosdb", err)
	}
	dotestGenericDaoSqlGdaoSave(t, testName, dao)
}

func TestGenericDaoCosmosdb_GdaoSaveTxModeOnWrite(t *testing.T) {
	testName := "TestGenericDaoCosmosdb_GdaoSaveTxModeOnWrite"
	dao := initDaoCosmosdb(os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, sql.FlavorCosmosDb)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.GetSqlConnect().Close()

	err := prepareTableCosmosdb(dao.GetSqlConnect(), dao.collectionName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableCosmosdb", err)
	}
	dao.SetTxModeOnWrite(true)
	dotestGenericDaoSqlGdaoSave(t, testName, dao)
}

func TestGenericDaoCosmosdb_FilterNull(t *testing.T) {
	testName := "TestGenericDaoCosmosdb_FilterNull"
	dao := initDaoCosmosdb(os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, sql.FlavorCosmosDb)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.GetSqlConnect().Close()

	err := prepareTableCosmosdb(dao.GetSqlConnect(), dao.collectionName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableCosmosdb", err)
	}
	dotestGenericDaoSqlGdaoFilterNull(t, testName, dao)
}

func TestGenericDaoCosmosdb_FilterNotNull(t *testing.T) {
	testName := "TestGenericDaoCosmosdb_FilterNotNull"
	dao := initDaoCosmosdb(os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, sql.FlavorCosmosDb)
	if dao == nil {
		t.SkipNow()
	}
	defer dao.GetSqlConnect().Close()

	err := prepareTableCosmosdb(dao.GetSqlConnect(), dao.collectionName)
	if err != nil {
		t.Fatalf("%s failed: %e", testName+"/prepareTableCosmosdb", err)
	}
	dotestGenericDaoSqlGdaoFilterNotNull(t, testName, dao)
}
