package cosmosdbsql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/btnguyen2k/consu/reddo"
	_ "github.com/btnguyen2k/gocosmos"
	"github.com/btnguyen2k/prom"

	"github.com/btnguyen2k/godal"
	godalsql "github.com/btnguyen2k/godal/sql"
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
	row, err := rm.ToBo("", time.Time{})
	if err == nil || row != nil {
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

/*--------------------------------------------------------------------------------*/

func prepareTableCosmosdb(sqlc *prom.SqlConnect, table string) error {
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

func newSqlConnect(t *testing.T, testName string, driver, url, timezone string, flavor prom.DbFlavor) (*prom.SqlConnect, error) {
	driver = strings.Trim(driver, "\"")
	url = strings.Trim(url, "\"")
	if driver == "" || url == "" {
		t.Skipf("%s skilled", testName)
	}

	url += ";Db=godal"

	urlTimezone := strings.ReplaceAll(timezone, "/", "%2f")
	url = strings.ReplaceAll(url, "${loc}", urlTimezone)
	url = strings.ReplaceAll(url, "${tz}", urlTimezone)
	url = strings.ReplaceAll(url, "${timezone}", urlTimezone)
	sqlc, err := prom.NewSqlConnectWithFlavor(driver, url, 10000, nil, flavor)
	if err != nil || sqlc == nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	loc, _ := time.LoadLocation(timezone)
	sqlc.SetLocation(loc)

	sqlc.GetDB().Exec("CREATE DATABASE godal WITH maxru=10000")

	return sqlc, err
}

func createDaoCosmosdb(sqlc *prom.SqlConnect, collectionName string) *UserDaoSql {
	dao := &UserDaoSql{collectionName: collectionName}
	dao.GenericDaoCosmosdb = NewGenericDaoCosmosdb(sqlc, godal.NewAbstractGenericDao(dao))
	dao.SetSqlFlavor(prom.FlavorCosmosDb).SetRowMapper(GenericRowMapperCosmosdbInstance)
	dao.SetTxModeOnWrite(false).SetTxIsolationLevel(sql.LevelDefault)
	dao.CosmosSetPkGboMapPath(map[string]string{collectionName: fieldGboGroup})
	return dao
}

func initDaoCosmosdb(t *testing.T, testName string, driver, url, tableName string, flavor prom.DbFlavor) *UserDaoSql {
	sqlc, _ := newSqlConnect(t, testName, driver, url, testTimeZone, flavor)
	return createDaoCosmosdb(sqlc, tableName)
}

const (
	testTableName = "test_user"

	colSqlId       = fieldGboId
	colSqlUsername = fieldGboUsername
	// colSqlData     = fieldGboData
	// colSqlGroup    = fieldGboGroup

	fieldGboId       = "id"
	fieldGboUsername = "username"
	fieldGboData     = "data"
	fieldGboGroup    = "group"

	testTimeZone = "Asia/Ho_Chi_Minh"
)

type UserDaoSql struct {
	*GenericDaoCosmosdb
	collectionName string
}

// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter.
func (dao *UserDaoSql) GdaoCreateFilter(collectionName string, bo godal.IGenericBo) interface{} {
	if collectionName == dao.collectionName {
		return map[string]interface{}{fieldGboId: bo.GboGetAttrUnsafe(fieldGboId, reddo.TypeString)}
	}
	return false
}

func (dao *UserDaoSql) toGbo(u *UserBoSql) godal.IGenericBo {
	js, _ := json.Marshal(u)
	gbo := godal.NewGenericBo()
	if err := gbo.GboImportViaJson(map[string]interface{}{fieldGboGroup: u.Group, fieldGboId: u.Id, fieldGboUsername: u.Username, fieldGboData: string(js)}); err != nil {
		return nil
	}
	return gbo
}

func (dao *UserDaoSql) toUser(gbo godal.IGenericBo) *UserBoSql {
	js := gbo.GboGetAttrUnsafe(fieldGboData, reddo.TypeString).(string)
	bo := UserBoSql{}
	if err := json.Unmarshal([]byte(js), &bo); err != nil {
		return nil
	}
	return &bo
}

type UserBoSql struct {
	Id       string    `json:"id"`
	Username string    `json:"username"`
	Name     string    `json:"name"`
	Version  int       `json:"version"`
	Active   bool      `json:"active"`
	Created  time.Time `json:"created"`
	Group    string    `json:"group"`
}

/*---------------------------------------------------------------*/

func TestGenericDaoCosmosdb_CosmosSetGetIdGboMapPath(t *testing.T) {
	name := "TestGenericDaoCosmosdb_CosmosSetGetIdGboMapPath"
	dao := initDaoCosmosdb(t, name, os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, prom.FlavorCosmosDb)
	idGboMapPath := map[string]string{"*": "myid"}
	dao.CosmosSetIdGboMapPath(idGboMapPath)
	idGboMapPath["*"] = "id"
	idGboMapPath2 := dao.CosmosGetIdGboMapPath()
	if idGboMapPath2 == nil || idGboMapPath2["*"] != "myid" {
		t.Fatalf("%s failed: %#v", name, idGboMapPath2)
	}
}

func TestGenericDaoCosmosdb_CosmosSetGetPkGboMapPath(t *testing.T) {
	name := "TestGenericDaoCosmosdb_CosmosSetGetPkGboMapPath"
	dao := initDaoCosmosdb(t, name, os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, prom.FlavorCosmosDb)
	pkGboMapPath := map[string]string{"*": "mypk"}
	dao.CosmosSetPkGboMapPath(pkGboMapPath)
	pkGboMapPath["*"] = "pk"
	pkGboMapPath2 := dao.CosmosGetPkGboMapPath()
	if pkGboMapPath2 == nil || pkGboMapPath2["*"] != "mypk" {
		t.Fatalf("%s failed: %#v", name, pkGboMapPath2)
	}
}

func TestGenericDaoCosmosdb_CosmosSetGetPkRowMapPath(t *testing.T) {
	name := "TestGenericDaoCosmosdb_CosmosSetGetPkRowMapPath"
	dao := initDaoCosmosdb(t, name, os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, prom.FlavorCosmosDb)
	pkRowMapPath := map[string]string{"*": "mypk"}
	dao.CosmosSetPkRowMapPath(pkRowMapPath)
	pkRowMapPath["*"] = "pk"
	pkRowMapPath2 := dao.CosmosGetPkRowMapPath()
	if pkRowMapPath2 == nil || pkRowMapPath2["*"] != "mypk" {
		t.Fatalf("%s failed: %#v", name, pkRowMapPath2)
	}
}

func TestGenericDaoCosmosdb_CosmosGetId(t *testing.T) {
	name := "TestGenericDaoCosmosdb_CosmosGetId"
	dao := initDaoCosmosdb(t, name, os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, prom.FlavorCosmosDb)

	gbo := godal.NewGenericBo()
	if id := dao.CosmosGetId("*", gbo); id != "" {
		t.Fatalf("%s failed: expected empty id-value but received %#v", name, id)
	}

	gbo = godal.NewGenericBo()
	gbo.GboSetAttr("id", "myid")
	if id := dao.CosmosGetId("*", gbo); id != "myid" {
		t.Fatalf("%s failed: expected %#v but received %#v", name, "myid", id)
	}

	dao.CosmosSetIdGboMapPath(map[string]string{"*": "uid"})
	if id := dao.CosmosGetId("*", gbo); id != "" {
		t.Fatalf("%s failed: expected empty id-value but received %#v", name, id)
	}

	gbo = godal.NewGenericBo()
	gbo.GboSetAttr("uid", "myid")
	if id := dao.CosmosGetId("*", gbo); id != "myid" {
		t.Fatalf("%s failed: expected %#v but received %#v", name, "myid", id)
	}
}

func TestGenericDaoCosmosdb_CosmosGetPk(t *testing.T) {
	name := "TestGenericDaoCosmosdb_CosmosGetPk"
	dao := initDaoCosmosdb(t, name, os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, prom.FlavorCosmosDb)

	gbo := godal.NewGenericBo()
	if pk := dao.CosmosGetPk("*", gbo); pk != "" {
		t.Fatalf("%s failed: expected empty pk-value but received %#v", name, pk)
	}

	gbo = godal.NewGenericBo()
	gbo.GboSetAttr("pk", "mypk")
	dao.CosmosSetPkRowMapPath(map[string]string{"*": "pk"})
	if id := dao.CosmosGetPk("*", gbo); id != "mypk" {
		t.Fatalf("%s failed: expected %#v but received %#v", name, "mypk", id)
	}

	gbo = godal.NewGenericBo()
	dao.CosmosSetPkGboMapPath(map[string]string{"*": "upk"})
	if pk := dao.CosmosGetPk("*", gbo); pk != "" {
		t.Fatalf("%s failed: expected empty pk-value but received %#v", name, pk)
	}

	gbo.GboSetAttr("upk", "myupk")
	if id := dao.CosmosGetPk("*", gbo); id != "myupk" {
		t.Fatalf("%s failed: expected %#v but received %#v", name, "myupk", id)
	}
}

/*--------------------------------------------------------------------------------*/

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

	filterUser := &UserBoSql{Id: "2"}
	if numRows, err := dao.GdaoDelete(dao.collectionName, dao.toGbo(filterUser)); err != nil {
		t.Fatalf("%s failed: %e", name, err)
	} else if numRows != 0 {
		t.Fatalf("%s failed: expected %#v row(s) deleted but received %#v", name, 0, numRows)
	}

	if numRows, err := dao.GdaoDelete(dao.collectionName, dao.toGbo(user)); err != nil {
		t.Fatalf("%s failed: %e", name, err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) deleted but received %#v", name, 1, numRows)
	}

	if u, err := dao.GdaoFetchOne(dao.collectionName, dao.GdaoCreateFilter(dao.collectionName, dao.toGbo(user))); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoFetchOne", err)
	} else if u != nil {
		t.Fatalf("%s failed: non-nil", name+"/GdaoFetchOne")
	}
}

func dotestGenericDaoSqlGdaoDeleteMany(t *testing.T, name string, dao *UserDaoSql) {
	filter := &godalsql.FilterOr{
		FilterAndOr: godalsql.FilterAndOr{
			Filters: []godalsql.IFilter{
				&godalsql.FilterFieldValue{Field: colSqlId, Operation: ">=", Value: "8"},
				&godalsql.FilterFieldValue{Field: colSqlId, Operation: "<", Value: "3"},
			},
		},
	}
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

	if numRows, err := dao.GdaoDeleteMany(dao.collectionName, filter); err != nil {
		t.Fatalf("%s failed: %e", name, err)
	} else if numRows != 5 {
		t.Fatalf("%s failed: expected %#v row(s) deleted but received %#v", name, 5, numRows)
	}
}

func dotestGenericDaoSqlGdaoFetchOne(t *testing.T, name string, dao *UserDaoSql) {
	filter := dao.GdaoCreateFilter(dao.collectionName, dao.toGbo(&UserBoSql{Id: "1"}))
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
		Created:  time.Now(),
	}
	_, err := dao.GdaoCreate(dao.collectionName, dao.toGbo(user))
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoCreate", err)
	}

	if gbo, err := dao.GdaoFetchOne(dao.collectionName, filter); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoFetchOne", err)
	} else if gbo == nil {
		t.Fatalf("%s failed: nil", name+"/GdaoFetchOne")
	} else {
		u := dao.toUser(gbo)
		if u.Id != user.Id || u.Username != user.Username || u.Name != user.Name || u.Active != user.Active ||
			u.Version != user.Version || u.Created.Unix() != user.Created.Unix() {
			t.Fatalf("%s failed: expected %#v but received %#v", name+"/GdaoFetchOne", user, u)
		}
	}
}

func dotestGenericDaoSqlGdaoFetchMany(t *testing.T, name string, dao *UserDaoSql) {
	filter := &godalsql.FilterAnd{
		FilterAndOr: godalsql.FilterAndOr{
			Filters: []godalsql.IFilter{
				&godalsql.FilterFieldValue{Field: colSqlId, Operation: "<=", Value: "8"},
				&godalsql.FilterFieldValue{Field: colSqlId, Operation: ">", Value: "3"},
			},
		},
	}
	if dbRows, err := dao.GdaoFetchMany(dao.collectionName, filter, nil, 1, 3); err != nil {
		t.Fatalf("%s failed: %e", name, err)
	} else if dbRows == nil || len(dbRows) != 0 {
		t.Fatalf("%s failed: expected %#v row(s) but received %#v", name, 0, dbRows)
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

	fetchIdList := []string{"7", "6", "5"}
	sorting := map[string]int{colSqlUsername: -1}
	if dbRows, err := dao.GdaoFetchMany(dao.collectionName, filter, sorting, 1, 3); err != nil {
		t.Fatalf("%s failed: %e", name, err)
	} else if dbRows == nil || len(dbRows) != 3 {
		t.Fatalf("%s failed: expected %#v row(s) but received %#v", name, 3, len(dbRows))
	} else {
		for i, row := range dbRows {
			u := dao.toUser(row)
			if u.Id != fetchIdList[i] {
				t.Fatalf("%s failed: expected %#v but received %#v", name, fetchIdList[i], u.Id)
			}
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
		Created:  time.Now(),
	}
	if numRows, err := dao.GdaoCreate(dao.collectionName, dao.toGbo(user)); err != nil {
		t.Fatalf("%s failed: %e", name, err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name, 1, numRows)
	}

	// duplicated id
	user.Username = "thanhn"
	if numRows, err := dao.GdaoCreate(dao.collectionName, dao.toGbo(user)); err != godal.GdaoErrorDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed: num rows %#v / error: %e", name, numRows, err)
	}

	// duplicated username
	user.Id = "2"
	user.Username = "btnguyen2k"
	if numRows, err := dao.GdaoCreate(dao.collectionName, dao.toGbo(user)); err != godal.GdaoErrorDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed: num rows %#v / error: %e", name, numRows, err)
	}

	filter := dao.GdaoCreateFilter(dao.collectionName, dao.toGbo(&UserBoSql{Id: "1"}))
	if gbo, err := dao.GdaoFetchOne(dao.collectionName, filter); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoFetchOne", err)
	} else if gbo == nil {
		t.Fatalf("%s failed: nil", name+"/GdaoFetchOne")
	} else {
		u := dao.toUser(gbo)
		if u.Username != "btnguyen2k" {
			t.Fatalf("%s failed: expected %v but received %v", name+"/GdaoFetchOne", "btnguyen2k", u.Username)
		}
	}
}

func dotestGenericDaoSqlGdaoUpdate(t *testing.T, name string, dao *UserDaoSql) {
	user1 := &UserBoSql{
		Id:       "1",
		Username: "btnguyen2k",
		Name:     "Thanh Nguyen",
		Version:  int(time.Now().Unix()),
		Active:   false,
		Created:  time.Now(),
		Group:    "users",
	}
	user2 := &UserBoSql{
		Id:       "2",
		Username: "nbthanh",
		Name:     "Thanh B. Nguyen",
		Version:  int(time.Now().Unix()),
		Active:   true,
		Created:  time.Now(),
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
		u := dao.toUser(gbo)
		if u.Username != "thanhn" {
			t.Fatalf("%s failed: expected %v but received %v", name+"/GdaoFetchOne", "thanhn", u.Username)
		}
	}

	user1.Username = user2.Username
	if numRows, err := dao.GdaoUpdate(dao.collectionName, dao.toGbo(user1)); err != godal.GdaoErrorDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed: expected 0/GdaoErrorDuplicatedEntry but received %#v/%#v", name+"/GdaoUpdate", numRows, err)
	}
}

func dotestGenericDaoSqlGdaoSave(t *testing.T, name string, dao *UserDaoSql) {
	user1 := &UserBoSql{
		Id:       "1",
		Username: "btnguyen2k",
		Name:     "Thanh Nguyen",
		Version:  int(time.Now().Unix()),
		Active:   false,
		Created:  time.Now(),
		Group:    "users",
	}
	user2 := &UserBoSql{
		Id:       "2",
		Username: "nbthanh",
		Name:     "Thanh B. Nguyen",
		Version:  int(time.Now().Unix()),
		Active:   true,
		Created:  time.Now(),
		Group:    "users",
	}

	if numRows, err := dao.GdaoSave(dao.collectionName, dao.toGbo(user1)); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoSave", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoSave", 1, numRows)
	}
	if numRows, err := dao.GdaoSave(dao.collectionName, dao.toGbo(user2)); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoSave", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoSave", 1, numRows)
	}

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
		u := dao.toUser(gbo)
		if u.Username != "thanhn" {
			t.Fatalf("%s failed: expected %v but received %v", name+"/GdaoFetchOne", "thanhn", u.Username)
		}
	}

	user1.Username = user2.Username
	if numRows, err := dao.GdaoSave(dao.collectionName, dao.toGbo(user1)); err != godal.GdaoErrorDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed: expected 0/GdaoErrorDuplicatedEntry but received %#v/%#v", name+"/GdaoUpdate", numRows, err)
	}
}

/*---------------------------------------------------------------*/

const (
	envCosmosdbDriver = "COSMOSDB_DRIVER"
	envCosmosdbUrl    = "COSMOSDB_URL"
)

func TestGenericDaoCosmosdb_SetGetSqlConnect(t *testing.T) {
	name := "TestGenericDaoCosmosdb_SetGetSqlConnect"
	dao := initDaoCosmosdb(t, name, os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, prom.FlavorCosmosDb)
	sqlc, _ := newSqlConnect(t, name, os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTimeZone, prom.FlavorCosmosDb)
	if sqlc == dao.GetSqlConnect() {
		t.Fatalf("%s failed: should not equal", name)
	}
	dao.SetSqlConnect(sqlc)
	if sqlc != dao.GetSqlConnect() {
		t.Fatalf("%s failed: should equal", name)
	}
}

func TestGenericDaoCosmosdb_GdaoDelete(t *testing.T) {
	name := "TestGenericDaoCosmosdb_GdaoDelete"
	dao := initDaoCosmosdb(t, name, os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, prom.FlavorCosmosDb)
	err := prepareTableCosmosdb(dao.GetSqlConnect(), dao.collectionName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableCosmosdb", err)
	}
	dotestGenericDaoSqlGdaoDelete(t, name, dao)
}

func TestGenericDaoCosmosdb_GdaoDeleteMany(t *testing.T) {
	name := "TestGenericDaoCosmosdb_GdaoDeleteMany"
	dao := initDaoCosmosdb(t, name, os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, prom.FlavorCosmosDb)
	err := prepareTableCosmosdb(dao.GetSqlConnect(), dao.collectionName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableCosmosdb", err)
	}
	dotestGenericDaoSqlGdaoDeleteMany(t, name, dao)
}

func TestGenericDaoCosmosdb_GdaoFetchOne(t *testing.T) {
	name := "TestGenericDaoCosmosdb_GdaoDeleteMany"
	dao := initDaoCosmosdb(t, name, os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, prom.FlavorCosmosDb)
	err := prepareTableCosmosdb(dao.GetSqlConnect(), dao.collectionName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableCosmosdb", err)
	}
	dotestGenericDaoSqlGdaoFetchOne(t, name, dao)
}

func TestGenericDaoCosmosdb_GdaoFetchMany(t *testing.T) {
	name := "TestGenericDaoCosmosdb_GdaoFetchMany"
	dao := initDaoCosmosdb(t, name, os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, prom.FlavorCosmosDb)
	err := prepareTableCosmosdb(dao.GetSqlConnect(), dao.collectionName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableCosmosdb", err)
	}
	dotestGenericDaoSqlGdaoFetchMany(t, name, dao)
}

func TestGenericDaoCosmosdb_GdaoCreate(t *testing.T) {
	name := "TestGenericDaoCosmosdb_GdaoCreate"
	dao := initDaoCosmosdb(t, name, os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, prom.FlavorCosmosDb)
	err := prepareTableCosmosdb(dao.GetSqlConnect(), dao.collectionName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableCosmosdb", err)
	}
	dotestGenericDaoSqlGdaoCreate(t, name, dao)
}

func TestGenericDaoCosmosdb_GdaoUpdate(t *testing.T) {
	name := "TestGenericDaoCosmosdb_GdaoUpdate"
	dao := initDaoCosmosdb(t, name, os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, prom.FlavorCosmosDb)
	err := prepareTableCosmosdb(dao.GetSqlConnect(), dao.collectionName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableCosmosdb", err)
	}
	dotestGenericDaoSqlGdaoUpdate(t, name, dao)
}

func TestGenericDaoCosmosdb_GdaoSave(t *testing.T) {
	name := "TestGenericDaoCosmosdb_GdaoSave"
	dao := initDaoCosmosdb(t, name, os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, prom.FlavorCosmosDb)
	err := prepareTableCosmosdb(dao.GetSqlConnect(), dao.collectionName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableCosmosdb", err)
	}
	dotestGenericDaoSqlGdaoSave(t, name, dao)
}

func TestGenericDaoCosmosdb_GdaoSaveTxModeOnWrite(t *testing.T) {
	name := "TestGenericDaoCosmosdb_GdaoSaveTxModeOnWrite"
	dao := initDaoCosmosdb(t, name, os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, prom.FlavorCosmosDb)
	err := prepareTableCosmosdb(dao.GetSqlConnect(), dao.collectionName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableCosmosdb", err)
	}
	dao.SetTxModeOnWrite(true)
	dotestGenericDaoSqlGdaoSave(t, name, dao)
}
