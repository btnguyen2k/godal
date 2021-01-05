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

	driver += ";Db=godal"

	urlTimezone := strings.ReplaceAll(timezone, "/", "%2f")
	url = strings.ReplaceAll(url, "${loc}", urlTimezone)
	url = strings.ReplaceAll(url, "${tz}", urlTimezone)
	url = strings.ReplaceAll(url, "${timezone}", urlTimezone)
	sqlc, err := prom.NewSqlConnectWithFlavor(driver, url, 10000, nil, flavor)
	if err == nil && sqlc != nil {
		loc, _ := time.LoadLocation(timezone)
		sqlc.SetLocation(loc)
	}

	sqlc.GetDB().Exec("CREATE DATABASE godal WITH maxru=10000")

	return sqlc, err
}

func createDaoCosmosdb(sqlc *prom.SqlConnect, tableName string) *UserDaoSql {
	rowMapper := GenericRowMapperCosmosdbInstance
	dao := &UserDaoSql{tableName: tableName}
	dao.GenericDaoCosmosdb = NewGenericDaoCosmosdb(sqlc, godal.NewAbstractGenericDao(dao))
	dao.SetSqlFlavor(prom.FlavorCosmosDb).SetRowMapper(rowMapper)
	dao.SetTxModeOnWrite(false).SetTxIsolationLevel(sql.LevelDefault)
	dao.pkBoPathMap = map[string]string{tableName: fieldGboGroup}
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
	tableName string
}

// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter.
func (dao *UserDaoSql) GdaoCreateFilter(tableName string, bo godal.IGenericBo) interface{} {
	if tableName == dao.tableName {
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
	_, err := dao.GdaoCreate(dao.tableName, dao.toGbo(user))
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoCreate", err)
	}

	filterUser := &UserBoSql{Id: "2"}
	if numRows, err := dao.GdaoDelete(dao.tableName, dao.toGbo(filterUser)); err != nil {
		t.Fatalf("%s failed: %e", name, err)
	} else if numRows != 0 {
		t.Fatalf("%s failed: expected %#v row(s) deleted but received %#v", name, 0, numRows)
	}

	if numRows, err := dao.GdaoDelete(dao.tableName, dao.toGbo(user)); err != nil {
		t.Fatalf("%s failed: %e", name, err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) deleted but received %#v", name, 1, numRows)
	}

	if u, err := dao.GdaoFetchOne(dao.tableName, dao.GdaoCreateFilter(dao.tableName, dao.toGbo(user))); err != nil {
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
	if numRows, err := dao.GdaoDeleteMany(dao.tableName, filter); err != nil {
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
		_, err := dao.GdaoCreate(dao.tableName, dao.toGbo(user))
		if err != nil {
			t.Fatalf("%s failed: %e", name+"/GdaoCreate", err)
		}
	}

	if numRows, err := dao.GdaoDeleteMany(dao.tableName, filter); err != nil {
		t.Fatalf("%s failed: %e", name, err)
	} else if numRows != 5 {
		t.Fatalf("%s failed: expected %#v row(s) deleted but received %#v", name, 5, numRows)
	}
}

func dotestGenericDaoSqlGdaoFetchOne(t *testing.T, name string, dao *UserDaoSql) {
	filter := dao.GdaoCreateFilter(dao.tableName, dao.toGbo(&UserBoSql{Id: "1"}))
	if gbo, err := dao.GdaoFetchOne(dao.tableName, filter); err != nil {
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
	_, err := dao.GdaoCreate(dao.tableName, dao.toGbo(user))
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoCreate", err)
	}

	if gbo, err := dao.GdaoFetchOne(dao.tableName, filter); err != nil {
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
	if dbRows, err := dao.GdaoFetchMany(dao.tableName, filter, nil, 1, 3); err != nil {
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
		_, err := dao.GdaoCreate(dao.tableName, dao.toGbo(user))
		if err != nil {
			t.Fatalf("%s failed: %e", name+"/GdaoCreate", err)
		}
	}

	fetchIdList := []string{"7", "6", "5"}
	sorting := map[string]int{colSqlUsername: -1}
	if dbRows, err := dao.GdaoFetchMany(dao.tableName, filter, sorting, 1, 3); err != nil {
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
	if numRows, err := dao.GdaoCreate(dao.tableName, dao.toGbo(user)); err != nil {
		t.Fatalf("%s failed: %e", name, err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name, 1, numRows)
	}

	// duplicated id
	user.Username = "thanhn"
	if numRows, err := dao.GdaoCreate(dao.tableName, dao.toGbo(user)); err != godal.GdaoErrorDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed: num rows %#v / error: %e", name, numRows, err)
	}

	// duplicated username
	user.Id = "2"
	user.Username = "btnguyen2k"
	if numRows, err := dao.GdaoCreate(dao.tableName, dao.toGbo(user)); err != godal.GdaoErrorDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed: num rows %#v / error: %e", name, numRows, err)
	}

	filter := dao.GdaoCreateFilter(dao.tableName, dao.toGbo(&UserBoSql{Id: "1"}))
	if gbo, err := dao.GdaoFetchOne(dao.tableName, filter); err != nil {
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
	user := &UserBoSql{
		Id:       "1",
		Username: "btnguyen2k",
		Name:     "Thanh Nguyen",
		Version:  int(time.Now().Unix()),
		Active:   false,
		Created:  time.Now(),
	}
	if numRows, err := dao.GdaoUpdate(dao.tableName, dao.toGbo(user)); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoUpdate", err)
	} else if numRows != 0 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoUpdate", 0, numRows)
	}
	if numRows, err := dao.GdaoCreate(dao.tableName, dao.toGbo(user)); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoCreate", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoCreate", 1, numRows)
	}

	user.Username = "thanhn"
	if numRows, err := dao.GdaoUpdate(dao.tableName, dao.toGbo(user)); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoUpdate", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoUpdate", 1, numRows)
	}

	filter := dao.GdaoCreateFilter(dao.tableName, dao.toGbo(&UserBoSql{Id: "1"}))
	if gbo, err := dao.GdaoFetchOne(dao.tableName, filter); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoFetchOne", err)
	} else if gbo == nil {
		t.Fatalf("%s failed: nil", name+"/GdaoFetchOne")
	} else {
		u := dao.toUser(gbo)
		if u.Username != "thanhn" {
			t.Fatalf("%s failed: expected %v but received %v", name+"/GdaoFetchOne", "thanhn", u.Username)
		}
	}
}

func dotestGenericDaoSqlGdaoSave(t *testing.T, name string, dao *UserDaoSql) {
	user := &UserBoSql{
		Id:       "1",
		Username: "btnguyen2k",
		Name:     "Thanh Nguyen",
		Version:  int(time.Now().Unix()),
		Active:   false,
		Created:  time.Now(),
	}
	if numRows, err := dao.GdaoSave(dao.tableName, dao.toGbo(user)); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoSave", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoSave", 1, numRows)
	}

	user.Username = "thanhn"
	if numRows, err := dao.GdaoSave(dao.tableName, dao.toGbo(user)); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoSave", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoSave", 1, numRows)
	}

	filter := dao.GdaoCreateFilter(dao.tableName, dao.toGbo(&UserBoSql{Id: "1"}))
	if gbo, err := dao.GdaoFetchOne(dao.tableName, filter); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoFetchOne", err)
	} else if gbo == nil {
		t.Fatalf("%s failed: nil", name+"/GdaoFetchOne")
	} else {
		u := dao.toUser(gbo)
		if u.Username != "thanhn" {
			t.Fatalf("%s failed: expected %v but received %v", name+"/GdaoFetchOne", "thanhn", u.Username)
		}
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
	err := prepareTableCosmosdb(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableCosmosdb", err)
	}
	dotestGenericDaoSqlGdaoDelete(t, name, dao)
}

func TestGenericDaoCosmosdb_GdaoDeleteMany(t *testing.T) {
	name := "TestGenericDaoCosmosdb_GdaoDeleteMany"
	dao := initDaoCosmosdb(t, name, os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, prom.FlavorCosmosDb)
	err := prepareTableCosmosdb(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableCosmosdb", err)
	}
	dotestGenericDaoSqlGdaoDeleteMany(t, name, dao)
}

func TestGenericDaoCosmosdb_GdaoFetchOne(t *testing.T) {
	name := "TestGenericDaoCosmosdb_GdaoDeleteMany"
	dao := initDaoCosmosdb(t, name, os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, prom.FlavorCosmosDb)
	err := prepareTableCosmosdb(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableCosmosdb", err)
	}
	dotestGenericDaoSqlGdaoFetchOne(t, name, dao)
}

func TestGenericDaoCosmosdb_GdaoFetchMany(t *testing.T) {
	name := "TestGenericDaoCosmosdb_GdaoFetchMany"
	dao := initDaoCosmosdb(t, name, os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, prom.FlavorCosmosDb)
	err := prepareTableCosmosdb(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableCosmosdb", err)
	}
	dotestGenericDaoSqlGdaoFetchMany(t, name, dao)
}

func TestGenericDaoCosmosdb_GdaoCreate(t *testing.T) {
	name := "TestGenericDaoCosmosdb_GdaoCreate"
	dao := initDaoCosmosdb(t, name, os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, prom.FlavorCosmosDb)
	err := prepareTableCosmosdb(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableCosmosdb", err)
	}
	dotestGenericDaoSqlGdaoCreate(t, name, dao)
}

func TestGenericDaoCosmosdb_GdaoUpdate(t *testing.T) {
	name := "TestGenericDaoCosmosdb_GdaoUpdate"
	dao := initDaoCosmosdb(t, name, os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, prom.FlavorCosmosDb)
	err := prepareTableCosmosdb(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableCosmosdb", err)
	}
	dotestGenericDaoSqlGdaoUpdate(t, name, dao)
}

func TestGenericDaoCosmosdb_GdaoSave(t *testing.T) {
	name := "TestGenericDaoCosmosdb_GdaoSave"
	dao := initDaoCosmosdb(t, name, os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, prom.FlavorCosmosDb)
	err := prepareTableCosmosdb(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableCosmosdb", err)
	}
	dotestGenericDaoSqlGdaoSave(t, name, dao)
}

func TestGenericDaoCosmosdb_GdaoSaveTxModeOnWrite(t *testing.T) {
	name := "TestGenericDaoCosmosdb_GdaoSaveTxModeOnWrite"
	dao := initDaoCosmosdb(t, name, os.Getenv(envCosmosdbDriver), os.Getenv(envCosmosdbUrl), testTableName, prom.FlavorCosmosDb)
	err := prepareTableCosmosdb(dao.GetSqlConnect(), dao.tableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareTableCosmosdb", err)
	}
	dao.SetTxModeOnWrite(true)
	dotestGenericDaoSqlGdaoSave(t, name, dao)
}
