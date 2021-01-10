package sql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/prom"

	"github.com/btnguyen2k/godal"
)

func newSqlConnect(t *testing.T, testName string, driver, url, timezone string, flavor prom.DbFlavor) (*prom.SqlConnect, error) {
	driver = strings.Trim(driver, "\"")
	url = strings.Trim(url, "\"")
	if driver == "" || url == "" {
		t.Skipf("%s skilled", testName)
	}

	urlTimezone := strings.ReplaceAll(timezone, "/", "%2f")
	url = strings.ReplaceAll(url, "${loc}", urlTimezone)
	url = strings.ReplaceAll(url, "${tz}", urlTimezone)
	url = strings.ReplaceAll(url, "${timezone}", urlTimezone)
	sqlc, err := prom.NewSqlConnectWithFlavor(driver, url, 10000, nil, flavor)
	if err == nil && sqlc != nil {
		loc, _ := time.LoadLocation(timezone)
		sqlc.SetLocation(loc)
	}
	return sqlc, err
}

func createDaoSql(sqlc *prom.SqlConnect, tableName string) *UserDaoSql {
	rowMapper := &GenericRowMapperSql{
		NameTransformation: NameTransfLowerCase,
		GboFieldToColNameTranslator: map[string]map[string]interface{}{
			tableName: {fieldGboId: colSqlId, fieldGboUsername: colSqlUsername, fieldGboData: colSqlData},
		},
		ColNameToGboFieldTranslator: map[string]map[string]interface{}{
			tableName: {colSqlId: fieldGboId, colSqlUsername: fieldGboUsername, colSqlData: fieldGboData},
		},
		ColumnsListMap: map[string][]string{
			tableName: {colSqlId, colSqlUsername, colSqlData},
		},
	}
	dao := &UserDaoSql{tableName: tableName}
	dao.GenericDaoSql = NewGenericDaoSql(sqlc, godal.NewAbstractGenericDao(dao))
	dao.SetSqlFlavor(sqlc.GetDbFlavor()).SetRowMapper(rowMapper)
	dao.SetTxModeOnWrite(false).SetTxIsolationLevel(sql.LevelDefault)
	return dao
}

func initDao(t *testing.T, testName string, driver, url, tableName string, flavor prom.DbFlavor) *UserDaoSql {
	sqlc, _ := newSqlConnect(t, testName, driver, url, testTimeZone, flavor)
	return createDaoSql(sqlc, tableName)
}

func TestGenericDaoSql_SetGetSqlFlavor(t *testing.T) {
	name := "TestGenericDaoSql_SetGetSqlFlavor"
	flavorList := []prom.DbFlavor{prom.FlavorDefault, prom.FlavorMySql, prom.FlavorPgSql, prom.FlavorMsSql, prom.FlavorOracle, prom.FlavorSqlite, prom.FlavorCosmosDb}
	dao := initDao(t, name, "mysql", "test:test@tcp(localhost:3306)/test", "Asia/Ho_Chi_Minh", prom.FlavorDefault)
	for _, flavor := range flavorList {
		dao.SetSqlFlavor(flavor)
		if dao.GetSqlFlavor() != flavor {
			t.Fatalf("%s failed: expected %#v but received %#v", name, flavor, dao.GetSqlFlavor())
		}
	}
}

func TestGenericDaoSql_TxMode(t *testing.T) {
	name := "TestGenericDaoSql_TxMode"
	dao := initDao(t, name, "mysql", "test:test@tcp(localhost:3306)/test", "Asia/Ho_Chi_Minh", prom.FlavorDefault)
	currentTxMode := dao.GetTxModeOnWrite()
	dao.SetTxModeOnWrite(!currentTxMode)
	if dao.GetTxModeOnWrite() == currentTxMode {
		t.Fatalf("%s failed: expected %#v but received %#v", name+"/TxModeOnWrite", !currentTxMode, dao.GetTxModeOnWrite())
	}
	dao.SetTxModeOnWrite(currentTxMode)
	if dao.GetTxModeOnWrite() != currentTxMode {
		t.Fatalf("%s failed: expected %#v but received %#v", name+"/TxModeOnWrite", currentTxMode, dao.GetTxModeOnWrite())
	}

	isoLevelList := []sql.IsolationLevel{sql.LevelDefault, sql.LevelReadUncommitted, sql.LevelReadCommitted, sql.LevelWriteCommitted,
		sql.LevelRepeatableRead, sql.LevelSnapshot, sql.LevelSerializable, sql.LevelLinearizable}
	for _, isoLevel := range isoLevelList {
		dao.SetTxIsolationLevel(isoLevel)
		if dao.GetTxIsolationLevel() != isoLevel {
			t.Fatalf("%s failed: expected %#v but received %#v", name+"/TxIsolationLevel", isoLevel, dao.GetTxIsolationLevel())
		}
	}
}

func TestGenericDaoSql_SetGetOptionOpLiteral(t *testing.T) {
	name := "TestGenericDaoSql_SetGetOptionOpLiteral"
	dao := initDao(t, name, "mysql", "test:test@tcp(localhost:3306)/test", "Asia/Ho_Chi_Minh", prom.FlavorDefault)
	dao.SetOptionOpLiteral(nil)
	if dao.GetOptionOpLiteral() != nil {
		t.Fatalf("%s failed: expected %#v but received %#v", name, nil, dao.GetOptionOpLiteral())
	}
}

func TestGenericDaoSql_SetGetFuncNewPlaceholderGenerator(t *testing.T) {
	name := "TestGenericDaoSql_SetGetFuncNewPlaceholderGenerator"
	dao := initDao(t, name, "mysql", "test:test@tcp(localhost:3306)/test", "Asia/Ho_Chi_Minh", prom.FlavorDefault)
	dao.SetFuncNewPlaceholderGenerator(nil)
	if dao.GetFuncNewPlaceholderGenerator() != nil {
		t.Fatalf("%s failed: expected nill", name)
	}
}

func TestGenericDaoSql_BuildFilter(t *testing.T) {
	name := "TestGenericDaoSql_BuildFilter"
	dao := initDao(t, name, "mysql", "test:test@tcp(localhost:3306)/test", "Asia/Ho_Chi_Minh", prom.FlavorDefault)
	if f, err := dao.BuildFilter(nil); f != nil || err != nil {
		t.Fatalf("%s failed: %#v / %s", name, f, err)
	}

	inputF := &FilterFieldValue{Field: "field", Operation: "=", Value: 1}
	if f, err := dao.BuildFilter(inputF); f != inputF || err != nil {
		t.Fatalf("%s failed: %#v / %s", name, f, err)
	}

	inputM := map[string]interface{}{"field": 1}
	dao.SetOptionOpLiteral(nil)
	if f, err := dao.BuildFilter(inputM); f == nil || err != nil {
		t.Fatalf("%s failed: %#v / %s", name, f, err)
	}

	if f, err := dao.BuildFilter(time.Time{}); f != nil || err == nil {
		t.Fatalf("%s failed: %#v / %s", name, f, err)
	}
}

func TestGenericDaoSql_BuildOrdering(t *testing.T) {
	name := "TestGenericDaoSql_BuildOrdering"
	dao := initDao(t, name, "mysql", "test:test@tcp(localhost:3306)/test", "Asia/Ho_Chi_Minh", prom.FlavorDefault)
	if f, err := dao.BuildOrdering(nil); f != nil || err != nil {
		t.Fatalf("%s failed: %#v / %s", name, f, err)
	}

	inputS := &GenericSorting{}
	if f, err := dao.BuildOrdering(inputS); f != inputS || err != nil {
		t.Fatalf("%s failed: %#v / %s", name, f, err)
	}

	inputM := map[string]interface{}{"field": 1}
	if f, err := dao.BuildOrdering(inputM); f == nil || err != nil {
		t.Fatalf("%s failed: %#v / %s", name, f, err)
	}

	if f, err := dao.BuildOrdering(time.Time{}); f != nil || err == nil {
		t.Fatalf("%s failed: %#v / %s", name, f, err)
	}
}

const (
	testTableName  = "test_user"
	colSqlId       = "userid"
	colSqlUsername = "uusername"
	colSqlData     = "udata"

	fieldGboId       = "id"
	fieldGboUsername = "username"
	fieldGboData     = "data"

	testTimeZone = "Asia/Ho_Chi_Minh"
)

type UserDaoSql struct {
	*GenericDaoSql
	tableName string
}

// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter.
func (dao *UserDaoSql) GdaoCreateFilter(tableName string, bo godal.IGenericBo) interface{} {
	if tableName == dao.tableName {
		return map[string]interface{}{colSqlId: bo.GboGetAttrUnsafe(fieldGboId, reddo.TypeString)}
	}
	return false
}

func (dao *UserDaoSql) toGbo(u *UserBoSql) godal.IGenericBo {
	js, _ := json.Marshal(u)
	gbo := godal.NewGenericBo()
	if err := gbo.GboImportViaJson(map[string]interface{}{fieldGboId: u.Id, fieldGboUsername: u.Username, fieldGboData: string(js)}); err != nil {
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
	return &bo
}

type UserBoSql struct {
	Id       string    `json:"id"`
	Username string    `json:"username"`
	Name     string    `json:"name"`
	Version  int       `json:"version"`
	Active   bool      `json:"active"`
	Created  time.Time `json:"created"`
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
	filter := &FilterOr{
		FilterAndOr: FilterAndOr{
			Filters: []IFilter{
				&FilterFieldValue{Field: colSqlId, Operation: ">=", Value: "8"},
				&FilterFieldValue{Field: colSqlId, Operation: "<", Value: "3"},
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
	filter := &FilterAnd{
		FilterAndOr: FilterAndOr{
			Filters: []IFilter{
				&FilterFieldValue{Field: colSqlId, Operation: "<=", Value: "8"},
				&FilterFieldValue{Field: colSqlId, Operation: ">", Value: "3"},
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
	user1 := &UserBoSql{
		Id:       "1",
		Username: "btnguyen2k",
		Name:     "Thanh Nguyen",
		Version:  int(time.Now().Unix()),
		Active:   false,
		Created:  time.Now(),
	}
	user2 := &UserBoSql{
		Id:       "2",
		Username: "nbthanh",
		Name:     "Thanh B. Nguyen",
		Version:  int(time.Now().Unix()),
		Active:   true,
		Created:  time.Now(),
	}

	// non-exist row
	if numRows, err := dao.GdaoUpdate(dao.tableName, dao.toGbo(user1)); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoUpdate", err)
	} else if numRows != 0 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoUpdate", 0, numRows)
	}

	// insert a few rows
	if numRows, err := dao.GdaoCreate(dao.tableName, dao.toGbo(user1)); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoCreate", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoCreate", 1, numRows)
	}
	if numRows, err := dao.GdaoCreate(dao.tableName, dao.toGbo(user2)); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoCreate", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoCreate", 1, numRows)
	}

	user1.Username = "thanhn"
	if numRows, err := dao.GdaoUpdate(dao.tableName, dao.toGbo(user1)); err != nil {
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

	user1.Username = user2.Username
	if numRows, err := dao.GdaoUpdate(dao.tableName, dao.toGbo(user1)); err != godal.GdaoErrorDuplicatedEntry || numRows != 0 {
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
	}
	user2 := &UserBoSql{
		Id:       "2",
		Username: "nbthanh",
		Name:     "Thanh B. Nguyen",
		Version:  int(time.Now().Unix()),
		Active:   true,
		Created:  time.Now(),
	}

	if numRows, err := dao.GdaoSave(dao.tableName, dao.toGbo(user1)); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoSave", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoSave", 1, numRows)
	}
	if numRows, err := dao.GdaoSave(dao.tableName, dao.toGbo(user2)); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoSave", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoSave", 1, numRows)
	}

	user1.Username = "thanhn"
	if numRows, err := dao.GdaoSave(dao.tableName, dao.toGbo(user1)); err != nil {
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

	user1.Username = user2.Username
	if numRows, err := dao.GdaoSave(dao.tableName, dao.toGbo(user1)); err != godal.GdaoErrorDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed: expected 0/GdaoErrorDuplicatedEntry but received %#v/%#v", name+"/GdaoUpdate", numRows, err)
	}

	user3 := &UserBoSql{
		Id:       "3",
		Username: user2.Username,
		Name:     "Thanh B. Nguyen",
		Version:  int(time.Now().Unix()),
		Active:   true,
		Created:  time.Now(),
	}
	if numRows, err := dao.GdaoSave(dao.tableName, dao.toGbo(user3)); err != godal.GdaoErrorDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed: expected 0/GdaoErrorDuplicatedEntry but received %#v/%#v", name+"/GdaoUpdate", numRows, err)
	}
}

func dotestGenericDaoSql_Tx(t *testing.T, name string, dao *UserDaoSql) {
	user1 := &UserBoSql{
		Id:       "1",
		Username: "user1",
		Name:     "First User",
		Version:  100,
		Active:   false,
		Created:  time.Now(),
	}
	dao.GdaoCreate(dao.tableName, dao.toGbo(user1))
	user2 := &UserBoSql{
		Id:       "2",
		Username: "user2",
		Name:     "Second User",
		Version:  120,
		Active:   true,
		Created:  time.Now(),
	}
	dao.GdaoCreate(dao.tableName, dao.toGbo(user2))
	dao.SetTxIsolationLevel(sql.LevelSerializable)

	var wg sync.WaitGroup
	numRoutines := 3
	wg.Add(numRoutines)
	errList := make([]error, numRoutines)
	for i := 0; i < numRoutines; i++ {
		go func(id int) {
			defer wg.Done()
			ctx := context.Background()
			tx, err := dao.StartTx(ctx)
			if err != nil {
				errList[id] = err
				return
			}
			if tx == nil {
				errList[id] = errors.New("cannot start transaction")
				return
			}
			defer tx.Commit()

			bo, err := dao.GdaoFetchOneWithTx(ctx, tx, dao.tableName, dao.GdaoCreateFilter(dao.tableName, dao.toGbo(&UserBoSql{Id: "1"})))
			if err != nil {
				errList[id] = err
				tx.Rollback()
				return
			}
			user1 := dao.toUser(bo)
			if user1 == nil {
				errList[id] = errors.New("user{1} not found")
				return
			}

			bo, err = dao.GdaoFetchOneWithTx(ctx, tx, dao.tableName, dao.GdaoCreateFilter(dao.tableName, dao.toGbo(&UserBoSql{Id: "2"})))
			if err != nil {
				errList[id] = err
				tx.Rollback()
				return
			}
			user2 := dao.toUser(bo)
			if user2 == nil {
				errList[id] = errors.New("user{2} not found")
				return
			}

			amountToTransfer := 40
			if user1.Version >= amountToTransfer {
				origin := user1.Version
				user1.Version -= amountToTransfer
				result1, err := dao.GdaoUpdateWithTx(ctx, tx, dao.tableName, dao.toGbo(user1))
				if err != nil {
					errList[id] = err
					tx.Rollback()
					return
				}
				fmt.Printf("\t{%d} - Withdraw %d tokens from user{1}[%d -> %d] / Status: %#v\n", id, amountToTransfer, origin, user1.Version, result1)

				origin = user2.Version
				user2.Version += amountToTransfer
				result2, err := dao.GdaoUpdateWithTx(ctx, tx, dao.tableName, dao.toGbo(user2))
				if err != nil {
					errList[id] = err
					tx.Rollback()
					return
				}
				fmt.Printf("\t{%d} - Topup %d tokens to user{2}[%d -> %d] / Status: %#v\n", id, amountToTransfer, origin, user2.Version, result2)
			} else {
				fmt.Printf("\t{%d} - User{1} has %d tokens, not enough to make the transfer of %d\n", id, user1.Version, amountToTransfer)
			}
		}(i)
	}
	wg.Wait()

	for id, err := range errList {
		if err != nil {
			msg := strings.ToLower(fmt.Sprintf("%e", err))
			if strings.Index(msg, "lock") < 0 && strings.Index(msg, "concurrent") < 0 {
				t.Fatalf("%s failed: {%d} - %s", name, id, err)
			}
		}
	}
}
