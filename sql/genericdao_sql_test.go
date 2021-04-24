package sql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
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
		t.Skipf("%s skipped", testName)
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
			tableName: {
				fieldGboId: colSqlId, fieldGboUsername: colSqlUsername, fieldGboData: colSqlData,
				fieldGboValPInt: colSqlValPInt, fieldGboValPFloat: colSqlValPFloat, fieldGboValPString: colSqlValPString, fieldGboValPTime: colSqlValPTime,
			},
		},
		ColNameToGboFieldTranslator: map[string]map[string]interface{}{
			tableName: {
				colSqlId: fieldGboId, colSqlUsername: fieldGboUsername, colSqlData: fieldGboData,
				colSqlValPInt: fieldGboValPInt, colSqlValPFloat: fieldGboValPFloat, colSqlValPString: fieldGboValPString, colSqlValPTime: fieldGboValPTime,
			},
		},
		ColumnsListMap: map[string][]string{
			tableName: {colSqlId, colSqlUsername, colSqlData, colSqlValPInt, colSqlValPFloat, colSqlValPString, colSqlValPTime},
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

// func TestGenericDaoSql_SetGetOptionOpLiteral(t *testing.T) {
// 	name := "TestGenericDaoSql_SetGetOptionOpLiteral"
// 	dao := initDao(t, name, "mysql", "test:test@tcp(localhost:3306)/test", "Asia/Ho_Chi_Minh", prom.FlavorDefault)
// 	dao.SetOptionOpLiteral(nil)
// 	if dao.GetOptionOpLiteral() != nil {
// 		t.Fatalf("%s failed: expected %#v but received %#v", name, nil, dao.GetOptionOpLiteral())
// 	}
// }

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
	tableName := "tbl_temp"
	dao := initDao(t, name, "mysql", "test:test@tcp(localhost:3306)/test", tableName, prom.FlavorDefault)
	if f, err := dao.BuildFilter(tableName, nil); f != nil || err != nil {
		t.Fatalf("%s failed: %#v / %s", name, f, err)
	}

	var inF godal.FilterOpt
	filterOpList := []godal.FilterOperator{godal.FilterOpEqual, godal.FilterOpNotEqual,
		godal.FilterOpGreater, godal.FilterOpGreaterOrEqual, godal.FilterOpLess, godal.FilterOpLessOrEqual}
	defOpStrList := []string{"=", "<>", ">", ">=", "<", "<="}

	for i, op := range filterOpList {
		filter := godal.FilterOptFieldOpValue{FieldName: fieldGboUsername, Operator: op, Value: "user1"}
		for _, inF = range []godal.FilterOpt{filter, &filter} {
			if f, err := dao.BuildFilter(tableName, inF); err != nil {
				t.Fatalf("%s failed: %s", name, err)
			} else if outF, ok := f.(*FilterFieldValue); !ok {
				t.Fatalf("%s failed: expected output of type *FilterFieldValue, but received %T", name, f)
			} else {
				if outF.Field != colSqlUsername {
					t.Fatalf("%s failed: expected %#v but received %#v", name, colSqlUsername, outF.Field)
				}
				if outF.Operator != defOpStrList[i] {
					t.Fatalf("%s failed: expected %#v but received %#v", name, defOpStrList[i], outF.Operator)
				}
				if outF.Value != "user1" {
					t.Fatalf("%s failed: expected %#v but received %#v", name, "user1", outF.Value)
				}
			}
		}
	}

	for i, op := range filterOpList {
		filter := godal.FilterOptFieldOpField{FieldNameLeft: fieldGboUsername, Operator: op, FieldNameRight: fieldGboId}
		for _, inF = range []godal.FilterOpt{filter, &filter} {
			if f, err := dao.BuildFilter(tableName, inF); err != nil {
				t.Fatalf("%s failed: %s", name, err)
			} else if outF, ok := f.(*FilterExpression); !ok {
				t.Fatalf("%s failed: expected output of type *FilterExpression, but received %T", name, f)
			} else {
				if outF.Left != colSqlUsername {
					t.Fatalf("%s failed: expected %#v but received %#v", name, colSqlUsername, outF.Left)
				}
				if outF.Operator != defOpStrList[i] {
					t.Fatalf("%s failed: expected %#v but received %#v", name, defOpStrList[i], outF.Operator)
				}
				if outF.Right != colSqlId {
					t.Fatalf("%s failed: expected %#v but received %#v", name, colSqlId, outF.Right)
				}
			}
		}
	}

	{
		filter := godal.FilterOptFieldIsNull{FieldName: fieldGboUsername}
		for _, inF = range []godal.FilterOpt{filter, &filter} {
			if f, err := dao.BuildFilter(tableName, inF); err != nil {
				t.Fatalf("%s failed: %s", name, err)
			} else if outF, ok := f.(*FilterIsNull); !ok {
				t.Fatalf("%s failed: expected output of type *FilterIsNull, but received %T", name, f)
			} else {
				if outF.Field != colSqlUsername {
					t.Fatalf("%s failed: expected %#v but received %#v", name, colSqlUsername, outF.Field)
				}
			}
		}
	}

	{
		filter := godal.FilterOptFieldIsNotNull{FieldName: fieldGboUsername}
		for _, inF = range []godal.FilterOpt{filter, &filter} {
			if f, err := dao.BuildFilter(tableName, inF); err != nil {
				t.Fatalf("%s failed: %s", name, err)
			} else if outF, ok := f.(*FilterIsNotNull); !ok {
				t.Fatalf("%s failed: expected output of type *FilterIsNotNull, but received %T", name, f)
			} else {
				if outF.Field != colSqlUsername {
					t.Fatalf("%s failed: expected %#v but received %#v", name, colSqlUsername, outF.Field)
				}
			}
		}
	}

	{
		filter := godal.FilterOptAnd{}
		filter.Add(godal.FilterOptFieldIsNull{FieldName: fieldGboId})
		{
			inner := godal.FilterOptOr{}
			inner.Add(godal.FilterOptFieldIsNotNull{FieldName: fieldGboUsername})
			inner.Add(godal.FilterOptFieldOpValue{FieldName: fieldGboData, Operator: godal.FilterOpEqual, Value: 1})
			filter.Add(inner)
		}
		for _, inF = range []godal.FilterOpt{filter, &filter} {
			if f, err := dao.BuildFilter(tableName, inF); err != nil {
				t.Fatalf("%s failed: %s", name, err)
			} else if outF, ok := f.(*FilterAnd); !ok {
				t.Fatalf("%s failed: expected output of type *FilterAnd, but received %T", name, f)
			} else {
				if innerOutF, ok := outF.Filters[0].(*FilterIsNull); !ok {
					t.Fatalf("%s failed: expected *FilterIsNull, but received %T", name, outF.Filters[0])
				} else {
					if innerOutF.Field != colSqlId {
						t.Fatalf("%s failed: expected %#v but received %#v", name, colSqlId, innerOutF.Field)
					}
				}
				if innerOutF, ok := outF.Filters[1].(*FilterOr); !ok {
					t.Fatalf("%s failed: expected *FilterOr, but received %T", name, outF.Filters[1])
				} else {
					if inner2OutF, ok := innerOutF.Filters[0].(*FilterIsNotNull); !ok {
						t.Fatalf("%s failed: expected *FilterIsNotNull, but received %T", name, innerOutF.Filters[0])
					} else {
						if inner2OutF.Field != colSqlUsername {
							t.Fatalf("%s failed: expected %#v but received %#v", name, colSqlUsername, inner2OutF.Field)
						}
					}

					if inner2OutF, ok := innerOutF.Filters[1].(*FilterFieldValue); !ok {
						t.Fatalf("%s failed: expected *FilterFieldValue, but received %T", name, innerOutF.Filters[1])
					} else {
						if inner2OutF.Field != colSqlData {
							t.Fatalf("%s failed: expected %#v but received %#v", name, colSqlData, inner2OutF.Field)
						}
						if inner2OutF.Operator != "=" {
							t.Fatalf("%s failed: expected %#v but received %#v", name, "=", inner2OutF.Operator)
						}
						if inner2OutF.Value != 1 {
							t.Fatalf("%s failed: expected %#v but received %#v", name, 1, inner2OutF.Value)
						}
					}
				}
			}
		}
	}

	{
		filter := godal.FilterOptOr{}
		filter.Add(godal.FilterOptFieldIsNull{FieldName: fieldGboId})
		{
			inner := godal.FilterOptAnd{}
			inner.Add(godal.FilterOptFieldIsNotNull{FieldName: fieldGboUsername})
			inner.Add(godal.FilterOptFieldOpValue{FieldName: fieldGboData, Operator: godal.FilterOpEqual, Value: 1})
			filter.Add(inner)
		}
		for _, inF = range []godal.FilterOpt{filter, &filter} {
			if f, err := dao.BuildFilter(tableName, inF); err != nil {
				t.Fatalf("%s failed: %s", name, err)
			} else if outF, ok := f.(*FilterOr); !ok {
				t.Fatalf("%s failed: expected output of type *FilterOr, but received %T", name, f)
			} else {
				if innerOutF, ok := outF.Filters[0].(*FilterIsNull); !ok {
					t.Fatalf("%s failed: expected *FilterIsNull, but received %T", name, outF.Filters[0])
				} else {
					if innerOutF.Field != colSqlId {
						t.Fatalf("%s failed: expected %#v but received %#v", name, colSqlId, innerOutF.Field)
					}
				}
				if innerOutF, ok := outF.Filters[1].(*FilterAnd); !ok {
					t.Fatalf("%s failed: expected *FilterAnd, but received %T", name, outF.Filters[1])
				} else {
					if inner2OutF, ok := innerOutF.Filters[0].(*FilterIsNotNull); !ok {
						t.Fatalf("%s failed: expected *FilterIsNotNull, but received %T", name, innerOutF.Filters[0])
					} else {
						if inner2OutF.Field != colSqlUsername {
							t.Fatalf("%s failed: expected %#v but received %#v", name, colSqlUsername, inner2OutF.Field)
						}
					}

					if inner2OutF, ok := innerOutF.Filters[1].(*FilterFieldValue); !ok {
						t.Fatalf("%s failed: expected *FilterFieldValue, but received %T", name, innerOutF.Filters[1])
					} else {
						if inner2OutF.Field != colSqlData {
							t.Fatalf("%s failed: expected %#v but received %#v", name, colSqlData, inner2OutF.Field)
						}
						if inner2OutF.Operator != "=" {
							t.Fatalf("%s failed: expected %#v but received %#v", name, "=", inner2OutF.Operator)
						}
						if inner2OutF.Value != 1 {
							t.Fatalf("%s failed: expected %#v but received %#v", name, 1, inner2OutF.Value)
						}
					}
				}
			}
		}
	}
}

func TestGenericDaoSql_BuildOrdering(t *testing.T) {
	name := "TestGenericDaoSql_BuildOrdering"
	dao := initDao(t, name, "mysql", "test:test@tcp(localhost:3306)/test", "tbl_test", prom.FlavorDefault)

	var opts *godal.SortingOpt = nil
	if ordering, err := dao.BuildSorting("tbl_test", opts); ordering != nil || err != nil {
		t.Fatalf("%s failed: %#v / %s", name, ordering, err)
	}

	opts = &godal.SortingOpt{}
	opts.Add(&godal.SortingField{FieldName: fieldGboUsername})
	expected := colSqlUsername
	if ordering, err := dao.BuildSorting("tbl_test", opts); err != nil || ordering == nil {
		t.Fatalf("%s failed: %#v / %s", name, ordering, err)
	} else if ordering.Build() != expected {
		t.Fatalf("%s failed: expected %#v but received %#v", name, expected, ordering.Build())
	}

	opts.Add(&godal.SortingField{FieldName: fieldGboId, Descending: true})
	expected += "," + colSqlId + " DESC"
	if ordering, err := dao.BuildSorting("tbl_test", opts); err != nil || ordering == nil {
		t.Fatalf("%s failed: %#v / %s", name, ordering, err)
	} else if ordering.Build() != expected {
		t.Fatalf("%s failed: expected %#v but received %#v", name, expected, ordering.Build())
	}
}

const (
	testTableName    = "test_user"
	colSqlId         = "userid"
	colSqlUsername   = "uusername"
	colSqlData       = "udata"
	colSqlValPInt    = "pint"
	colSqlValPFloat  = "pfloat"
	colSqlValPString = "pstring"
	colSqlValPTime   = "ptime"

	fieldGboId         = "id"
	fieldGboUsername   = "username"
	fieldGboData       = "data"
	fieldGboValPInt    = "pint"
	fieldGboValPFloat  = "pfloat"
	fieldGboValPString = "pstring"
	fieldGboValPTime   = "ptime"

	testTimeZone = "Asia/Ho_Chi_Minh"
)

type UserDaoSql struct {
	*GenericDaoSql
	tableName string
}

// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter.
func (dao *UserDaoSql) GdaoCreateFilter(tableName string, bo godal.IGenericBo) godal.FilterOpt {
	if tableName == dao.tableName {
		return &godal.FilterOptFieldOpValue{
			FieldName: fieldGboId,
			Operator:  godal.FilterOpEqual,
			Value:     bo.GboGetAttrUnsafe(fieldGboId, reddo.TypeString),
		}
		// return map[string]interface{}{colSqlId: bo.GboGetAttrUnsafe(fieldGboId, reddo.TypeString)}
	}
	return nil
}

func (dao *UserDaoSql) toGbo(u *UserBoSql) godal.IGenericBo {
	js, _ := json.Marshal(u)
	gbo := godal.NewGenericBo()
	if err := gbo.GboImportViaMap(map[string]interface{}{
		fieldGboId: u.Id, fieldGboUsername: u.Username, fieldGboData: string(js),
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
	return &bo
}

type UserBoSql struct {
	Id         string     `json:"id"`
	Username   string     `json:"username"`
	Name       string     `json:"name"`
	Version    int        `json:"version"`
	Active     bool       `json:"active"`
	Created    time.Time  `json:"created"`
	ValPInt    *int64     `json:"pint"`
	ValPFloat  *float64   `json:"pfloat"`
	ValPString *string    `json:"pstring"`
	ValPTime   *time.Time `json:"ptime"`
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
	filter := &godal.FilterOptOr{Filters: []godal.FilterOpt{
		&godal.FilterOptFieldOpValue{FieldName: fieldGboId, Operator: godal.FilterOpGreaterOrEqual, Value: "8"},
		&godal.FilterOptFieldOpValue{FieldName: fieldGboId, Operator: godal.FilterOpLess, Value: "3"},
	}}

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
	filter := &godal.FilterOptAnd{Filters: []godal.FilterOpt{
		&godal.FilterOptFieldOpValue{FieldName: fieldGboId, Operator: godal.FilterOpLessOrEqual, Value: "8"},
		&godal.FilterOptFieldOpValue{FieldName: fieldGboId, Operator: godal.FilterOpGreater, Value: "3"},
	}}

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
	sorting := (&godal.SortingOpt{}).Add(&godal.SortingField{FieldName: fieldGboUsername, Descending: true})
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
				fmt.Printf("\tRoutine #{%d} - Withdraw %d tokens from user{1} [%d -> %d] / Status: %#v\n", id, amountToTransfer, origin, user1.Version, result1)

				origin = user2.Version
				user2.Version += amountToTransfer
				result2, err := dao.GdaoUpdateWithTx(ctx, tx, dao.tableName, dao.toGbo(user2))
				if err != nil {
					errList[id] = err
					tx.Rollback()
					return
				}
				fmt.Printf("\tRoutine {%d} - Topup %d tokens to user{2} [%d -> %d] / Status: %#v\n", id, amountToTransfer, origin, user2.Version, result2)
			} else {
				fmt.Printf("\tRoutine {%d} - User{1} has %d tokens, not enough to make the transfer of %d\n", id, user1.Version, amountToTransfer)
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

func _checkFilterNull(t *testing.T, name string, expected, target *UserBoSql) {
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
	if target.Created.Format(layout) != expected.Created.Format(layout) {
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
	if (expected.ValPTime != nil && (target.ValPTime == nil || target.ValPTime.Format(layout) != expected.ValPTime.Format(layout))) || (expected.ValPTime == nil && target.ValPTime != nil) {
		t.Fatalf("%s failed: field [PTime] mismatched - %#v / %#v", name, expected.ValPTime, target.ValPTime)
	}
}

func dotestGenericDaoSqlGdao_FilterNull(t *testing.T, name string, dao *UserDaoSql) {
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
		_, err := dao.GdaoCreate(dao.tableName, dao.toGbo(user))
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
		gboList, err := dao.GdaoFetchMany(dao.tableName, filter, nil, 0, 0)
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

			_checkFilterNull(t, name, expected, user)
		}
	}
}

func dotestGenericDaoSqlGdao_FilterNotNull(t *testing.T, name string, dao *UserDaoSql) {
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
		_, err := dao.GdaoCreate(dao.tableName, dao.toGbo(user))
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
		gboList, err := dao.GdaoFetchMany(dao.tableName, filter, nil, 0, 0)
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

			_checkFilterNull(t, name, expected, user)
		}
	}
}
