package mongo

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/prom"
)

func _createMongoConnect(t *testing.T, testName string) *prom.MongoConnect {
	mongoDb := strings.ReplaceAll(os.Getenv("MONGO_DB"), `"`, "")
	mongoUrl := strings.ReplaceAll(os.Getenv("MONGO_URL"), `"`, "")
	if mongoDb == "" || mongoUrl == "" {
		t.Skipf("%s skipped", testName)
		return nil
	}
	mc, _ := prom.NewMongoConnectWithPoolOptions(mongoUrl, mongoDb, 10000, &prom.MongoPoolOpts{
		ConnectTimeout:         10 * time.Second,
		SocketTimeout:          10 * time.Second,
		ServerSelectionTimeout: 10 * time.Second,
	})
	return mc
}

func prepareMongoCollection(mc *prom.MongoConnect, collectionName string) error {
	if err := mc.GetCollection(collectionName).Drop(nil); err != nil {
		return err
	}
	if _, err := mc.CreateCollection(collectionName); err != nil {
		return err
	}
	indexes := []interface{}{
		map[string]interface{}{
			"key":    map[string]interface{}{"username": 1},
			"name":   "uidx_username",
			"unique": true,
		},
	}
	if _, err := mc.CreateCollectionIndexes(collectionName, indexes); err != nil {
		return err
	}
	return nil
}

func createDaoMongo(mc *prom.MongoConnect, collectionName string) *UserDaoMongo {
	dao := &UserDaoMongo{collectionName: collectionName}
	dao.GenericDaoMongo = NewGenericDaoMongo(mc, godal.NewAbstractGenericDao(dao))
	dao.SetTxModeOnWrite(false)
	return dao
}

type UserBoMongo struct {
	Id       string    `json:"_id"`
	Username string    `json:"username"`
	Name     string    `json:"name"`
	Version  int       `json:"version"`
	Active   bool      `json:"active"`
	Created  time.Time `json:"created"`
}

const (
	testMongoCollectionName = "test_user"
	fieldId                 = "_id"
)

type UserDaoMongo struct {
	*GenericDaoMongo
	collectionName string
}

// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter.
func (dao *UserDaoMongo) GdaoCreateFilter(collectionName string, bo godal.IGenericBo) godal.FilterOpt {
	if collectionName == dao.collectionName {
		return godal.FilterOptFieldOpValue{FieldName: fieldId, Operator: godal.FilterOpEqual, Value: bo.GboGetAttrUnsafe(fieldId, reddo.TypeString)}
	}
	return nil
}

func (dao *UserDaoMongo) toGbo(u *UserBoMongo) godal.IGenericBo {
	gbo := godal.NewGenericBo()
	if err := gbo.GboImportViaJson(u); err != nil {
		return nil
	}
	return gbo
}

func (dao *UserDaoMongo) toUser(gbo godal.IGenericBo) *UserBoMongo {
	bo := UserBoMongo{}
	if err := gbo.GboTransferViaJson(&bo); err != nil {
		return nil
	}
	return &bo
}

/*----------------------------------------------------------------------*/
func _initDao(t *testing.T, testName, collectionName string) *UserDaoMongo {
	mc := _createMongoConnect(t, testName)
	return createDaoMongo(mc, collectionName)
}

func TestGenericRowMapperMongo_ColumnsList(t *testing.T) {
	name := "TestGenericRowMapperMongo_ColumnsList"
	table := "table"
	rowmapper := &GenericRowMapperMongo{}

	colList := rowmapper.ColumnsList(table)
	if len(colList) != 1 || colList[0] != "*" {
		t.Fatalf("%s failed: %v", name, colList)
	}
}

func testToBo(t *testing.T, name string, rowmapper godal.IRowMapper, table string, row interface{}) {
	colA, colB, colC, col1, col2 := "cola", "ColB", "colC", "Col1", "coL2"
	valA, valB, val1, val2 := "a", "B", int64(1), int64(2)

	bo, err := rowmapper.ToBo(table, row)
	if err != nil || bo == nil {
		t.Fatalf("%s failed: %s / %v", name, err, bo)
	}
	if bo.GboGetAttrUnsafe(colA, reddo.TypeString) != valA ||
		bo.GboGetAttrUnsafe(colB, reddo.TypeString) != valB ||
		bo.GboGetAttrUnsafe(colC, reddo.TypeString) != nil ||
		bo.GboGetAttrUnsafe(col1, reddo.TypeInt).(int64) != val1 ||
		bo.GboGetAttrUnsafe(col2, reddo.TypeInt).(int64) != val2 {
		t.Fatalf("%s failed, Row: %v - Bo: %v", name, row, bo)
	}
}

func TestGenericRowMapperMongo_ToBo(t *testing.T) {
	name := "TestGenericRowMapperMongo_ToBo"
	table := "table"
	colA, colB, col1, col2 := "cola", "ColB", "Col1", "coL2"
	valA, valB, val1, val2 := "a", "B", int64(1), int64(2)
	rowmapper := &GenericRowMapperMongo{}

	if v, err := rowmapper.ToBo("", time.Time{}); v != nil || err == nil {
		t.Fatalf("%s failed: %#v / %s", name, v, err)
	}

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
			t.Fatalf("%s failed: %s / %v", name, err, bo)
		}
		if bo, err := rowmapper.ToBo(table, &row); err != nil || bo != nil {
			t.Fatalf("%s failed: %s / %v", name, err, bo)
		}
		row2 := &row
		if bo, err := rowmapper.ToBo(table, &row2); err != nil || bo != nil {
			t.Fatalf("%s failed: %s / %v", name, err, bo)
		}
	}

	{
		var row *string = nil
		if bo, err := rowmapper.ToBo(table, row); err != nil || bo != nil {
			t.Fatalf("%s failed: %s / %v", name, err, bo)
		}
		if bo, err := rowmapper.ToBo(table, &row); err != nil || bo != nil {
			t.Fatalf("%s failed: %s / %v", name, err, bo)
		}
		row2 := &row
		if bo, err := rowmapper.ToBo(table, &row2); err != nil || bo != nil {
			t.Fatalf("%s failed: %s / %v", name, err, bo)
		}
	}

	{
		var row []byte = nil
		if bo, err := rowmapper.ToBo(table, row); err != nil || bo != nil {
			t.Fatalf("%s failed: %s / %v", name, err, bo)
		}
		if bo, err := rowmapper.ToBo(table, &row); err != nil || bo != nil {
			t.Fatalf("%s failed: %s / %v", name, err, bo)
		}
		row2 := &row
		if bo, err := rowmapper.ToBo(table, &row2); err != nil || bo != nil {
			t.Fatalf("%s failed: %s / %v", name, err, bo)
		}
	}

	{
		var row *[]byte = nil
		if bo, err := rowmapper.ToBo(table, row); err != nil || bo != nil {
			t.Fatalf("%s failed: %s / %v", name, err, bo)
		}
		if bo, err := rowmapper.ToBo(table, &row); err != nil || bo != nil {
			t.Fatalf("%s failed: %s / %v", name, err, bo)
		}
		row2 := &row
		if bo, err := rowmapper.ToBo(table, &row2); err != nil || bo != nil {
			t.Fatalf("%s failed: %s / %v", name, err, bo)
		}
	}
}

func TestGenericRowMapperMongo_ToRow(t *testing.T) {
	name := "TestGenericRowMapperMongo_ToRow"
	table := "table"
	colA, colB, colC, col1, col2 := "cola", "ColB", "colC", "Col1", "coL2"
	valA, valB, val1, val2 := "a", "B", int64(1), int64(2)
	rowmapper := &GenericRowMapperMongo{}

	if v, err := rowmapper.ToRow("", nil); v != nil || err != nil {
		t.Fatalf("%s failed: %#v / %s", name, v, err)
	}

	{
		bo := godal.NewGenericBo()
		bo.GboSetAttr(colA, valA)
		bo.GboSetAttr(colB, valB)
		bo.GboSetAttr(col1, val1)
		bo.GboSetAttr(col2, val2)

		row, err := rowmapper.ToRow(table, bo)
		if err != nil || row == nil {
			t.Fatalf("%s failed: %s / %v", name, err, row)
		}
		if bo.GboGetAttrUnsafe(colA, reddo.TypeString) != valA ||
			bo.GboGetAttrUnsafe(colB, reddo.TypeString) != valB ||
			bo.GboGetAttrUnsafe(colC, reddo.TypeString) != nil ||
			bo.GboGetAttrUnsafe(col1, reddo.TypeInt).(int64) != val1 ||
			bo.GboGetAttrUnsafe(col2, reddo.TypeInt).(int64) != val2 {
			t.Fatalf("%s failed, Row: %v - Bo: %v", name, row, bo)
		}
	}
}

func TestGenericRowMapperMongo_ToDbColName(t *testing.T) {
	name := "TestGenericRowMapperMongo_ToDbColName"
	table := "table"
	colA, colB := "cola", "ColB"
	rowMapper := &GenericRowMapperMongo{}

	if fieldName := rowMapper.ToDbColName(table, colA); fieldName != colA {
		t.Fatalf("%s failed, expect %#v but received %#v", name, colA, fieldName)
	}

	if fieldName := rowMapper.ToDbColName("table", colB); fieldName != colB {
		t.Fatalf("%s failed, expect %#v but received %#v", name, colB, fieldName)
	}
}

func TestGenericRowMapperMongo_ToBoFieldName(t *testing.T) {
	name := "TestGenericRowMapperMongo_ToBoFieldName"
	table := "table"
	colA, colB := "cola", "ColB"
	rowMapper := &GenericRowMapperMongo{}

	if colName := rowMapper.ToBoFieldName(table, colA); colName != colA {
		t.Fatalf("%s failed, expect %#v but received %#v", name, colA, colName)
	}

	if colName := rowMapper.ToBoFieldName("table", colB); colName != colB {
		t.Fatalf("%s failed, expect %#v but received %#v", name, colB, colName)
	}
}

func TestNewGenericDaoMongo(t *testing.T) {
	name := "TestNewGenericDaoMongo"
	dao := _initDao(t, name, testMongoCollectionName)
	if dao == nil {
		t.Fatalf("%s failed: nil", name)
	}
}

func TestGenericDaoMongo_SetGetMongoConnect(t *testing.T) {
	name := "TestGenericDaoMongo_SetGetMongoConnect"
	dao := _initDao(t, name, testMongoCollectionName)
	if dao == nil {
		t.Fatalf("%s failed: nil", name)
	}

	mc := _createMongoConnect(t, name)
	dao.SetMongoConnect(mc)
	if dao.GetMongoConnect() != mc {
		t.Fatalf("%s failed", name)
	}
}

func TestGenericDaoMongo_SetGetTxModeOnWrite(t *testing.T) {
	name := "TestGenericDaoMongo_SetGetTxModeOnWrite"
	dao := _initDao(t, name, testMongoCollectionName)
	if dao == nil {
		t.Fatalf("%s failed: nil", name)
	}
	txModeOnWrite := dao.GetTxModeOnWrite()
	dao.SetTxModeOnWrite(!txModeOnWrite)
	txModeOnWrite2 := dao.GetTxModeOnWrite()
	if txModeOnWrite == txModeOnWrite2 {
		t.Fatalf("%s failed", name)
	}
}

// func TestToMap(t *testing.T) {
// 	name := "TestToMap"
//
// 	input := make(map[string]interface{})
// 	if m, err := toMap(input); err != nil || m == nil {
// 		t.Fatalf("%s failed: %#v / %s", name, m, err)
// 	}
// 	if m, err := toMap(&input); err != nil || m == nil {
// 		t.Fatalf("%s failed: %#v / %s", name, m, err)
// 	}
//
// 	inputString := `{"id":"1", "username":"btnguyen2k"}`
// 	if m, err := toMap(inputString); err != nil || m == nil {
// 		t.Fatalf("%s failed: %#v / %s", name, m, err)
// 	}
// 	if m, err := toMap(&inputString); err != nil || m == nil {
// 		t.Fatalf("%s failed: %#v / %s", name, m, err)
// 	}
//
// 	inputBytes := []byte(`{"id":"1", "username":"btnguyen2k"}`)
// 	if m, err := toMap(inputBytes); err != nil || m == nil {
// 		t.Fatalf("%s failed: %#v / %s", name, m, err)
// 	}
// 	if m, err := toMap(&inputBytes); err != nil || m == nil {
// 		t.Fatalf("%s failed: %#v / %s", name, m, err)
// 	}
//
// 	if m, err := toMap(nil); m != nil || err != nil {
// 		t.Fatalf("%s failed: %#v / %s", name, m, err)
// 	}
// 	if m, err := toMap([]interface{}{"invalid", "input"}); m != nil || err == nil {
// 		t.Fatalf("%s failed: %#v / %s", name, m, err)
// 	}
// 	if m, err := toMap(time.Time{}); m != nil || err == nil {
// 		t.Fatalf("%s failed: %#v / %s", name, m, err)
// 	}
// }

func TestGenericDaoMongo_GdaoDelete(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoDelete"
	dao := _initDao(t, name, testMongoCollectionName)
	err := prepareMongoCollection(dao.GetMongoConnect(), dao.collectionName)
	if err != nil {
		t.Fatalf("%s failed: %s", name+"/prepareMongoCollection", err)
	}

	user := &UserBoMongo{
		Id:       "1",
		Username: "btnguyen2k",
		Name:     "Thanh Nguyen",
		Version:  int(time.Now().Unix()),
		Active:   false,
		Created:  time.Now(),
	}
	_, err = dao.GdaoCreate(dao.collectionName, dao.toGbo(user))
	if err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoCreate", err)
	}

	filterUser := &UserBoMongo{Id: "2"}
	if numRows, err := dao.GdaoDelete(dao.collectionName, dao.toGbo(filterUser)); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if numRows != 0 {
		t.Fatalf("%s failed: expected %#v row(s) deleted but received %#v", name, 0, numRows)
	}

	if numRows, err := dao.GdaoDelete(dao.collectionName, dao.toGbo(user)); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) deleted but received %#v", name, 1, numRows)
	}

	if u, err := dao.GdaoFetchOne(dao.collectionName, dao.GdaoCreateFilter(dao.collectionName, dao.toGbo(user))); err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoFetchOne", err)
	} else if u != nil {
		t.Fatalf("%s failed: non-nil", name+"/GdaoFetchOne")
	}
}

func TestGenericDaoMongo_GdaoDeleteMany(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoDeleteMany"
	dao := _initDao(t, name, testMongoCollectionName)
	err := prepareMongoCollection(dao.GetMongoConnect(), dao.collectionName)
	if err != nil {
		t.Fatalf("%s failed: %s", name+"/prepareMongoCollection", err)
	}

	filter := &godal.FilterOptOr{Filters: []godal.FilterOpt{
		&godal.FilterOptFieldOpValue{FieldName: fieldId, Operator: godal.FilterOpGreaterOrEqual, Value: "8"},
		&godal.FilterOptFieldOpValue{FieldName: fieldId, Operator: godal.FilterOpLess, Value: "3"},
	}}

	if numRows, err := dao.GdaoDeleteMany(dao.collectionName, filter); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if numRows != 0 {
		t.Fatalf("%s failed: expected %#v row(s) deleted but received %#v", name, 0, numRows)
	}

	for i := 0; i < 10; i++ {
		id := strconv.Itoa(i)
		user := &UserBoMongo{
			Id:       id,
			Username: "user" + id,
			Name:     "Thanh " + id,
			Version:  int(time.Now().UnixNano()),
			Active:   i%3 == 0,
			Created:  time.Now(),
		}
		_, err = dao.GdaoCreate(dao.collectionName, dao.toGbo(user))
		if err != nil {
			t.Fatalf("%s failed: %s", name+"/GdaoCreate", err)
		}
	}

	if numRows, err := dao.GdaoDeleteMany(dao.collectionName, filter); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if numRows != 5 {
		t.Fatalf("%s failed: expected %#v row(s) deleted but received %#v", name, 5, numRows)
	}
}

func TestGenericDaoMongo_GdaoFetchOne(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoFetchOne"
	dao := _initDao(t, name, testMongoCollectionName)
	err := prepareMongoCollection(dao.GetMongoConnect(), dao.collectionName)
	if err != nil {
		t.Fatalf("%s failed: %s", name+"/prepareMongoCollection", err)
	}

	filter := dao.GdaoCreateFilter(dao.collectionName, dao.toGbo(&UserBoMongo{Id: "1"}))
	if gbo, err := dao.GdaoFetchOne(dao.collectionName, filter); err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoFetchOne", err)
	} else if gbo != nil {
		t.Fatalf("%s failed: non-nil", name+"/GdaoFetchOne")
	}

	user := &UserBoMongo{
		Id:       "1",
		Username: "btnguyen2k",
		Name:     "Thanh Nguyen",
		Version:  int(time.Now().Unix()),
		Active:   false,
		Created:  time.Now(),
	}
	_, err = dao.GdaoCreate(dao.collectionName, dao.toGbo(user))
	if err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoCreate", err)
	}

	if gbo, err := dao.GdaoFetchOne(dao.collectionName, filter); err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoFetchOne", err)
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

func TestGenericDaoMongo_GdaoFetchMany(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoFetchMany"
	dao := _initDao(t, name, testMongoCollectionName)
	err := prepareMongoCollection(dao.GetMongoConnect(), dao.collectionName)
	if err != nil {
		t.Fatalf("%s failed: %s", name+"/prepareMongoCollection", err)
	}

	filter := &godal.FilterOptAnd{Filters: []godal.FilterOpt{
		&godal.FilterOptFieldOpValue{FieldName: fieldId, Operator: godal.FilterOpLessOrEqual, Value: "8"},
		&godal.FilterOptFieldOpValue{FieldName: fieldId, Operator: godal.FilterOpGreater, Value: "3"},
	}}

	if dbRows, err := dao.GdaoFetchMany(dao.collectionName, filter, nil, 1, 3); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if dbRows == nil || len(dbRows) != 0 {
		t.Fatalf("%s failed: expected %#v row(s) but received %#v", name, 0, dbRows)
	}

	for i := 0; i < 10; i++ {
		id := strconv.Itoa(i)
		user := &UserBoMongo{
			Id:       id,
			Username: "user" + id,
			Name:     "Thanh " + id,
			Version:  int(time.Now().UnixNano()),
			Active:   i%3 == 0,
			Created:  time.Now(),
		}
		_, err = dao.GdaoCreate(dao.collectionName, dao.toGbo(user))
		if err != nil {
			t.Fatalf("%s failed: %s", name+"/GdaoCreate", err)
		}
	}

	fetchIdList := []string{"7", "6", "5"}
	sorting := (&godal.SortingOpt{}).Add(&godal.SortingField{FieldName: fieldId, Descending: true})
	if dbRows, err := dao.GdaoFetchMany(dao.collectionName, filter, sorting, 1, 3); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if dbRows == nil || len(dbRows) != 3 {
		t.Fatalf("%s failed: expected %#v row(s) but received %#v", name, 3, dbRows)
	} else {
		for i, row := range dbRows {
			u := dao.toUser(row)
			if u.Id != fetchIdList[i] {
				t.Fatalf("%s failed: expected %#v but received %#v", name, fetchIdList[i], u.Id)
			}
		}
	}
}

func TestGenericDaoMongo_GdaoCreate(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoCreate"
	dao := _initDao(t, name, testMongoCollectionName)
	err := prepareMongoCollection(dao.GetMongoConnect(), dao.collectionName)
	if err != nil {
		t.Fatalf("%s failed: %s", name+"/prepareMongoCollection", err)
	}

	user := &UserBoMongo{
		Id:       "1",
		Username: "btnguyen2k",
		Name:     "Thanh Nguyen",
		Version:  int(time.Now().Unix()),
		Active:   false,
		Created:  time.Now(),
	}
	if numRows, err := dao.GdaoCreate(dao.collectionName, dao.toGbo(user)); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name, 1, numRows)
	}
	user.Username = "thanhn"
	if numRows, err := dao.GdaoCreate(dao.collectionName, dao.toGbo(user)); err != godal.GdaoErrorDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed: num rows %#v / error: %s", name, numRows, err)
	}
	filter := dao.GdaoCreateFilter(dao.collectionName, dao.toGbo(&UserBoMongo{Id: "1"}))
	if gbo, err := dao.GdaoFetchOne(dao.collectionName, filter); err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoFetchOne", err)
	} else if gbo == nil {
		t.Fatalf("%s failed: nil", name+"/GdaoFetchOne")
	} else {
		u := dao.toUser(gbo)
		if u.Username != "btnguyen2k" {
			t.Fatalf("%s failed: expected %v but received %v", name+"/GdaoFetchOne", "btnguyen2k", u.Username)
		}
	}
}

func TestGenericDaoMongo_GdaoCreate_TxOn(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoCreate_TxOn"
	dao := _initDao(t, name, testMongoCollectionName)
	err := prepareMongoCollection(dao.GetMongoConnect(), dao.collectionName)
	if err != nil {
		t.Fatalf("%s failed: %s", name+"/prepareMongoCollection", err)
	}
	if strings.Index(dao.mongoConnect.GetUrl(), "replicaSet=") < 0 {
		t.Skipf("%s skipped", name)
	}
	dao.SetTxModeOnWrite(true)

	user := &UserBoMongo{
		Id:       "1",
		Username: "btnguyen2k",
		Name:     "Thanh Nguyen",
		Version:  int(time.Now().Unix()),
		Active:   false,
		Created:  time.Now(),
	}
	if numRows, err := dao.GdaoCreate(dao.collectionName, dao.toGbo(user)); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name, 1, numRows)
	}
	user.Username = "thanhn"
	if numRows, err := dao.GdaoCreate(dao.collectionName, dao.toGbo(user)); err != godal.GdaoErrorDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed: num rows %#v / error: %s", name, numRows, err)
	}
	filter := dao.GdaoCreateFilter(dao.collectionName, dao.toGbo(&UserBoMongo{Id: "1"}))
	if gbo, err := dao.GdaoFetchOne(dao.collectionName, filter); err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoFetchOne", err)
	} else if gbo == nil {
		t.Fatalf("%s failed: nil", name+"/GdaoFetchOne")
	} else {
		u := dao.toUser(gbo)
		if u.Username != "btnguyen2k" {
			t.Fatalf("%s failed: expected %v but received %v", name+"/GdaoFetchOne", "btnguyen2k", u.Username)
		}
	}
}

func TestGenericDaoMongo_GdaoUpdate(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoUpdate"
	dao := _initDao(t, name, testMongoCollectionName)
	err := prepareMongoCollection(dao.GetMongoConnect(), dao.collectionName)
	if err != nil {
		t.Fatalf("%s failed: %s", name+"/prepareMongoCollection", err)
	}

	user := &UserBoMongo{
		Id:       "1",
		Username: "btnguyen2k",
		Name:     "Thanh Nguyen",
		Version:  int(time.Now().Unix()),
		Active:   false,
		Created:  time.Now(),
	}
	if numRows, err := dao.GdaoUpdate(dao.collectionName, dao.toGbo(user)); err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoUpdate", err)
	} else if numRows != 0 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoUpdate", 0, numRows)
	}
	if numRows, err := dao.GdaoCreate(dao.collectionName, dao.toGbo(user)); err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoCreate", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoCreate", 1, numRows)
	}

	user.Username = "thanhn"
	if numRows, err := dao.GdaoUpdate(dao.collectionName, dao.toGbo(user)); err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoUpdate", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoUpdate", 1, numRows)
	}

	filter := dao.GdaoCreateFilter(dao.collectionName, dao.toGbo(&UserBoMongo{Id: "1"}))
	if gbo, err := dao.GdaoFetchOne(dao.collectionName, filter); err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoFetchOne", err)
	} else if gbo == nil {
		t.Fatalf("%s failed: nil", name+"/GdaoFetchOne")
	} else {
		u := dao.toUser(gbo)
		if u.Username != "thanhn" {
			t.Fatalf("%s failed: expected %v but received %v", name+"/GdaoFetchOne", "thanhn", u.Username)
		}
	}
}

func TestGenericDaoMongo_GdaoUpdateDuplicated(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoUpdateDuplicated"
	dao := _initDao(t, name, testMongoCollectionName)
	err := prepareMongoCollection(dao.GetMongoConnect(), dao.collectionName)
	if err != nil {
		t.Fatalf("%s failed: %s", name+"/prepareMongoCollection", err)
	}

	user1 := &UserBoMongo{
		Id:       "1",
		Username: "user1",
	}
	if numRows, err := dao.GdaoCreate(dao.collectionName, dao.toGbo(user1)); err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoCreate", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoCreate", 1, numRows)
	}

	user2 := &UserBoMongo{
		Id:       "2",
		Username: "user2",
	}
	if numRows, err := dao.GdaoCreate(dao.collectionName, dao.toGbo(user2)); err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoCreate", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoCreate", 1, numRows)
	}

	user2.Username = "user1"
	if numRows, err := dao.GdaoUpdate(dao.collectionName, dao.toGbo(user2)); err != godal.GdaoErrorDuplicatedEntry || numRows != 0 {
		fmt.Println("Error:", err)
		fmt.Printf("Error: %#v\n", err)
		t.Fatalf("%s failed: num rows %#v / error: %s", name, numRows, err)
	}
}

func TestGenericDaoMongo_GdaoSave(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoSave"
	dao := _initDao(t, name, testMongoCollectionName)
	err := prepareMongoCollection(dao.GetMongoConnect(), dao.collectionName)
	if err != nil {
		t.Fatalf("%s failed: %s", name+"/prepareMongoCollection", err)
	}

	user := &UserBoMongo{
		Id:       "1",
		Username: "btnguyen2k",
		Name:     "Thanh Nguyen",
		Version:  int(time.Now().Unix()),
		Active:   false,
		Created:  time.Now(),
	}
	if numRows, err := dao.GdaoSave(dao.collectionName, dao.toGbo(user)); err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoSave", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoSave", 1, numRows)
	}

	user.Username = "thanhn"
	if numRows, err := dao.GdaoSave(dao.collectionName, dao.toGbo(user)); err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoSave", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoSave", 1, numRows)
	}

	filter := dao.GdaoCreateFilter(dao.collectionName, dao.toGbo(&UserBoMongo{Id: "1"}))
	if gbo, err := dao.GdaoFetchOne(dao.collectionName, filter); err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoFetchOne", err)
	} else if gbo == nil {
		t.Fatalf("%s failed: nil", name+"/GdaoFetchOne")
	} else {
		u := dao.toUser(gbo)
		if u.Username != "thanhn" {
			t.Fatalf("%s failed: expected %v but received %v", name+"/GdaoFetchOne", "thanhn", u.Username)
		}
	}
}

func TestGenericDaoMongo_GdaoSaveShouldReplace(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoSaveShouldReplace"
	dao := _initDao(t, name, testMongoCollectionName)
	err := prepareMongoCollection(dao.GetMongoConnect(), dao.collectionName)
	if err != nil {
		t.Fatalf("%s failed: %s", name+"/prepareMongoCollection", err)
	}

	gbo := godal.NewGenericBo()

	data1 := map[string]interface{}{
		"_id":      "1",
		"username": "btnguyen2k",
		"active":   false,
		"version":  1,
	}
	gbo.GboImportViaJson(data1)
	if numRows, err := dao.GdaoSave(dao.collectionName, gbo); err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoSave", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoSave", 1, numRows)
	}

	data2 := map[string]interface{}{
		"_id":    "1",
		"name":   "Thanh Nguyen",
		"active": true,
	}
	gbo.GboImportViaJson(data2)
	if numRows, err := dao.GdaoSave(dao.collectionName, gbo); err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoSave", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoSave", 1, numRows)
	}

	filter := godal.FilterOptFieldOpValue{FieldName: fieldId, Operator: godal.FilterOpEqual, Value: "1"}
	if gbo, err := dao.GdaoFetchOne(dao.collectionName, filter); err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoFetchOne", err)
	} else if gbo == nil {
		t.Fatalf("%s failed: nil", name+"/GdaoFetchOne")
	} else {
		data := make(map[string]interface{})
		gbo.GboTransferViaJson(&data)
		if !reflect.DeepEqual(data2, data) {
			t.Fatalf("%s failed: expected %v but received %v", name+"/GdaoFetchOne", data2, data)
		}
	}
}

func TestGenericDaoMongo_GdaoSaveDuplicated(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoSaveDuplicated"
	dao := _initDao(t, name, testMongoCollectionName)
	err := prepareMongoCollection(dao.GetMongoConnect(), dao.collectionName)
	if err != nil {
		t.Fatalf("%s failed: %s", name+"/prepareMongoCollection", err)
	}

	user1 := &UserBoMongo{
		Id:       "1",
		Username: "user1",
	}
	if numRows, err := dao.GdaoSave(dao.collectionName, dao.toGbo(user1)); err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoCreate", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoCreate", 1, numRows)
	}

	user2 := &UserBoMongo{
		Id:       "2",
		Username: "user2",
	}
	if numRows, err := dao.GdaoSave(dao.collectionName, dao.toGbo(user2)); err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoCreate", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoCreate", 1, numRows)
	}

	user2.Username = "user1"
	if numRows, err := dao.GdaoSave(dao.collectionName, dao.toGbo(user2)); err != godal.GdaoErrorDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed: num rows %#v / error: %s", name, numRows, err)
	}
}
