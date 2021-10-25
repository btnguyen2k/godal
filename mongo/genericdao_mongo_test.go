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
	"go.mongodb.org/mongo-driver/bson"
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
	testTimeZone            = "Asia/Ho_Chi_Minh"
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

func _compareUsers(t *testing.T, name string, expected, target *UserBoMongo) {
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

	// if (expected.ValPInt != nil && (target.ValPInt == nil || *target.ValPInt != *expected.ValPInt)) || (expected.ValPInt == nil && target.ValPInt != nil) {
	// 	t.Fatalf("%s failed: field [PInt] mismatched - %#v / %#v", name, expected.ValPInt, target.ValPInt)
	// }
	// if (expected.ValPFloat != nil && (target.ValPFloat == nil || *target.ValPFloat != *expected.ValPFloat)) || (expected.ValPFloat == nil && target.ValPFloat != nil) {
	// 	t.Fatalf("%s failed: field [PFloat] mismatched - %#v / %#v", name, expected.ValPFloat, target.ValPFloat)
	// }
	// if (expected.ValPString != nil && (target.ValPString == nil || *target.ValPString != *expected.ValPString)) || (expected.ValPString == nil && target.ValPString != nil) {
	// 	t.Fatalf("%s failed: field [PString] mismatched - %#v / %#v", name, expected.ValPString, target.ValPString)
	// }
	// if (expected.ValPTime != nil && (target.ValPTime == nil || target.ValPTime.In(loc).Format(layout) != expected.ValPTime.In(loc).Format(layout))) || (expected.ValPTime == nil && target.ValPTime != nil) {
	// 	t.Fatalf("%s failed: field [PTime] mismatched - %#v / %#v", name, expected.ValPTime, target.ValPTime)
	// }
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
	defer dao.mongoConnect.Close(nil)
}

func TestGenericDaoMongo_SetGetMongoConnect(t *testing.T) {
	name := "TestGenericDaoMongo_SetGetMongoConnect"
	dao := _initDao(t, name, testMongoCollectionName)
	if dao == nil {
		t.Fatalf("%s failed: nil", name)
	}
	dao.mongoConnect.Close(nil)

	mc := _createMongoConnect(t, name)
	defer mc.Close(nil)
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
	defer dao.mongoConnect.Close(nil)

	txModeOnWrite := dao.GetTxModeOnWrite()
	dao.SetTxModeOnWrite(!txModeOnWrite)
	txModeOnWrite2 := dao.GetTxModeOnWrite()
	if txModeOnWrite == txModeOnWrite2 {
		t.Fatalf("%s failed", name)
	}
}

func TestGenericDaoMongo_BuildFilter(t *testing.T) {
	name := "TestGenericDaoMongo_BuildFilter"
	dao := _initDao(t, name, testMongoCollectionName)
	if dao == nil {
		t.Fatalf("%s failed: nil", name)
	}
	defer dao.mongoConnect.Close(nil)

	var input godal.FilterOpt
	var output, expected bson.M
	var err error

	input = nil
	expected = nil
	if output, err = dao.BuildFilter(testMongoCollectionName, input); err != nil || !reflect.DeepEqual(expected, output) {
		t.Fatalf("%s failed: expected %#v but received %#v / Error: %s", name, expected, output, err)
	}

	// _n := expression.Name("field")
	// _v := expression.Value("value")
	optsList := []godal.FilterOperator{godal.FilterOpEqual, godal.FilterOpNotEqual, godal.FilterOpGreater, godal.FilterOpGreaterOrEqual, godal.FilterOpLess, godal.FilterOpLessOrEqual}
	expectedList := []bson.M{{"field": bson.M{"$eq": 0}}, {"field": bson.M{"$ne": 1}}, {"field": bson.M{"$gt": 2}}, {"field": bson.M{"$gte": 3}}, {"field": bson.M{"$lt": 4}}, {"field": bson.M{"$lte": 5}}}
	for i, opt := range optsList {
		expected = expectedList[i]
		input = godal.FilterOptFieldOpValue{FieldName: "field", Operator: opt, Value: i}
		if output, err = dao.BuildFilter(testMongoCollectionName, input); err != nil || !reflect.DeepEqual(expected, output) {
			t.Fatalf("%s failed: expected %#v but received %#v / Error: %s", name, expected, output, err)
		}
		input = &godal.FilterOptFieldOpValue{FieldName: "field", Operator: opt, Value: i}
		if output, err = dao.BuildFilter(testMongoCollectionName, input); err != nil || !reflect.DeepEqual(expected, output) {
			t.Fatalf("%s failed: expected %#v but received %#v / Error: %s", name, expected, output, err)
		}
	}

	expected = bson.M{"field": bson.M{"$eq": nil}}
	input = godal.FilterOptFieldIsNull{FieldName: "field"}
	if output, err = dao.BuildFilter(testMongoCollectionName, input); err != nil || !reflect.DeepEqual(expected, output) {
		t.Fatalf("%s failed: expected %#v but received %#v / Error: %s", name, expected, output, err)
	}
	input = &godal.FilterOptFieldIsNull{FieldName: "field"}
	if output, err = dao.BuildFilter(testMongoCollectionName, input); err != nil || !reflect.DeepEqual(expected, output) {
		t.Fatalf("%s failed: expected %#v but received %#v / Error: %s", name, expected, output, err)
	}

	expected = bson.M{"field": bson.M{"$ne": nil}}
	input = godal.FilterOptFieldIsNotNull{FieldName: "field"}
	if output, err = dao.BuildFilter(testMongoCollectionName, input); err != nil || !reflect.DeepEqual(expected, output) {
		t.Fatalf("%s failed: expected %#v but received %#v / Error: %s", name, expected, output, err)
	}
	input = &godal.FilterOptFieldIsNotNull{FieldName: "field"}
	if output, err = dao.BuildFilter(testMongoCollectionName, input); err != nil || !reflect.DeepEqual(expected, output) {
		t.Fatalf("%s failed: expected %#v but received %#v / Error: %s", name, expected, output, err)
	}

	expected = bson.M{"$and": bson.A{bson.M{"field1": bson.M{"$gt": 1}}, bson.M{"field2": bson.M{"$lte": "3"}}}}
	input = godal.FilterOptAnd{Filters: []godal.FilterOpt{
		godal.FilterOptFieldOpValue{FieldName: "field1", Operator: godal.FilterOpGreater, Value: 1},
		godal.FilterOptFieldOpValue{FieldName: "field2", Operator: godal.FilterOpLessOrEqual, Value: "3"}}}
	if output, err = dao.BuildFilter(testMongoCollectionName, input); err != nil || !reflect.DeepEqual(expected, output) {
		t.Fatalf("%s failed: expected %#v but received %#v / Error: %s", name, expected, output, err)
	}
	input = &godal.FilterOptAnd{Filters: []godal.FilterOpt{
		godal.FilterOptFieldOpValue{FieldName: "field1", Operator: godal.FilterOpGreater, Value: 1},
		godal.FilterOptFieldOpValue{FieldName: "field2", Operator: godal.FilterOpLessOrEqual, Value: "3"}}}
	if output, err = dao.BuildFilter(testMongoCollectionName, input); err != nil || !reflect.DeepEqual(expected, output) {
		t.Fatalf("%s failed: expected %#v but received %#v / Error: %s", name, expected, output, err)
	}

	expected = bson.M{"$or": bson.A{bson.M{"field3": bson.M{"$gt": "5"}}, bson.M{"field4": bson.M{"$lte": 7}}}}
	input = godal.FilterOptOr{Filters: []godal.FilterOpt{
		godal.FilterOptFieldOpValue{FieldName: "field3", Operator: godal.FilterOpGreater, Value: "5"},
		godal.FilterOptFieldOpValue{FieldName: "field4", Operator: godal.FilterOpLessOrEqual, Value: 7}}}
	if output, err = dao.BuildFilter(testMongoCollectionName, input); err != nil || !reflect.DeepEqual(expected, output) {
		t.Fatalf("%s failed: expected %#v but received %#v / Error: %s", name, expected, output, err)
	}
	input = &godal.FilterOptOr{Filters: []godal.FilterOpt{
		godal.FilterOptFieldOpValue{FieldName: "field3", Operator: godal.FilterOpGreater, Value: "5"},
		godal.FilterOptFieldOpValue{FieldName: "field4", Operator: godal.FilterOpLessOrEqual, Value: 7}}}
	if output, err = dao.BuildFilter(testMongoCollectionName, input); err != nil || !reflect.DeepEqual(expected, output) {
		t.Fatalf("%s failed: expected %#v but received %#v / Error: %s", name, expected, output, err)
	}
}

func TestGenericDaoMongo_GdaoDelete(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoDelete"
	dao := _initDao(t, name, testMongoCollectionName)
	defer dao.mongoConnect.Close(nil)
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

	filterUser = &UserBoMongo{Id: user.Id}
	if u, err := dao.GdaoFetchOne(dao.collectionName, dao.GdaoCreateFilter(dao.collectionName, dao.toGbo(filterUser))); err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoFetchOne", err)
	} else if u != nil {
		t.Fatalf("%s failed: non-nil", name+"/GdaoFetchOne")
	}
}

func TestGenericDaoMongo_GdaoDeleteMany(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoDeleteMany"
	dao := _initDao(t, name, testMongoCollectionName)
	defer dao.mongoConnect.Close(nil)
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
	defer dao.mongoConnect.Close(nil)
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
		fetchedUser := dao.toUser(gbo)
		_compareUsers(t, name, user, fetchedUser)
	}
}

func TestGenericDaoMongo_GdaoFetchMany(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoFetchMany"
	dao := _initDao(t, name, testMongoCollectionName)
	defer dao.mongoConnect.Close(nil)
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

	userMap := make(map[string]*UserBoMongo)
	for i := 0; i < 10; i++ {
		id := strconv.Itoa(i)
		user := &UserBoMongo{
			Id:       id,
			Username: "user" + id,
			Name:     "Thanh " + id,
			Version:  int(time.Now().Unix()),
			Active:   i%3 == 0,
			Created:  time.Now(),
		}
		_, err = dao.GdaoCreate(dao.collectionName, dao.toGbo(user))
		if err != nil {
			t.Fatalf("%s failed: %s", name+"/GdaoCreate", err)
		}
		userMap[id] = user
	}

	sorting := (&godal.SortingOpt{}).Add(&godal.SortingField{FieldName: fieldId, Descending: true})
	if dbRows, err := dao.GdaoFetchMany(dao.collectionName, filter, sorting, 1, 3); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if dbRows == nil || len(dbRows) != 3 {
		t.Fatalf("%s failed: expected %#v row(s) but received %#v", name, 3, dbRows)
	} else {
		for _, row := range dbRows {
			fetchedUser := dao.toUser(row)
			_compareUsers(t, name, userMap[fetchedUser.Id], fetchedUser)
		}
	}
}

func TestGenericDaoMongo_GdaoCreate(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoCreate"
	dao := _initDao(t, name, testMongoCollectionName)
	defer dao.mongoConnect.Close(nil)
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

	// duplicated id
	clone := *user
	clone.Username = "thanhn"
	if numRows, err := dao.GdaoCreate(dao.collectionName, dao.toGbo(&clone)); err != godal.ErrGdaoDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed: num rows %#v / error: %s", name, numRows, err)
	}

	filter := dao.GdaoCreateFilter(dao.collectionName, dao.toGbo(&UserBoMongo{Id: "1"}))
	if gbo, err := dao.GdaoFetchOne(dao.collectionName, filter); err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoFetchOne", err)
	} else if gbo == nil {
		t.Fatalf("%s failed: nil", name+"/GdaoFetchOne")
	} else {
		fetchedUser := dao.toUser(gbo)
		_compareUsers(t, name, user, fetchedUser)
	}
}

func TestGenericDaoMongo_GdaoCreate_TxOn(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoCreate_TxOn"
	dao := _initDao(t, name, testMongoCollectionName)
	defer dao.mongoConnect.Close(nil)
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

	// duplicated id
	clone := *user
	clone.Username = "thanhn"
	if numRows, err := dao.GdaoCreate(dao.collectionName, dao.toGbo(&clone)); err != godal.ErrGdaoDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed: num rows %#v / error: %s", name, numRows, err)
	}

	filter := dao.GdaoCreateFilter(dao.collectionName, dao.toGbo(&UserBoMongo{Id: "1"}))
	if gbo, err := dao.GdaoFetchOne(dao.collectionName, filter); err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoFetchOne", err)
	} else if gbo == nil {
		t.Fatalf("%s failed: nil", name+"/GdaoFetchOne")
	} else {
		fetchedUser := dao.toUser(gbo)
		_compareUsers(t, name, user, fetchedUser)
	}
}

func TestGenericDaoMongo_GdaoUpdate(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoUpdate"
	dao := _initDao(t, name, testMongoCollectionName)
	defer dao.mongoConnect.Close(nil)
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
		fetchedUser := dao.toUser(gbo)
		_compareUsers(t, name, user, fetchedUser)
	}
}

func TestGenericDaoMongo_GdaoUpdateDuplicated(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoUpdateDuplicated"
	dao := _initDao(t, name, testMongoCollectionName)
	defer dao.mongoConnect.Close(nil)
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

	user2.Username = user1.Username
	if numRows, err := dao.GdaoUpdate(dao.collectionName, dao.toGbo(user2)); err != godal.ErrGdaoDuplicatedEntry || numRows != 0 {
		fmt.Println("Error:", err)
		fmt.Printf("Error: %#v\n", err)
		t.Fatalf("%s failed: num rows %#v / error: %s", name, numRows, err)
	}
}

func TestGenericDaoMongo_GdaoSave(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoSave"
	dao := _initDao(t, name, testMongoCollectionName)
	defer dao.mongoConnect.Close(nil)
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
		fetchedUser := dao.toUser(gbo)
		_compareUsers(t, name, user, fetchedUser)
	}
}

func TestGenericDaoMongo_GdaoSaveShouldReplace(t *testing.T) {
	name := "TestGenericDaoMongo_GdaoSaveShouldReplace"
	dao := _initDao(t, name, testMongoCollectionName)
	defer dao.mongoConnect.Close(nil)
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
	defer dao.mongoConnect.Close(nil)
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

	// duplicated id
	user2.Username = user1.Username
	if numRows, err := dao.GdaoSave(dao.collectionName, dao.toGbo(user2)); err != godal.ErrGdaoDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed: num rows %#v / error: %s", name, numRows, err)
	}
}
