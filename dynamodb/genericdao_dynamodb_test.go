package dynamodb

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/prom"
	"strconv"
	"sync"
	"testing"
	"time"
)

func createAwsDynamodbConnect() *prom.AwsDynamodbConnect {
	region := "ap-southeast-1"
	cfg := &aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewEnvCredentials(),
	}
	adc, err := prom.NewAwsDynamodbConnect(cfg, nil, nil, 10000)
	if err != nil {
		panic(err)
	}
	return adc
}

func initDataDynamodb(adc *prom.AwsDynamodbConnect, tableName string) {
	var schema, key []prom.AwsDynamodbNameAndType

	if ok, err := adc.HasTable(nil, tableName); err != nil {
		panic(err)
	} else if !ok {
		schema = []prom.AwsDynamodbNameAndType{
			{fieldId, prom.AwsAttrTypeString},
		}
		key = []prom.AwsDynamodbNameAndType{
			{fieldId, prom.AwsKeyTypePartition},
		}
		if err := adc.CreateTable(nil, tableName, 2, 2, schema, key); err != nil {
			panic(err)
		}
		time.Sleep(1 * time.Second)
		for status, err := adc.GetTableStatus(nil, tableName); status != "ACTIVE" && err == nil; {
			fmt.Printf("    Table [%s] status: %v - %e\n", tableName, status, err)
			time.Sleep(1 * time.Second)
			status, err = adc.GetTableStatus(nil, tableName)
		}
	}

	if status, err := adc.GetGlobalSecondaryIndexStatus(nil, tableName, indexName); err != nil {
		panic(err)
	} else if status == "" {
		schema = []prom.AwsDynamodbNameAndType{
			{fieldActived, prom.AwsAttrTypeNumber},
			{fieldVersion, prom.AwsAttrTypeNumber},
		}
		key = []prom.AwsDynamodbNameAndType{
			{fieldActived, prom.AwsKeyTypePartition},
			{fieldVersion, prom.AwsKeyTypeSort},
		}
		if err := adc.CreateGlobalSecondaryIndex(nil, tableName, indexName, 1, 1, schema, key); err != nil {
			panic(err)
		}
		time.Sleep(5 * time.Second)
		for status, err := adc.GetGlobalSecondaryIndexStatus(nil, tableName, indexName); status != "ACTIVE" && err == nil; {
			fmt.Printf("    GSI [%s] on table [%s] status: %v - %e\n", indexName, tableName, status, err)
			time.Sleep(5 * time.Second)
			status, err = adc.GetGlobalSecondaryIndexStatus(nil, tableName, indexName)
		}
	}

	// delete all items
	pkAttrs := []string{fieldId}
	adc.ScanItemsWithCallback(nil, tableName, nil, prom.AwsDynamodbNoIndex, nil, func(item prom.AwsDynamodbItem, lastEvaluatedKey map[string]*dynamodb.AttributeValue) (b bool, e error) {
		keyFilter := make(map[string]interface{})
		for _, v := range pkAttrs {
			keyFilter[v] = item[v]
		}
		_, err := adc.DeleteItem(nil, tableName, keyFilter, nil)
		if err != nil {
			fmt.Printf("    Delete item from table [%s] with key %s: %e\n", tableName, keyFilter, err)
		}
		// fmt.Printf("    Delete item from table [%s] with key %s: %e\n", table, keyFilter, err)
		return true, nil
	})
}

func createDaoDynamodb(adc *prom.AwsDynamodbConnect, tableName string) *MyDaoDynamodb {
	dao := &MyDaoDynamodb{tableName: tableName}
	dao.GenericDaoDynamodb = NewGenericDaoDynamodb(adc, godal.NewAbstractGenericDao(dao))
	dao.SetRowMapper(&GenericRowMapperDynamodb{ColumnsListMap: map[string][]string{tableName: {fieldId}}})
	return dao
}

type MyBo struct {
	Id       string `json:"id"`
	Username string `json:"username"`
	Name     string `json:"name"`
	Version  int    `json:"version"`
	Actived  int    `json:"actived"`
}

func (bo *MyBo) ToGbo() godal.IGenericBo {
	bo.Actived = 1
	gbo := godal.NewGenericBo()
	if err := gbo.GboImportViaJson(bo); err != nil {
		panic(err)
	}
	return gbo
}

func fromGbo(gbo godal.IGenericBo) *MyBo {
	bo := MyBo{}
	gbo.GboTransferViaJson(&bo)
	return &bo
}

const (
	tableName     = "test"
	indexName     = "idx_sorted"
	fieldId       = "id"
	fieldUsername = "username"
	fieldVersion  = "version"
	fieldActived  = "actived"
)

type MyDaoDynamodb struct {
	*GenericDaoDynamodb
	tableName string
}

// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter.
func (dao *MyDaoDynamodb) GdaoCreateFilter(storageId string, bo godal.IGenericBo) interface{} {
	return map[string]interface{}{fieldId: bo.GboGetAttrUnsafe(fieldId, reddo.TypeString)}
}

/*----------------------------------------------------------------------*/
func initDao() *MyDaoDynamodb {
	adc := createAwsDynamodbConnect()
	initDataDynamodb(adc, tableName)
	return createDaoDynamodb(adc, tableName)
}

func TestGenericRowMapperDynamodb_ColumnsList(t *testing.T) {
	name := "TestGenericRowMapperDynamodb_ColumnsList"
	table := "table"
	colA, colB, colC := "cola", "ColB", "colC"
	cols := []string{colA, colB, colC}
	rowmapper := &GenericRowMapperDynamodb{ColumnsListMap: map[string][]string{table: cols}}

	colList := rowmapper.ColumnsList(table)
	if len(colList) != 3 || colList[0] != colA || colList[1] != colB || colList[2] != colC {
		t.Fatalf("%s failed, expect table [%s] has columns %#v but received %#v", name, table, []string{colA, colB, colC}, cols)
	}

	if rowmapper.ColumnsList("not_exists") != nil {
		t.Fatalf("%s failed", table)
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

func TestGenericRowMapperDynamodb_ToBo(t *testing.T) {
	name := "TestGenericRowMapperDynamodb_ToBo"
	table := "table"
	colA, colB, colC, col1, col2 := "cola", "ColB", "colC", "Col1", "coL2"
	valA, valB, val1, val2 := "a", "B", int64(1), int64(2)
	cols := []string{colA, colB, colC}
	rowmapper := &GenericRowMapperDynamodb{ColumnsListMap: map[string][]string{table: cols}}

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

func TestGenericRowMapperDynamodb_ToRow(t *testing.T) {
	name := "TestGenericRowMapperDynamodb_ToRow"
	table := "table"
	colA, colB, colC, col1, col2 := "cola", "ColB", "colC", "Col1", "coL2"
	valA, valB, val1, val2 := "a", "B", int64(1), int64(2)
	cols := []string{colA, colB, colC}
	rowmapper := &GenericRowMapperDynamodb{ColumnsListMap: map[string][]string{table: cols}}

	{
		bo := godal.NewGenericBo()
		bo.GboSetAttr(colA, valA)
		bo.GboSetAttr(colB, valB)
		bo.GboSetAttr(col1, val1)
		bo.GboSetAttr(col2, val2)

		row, err := rowmapper.ToRow(table, bo)
		if err != nil || row == nil {
			t.Fatalf("%s failed: %e / %v", name, err, row)
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

func TestGenericDaoDynamodb_Empty(t *testing.T) {
	name := "TestGenericDaoDynamodb_Empty"
	dao := initDao()

	boList, err := dao.GdaoFetchMany(dao.tableName, nil, nil, 0, 0)
	if err != nil {
		t.Fatalf("%s failed, has error: %e", name, err)
	}
	if boList == nil {
		t.Fatalf("%s failed, nil result", name)
	}
	if len(boList) != 0 {
		t.Fatalf("%s failed, non-empty result: %v", name, boList)
	}

	bo, err := dao.GdaoFetchOne(dao.tableName, map[string]interface{}{fieldId: "any"})
	if err != nil {
		t.Fatalf("%s failed, has error: %e", name, err)
	}
	if bo != nil {
		t.Fatalf("%s failed, should have nill result, but received: %v", name, bo)
	}
}

func TestGenericDaoDynamodb_GdaoCreateDuplicated(t *testing.T) {
	name := "TestGenericDaoDynamodb_GdaoCreateDuplicated"
	dao := initDao()
	bo1 := &MyBo{
		Id:       "1",
		Username: "1",
		Name:     "BO - 1",
		Version:  1,
	}
	if numRows, err := dao.GdaoCreate(dao.tableName, bo1.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
	bo2 := &MyBo{
		Id:       "1",
		Username: "2",
		Name:     "BO - 2",
		Version:  2,
	}
	if numRows, err := dao.GdaoCreate(dao.tableName, bo2.ToGbo()); err != godal.GdaoErrorDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
}

func TestGenericDaoDynamodb_GdaoCreateGet(t *testing.T) {
	name := "TestGenericDaoDynamodb_GdaoCreateGet"
	dao := initDao()
	bo := &MyBo{
		Id:       "1",
		Username: "2",
		Name:     "BO - 3",
		Version:  4,
	}
	if numRows, err := dao.GdaoCreate(dao.tableName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	gbo, err := dao.GdaoFetchOne(dao.tableName, map[string]interface{}{fieldId: "1"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != bo.Id || myBo.Username != bo.Username || myBo.Name != bo.Name || myBo.Version != bo.Version {
		t.Fatalf("%s failed - Expected: %v / Received: %v", name, bo, myBo)
	}
}

func TestGenericDaoDynamodb_GdaoCreateTwiceGet(t *testing.T) {
	name := "TestGenericDaoDynamodb_GdaoCreateTwiceGet"
	dao := initDao()
	bo := &MyBo{
		Id:       "1",
		Username: "2",
		Name:     "BO - 3",
		Version:  4,
	}
	if numRows, err := dao.GdaoCreate(dao.tableName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
	bo.Version = bo.Version + 1
	if numRows, err := dao.GdaoCreate(dao.tableName, bo.ToGbo()); err != godal.GdaoErrorDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	bo.Version = bo.Version - 1
	gbo, err := dao.GdaoFetchOne(dao.tableName, map[string]interface{}{fieldId: "1"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != bo.Id || myBo.Username != bo.Username || myBo.Name != bo.Name || myBo.Version != bo.Version {
		t.Fatalf("%s failed - Expected: %v / Received: %v", name, bo, myBo)
	}
}

func TestGenericDaoDynamodb_GdaoCreateMultiThreadsGet(t *testing.T) {
	name := "TestGenericDaoDynamodb_GdaoCreateMultiThreadsGet"
	dao := initDao()
	numThreads := 8
	numLoopsPerThread := 10
	var wg sync.WaitGroup
	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go func(threadNum int, bo *MyBo) {
			for j := 0; j < numLoopsPerThread; j++ {
				if _, err := dao.GdaoCreate(dao.tableName, bo.ToGbo()); err != nil && err != godal.GdaoErrorDuplicatedEntry {
					t.Fatalf("%s failed - Thread: %v / Error: %e", name, threadNum, err)
				}
				bo.Version = bo.Version + 1
			}
			wg.Done()
		}(i, &MyBo{
			Id:       "1",
			Username: "2",
			Name:     "BO - " + strconv.Itoa(i),
			Version:  3,
		})
	}
	wg.Wait()

	gbo, err := dao.GdaoFetchOne(dao.tableName, map[string]interface{}{fieldId: "1"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != "1" || myBo.Username != "2" || myBo.Version != 3 {
		t.Fatalf("%s failed - Received: %v", name, myBo)
	}
}

func TestGenericDaoDynamodb_GdaoCreateDelete(t *testing.T) {
	name := "TestGenericDaoDynamodb_GdaoCreateDelete"
	dao := initDao()
	bo := &MyBo{
		Id:       "1",
		Username: "2",
		Name:     "BO - 3",
		Version:  4,
	}
	if numRows, err := dao.GdaoCreate(dao.tableName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	gbo, err := dao.GdaoFetchOne(dao.tableName, map[string]interface{}{fieldId: "1"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != bo.Id || myBo.Username != bo.Username || myBo.Name != bo.Name || myBo.Version != bo.Version {
		t.Fatalf("%s failed - Expected: %v / Received: %v", name, bo, myBo)
	}

	if numRows, err := dao.GdaoDelete(dao.tableName, gbo); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	gbo, err = dao.GdaoFetchOne(dao.tableName, map[string]interface{}{fieldId: "1"})
	if err != nil {
		t.Fatalf("%s failed, has error: %e", name, err)
	}
	if gbo != nil {
		t.Fatalf("%s failed, should have nill result, but received: %v", name, gbo)
	}
}

func TestGenericDaoDynamodb_GdaoCreateDeleteAll(t *testing.T) {
	name := "TestGenericDaoDynamodb_GdaoCreateDeleteAll"
	dao := initDao()
	bo := &MyBo{
		Id:       "1",
		Username: "11",
		Name:     "BO - 1",
		Version:  111,
	}
	if numRows, err := dao.GdaoCreate(dao.tableName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
	bo = &MyBo{
		Id:       "2",
		Username: "22",
		Name:     "BO - 2",
		Version:  222,
	}
	if numRows, err := dao.GdaoCreate(dao.tableName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	if numRows, err := dao.GdaoDeleteMany(dao.tableName, nil); err != nil || numRows != 2 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	gbo, err := dao.GdaoFetchOne(dao.tableName, map[string]interface{}{fieldId: "1"})
	if err != nil {
		t.Fatalf("%s failed, has error: %e", name, err)
	}
	if gbo != nil {
		t.Fatalf("%s failed, should have nill result, but received: %v", name, gbo)
	}
}

func TestGenericDaoDynamodb_GdaoCreateDeleteMany(t *testing.T) {
	name := "TestGenericDaoDynamodb_GdaoCreateDeleteMany"
	dao := initDao()
	totalRows := 10
	for i := 0; i < totalRows; i++ {
		bo := &MyBo{
			Id:       strconv.Itoa(i),
			Username: strconv.Itoa(i + 1),
			Name:     "BO - " + strconv.Itoa(i+2),
			Version:  i + 3,
		}
		if numRows, err := dao.GdaoCreate(dao.tableName, bo.ToGbo()); err != nil || numRows != 1 {
			t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
		}
	}

	filter := expression.Name("version").GreaterThanEqual(expression.Value(4)).And(expression.Name("version").LessThanEqual(expression.Value(11)))
	if numRows, err := dao.GdaoDeleteMany(dao.tableName, filter); err != nil || numRows != totalRows-2 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	for i := 0; i < totalRows; i++ {
		gbo, err := dao.GdaoFetchOne(dao.tableName, map[string]interface{}{fieldId: strconv.Itoa(i)})
		if i == 0 || i == totalRows-1 {
			if err != nil || gbo == nil {
				t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
			}
		} else {
			if err != nil {
				t.Fatalf("%s failed, has error: %e", name, err)
			}
			if gbo != nil {
				t.Fatalf("%s failed, should have nill result, but received: %v", name, gbo)
			}
		}
	}
}

func TestGenericDaoDynamodb_GdaoFetchManyWithPaging(t *testing.T) {
	name := "TestGenericDaoDynamodb_GdaoFetchManyWithPaging"
	dao := initDao()
	numItems := 100
	for i := 0; i < numItems; i++ {
		bo := &MyBo{
			Id:       strconv.Itoa(numItems - i - 1),
			Username: strconv.Itoa(numItems - i - 1),
			Name:     "BO - " + strconv.Itoa(numItems-i-1),
			Version:  numItems - i - 1,
		}
		if numRows, err := dao.GdaoCreate(dao.tableName, bo.ToGbo()); err != nil || numRows != 1 {
			t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
		}
	}

	time.Sleep(5 * time.Second) // sleep a few seconds due to eventually consistent

	filter := expression.Name(fieldActived).Equal(expression.Value(1)).And(expression.Name(fieldVersion).GreaterThanEqual(expression.Value(80)))
	gboList, err := dao.GdaoFetchMany(dao.tableName+":"+indexName, filter, nil, 5, 20)
	if err != nil || gboList == nil || len(gboList) != 15 {
		t.Fatalf("%s failed - NumItems: %v / Error: %e", name, len(gboList), err)
	}
	for i, gbo := range gboList {
		if bo := fromGbo(gbo); bo.Id != strconv.Itoa(80+i+5) {
			t.Fatalf("%s failed - Expected: %v / Received: %v", name, 80+i+5, bo)
		}
	}
}

func TestGenericDaoDynamodb_GdaoUpdateNotExist(t *testing.T) {
	name := "TestGenericDaoDynamodb_GdaoUpdateNotExist"
	dao := initDao()
	bo := &MyBo{
		Id:       "1",
		Username: "1",
		Name:     name,
		Version:  1,
	}
	if numRows, err := dao.GdaoUpdate(dao.tableName, bo.ToGbo()); err != nil || numRows != 0 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
}

func TestGenericDaoDynamodb_GdaoUpdate(t *testing.T) {
	name := "TestGenericDaoDynamodb_GdaoUpdate"
	dao := initDao()
	for i := 0; i < 3; i++ {
		bo := &MyBo{
			Id:       strconv.Itoa(i),
			Username: strconv.Itoa(i),
			Name:     "BO - " + strconv.Itoa(i) + " " + name,
			Version:  i,
		}
		if numRows, err := dao.GdaoCreate(dao.tableName, bo.ToGbo()); err != nil || numRows != 1 {
			t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
		}
	}

	bo := &MyBo{
		Id:       "0",
		Username: strconv.Itoa(100) + " " + name,
		Name:     "BO",
		Version:  100,
	}
	if numRows, err := dao.GdaoUpdate(dao.tableName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}

	for i := 0; i < 3; i++ {
		gbo, err := dao.GdaoFetchOne(dao.tableName, map[string]interface{}{fieldId: strconv.Itoa(i)})
		if err != nil || gbo == nil {
			t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
		}
		if myBo := fromGbo(gbo); myBo == nil {
			t.Fatalf("%s failed - not found: %v", name, i)
		} else if i == 0 && (myBo.Id != bo.Id || myBo.Name != bo.Name || myBo.Version != bo.Version) {
			t.Fatalf("%s failed - Expected: %v / Received: %v", name, bo, myBo)
		} else if i != 0 && myBo.Version != i {
			t.Fatalf("%s failed - Expected: %v / Received: %v", name, i, myBo.Version)
		}
	}
}

func TestGenericDaoDynamodb_GdaoSave(t *testing.T) {
	name := "TestGenericDaoDynamodb_GdaoSave"
	dao := initDao()

	bo := &MyBo{
		Id:       "1",
		Username: "1",
		Name:     "BO - " + name,
		Version:  1,
	}
	if numRows, err := dao.GdaoSave(dao.tableName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
	gbo, err := dao.GdaoFetchOne(dao.tableName, map[string]interface{}{fieldId: "1"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != bo.Id || myBo.Username != bo.Username || myBo.Name != bo.Name || myBo.Version != bo.Version {
		t.Fatalf("%s failed - Expected: %v / Received: %v", name, bo, myBo)
	}

	bo.Name = "BO (updated) - " + name
	bo.Version = 10
	if numRows, err := dao.GdaoSave(dao.tableName, bo.ToGbo()); err != nil || numRows != 1 {
		t.Fatalf("%s failed - NumRows: %v / Error: %e", name, numRows, err)
	}
	gbo, err = dao.GdaoFetchOne(dao.tableName, map[string]interface{}{fieldId: "1"})
	if err != nil || gbo == nil {
		t.Fatalf("%s failed - Gbo: %v / Error: %e", name, gbo, err)
	}
	if myBo := fromGbo(gbo); myBo == nil || myBo.Id != bo.Id || myBo.Username != bo.Username || myBo.Name != bo.Name || myBo.Version != bo.Version {
		t.Fatalf("%s failed - Expected: %v / Received: %v", name, bo, myBo)
	}
}
