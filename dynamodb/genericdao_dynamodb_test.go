package dynamodb

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/prom"

	"github.com/btnguyen2k/godal"
)

func createAwsDynamodbConnect(region string) (*prom.AwsDynamodbConnect, error) {
	cfg := &aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewEnvCredentials(),
	}
	return prom.NewAwsDynamodbConnect(cfg, nil, nil, 10000)
}

func inSlide(item string, slide []string) bool {
	for _, s := range slide {
		if item == s {
			return true
		}
	}
	return false
}

func waitForTable(adc *prom.AwsDynamodbConnect, table string, statusList []string, delay int) {
	for status, err := adc.GetTableStatus(nil, table); !inSlide(status, statusList) && err == nil; {
		fmt.Printf("    Table [%s] status: %v - %e\n", table, status, err)
		if delay > 0 {
			time.Sleep(time.Duration(delay) * time.Second)
		}
		status, err = adc.GetTableStatus(nil, table)
	}
}

func waitForGsi(adc *prom.AwsDynamodbConnect, table, index string, statusList []string, delay int) {
	for status, err := adc.GetGlobalSecondaryIndexStatus(nil, table, index); !inSlide(status, statusList) && err == nil; {
		fmt.Printf("    GSI [%s] on table [%s] status: %v - %e\n", index, table, status, err)
		if delay > 0 {
			time.Sleep(time.Duration(delay) * time.Second)
		}
		status, err = adc.GetGlobalSecondaryIndexStatus(nil, table, index)
	}
}

func prepareAwsDynamodbTable(adc *prom.AwsDynamodbConnect, table string) error {
	err := adc.DeleteTable(nil, table)
	if prom.AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeResourceInUseException) != nil {
		return err
	}
	fmt.Printf("  Deleted table [%s]\n", table)
	waitForTable(adc, table, []string{""}, 1)

	err = adc.CreateTable(nil, table, 2, 2,
		[]prom.AwsDynamodbNameAndType{{fieldId, prom.AwsAttrTypeString}},
		[]prom.AwsDynamodbNameAndType{{fieldId, prom.AwsKeyTypePartition}})
	if prom.AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeResourceInUseException) != nil {
		return err
	}
	waitForTable(adc, table, []string{"ACTIVE"}, 1)
	return nil
}

// func initDataDynamodb(adc *prom.AwsDynamodbConnect, tableName, gsiName string) {
// 	var schema, key []prom.AwsDynamodbNameAndType
//
// 	if ok, err := adc.HasTable(nil, tableName); err != nil {
// 		panic(err)
// 	} else if !ok {
// 		schema = []prom.AwsDynamodbNameAndType{
// 			{fieldId, prom.AwsAttrTypeString},
// 		}
// 		key = []prom.AwsDynamodbNameAndType{
// 			{fieldId, prom.AwsKeyTypePartition},
// 		}
// 		if err := adc.CreateTable(nil, tableName, 2, 2, schema, key); err != nil {
// 			panic(err)
// 		}
// 		time.Sleep(1 * time.Second)
// 		for status, err := adc.GetTableStatus(nil, tableName); status != "ACTIVE" && err == nil; {
// 			fmt.Printf("    Table [%s] status: %v - %e\n", tableName, status, err)
// 			time.Sleep(1 * time.Second)
// 			status, err = adc.GetTableStatus(nil, tableName)
// 		}
// 	}
//
// 	if status, err := adc.GetGlobalSecondaryIndexStatus(nil, tableName, gsiName); err != nil {
// 		panic(err)
// 	} else if status == "" {
// 		schema = []prom.AwsDynamodbNameAndType{
// 			{fieldActived, prom.AwsAttrTypeNumber},
// 			{fieldVersion, prom.AwsAttrTypeNumber},
// 		}
// 		key = []prom.AwsDynamodbNameAndType{
// 			{fieldActived, prom.AwsKeyTypePartition},
// 			{fieldVersion, prom.AwsKeyTypeSort},
// 		}
// 		if err := adc.CreateGlobalSecondaryIndex(nil, tableName, gsiName, 1, 1, schema, key); err != nil {
// 			panic(err)
// 		}
// 		time.Sleep(5 * time.Second)
// 		for status, err := adc.GetGlobalSecondaryIndexStatus(nil, tableName, gsiName); status != "ACTIVE" && err == nil; {
// 			fmt.Printf("    GSI [%s] on table [%s] status: %v - %e\n", gsiName, tableName, status, err)
// 			time.Sleep(5 * time.Second)
// 			status, err = adc.GetGlobalSecondaryIndexStatus(nil, tableName, gsiName)
// 		}
// 	}
//
// 	// delete all items
// 	pkAttrs := []string{fieldId}
// 	adc.ScanItemsWithCallback(nil, tableName, nil, prom.AwsDynamodbNoIndex, nil, func(item prom.AwsDynamodbItem, lastEvaluatedKey map[string]*dynamodb.AttributeValue) (b bool, e error) {
// 		keyFilter := make(map[string]interface{})
// 		for _, v := range pkAttrs {
// 			keyFilter[v] = item[v]
// 		}
// 		_, err := adc.DeleteItem(nil, tableName, keyFilter, nil)
// 		if err != nil {
// 			fmt.Printf("    Delete item from table [%s] with key %s: %e\n", tableName, keyFilter, err)
// 		}
// 		// fmt.Printf("    Delete item from table [%s] with key %s: %e\n", table, keyFilter, err)
// 		return true, nil
// 	})
// }

func createDaoDynamodb(adc *prom.AwsDynamodbConnect, tableName string) *MyUserDaoDynamodb {
	dao := &MyUserDaoDynamodb{tableName: tableName}
	dao.GenericDaoDynamodb = NewGenericDaoDynamodb(adc, godal.NewAbstractGenericDao(dao))
	dao.SetRowMapper(&GenericRowMapperDynamodb{ColumnsListMap: map[string][]string{tableName: {fieldId}}})
	return dao
}

type UserBoDynamodb struct {
	Id       string    `json:"id"`
	Username string    `json:"username"`
	Name     string    `json:"name"`
	Version  int       `json:"version"`
	Active   bool      `json:"active"`
	Created  time.Time `json:"created"`
}

const (
	fieldId       = "id"
	fieldUsername = "username"
	fieldVersion  = "version"
	fieldActived  = "actived"
)

type MyUserDaoDynamodb struct {
	*GenericDaoDynamodb
	tableName string
}

// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter.
func (dao *MyUserDaoDynamodb) GdaoCreateFilter(tableName string, bo godal.IGenericBo) interface{} {
	if tableName == dao.tableName {
		return map[string]interface{}{fieldId: bo.GboGetAttrUnsafe(fieldId, reddo.TypeString)}
	}
	return nil
}

func (dao *MyUserDaoDynamodb) toGbo(u *UserBoDynamodb) godal.IGenericBo {
	gbo := godal.NewGenericBo()
	if err := gbo.GboImportViaJson(u); err != nil {
		return nil
	}
	return gbo
}

func (dao *MyUserDaoDynamodb) toUser(gbo godal.IGenericBo) *UserBoDynamodb {
	bo := UserBoDynamodb{}
	if err := gbo.GboTransferViaJson(&bo); err != nil {
		return nil
	}
	return &bo
}

/*----------------------------------------------------------------------*/
func initDao(tableName string) *MyUserDaoDynamodb {
	adc, _ := createAwsDynamodbConnect("ap-southeast-1")
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

const (
	dynamodbAwsAccessKeyId     = "AWS_ACCESS_KEY_ID"
	dynamodbAwsSecretAccessKey = "AWS_SECRET_ACCESS_KEY"
	dynamodbTestTableName      = "DYNAMODB_TEST_TABLE_NAME"
	dynamodbTestGsiName        = "DYNAMODB_TEST_GSI_NAME"
)

var (
	testDynamodbTableName = "test_user"
	testDynamodbGsiName   = "gsi_test_user_email"
)

func TestNewGenericDaoDynamodb(t *testing.T) {
	name := "TestNewGenericDaoDynamodb"

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	dao := initDao(dynamodbTestTableName)
	if dao == nil {
		t.Fatalf("%s failed: nil", name)
	}
}

func TestGenericDaoDynamodb_SetGetAwsDynamodbConnect(t *testing.T) {
	name := "TestGenericDaoDynamodb_GetAwsDynamodbConnect"

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	dao := initDao(dynamodbTestTableName)
	adc, _ := createAwsDynamodbConnect("ap-southeast-1")
	dao.SetAwsDynamodbConnect(adc)
	if dao.GetAwsDynamodbConnect() != adc {
		t.Fatalf("%s failed", name)
	}
}

func TestToConditionBuilder(t *testing.T) {
	name := "TestToConditionBuilder"

	exp := expression.Name("id").Equal(expression.Value("1"))
	if cond, err := toConditionBuilder(exp); err != nil || cond == nil {
		t.Fatalf("%s failed: %#v / %e", name, cond, err)
	}
	if cond, err := toConditionBuilder(&exp); err != nil || cond == nil {
		t.Fatalf("%s failed: %#v / %e", name, cond, err)
	}

	filterString := `{"id":"1", "username":"btnguyen2k"}`
	if cond, err := toConditionBuilder(filterString); err != nil || cond == nil {
		t.Fatalf("%s failed: %#v / %e", name, cond, err)
	}
	if cond, err := toConditionBuilder(&filterString); err != nil || cond == nil {
		t.Fatalf("%s failed: %#v / %e", name, cond, err)
	}

	filterBytes := []byte(`{"id":"1", "username":"btnguyen2k"}`)
	if cond, err := toConditionBuilder(filterBytes); err != nil || cond == nil {
		t.Fatalf("%s failed: %#v / %e", name, cond, err)
	}
	if cond, err := toConditionBuilder(&filterBytes); err != nil || cond == nil {
		t.Fatalf("%s failed: %#v / %e", name, cond, err)
	}
}

func TestToMap(t *testing.T) {
	name := "TestToMap"

	input := make(map[string]interface{})
	if m, err := toMap(input); err != nil || m == nil {
		t.Fatalf("%s failed: %#v / %e", name, m, err)
	}
	if m, err := toMap(&input); err != nil || m == nil {
		t.Fatalf("%s failed: %#v / %e", name, m, err)
	}

	inputString := `{"id":"1", "username":"btnguyen2k"}`
	if m, err := toMap(inputString); err != nil || m == nil {
		t.Fatalf("%s failed: %#v / %e", name, m, err)
	}
	if m, err := toMap(&inputString); err != nil || m == nil {
		t.Fatalf("%s failed: %#v / %e", name, m, err)
	}

	inputBytes := []byte(`{"id":"1", "username":"btnguyen2k"}`)
	if m, err := toMap(inputBytes); err != nil || m == nil {
		t.Fatalf("%s failed: %#v / %e", name, m, err)
	}
	if m, err := toMap(&inputBytes); err != nil || m == nil {
		t.Fatalf("%s failed: %#v / %e", name, m, err)
	}
}

func TestGenericDaoDynamodb_GdaoDelete(t *testing.T) {
	if os.Getenv(dynamodbAwsAccessKeyId) == "" || os.Getenv(dynamodbAwsSecretAccessKey) == "" {
		return
	}
	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	name := "TestGenericDaoDynamodb_GdaoDelete"

	dao := initDao(testDynamodbTableName)
	err := prepareAwsDynamodbTable(dao.GetAwsDynamodbConnect(), testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareAwsDynamodbTable", err)
	}

	user := &UserBoDynamodb{
		Id:       "1",
		Username: "btnguyen2k",
		Name:     "Thanh Nguyen",
		Version:  int(time.Now().Unix()),
		Active:   false,
		Created:  time.Now(),
	}
	_, err = dao.GdaoCreate(dao.tableName, dao.toGbo(user))
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoCreate", err)
	}

	filterUser := &UserBoDynamodb{Id: "2"}
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

func TestGenericDaoDynamodb_GdaoDeleteMany(t *testing.T) {
	if os.Getenv(dynamodbAwsAccessKeyId) == "" || os.Getenv(dynamodbAwsSecretAccessKey) == "" {
		return
	}
	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	name := "TestGenericDaoDynamodb_GdaoDeleteMany"

	dao := initDao(testDynamodbTableName)
	err := prepareAwsDynamodbTable(dao.GetAwsDynamodbConnect(), testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareAwsDynamodbTable", err)
	}

	filter := expression.Name(fieldId).GreaterThanEqual(expression.Value("5"))
	if numRows, err := dao.GdaoDeleteMany(dao.tableName, filter); err != nil {
		t.Fatalf("%s failed: %e", name, err)
	} else if numRows != 0 {
		t.Fatalf("%s failed: expected %#v row(s) deleted but received %#v", name, 0, numRows)
	}

	for i := 0; i < 10; i++ {
		id := strconv.Itoa(i)
		user := &UserBoDynamodb{
			Id:       id,
			Username: "user" + id,
			Name:     "Thanh " + id,
			Version:  int(time.Now().UnixNano()),
			Active:   i%3 == 0,
			Created:  time.Now(),
		}
		_, err = dao.GdaoCreate(dao.tableName, dao.toGbo(user))
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

func TestGenericDaoDynamodb_GdaoFetchOne(t *testing.T) {
	if os.Getenv(dynamodbAwsAccessKeyId) == "" || os.Getenv(dynamodbAwsSecretAccessKey) == "" {
		return
	}
	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	name := "TestGenericDaoDynamodb_GdaoFetchOne"

	dao := initDao(testDynamodbTableName)
	err := prepareAwsDynamodbTable(dao.GetAwsDynamodbConnect(), testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareAwsDynamodbTable", err)
	}

	filter := dao.GdaoCreateFilter(dao.tableName, dao.toGbo(&UserBoDynamodb{Id: "1"}))
	if gbo, err := dao.GdaoFetchOne(dao.tableName, filter); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoFetchOne", err)
	} else if gbo != nil {
		t.Fatalf("%s failed: non-nil", name+"/GdaoFetchOne")
	}

	user := &UserBoDynamodb{
		Id:       "1",
		Username: "btnguyen2k",
		Name:     "Thanh Nguyen",
		Version:  int(time.Now().Unix()),
		Active:   false,
		Created:  time.Now(),
	}
	_, err = dao.GdaoCreate(dao.tableName, dao.toGbo(user))
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

func TestGenericDaoDynamodb_GdaoFetchMany(t *testing.T) {
	if os.Getenv(dynamodbAwsAccessKeyId) == "" || os.Getenv(dynamodbAwsSecretAccessKey) == "" {
		return
	}
	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	name := "TestGenericDaoDynamodb_GdaoFetchMany"

	dao := initDao(testDynamodbTableName)
	err := prepareAwsDynamodbTable(dao.GetAwsDynamodbConnect(), testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareAwsDynamodbTable", err)
	}

	filter := expression.Name(fieldId).GreaterThanEqual(expression.Value("5"))
	if dbRows, err := dao.GdaoFetchMany(dao.tableName, filter, nil, 0, 0); err != nil {
		t.Fatalf("%s failed: %e", name, err)
	} else if dbRows == nil || len(dbRows) != 0 {
		t.Fatalf("%s failed: expected %#v row(s) but received %#v", name, 0, dbRows)
	}

	for i := 0; i < 10; i++ {
		id := strconv.Itoa(i)
		user := &UserBoDynamodb{
			Id:       id,
			Username: "user" + id,
			Name:     "Thanh " + id,
			Version:  int(time.Now().UnixNano()),
			Active:   i%3 == 0,
			Created:  time.Now(),
		}
		_, err = dao.GdaoCreate(dao.tableName, dao.toGbo(user))
		if err != nil {
			t.Fatalf("%s failed: %e", name+"/GdaoCreate", err)
		}
	}

	if dbRows, err := dao.GdaoFetchMany(dao.tableName, filter, nil, 0, 3); err != nil {
		t.Fatalf("%s failed: %e", name, err)
	} else if dbRows == nil || len(dbRows) != 3 {
		t.Fatalf("%s failed: expected %#v row(s) but received %#v", name, 3, dbRows)
	}
}

func TestGenericDaoDynamodb_GdaoCreate(t *testing.T) {
	if os.Getenv(dynamodbAwsAccessKeyId) == "" || os.Getenv(dynamodbAwsSecretAccessKey) == "" {
		return
	}
	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	name := "TestGenericDaoDynamodb_GdaoCreate"

	dao := initDao(testDynamodbTableName)
	err := prepareAwsDynamodbTable(dao.GetAwsDynamodbConnect(), testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareAwsDynamodbTable", err)
	}

	user := &UserBoDynamodb{
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
	user.Username = "thanhn"
	if numRows, err := dao.GdaoCreate(dao.tableName, dao.toGbo(user)); err != godal.GdaoErrorDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed: num rows %#v / error: %e", name, numRows, err)
	}
	filter := dao.GdaoCreateFilter(dao.tableName, dao.toGbo(&UserBoDynamodb{Id: "1"}))
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

func TestGenericDaoDynamodb_GdaoUpdate(t *testing.T) {
	if os.Getenv(dynamodbAwsAccessKeyId) == "" || os.Getenv(dynamodbAwsSecretAccessKey) == "" {
		return
	}
	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	name := "TestGenericDaoDynamodb_GdaoUpdate"

	dao := initDao(testDynamodbTableName)
	err := prepareAwsDynamodbTable(dao.GetAwsDynamodbConnect(), testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareAwsDynamodbTable", err)
	}

	user := &UserBoDynamodb{
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

	filter := dao.GdaoCreateFilter(dao.tableName, dao.toGbo(&UserBoDynamodb{Id: "1"}))
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

func TestGenericDaoDynamodb_GdaoSave(t *testing.T) {
	if os.Getenv(dynamodbAwsAccessKeyId) == "" || os.Getenv(dynamodbAwsSecretAccessKey) == "" {
		return
	}
	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	name := "TestGenericDaoDynamodb_GdaoSave"

	dao := initDao(testDynamodbTableName)
	err := prepareAwsDynamodbTable(dao.GetAwsDynamodbConnect(), testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareAwsDynamodbTable", err)
	}

	user := &UserBoDynamodb{
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
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoSave", 0, numRows)
	}

	user.Username = "thanhn"
	if numRows, err := dao.GdaoSave(dao.tableName, dao.toGbo(user)); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoSave", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoSave", 1, numRows)
	}

	filter := dao.GdaoCreateFilter(dao.tableName, dao.toGbo(&UserBoDynamodb{Id: "1"}))
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
