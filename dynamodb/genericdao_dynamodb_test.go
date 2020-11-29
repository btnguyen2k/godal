package dynamodb

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
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

func _createAwsDynamodbConnect(t *testing.T, testName string) *prom.AwsDynamodbConnect {
	awsRegion := strings.ReplaceAll(os.Getenv("AWS_REGION"), `"`, "")
	awsAccessKeyId := strings.ReplaceAll(os.Getenv("AWS_ACCESS_KEY_ID"), `"`, "")
	awsSecretAccessKey := strings.ReplaceAll(os.Getenv("AWS_SECRET_ACCESS_KEY"), `"`, "")
	if awsRegion == "" || awsAccessKeyId == "" || awsSecretAccessKey == "" {
		t.Skipf("%s skipped", testName)
		return nil
	}
	cfg := &aws.Config{
		Region:      aws.String(awsRegion),
		Credentials: credentials.NewEnvCredentials(),
	}
	if awsDynamodbEndpoint := strings.ReplaceAll(os.Getenv("AWS_DYNAMODB_ENDPOINT"), `"`, ""); awsDynamodbEndpoint != "" {
		cfg.Endpoint = aws.String(awsDynamodbEndpoint)
		if strings.HasPrefix(awsDynamodbEndpoint, "http://") {
			cfg.DisableSSL = aws.Bool(true)
		}
	}
	adc, err := prom.NewAwsDynamodbConnect(cfg, nil, nil, 10000)
	if err != nil {
		t.Fatalf("%s/%s failed: %s", testName, "NewAwsDynamodbConnect", err)
	}
	return adc
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

func prepareAwsDynamodbTableCompoundKey(adc *prom.AwsDynamodbConnect, tableName string) error {
	err := adc.DeleteTable(nil, tableName)
	if prom.AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeResourceInUseException) != nil {
		return err
	}
	fmt.Printf("  Deleted table [%s]\n", tableName)
	waitForTable(adc, tableName, []string{""}, 1)

	err = adc.CreateTable(nil, tableName, 2, 2,
		[]prom.AwsDynamodbNameAndType{{fieldSubject, prom.AwsAttrTypeString}, {fieldLevel, prom.AwsAttrTypeNumber}},
		[]prom.AwsDynamodbNameAndType{{fieldSubject, prom.AwsKeyTypePartition}, {fieldLevel, prom.AwsKeyTypeSort}})
	if prom.AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeResourceInUseException) != nil {
		return err
	}
	waitForTable(adc, tableName, []string{"ACTIVE"}, 1)

	return nil
}

func prepareAwsDynamodbTable(adc *prom.AwsDynamodbConnect, tableName string) error {
	err := adc.DeleteTable(nil, tableName)
	if prom.AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeResourceInUseException) != nil {
		return err
	}
	fmt.Printf("  Deleted table [%s]\n", tableName)
	waitForTable(adc, tableName, []string{""}, 1)

	err = adc.CreateTable(nil, tableName, 2, 2,
		[]prom.AwsDynamodbNameAndType{{fieldId, prom.AwsAttrTypeString}},
		[]prom.AwsDynamodbNameAndType{{fieldId, prom.AwsKeyTypePartition}})
	if prom.AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeResourceInUseException) != nil {
		return err
	}
	waitForTable(adc, tableName, []string{"ACTIVE"}, 1)

	gsiName := "gsi_" + tableName + "_" + fieldUsername
	err = adc.CreateGlobalSecondaryIndex(nil, tableName, gsiName, 2, 2,
		[]prom.AwsDynamodbNameAndType{{fieldUsername, prom.AwsAttrTypeString}},
		[]prom.AwsDynamodbNameAndType{{fieldUsername, prom.AwsKeyTypePartition}})
	if prom.AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeResourceInUseException) != nil {
		return err
	}
	waitForGsi(adc, tableName, gsiName, []string{"ACTIVE"}, 1)

	gsiName = "gsi_" + tableName + "_" + fieldSubject + "_" + fieldLevel
	err = adc.CreateGlobalSecondaryIndex(nil, tableName, gsiName, 2, 2,
		[]prom.AwsDynamodbNameAndType{{fieldSubject, prom.AwsAttrTypeString}, {fieldLevel, prom.AwsAttrTypeNumber}},
		[]prom.AwsDynamodbNameAndType{{fieldSubject, prom.AwsKeyTypePartition}, {fieldLevel, prom.AwsKeyTypeSort}})
	if prom.AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeResourceInUseException) != nil {
		return err
	}
	waitForGsi(adc, tableName, gsiName, []string{"ACTIVE"}, 1)

	return nil
}

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
	Subject  string    `json:"subject"`
	Level    int       `json:"level"`
	Version  int       `json:"version"`
	Active   bool      `json:"active"`
	Created  time.Time `json:"created"`
}

const (
	fieldId       = "id"
	fieldUsername = "username"
	fieldSubject  = "subject"
	fieldLevel    = "level"
)

type MyUserDaoDynamodb struct {
	*GenericDaoDynamodb
	tableName string
}

// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter.
func (dao *MyUserDaoDynamodb) GdaoCreateFilter(tableName string, bo godal.IGenericBo) interface{} {
	if tableName == dao.tableName {
		colList := dao.GetRowMapper().ColumnsList(tableName)
		result := make(map[string]interface{})
		for _, col := range colList {
			result[col] = bo.GboGetAttrUnsafe(col, nil)
		}
		return result
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
func _initDao(t *testing.T, testName, tableName string) *MyUserDaoDynamodb {
	adc := _createAwsDynamodbConnect(t, testName)
	return createDaoDynamodb(adc, tableName)
}

func TestGenericRowMapperDynamodb_ColumnsList(t *testing.T) {
	name := "TestGenericRowMapperDynamodb_ColumnsList"
	table := "table"
	colA, colB, colC := "cola", "ColB", "colC"
	cols := []string{colA, colB, colC}
	rowMapper := &GenericRowMapperDynamodb{ColumnsListMap: map[string][]string{table: cols}}

	colList := rowMapper.ColumnsList(table)
	if len(colList) != 3 || colList[0] != colA || colList[1] != colB || colList[2] != colC {
		t.Fatalf("%s failed, expect table [%s] has columns %#v but received %#v", name, table, []string{colA, colB, colC}, cols)
	}

	if rowMapper.ColumnsList("not_exists") != nil {
		t.Fatalf("%s failed", table)
	}
}

func testToBo(t *testing.T, name string, rowMapper godal.IRowMapper, table string, row interface{}) {
	colA, colB, colC, col1, col2 := "cola", "ColB", "colC", "Col1", "coL2"
	valA, valB, val1, val2 := "a", "B", int64(1), int64(2)

	bo, err := rowMapper.ToBo(table, row)
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
	rowMapper := &GenericRowMapperDynamodb{ColumnsListMap: map[string][]string{table: cols}}

	{
		row := map[string]interface{}{colA: valA, colB: valB, col1: val1, col2: val2}
		testToBo(t, name, rowMapper, table, row)
		testToBo(t, name, rowMapper, table, &row)
		testToBo(t, name, rowMapper, table+"-not-exists", row)
		row2 := &row
		testToBo(t, name, rowMapper, table, &row2)
	}

	{
		row := fmt.Sprintf(`{"%s": "%v", "%s": "%v", "%s": %v, "%s": %v}`, colA, valA, colB, valB, col1, val1, col2, val2)
		testToBo(t, name, rowMapper, table, row)
		testToBo(t, name, rowMapper, table, &row)
		testToBo(t, name, rowMapper, table+"-not-exists", row)
		row2 := &row
		testToBo(t, name, rowMapper, table, &row2)
	}

	{
		row := []byte(fmt.Sprintf(`{"%s": "%v", "%s": "%v", "%s": %v, "%s": %v}`, colA, valA, colB, valB, col1, val1, col2, val2))
		testToBo(t, name, rowMapper, table, row)
		testToBo(t, name, rowMapper, table, &row)
		testToBo(t, name, rowMapper, table+"-not-exists", row)
		row2 := &row
		testToBo(t, name, rowMapper, table, &row2)
	}

	{
		var row interface{} = nil
		if bo, err := rowMapper.ToBo(table, row); err != nil || bo != nil {
			t.Fatalf("%s failed: %e / %v", name, err, bo)
		}
		if bo, err := rowMapper.ToBo(table, &row); err != nil || bo != nil {
			t.Fatalf("%s failed: %e / %v", name, err, bo)
		}
		row2 := &row
		if bo, err := rowMapper.ToBo(table, &row2); err != nil || bo != nil {
			t.Fatalf("%s failed: %e / %v", name, err, bo)
		}
	}

	{
		var row *string = nil
		if bo, err := rowMapper.ToBo(table, row); err != nil || bo != nil {
			t.Fatalf("%s failed: %e / %v", name, err, bo)
		}
		if bo, err := rowMapper.ToBo(table, &row); err != nil || bo != nil {
			t.Fatalf("%s failed: %e / %v", name, err, bo)
		}
		row2 := &row
		if bo, err := rowMapper.ToBo(table, &row2); err != nil || bo != nil {
			t.Fatalf("%s failed: %e / %v", name, err, bo)
		}
	}

	{
		var row []byte = nil
		if bo, err := rowMapper.ToBo(table, row); err != nil || bo != nil {
			t.Fatalf("%s failed: %e / %v", name, err, bo)
		}
		if bo, err := rowMapper.ToBo(table, &row); err != nil || bo != nil {
			t.Fatalf("%s failed: %e / %v", name, err, bo)
		}
		row2 := &row
		if bo, err := rowMapper.ToBo(table, &row2); err != nil || bo != nil {
			t.Fatalf("%s failed: %e / %v", name, err, bo)
		}
	}

	{
		var row *[]byte = nil
		if bo, err := rowMapper.ToBo(table, row); err != nil || bo != nil {
			t.Fatalf("%s failed: %e / %v", name, err, bo)
		}
		if bo, err := rowMapper.ToBo(table, &row); err != nil || bo != nil {
			t.Fatalf("%s failed: %e / %v", name, err, bo)
		}
		row2 := &row
		if bo, err := rowMapper.ToBo(table, &row2); err != nil || bo != nil {
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
	rowMapper := &GenericRowMapperDynamodb{ColumnsListMap: map[string][]string{table: cols}}

	{
		bo := godal.NewGenericBo()
		bo.GboSetAttr(colA, valA)
		bo.GboSetAttr(colB, valB)
		bo.GboSetAttr(col1, val1)
		bo.GboSetAttr(col2, val2)

		row, err := rowMapper.ToRow(table, bo)
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
	envAwsDynamodbTestTableName = "DYNAMODB_TEST_TABLE_NAME"
	envAwsDynamodbTestGsiName   = "DYNAMODB_TEST_GSI_NAME"
)

var (
	testDynamodbTableName = "test_godal"
)

func TestNewGenericDaoDynamodb(t *testing.T) {
	name := "TestNewGenericDaoDynamodb"

	if os.Getenv(envAwsDynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(envAwsDynamodbTestTableName)
	}
	dao := _initDao(t, name, testDynamodbTableName)
	if dao == nil {
		t.Fatalf("%s failed: nil", name)
	}
}

func TestGenericDaoDynamodb_SetGetAwsDynamodbConnect(t *testing.T) {
	name := "TestGenericDaoDynamodb_GetAwsDynamodbConnect"

	if os.Getenv(envAwsDynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(envAwsDynamodbTestTableName)
	}
	dao := _initDao(t, name, testDynamodbTableName)
	adc := _createAwsDynamodbConnect(t, name)
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

	inputMap := map[string]string{"key1": "value1", "key2": "value2", "key3": "value3"}
	if m, err := toMap(inputMap); err != nil || m == nil {
		t.Fatalf("%s failed: %#v / %e", name, m, err)
	}
	if m, err := toMap(&inputMap); err != nil || m == nil {
		t.Fatalf("%s failed: %#v / %e", name, m, err)
	}
}

func TestGenericDaoDynamodb_GdaoDelete(t *testing.T) {
	name := "TestGenericDaoDynamodb_GdaoDelete"

	if os.Getenv(envAwsDynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(envAwsDynamodbTestTableName)
	}
	dao := _initDao(t, name, testDynamodbTableName)
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
		Subject:  "English",
		Level:    1,
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

func TestGenericDaoDynamodb_GdaoDeleteMany_Scan(t *testing.T) {
	name := "TestGenericDaoDynamodb_GdaoDeleteMany_Scan"

	if os.Getenv(envAwsDynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(envAwsDynamodbTestTableName)
	}
	dao := _initDao(t, name, testDynamodbTableName)
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
			Subject:  "Subject" + strconv.Itoa(i%4),
			Level:    i,
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

func TestGenericDaoDynamodb_GdaoDeleteMany_Query(t *testing.T) {
	name := "TestGenericDaoDynamodb_GdaoDeleteMany_Query"

	if os.Getenv(envAwsDynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(envAwsDynamodbTestTableName)
	}
	dao := _initDao(t, name, testDynamodbTableName)
	dao.SetRowMapper(&GenericRowMapperDynamodb{ColumnsListMap: map[string][]string{testDynamodbTableName: {fieldSubject, fieldLevel}}})
	err := prepareAwsDynamodbTableCompoundKey(dao.GetAwsDynamodbConnect(), testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareAwsDynamodbTable", err)
	}

	filter := expression.Name(fieldSubject).Equal(expression.Value("Subject1")).
		And(expression.Name(fieldLevel).GreaterThanEqual(expression.Value(5)))
	if numRows, err := dao.GdaoDeleteMany("@"+dao.tableName, filter); err != nil {
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
			Subject:  "Subject" + strconv.Itoa(i%4),
			Level:    i,
		}
		_, err = dao.GdaoCreate(dao.tableName, dao.toGbo(user))
		if err != nil {
			t.Fatalf("%s failed: %e", name+"/GdaoCreate", err)
		}
	}

	if numRows, err := dao.GdaoDeleteMany("@"+dao.tableName, filter); err != nil {
		t.Fatalf("%s failed: %e", name, err)
	} else if numRows != 2 {
		t.Fatalf("%s failed: expected %#v row(s) deleted but received %#v", name, 2, numRows)
	}
}

func TestGenericDaoDynamodb_GdaoDeleteManyGSI_Scan(t *testing.T) {
	name := "TestGenericDaoDynamodb_GdaoDeleteManyGSI_Scan"

	if os.Getenv(envAwsDynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(envAwsDynamodbTestTableName)
	}
	dao := _initDao(t, name, testDynamodbTableName)
	err := prepareAwsDynamodbTable(dao.GetAwsDynamodbConnect(), testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareAwsDynamodbTable", err)
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
			Subject:  "Subject" + strconv.Itoa(i%4),
			Level:    i,
		}
		_, err = dao.GdaoCreate(dao.tableName, dao.toGbo(user))
		if err != nil {
			t.Fatalf("%s failed: %e", name+"/GdaoCreate", err)
		}
	}

	gsiName := "gsi_" + dao.tableName + "_" + fieldUsername
	filter := expression.Name(fieldUsername).GreaterThanEqual(expression.Value("user5"))
	if numRows, err := dao.GdaoDeleteMany(dao.tableName+":"+gsiName, filter); err != nil {
		t.Fatalf("%s failed: %e", name, err)
	} else if numRows != 5 {
		t.Fatalf("%s failed: expected %#v row(s) deleted but received %#v", name, 5, numRows)
	}
}

func TestGenericDaoDynamodb_GdaoDeleteManyGSI_Query(t *testing.T) {
	name := "TestGenericDaoDynamodb_GdaoDeleteManyGSI_Query"

	if os.Getenv(envAwsDynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(envAwsDynamodbTestTableName)
	}
	dao := _initDao(t, name, testDynamodbTableName)
	err := prepareAwsDynamodbTable(dao.GetAwsDynamodbConnect(), testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareAwsDynamodbTable", err)
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
			Subject:  "Subject" + strconv.Itoa(i%4),
			Level:    i,
		}
		_, err = dao.GdaoCreate(dao.tableName, dao.toGbo(user))
		if err != nil {
			t.Fatalf("%s failed: %e", name+"/GdaoCreate", err)
		}
	}

	gsiName := "gsi_" + dao.tableName + "_" + fieldSubject + "_" + fieldLevel
	filter := expression.Name(fieldSubject).Equal(expression.Value("Subject1")).
		And(expression.Name(fieldLevel).GreaterThanEqual(expression.Value(5)))
	if numRows, err := dao.GdaoDeleteMany("@"+dao.tableName+":"+gsiName, filter); err != nil {
		t.Fatalf("%s failed: %e", name, err)
	} else if numRows != 2 {
		t.Fatalf("%s failed: expected %#v row(s) deleted but received %#v", name, 2, numRows)
	}
}

func TestGenericDaoDynamodb_GdaoFetchOne(t *testing.T) {
	name := "TestGenericDaoDynamodb_GdaoFetchOne"

	if os.Getenv(envAwsDynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(envAwsDynamodbTestTableName)
	}
	dao := _initDao(t, name, testDynamodbTableName)
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
		Subject:  "English",
		Level:    1,
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

func TestGenericDaoDynamodb_GdaoFetchMany_Scan(t *testing.T) {
	name := "TestGenericDaoDynamodb_GdaoFetchMany_Scan"

	if os.Getenv(envAwsDynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(envAwsDynamodbTestTableName)
	}
	dao := _initDao(t, name, testDynamodbTableName)
	err := prepareAwsDynamodbTable(dao.GetAwsDynamodbConnect(), testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareAwsDynamodbTable", err)
	}

	for i := 0; i < 10; i++ {
		user := &UserBoDynamodb{
			Id:       strconv.Itoa(i),
			Username: "user" + strconv.Itoa(9-i),
			Name:     "Thanh",
			Version:  int(time.Now().UnixNano()),
			Active:   i%3 == 0,
			Created:  time.Now(),
			Subject:  "Subject" + strconv.Itoa(i%4),
			Level:    i,
		}
		_, err = dao.GdaoCreate(dao.tableName, dao.toGbo(user))
		if err != nil {
			t.Fatalf("%s failed: %e", name+"/GdaoCreate", err)
		}
	}

	filter := expression.Name(fieldId).GreaterThanEqual(expression.Value("5"))
	if dbRows, err := dao.GdaoFetchMany(dao.tableName, filter, nil, 0, 3); err != nil {
		t.Fatalf("%s failed: %e", name, err)
	} else if dbRows == nil || len(dbRows) != 3 {
		t.Fatalf("%s failed: expected %#v row(s) but received %#v", name, 3, dbRows)
	} else {
		for _, row := range dbRows {
			if user := dao.toUser(row); user.Id < "5" {
				t.Fatalf("%s failed: invalid row %s", name, row.GboToJsonUnsafe())
			}
		}
	}

	filter = expression.Name(fieldUsername).GreaterThanEqual(expression.Value("user5"))
	if dbRows, err := dao.GdaoFetchMany(dao.tableName, filter, nil, 0, 3); err != nil {
		t.Fatalf("%s failed: %e", name, err)
	} else if dbRows == nil || len(dbRows) != 3 {
		t.Fatalf("%s failed: expected %#v row(s) but received %#v", name, 3, dbRows)
	} else {
		for _, row := range dbRows {
			if user := dao.toUser(row); user.Username < "user5" {
				t.Fatalf("%s failed: invalid row %s", name, row.GboToJsonUnsafe())
			}
		}
	}
}

func TestGenericDaoDynamodb_GdaoFetchMany_Query(t *testing.T) {
	name := "TestGenericDaoDynamodb_GdaoFetchMany_Query"

	if os.Getenv(envAwsDynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(envAwsDynamodbTestTableName)
	}
	dao := _initDao(t, name, testDynamodbTableName)
	dao.SetRowMapper(&GenericRowMapperDynamodb{ColumnsListMap: map[string][]string{testDynamodbTableName: {fieldSubject, fieldLevel}}})
	err := prepareAwsDynamodbTableCompoundKey(dao.GetAwsDynamodbConnect(), testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareAwsDynamodbTable", err)
	}

	for i := 0; i < 10; i++ {
		user := &UserBoDynamodb{
			Id:       strconv.Itoa(i),
			Username: "user" + strconv.Itoa(9-i),
			Name:     "Thanh",
			Version:  int(time.Now().UnixNano()),
			Active:   i%3 == 0,
			Created:  time.Now(),
			Subject:  "Subject" + strconv.Itoa(i%4),
			Level:    i,
		}
		_, err = dao.GdaoCreate(dao.tableName, dao.toGbo(user))
		if err != nil {
			t.Fatalf("%s failed: %e", name+"/GdaoCreate", err)
		}
	}

	filter := expression.Name(fieldSubject).Equal(expression.Value("Subject1")).
		And(expression.Name(fieldLevel).GreaterThanEqual(expression.Value(5)))
	if dbRows, err := dao.GdaoFetchMany("@"+dao.tableName, filter, nil, 0, 3); err != nil {
		t.Fatalf("%s failed: %e", name, err)
	} else if dbRows == nil || len(dbRows) != 2 {
		t.Fatalf("%s failed: expected %#v row(s) but received %#v", name, 3, len(dbRows))
	} else {
		for _, row := range dbRows {
			if user := dao.toUser(row); user.Level < 5 || user.Name != "Thanh" {
				t.Fatalf("%s failed: invalid row %s", name, row.GboToJsonUnsafe())
			}
		}
	}
}

func TestGenericDaoDynamodb_GdaoFetchManyGSI_Scan(t *testing.T) {
	name := "TestGenericDaoDynamodb_GdaoFetchManyGSI_Scan"

	if os.Getenv(envAwsDynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(envAwsDynamodbTestTableName)
	}
	dao := _initDao(t, name, testDynamodbTableName)
	err := prepareAwsDynamodbTable(dao.GetAwsDynamodbConnect(), testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareAwsDynamodbTable", err)
	}

	for i := 0; i < 10; i++ {
		user := &UserBoDynamodb{
			Id:       strconv.Itoa(i),
			Username: "user" + strconv.Itoa(9-i),
			Name:     "Thanh",
			Version:  int(time.Now().UnixNano()),
			Active:   i%3 == 0,
			Created:  time.Now(),
			Subject:  "Subject" + strconv.Itoa(i%4),
			Level:    i,
		}
		_, err = dao.GdaoCreate(dao.tableName, dao.toGbo(user))
		if err != nil {
			t.Fatalf("%s failed: %e", name+"/GdaoCreate", err)
		}
	}

	gsiName := "gsi_" + dao.tableName + "_" + fieldUsername

	filter := expression.Name(fieldId).GreaterThanEqual(expression.Value("5"))
	if dbRows, err := dao.GdaoFetchMany(dao.tableName+":"+gsiName+":false", filter, nil, 0, 3); err != nil {
		t.Fatalf("%s failed: %e", name, err)
	} else if dbRows == nil || len(dbRows) != 3 {
		t.Fatalf("%s failed: expected %#v row(s) but received %#v", name, 3, dbRows)
	} else {
		for _, row := range dbRows {
			if user := dao.toUser(row); user.Id < "5" || user.Name != "" {
				t.Fatalf("%s failed: invalid row %s", name, row.GboToJsonUnsafe())
			}
		}
	}

	filter = expression.Name(fieldUsername).GreaterThanEqual(expression.Value("user5"))
	if dbRows, err := dao.GdaoFetchMany(dao.tableName+":"+gsiName+":true", filter, nil, 0, 3); err != nil {
		t.Fatalf("%s failed: %e", name, err)
	} else if dbRows == nil || len(dbRows) != 3 {
		t.Fatalf("%s failed: expected %#v row(s) but received %#v", name, 3, dbRows)
	} else {
		for _, row := range dbRows {
			if user := dao.toUser(row); user.Username < "user5" || user.Name != "Thanh" {
				t.Fatalf("%s failed: invalid row %s", name, row.GboToJsonUnsafe())
			}
		}
	}
}

func TestGenericDaoDynamodb_GdaoFetchManyGSI_Query(t *testing.T) {
	name := "TestGenericDaoDynamodb_GdaoFetchManyGSI_Query"

	if os.Getenv(envAwsDynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(envAwsDynamodbTestTableName)
	}
	dao := _initDao(t, name, testDynamodbTableName)
	err := prepareAwsDynamodbTable(dao.GetAwsDynamodbConnect(), testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareAwsDynamodbTable", err)
	}

	for i := 0; i < 10; i++ {
		user := &UserBoDynamodb{
			Id:       strconv.Itoa(i),
			Username: "user" + strconv.Itoa(9-i),
			Name:     "Thanh",
			Version:  int(time.Now().UnixNano()),
			Active:   i%3 == 0,
			Created:  time.Now(),
			Subject:  "Subject" + strconv.Itoa(i%4),
			Level:    i,
		}
		// fmt.Println(user.Subject, user.Level)
		_, err = dao.GdaoCreate(dao.tableName, dao.toGbo(user))
		if err != nil {
			t.Fatalf("%s failed: %e", name+"/GdaoCreate", err)
		}
	}

	gsiName := "gsi_" + dao.tableName + "_" + fieldSubject + "_" + fieldLevel
	filter := expression.Name(fieldSubject).Equal(expression.Value("Subject1")).
		And(expression.Name(fieldLevel).GreaterThanEqual(expression.Value(5)))

	if dbRows, err := dao.GdaoFetchMany("@"+dao.tableName+":"+gsiName+":true", filter, nil, 0, 3); err != nil {
		t.Fatalf("%s failed: %e", name, err)
	} else if dbRows == nil || len(dbRows) != 2 {
		t.Fatalf("%s failed: expected %#v row(s) but received %#v", name, 3, len(dbRows))
	} else {
		for _, row := range dbRows {
			if user := dao.toUser(row); user.Level < 5 || user.Name != "Thanh" {
				t.Fatalf("%s failed: invalid row %s", name, row.GboToJsonUnsafe())
			}
		}
	}

	if dbRows, err := dao.GdaoFetchMany("@"+dao.tableName+":"+gsiName+":false", filter, nil, 0, 3); err != nil {
		t.Fatalf("%s failed: %e", name, err)
	} else if dbRows == nil || len(dbRows) != 2 {
		t.Fatalf("%s failed: expected %#v row(s) but received %#v", name, 3, len(dbRows))
	} else {
		for _, row := range dbRows {
			if user := dao.toUser(row); user.Level < 5 || user.Name != "" {
				t.Fatalf("%s failed: invalid row %s", name, row.GboToJsonUnsafe())
			}
		}
	}
}

func TestGenericDaoDynamodb_GdaoCreate(t *testing.T) {
	name := "TestGenericDaoDynamodb_GdaoCreate"

	if os.Getenv(envAwsDynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(envAwsDynamodbTestTableName)
	}
	dao := _initDao(t, name, testDynamodbTableName)
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
		Subject:  "English",
		Level:    1,
	}
	if numRows, err := dao.GdaoCreate(dao.tableName, dao.toGbo(user)); err != nil {
		t.Fatalf("%s failed: %e", name, err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name, 1, numRows)
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

func TestGenericDaoDynamodb_GdaoCreateDuplicated(t *testing.T) {
	name := "TestGenericDaoDynamodb_GdaoCreateDuplicated"

	if os.Getenv(envAwsDynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(envAwsDynamodbTestTableName)
	}
	dao := _initDao(t, name, testDynamodbTableName)
	dao.SetRowMapper(&GenericRowMapperDynamodb{ColumnsListMap: map[string][]string{testDynamodbTableName: {fieldSubject, fieldLevel}}})
	err := prepareAwsDynamodbTableCompoundKey(dao.GetAwsDynamodbConnect(), testDynamodbTableName)
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
		Subject:  "English",
		Level:    1,
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

	filter := dao.GdaoCreateFilter(dao.tableName, dao.toGbo(&UserBoDynamodb{Subject: "English", Level: 1}))
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
	name := "TestGenericDaoDynamodb_GdaoUpdate"

	if os.Getenv(envAwsDynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(envAwsDynamodbTestTableName)
	}
	dao := _initDao(t, name, testDynamodbTableName)
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
		Subject:  "English",
		Level:    1,
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

// func TestGenericDaoDynamodb_GdaoUpdateDuplicated(t *testing.T) {
// 	name := "TestGenericDaoDynamodb_GdaoUpdateDuplicated"
//
// 	if os.Getenv(envAwsDynamodbTestTableName) != "" {
// 		testDynamodbTableName = os.Getenv(envAwsDynamodbTestTableName)
// 	}
// 	dao := _initDao(t, name, testDynamodbTableName)
// 	dao.SetRowMapper(&GenericRowMapperDynamodb{ColumnsListMap: map[string][]string{testDynamodbTableName: {fieldSubject, fieldLevel}}})
// 	err := prepareAwsDynamodbTableCompoundKey(dao.GetAwsDynamodbConnect(), testDynamodbTableName)
// 	if err != nil {
// 		t.Fatalf("%s failed: %e", name+"/prepareAwsDynamodbTable", err)
// 	}
//
// 	user1 := &UserBoDynamodb{
// 		Id:       "1",
// 		Username: "user1",
// 		Name:     "Thanh Nguyen",
// 		Version:  int(time.Now().Unix()),
// 		Active:   false,
// 		Created:  time.Now(),
// 		Subject:  "English",
// 		Level:    1,
// 	}
// 	user2 := &UserBoDynamodb{
// 		Id:       "2",
// 		Username: "user2",
// 		Name:     "Thanh Nguyen",
// 		Version:  int(time.Now().Unix()),
// 		Active:   false,
// 		Created:  time.Now(),
// 		Subject:  "English",
// 		Level:    2,
// 	}
// 	if numRows, err := dao.GdaoCreate(dao.tableName, dao.toGbo(user1)); err != nil {
// 		t.Fatalf("%s failed: %e", name+"/GdaoCreate", err)
// 	} else if numRows != 1 {
// 		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoCreate", 1, numRows)
// 	}
// 	if numRows, err := dao.GdaoCreate(dao.tableName, dao.toGbo(user2)); err != nil {
// 		t.Fatalf("%s failed: %e", name+"/GdaoCreate", err)
// 	} else if numRows != 1 {
// 		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoCreate", 1, numRows)
// 	}
//
// 	user1.Level = 2
// 	if numRows, err := dao.GdaoUpdate(dao.tableName, dao.toGbo(user1)); err != godal.GdaoErrorDuplicatedEntry || numRows != 0 {
// 		t.Fatalf("%s failed: num rows %#v / error: %e", name, numRows, err)
// 	}
// }

func TestGenericDaoDynamodb_GdaoSave(t *testing.T) {
	name := "TestGenericDaoDynamodb_GdaoSave"

	if os.Getenv(envAwsDynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(envAwsDynamodbTestTableName)
	}
	dao := _initDao(t, name, testDynamodbTableName)
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
		Subject:  "English",
		Level:    1,
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

func TestGenericDaoDynamodb_GdaoSaveShouldReplace(t *testing.T) {
	name := "TestGenericDaoDynamodb_GdaoSaveShouldReplace"

	if os.Getenv(envAwsDynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(envAwsDynamodbTestTableName)
	}
	dao := _initDao(t, name, testDynamodbTableName)
	err := prepareAwsDynamodbTable(dao.GetAwsDynamodbConnect(), testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/prepareAwsDynamodbTable", err)
	}

	gbo := godal.NewGenericBo()

	data1 := map[string]interface{}{
		"id":       "1",
		"username": "btnguyen2k",
		"active":   false,
		"version":  1,
	}
	gbo.GboImportViaJson(data1)
	if numRows, err := dao.GdaoSave(dao.tableName, gbo); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoSave", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoSave", 1, numRows)
	}

	data2 := map[string]interface{}{
		"id":     "1",
		"name":   "Thanh Nguyen",
		"active": true,
	}
	gbo.GboImportViaJson(data2)
	if numRows, err := dao.GdaoSave(dao.tableName, gbo); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoSave", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoSave", 1, numRows)
	}

	filter := map[string]interface{}{"id": "1"}
	if gbo, err := dao.GdaoFetchOne(dao.tableName, filter); err != nil {
		t.Fatalf("%s failed: %e", name+"/GdaoFetchOne", err)
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

// func TestGenericDaoDynamodb_GdaoSaveDuplicated(t *testing.T) {
// 	name := "TestGenericDaoDynamodb_GdaoSaveDuplicated"
//
// 	if os.Getenv(envAwsDynamodbTestTableName) != "" {
// 		testDynamodbTableName = os.Getenv(envAwsDynamodbTestTableName)
// 	}
// 	dao := _initDao(t, name, testDynamodbTableName)
// 	dao.SetRowMapper(&GenericRowMapperDynamodb{ColumnsListMap: map[string][]string{testDynamodbTableName: {fieldSubject, fieldLevel}}})
// 	err := prepareAwsDynamodbTableCompoundKey(dao.GetAwsDynamodbConnect(), testDynamodbTableName)
// 	if err != nil {
// 		t.Fatalf("%s failed: %e", name+"/prepareAwsDynamodbTable", err)
// 	}
//
// 	user1 := &UserBoDynamodb{
// 		Id:       "1",
// 		Username: "user1",
// 		Name:     "Thanh Nguyen",
// 		Version:  int(time.Now().Unix()),
// 		Active:   false,
// 		Created:  time.Now(),
// 		Subject:  "English",
// 		Level:    1,
// 	}
// 	user2 := &UserBoDynamodb{
// 		Id:       "2",
// 		Username: "user2",
// 		Name:     "Thanh Nguyen",
// 		Version:  int(time.Now().Unix()),
// 		Active:   false,
// 		Created:  time.Now(),
// 		Subject:  "English",
// 		Level:    2,
// 	}
// 	if numRows, err := dao.GdaoSave(dao.tableName, dao.toGbo(user1)); err != nil {
// 		t.Fatalf("%s failed: %e", name+"/GdaoCreate", err)
// 	} else if numRows != 1 {
// 		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoCreate", 1, numRows)
// 	}
// 	if numRows, err := dao.GdaoSave(dao.tableName, dao.toGbo(user2)); err != nil {
// 		t.Fatalf("%s failed: %e", name+"/GdaoCreate", err)
// 	} else if numRows != 1 {
// 		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoCreate", 1, numRows)
// 	}
//
// 	user1.Level = 2
// 	if numRows, err := dao.GdaoSave(dao.tableName, dao.toGbo(user1)); err != godal.GdaoErrorDuplicatedEntry || numRows != 0 {
// 		t.Fatalf("%s failed: num rows %#v / error: %e", name, numRows, err)
// 	}
// }