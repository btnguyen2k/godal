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
		fmt.Printf("    Table [%s] status: %v - %s\n", table, status, err)
		if delay > 0 {
			time.Sleep(time.Duration(delay) * time.Second)
		}
		status, err = adc.GetTableStatus(nil, table)
	}
}

func waitForGsi(adc *prom.AwsDynamodbConnect, table, index string, statusList []string, delay int) {
	for status, err := adc.GetGlobalSecondaryIndexStatus(nil, table, index); !inSlide(status, statusList) && err == nil; {
		fmt.Printf("    GSI [%s] on table [%s] status: %v - %s\n", index, table, status, err)
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
func (dao *MyUserDaoDynamodb) GdaoCreateFilter(tableName string, bo godal.IGenericBo) godal.FilterOpt {
	if tableName == dao.tableName {
		colList := dao.GetRowMapper().ColumnsList(tableName)
		if len(colList) == 1 {
			return &godal.FilterOptFieldOpValue{
				FieldName: colList[0],
				Operator:  godal.FilterOpEqual,
				Value:     bo.GboGetAttrUnsafe(colList[0], nil),
			}
		}
		result := &godal.FilterOptAnd{}
		for _, col := range colList {
			result.Add(&godal.FilterOptFieldOpValue{
				FieldName: col,
				Operator:  godal.FilterOpEqual,
				Value:     bo.GboGetAttrUnsafe(col, nil),
			})
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
			t.Fatalf("%s failed: %s / %v", name, err, bo)
		}
		if bo, err := rowMapper.ToBo(table, &row); err != nil || bo != nil {
			t.Fatalf("%s failed: %s / %v", name, err, bo)
		}
		row2 := &row
		if bo, err := rowMapper.ToBo(table, &row2); err != nil || bo != nil {
			t.Fatalf("%s failed: %s / %v", name, err, bo)
		}
	}

	{
		var row *string = nil
		if bo, err := rowMapper.ToBo(table, row); err != nil || bo != nil {
			t.Fatalf("%s failed: %s / %v", name, err, bo)
		}
		if bo, err := rowMapper.ToBo(table, &row); err != nil || bo != nil {
			t.Fatalf("%s failed: %s / %v", name, err, bo)
		}
		row2 := &row
		if bo, err := rowMapper.ToBo(table, &row2); err != nil || bo != nil {
			t.Fatalf("%s failed: %s / %v", name, err, bo)
		}
	}

	{
		var row []byte = nil
		if bo, err := rowMapper.ToBo(table, row); err != nil || bo != nil {
			t.Fatalf("%s failed: %s / %v", name, err, bo)
		}
		if bo, err := rowMapper.ToBo(table, &row); err != nil || bo != nil {
			t.Fatalf("%s failed: %s / %v", name, err, bo)
		}
		row2 := &row
		if bo, err := rowMapper.ToBo(table, &row2); err != nil || bo != nil {
			t.Fatalf("%s failed: %s / %v", name, err, bo)
		}
	}

	{
		var row *[]byte = nil
		if bo, err := rowMapper.ToBo(table, row); err != nil || bo != nil {
			t.Fatalf("%s failed: %s / %v", name, err, bo)
		}
		if bo, err := rowMapper.ToBo(table, &row); err != nil || bo != nil {
			t.Fatalf("%s failed: %s / %v", name, err, bo)
		}
		row2 := &row
		if bo, err := rowMapper.ToBo(table, &row2); err != nil || bo != nil {
			t.Fatalf("%s failed: %s / %v", name, err, bo)
		}
	}
}

func TestGenericRowMapperDynamodb_ToBo_Invalid(t *testing.T) {
	name := "TestGenericRowMapperDynamodb_ToBo_Invalid"
	rowMapper := &GenericRowMapperDynamodb{}
	if bo, err := rowMapper.ToBo("", time.Time{}); bo != nil || err == nil {
		t.Fatalf("%s failed: expected nil/err but received %#v/%#v", name, bo, err)
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

func TestGenericRowMapperDynamodb_ToRow_Nil(t *testing.T) {
	name := "TestGenericRowMapperDynamodb_ToRow_Nil"
	rm := &GenericRowMapperDynamodb{}
	row, err := rm.ToRow("", nil)
	if err != nil || row != nil {
		t.Fatalf("%s failed: error: %#v", name, err)
	}
}

func TestGenericRowMapperDynamodb_ToDbColName(t *testing.T) {
	name := "TestGenericRowMapperDynamodb_ToDbColName"
	table := "table"
	colA, colB, colC := "cola", "ColB", "colC"
	cols := []string{colA, colB, colC}
	rowMapper := &GenericRowMapperDynamodb{ColumnsListMap: map[string][]string{table: cols}}

	if fieldName := rowMapper.ToDbColName(table, colA); fieldName != colA {
		t.Fatalf("%s failed, expect %#v but received %#v", name, colA, fieldName)
	}

	if fieldName := rowMapper.ToDbColName("table", colB); fieldName != colB {
		t.Fatalf("%s failed, expect %#v but received %#v", name, colB, fieldName)
	}
}

func TestGenericRowMapperDynamodb_ToBoFieldName(t *testing.T) {
	name := "TestGenericRowMapperDynamodb_ToBoFieldName"
	table := "table"
	colA, colB, colC := "cola", "ColB", "colC"
	cols := []string{colA, colB, colC}
	rowMapper := &GenericRowMapperDynamodb{ColumnsListMap: map[string][]string{table: cols}}

	if colName := rowMapper.ToBoFieldName(table, colA); colName != colA {
		t.Fatalf("%s failed, expect %#v but received %#v", name, colA, colName)
	}

	if colName := rowMapper.ToBoFieldName("table", colB); colName != colB {
		t.Fatalf("%s failed, expect %#v but received %#v", name, colB, colName)
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

func TestToFilterMap(t *testing.T) {
	name := "TestToFilterMap"

	var input godal.FilterOpt
	var output, expected map[string]interface{}
	var err error

	input = nil
	expected = nil
	if output, err = toFilterMap(input); err != nil || !reflect.DeepEqual(expected, output) {
		t.Fatalf("%s failed: expected %#v but received %#v / Error: %s", name, expected, output, err)
	}

	invalidOpts := []godal.FilterOperator{godal.FilterOpNotEqual, godal.FilterOpGreater, godal.FilterOpGreaterOrEqual, godal.FilterOpLess, godal.FilterOpLessOrEqual}
	for _, opt := range invalidOpts {
		input = &godal.FilterOptFieldOpValue{FieldName: "field", Operator: opt, Value: "value"}
		if _, err = toFilterMap(input); err == nil {
			t.Fatalf("%s failed: expected error for operator %#v", name, opt)
		}
	}

	input = godal.FilterOptFieldOpValue{FieldName: "field", Operator: godal.FilterOpEqual, Value: "value"}
	expected = map[string]interface{}{"field": "value"}
	if output, err = toFilterMap(input); err != nil || !reflect.DeepEqual(expected, output) {
		t.Fatalf("%s failed: expected %#v but received %#v / Error: %s", name, expected, output, err)
	}
	input = &godal.FilterOptFieldOpValue{FieldName: "field", Operator: godal.FilterOpEqual, Value: 1.2}
	expected = map[string]interface{}{"field": 1.2}
	if output, err = toFilterMap(input); err != nil || !reflect.DeepEqual(expected, output) {
		t.Fatalf("%s failed: expected %#v but received %#v / Error: %s", name, expected, output, err)
	}

	input = godal.FilterOptFieldIsNull{FieldName: "field1"}
	expected = map[string]interface{}{"field1": nil}
	if output, err = toFilterMap(input); err != nil || !reflect.DeepEqual(expected, output) {
		t.Fatalf("%s failed: expected %#v but received %#v / Error: %s", name, expected, output, err)
	}
	input = &godal.FilterOptFieldIsNull{FieldName: "field2"}
	expected = map[string]interface{}{"field2": nil}
	if output, err = toFilterMap(input); err != nil || !reflect.DeepEqual(expected, output) {
		t.Fatalf("%s failed: expected %#v but received %#v / Error: %s", name, expected, output, err)
	}

	temp := (&godal.FilterOptAnd{}).
		Add(&godal.FilterOptFieldOpValue{FieldName: "field1", Operator: godal.FilterOpEqual, Value: "nil"}).
		Add(&godal.FilterOptFieldIsNull{FieldName: "field2"})
	input = *temp
	expected = map[string]interface{}{"field1": "nil", "field2": nil}
	if output, err = toFilterMap(input); err != nil || !reflect.DeepEqual(expected, output) {
		t.Fatalf("%s failed: expected %#v but received %#v / Error: %s", name, expected, output, err)
	}
	input = temp
	if output, err = toFilterMap(input); err != nil || !reflect.DeepEqual(expected, output) {
		t.Fatalf("%s failed: expected %#v but received %#v / Error: %s", name, expected, output, err)
	}

	input = godal.FilterOptFieldIsNotNull{FieldName: "field"}
	if _, err = toFilterMap(input); err == nil {
		t.Fatalf("%s failed: expected error for input %#v", name, input)
	}
	input = &godal.FilterOptFieldIsNotNull{FieldName: "field"}
	if _, err = toFilterMap(input); err == nil {
		t.Fatalf("%s failed: expected error for input %#v", name, input)
	}

	temp2 := (&godal.FilterOptOr{}).
		Add(&godal.FilterOptFieldOpValue{FieldName: "field1", Operator: godal.FilterOpEqual, Value: "nil"}).
		Add(&godal.FilterOptFieldIsNull{FieldName: "field2"})
	input = *temp2
	if _, err = toFilterMap(input); err == nil {
		t.Fatalf("%s failed: expected error for input %#v", name, input)
	}
	input = temp2
	if _, err = toFilterMap(input); err == nil {
		t.Fatalf("%s failed: expected error for input %#v", name, input)
	}
}

const testTimeZone = "Asia/Ho_Chi_Minh"

func _compareUsers(t *testing.T, name string, expected, target *UserBoDynamodb) {
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

func TestGenericDaoDynamodb_BuildConditionBuilder(t *testing.T) {
	name := "TestGenericDaoDynamodb_BuildConditionBuilder"

	if os.Getenv(envAwsDynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(envAwsDynamodbTestTableName)
	}
	dao := _initDao(t, name, testDynamodbTableName)
	err := prepareAwsDynamodbTable(dao.GetAwsDynamodbConnect(), testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: %s", name+"/prepareAwsDynamodbTable", err)
	}

	var input godal.FilterOpt
	var output, expected *expression.ConditionBuilder

	input = nil
	expected = nil
	if output, err = dao.BuildConditionBuilder(testDynamodbTableName, input); err != nil || !reflect.DeepEqual(expected, output) {
		t.Fatalf("%s failed: expected %#v but received %#v / Error: %s", name, expected, output, err)
	}

	_n := expression.Name("field")
	_v := expression.Value("value")
	optsList := []godal.FilterOperator{godal.FilterOpEqual, godal.FilterOpNotEqual, godal.FilterOpGreater, godal.FilterOpGreaterOrEqual, godal.FilterOpLess, godal.FilterOpLessOrEqual}
	expectedList := []expression.ConditionBuilder{_n.Equal(_v), _n.NotEqual(_v), _n.GreaterThan(_v), _n.GreaterThanEqual(_v), _n.LessThan(_v), _n.LessThanEqual(_v)}
	for i, opt := range optsList {
		expected = &expectedList[i]
		input = godal.FilterOptFieldOpValue{FieldName: "field", Operator: opt, Value: "value"}
		if output, err = dao.BuildConditionBuilder(testDynamodbTableName, input); err != nil || !reflect.DeepEqual(expected, output) {
			t.Fatalf("%s failed: expected %#v but received %#v / Error: %s", name, expected, output, err)
		}
		input = &godal.FilterOptFieldOpValue{FieldName: "field", Operator: opt, Value: "value"}
		if output, err = dao.BuildConditionBuilder(testDynamodbTableName, input); err != nil || !reflect.DeepEqual(expected, output) {
			t.Fatalf("%s failed: expected %#v but received %#v / Error: %s", name, expected, output, err)
		}
	}

	_expected := expression.Name("field").AttributeNotExists()
	expected = &_expected
	input = godal.FilterOptFieldIsNull{FieldName: "field"}
	if output, err = dao.BuildConditionBuilder(testDynamodbTableName, input); err != nil || !reflect.DeepEqual(expected, output) {
		t.Fatalf("%s failed: expected %#v but received %#v / Error: %s", name, expected, output, err)
	}
	input = &godal.FilterOptFieldIsNull{FieldName: "field"}
	if output, err = dao.BuildConditionBuilder(testDynamodbTableName, input); err != nil || !reflect.DeepEqual(expected, output) {
		t.Fatalf("%s failed: expected %#v but received %#v / Error: %s", name, expected, output, err)
	}

	_expected = expression.Name("field").AttributeExists()
	expected = &_expected
	input = godal.FilterOptFieldIsNotNull{FieldName: "field"}
	if output, err = dao.BuildConditionBuilder(testDynamodbTableName, input); err != nil || !reflect.DeepEqual(expected, output) {
		t.Fatalf("%s failed: expected %#v but received %#v / Error: %s", name, expected, output, err)
	}
	input = &godal.FilterOptFieldIsNotNull{FieldName: "field"}
	if output, err = dao.BuildConditionBuilder(testDynamodbTableName, input); err != nil || !reflect.DeepEqual(expected, output) {
		t.Fatalf("%s failed: expected %#v but received %#v / Error: %s", name, expected, output, err)
	}

	_expected = expression.Name("field1").GreaterThan(expression.Value(1)).
		And(expression.Name("field2").LessThanEqual(expression.Value("3")))
	input = godal.FilterOptAnd{Filters: []godal.FilterOpt{
		godal.FilterOptFieldOpValue{FieldName: "field1", Operator: godal.FilterOpGreater, Value: 1},
		godal.FilterOptFieldOpValue{FieldName: "field2", Operator: godal.FilterOpLessOrEqual, Value: "3"}}}
	if output, err = dao.BuildConditionBuilder(testDynamodbTableName, input); err != nil || !reflect.DeepEqual(expected, output) {
		t.Fatalf("%s failed: expected %#v but received %#v / Error: %s", name, expected, output, err)
	}
	input = &godal.FilterOptAnd{Filters: []godal.FilterOpt{
		godal.FilterOptFieldOpValue{FieldName: "field1", Operator: godal.FilterOpGreater, Value: 1},
		godal.FilterOptFieldOpValue{FieldName: "field2", Operator: godal.FilterOpLessOrEqual, Value: "3"}}}
	if output, err = dao.BuildConditionBuilder(testDynamodbTableName, input); err != nil || !reflect.DeepEqual(expected, output) {
		t.Fatalf("%s failed: expected %#v but received %#v / Error: %s", name, expected, output, err)
	}

	_expected = expression.Name("field3").GreaterThan(expression.Value("5")).
		Or(expression.Name("field4").LessThanEqual(expression.Value(7)))
	input = godal.FilterOptOr{Filters: []godal.FilterOpt{
		godal.FilterOptFieldOpValue{FieldName: "field3", Operator: godal.FilterOpGreater, Value: "5"},
		godal.FilterOptFieldOpValue{FieldName: "field4", Operator: godal.FilterOpLessOrEqual, Value: 7}}}
	if output, err = dao.BuildConditionBuilder(testDynamodbTableName, input); err != nil || !reflect.DeepEqual(expected, output) {
		t.Fatalf("%s failed: expected %#v but received %#v / Error: %s", name, expected, output, err)
	}
	input = &godal.FilterOptOr{Filters: []godal.FilterOpt{
		godal.FilterOptFieldOpValue{FieldName: "field3", Operator: godal.FilterOpGreater, Value: "5"},
		godal.FilterOptFieldOpValue{FieldName: "field4", Operator: godal.FilterOpLessOrEqual, Value: 7}}}
	if output, err = dao.BuildConditionBuilder(testDynamodbTableName, input); err != nil || !reflect.DeepEqual(expected, output) {
		t.Fatalf("%s failed: expected %#v but received %#v / Error: %s", name, expected, output, err)
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
		t.Fatalf("%s failed: %s", name+"/prepareAwsDynamodbTable", err)
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
		t.Fatalf("%s failed: %s", name+"/GdaoCreate", err)
	}

	// GdaoDelete should be successful and number of affected rows should be 0
	filterUser := &UserBoDynamodb{Id: "2"}
	if numRows, err := dao.GdaoDelete(dao.tableName, dao.toGbo(filterUser)); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if numRows != 0 {
		t.Fatalf("%s failed: expected %#v row(s) deleted but received %#v", name, 0, numRows)
	}

	// GdaoDelete should be successful and number of affected rows should be 1
	filterUser = &UserBoDynamodb{Id: user.Id}
	if numRows, err := dao.GdaoDelete(dao.tableName, dao.toGbo(filterUser)); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) deleted but received %#v", name, 1, numRows)
	}

	// GdaoFetchOne should be successful and the returned BO should be nil
	if u, err := dao.GdaoFetchOne(dao.tableName, dao.GdaoCreateFilter(dao.tableName, dao.toGbo(user))); err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoFetchOne", err)
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
		t.Fatalf("%s failed: %s", name+"/prepareAwsDynamodbTable", err)
	}

	// GdaoDeleteMany should be successful but no row deleted
	filter := &godal.FilterOptFieldOpValue{FieldName: fieldId, Operator: godal.FilterOpGreaterOrEqual, Value: "5"}
	if numRows, err := dao.GdaoDeleteMany(dao.tableName, filter); err != nil {
		t.Fatalf("%s failed: %s", name, err)
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
			t.Fatalf("%s failed: %s", name+"/GdaoCreate", err)
		}
	}

	// GdaoDeleteMany should be successful and 5 rows deleted
	if numRows, err := dao.GdaoDeleteMany(dao.tableName, filter); err != nil {
		t.Fatalf("%s failed: %s", name, err)
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
		t.Fatalf("%s failed: %s", name+"/prepareAwsDynamodbTable", err)
	}

	// GdaoDeleteMany should be successful but no row deleted
	filter := (&godal.FilterOptAnd{}).
		Add(&godal.FilterOptFieldOpValue{FieldName: fieldSubject, Operator: godal.FilterOpEqual, Value: "Subject1"}).
		Add(&godal.FilterOptFieldOpValue{FieldName: fieldLevel, Operator: godal.FilterOpGreaterOrEqual, Value: 5})
	if numRows, err := dao.GdaoDeleteMany("@"+dao.tableName, filter); err != nil {
		t.Fatalf("%s failed: %s", name, err)
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
			t.Fatalf("%s failed: %s", name+"/GdaoCreate", err)
		}
	}

	// GdaoDeleteMany should be successful and 2 rows deleted
	// the "@" prefix instructs that GdaoDeleteMany should use "query" instead of "scan"
	if numRows, err := dao.GdaoDeleteMany("@"+dao.tableName, filter); err != nil {
		t.Fatalf("%s failed: %s", name, err)
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
		t.Fatalf("%s failed: %s", name+"/prepareAwsDynamodbTable", err)
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
			t.Fatalf("%s failed: %s", name+"/GdaoCreate", err)
		}
	}

	// GdaoDeleteMany should be successful and 5 rows deleted
	// use format <table-name>:<gsi-name> to filter using GSI
	gsiName := "gsi_" + dao.tableName + "_" + fieldUsername
	filter := &godal.FilterOptFieldOpValue{FieldName: fieldUsername, Operator: godal.FilterOpGreaterOrEqual, Value: "user5"}
	if numRows, err := dao.GdaoDeleteMany(dao.tableName+":"+gsiName, filter); err != nil {
		t.Fatalf("%s failed: %s", name, err)
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
		t.Fatalf("%s failed: %s", name+"/prepareAwsDynamodbTable", err)
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
			t.Fatalf("%s failed: %s", name+"/GdaoCreate", err)
		}
	}

	// GdaoDeleteMany should be successful and 2 rows deleted
	// use format <table-name>:<gsi-name> to filter using GSI
	// the "@" prefix instructs that GdaoDeleteMany should use "query" instead of "scan"
	gsiName := "gsi_" + dao.tableName + "_" + fieldSubject + "_" + fieldLevel
	filter := (&godal.FilterOptAnd{}).
		Add(&godal.FilterOptFieldOpValue{FieldName: fieldSubject, Operator: godal.FilterOpEqual, Value: "Subject1"}).
		Add(&godal.FilterOptFieldOpValue{FieldName: fieldLevel, Operator: godal.FilterOpGreaterOrEqual, Value: 5})
	if numRows, err := dao.GdaoDeleteMany("@"+dao.tableName+":"+gsiName, filter); err != nil {
		t.Fatalf("%s failed: %s", name, err)
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
		t.Fatalf("%s failed: %s", name+"/prepareAwsDynamodbTable", err)
	}

	// GdaoFetchOne should be successful but no row is returned
	filter := dao.GdaoCreateFilter(dao.tableName, dao.toGbo(&UserBoDynamodb{Id: "1"}))
	if gbo, err := dao.GdaoFetchOne(dao.tableName, filter); err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoFetchOne", err)
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
		t.Fatalf("%s failed: %s", name+"/GdaoCreate", err)
	}

	if gbo, err := dao.GdaoFetchOne(dao.tableName, filter); err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoFetchOne", err)
	} else if gbo == nil {
		t.Fatalf("%s failed: nil", name+"/GdaoFetchOne")
	} else {
		fetchedUser := dao.toUser(gbo)
		_compareUsers(t, name, user, fetchedUser)
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
		t.Fatalf("%s failed: %s", name+"/prepareAwsDynamodbTable", err)
	}

	usermap := make(map[string]*UserBoDynamodb)
	for i := 0; i < 10; i++ {
		user := &UserBoDynamodb{
			Id:       strconv.Itoa(i),
			Username: "user" + strconv.Itoa(9-i),
			Name:     "Thanh",
			Version:  int(time.Now().Unix()),
			Active:   i%3 == 0,
			Created:  time.Now(),
			Subject:  "Subject" + strconv.Itoa(i%4),
			Level:    i,
		}
		_, err = dao.GdaoCreate(dao.tableName, dao.toGbo(user))
		if err != nil {
			t.Fatalf("%s failed: %s", name+"/GdaoCreate", err)
		}
		usermap[user.Id] = user
	}

	filter := &godal.FilterOptFieldOpValue{FieldName: fieldId, Operator: godal.FilterOpGreaterOrEqual, Value: "5"}
	if dbRows, err := dao.GdaoFetchMany(dao.tableName, filter, nil, 0, 3); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if dbRows == nil || len(dbRows) != 3 {
		t.Fatalf("%s failed: expected %#v row(s) but received %#v", name, 3, dbRows)
	} else {
		for _, row := range dbRows {
			fetchedUser := dao.toUser(row)
			_compareUsers(t, name, usermap[fetchedUser.Id], fetchedUser)
		}
	}

	filter = &godal.FilterOptFieldOpValue{FieldName: fieldUsername, Operator: godal.FilterOpGreaterOrEqual, Value: "user5"}
	if dbRows, err := dao.GdaoFetchMany(dao.tableName, filter, nil, 0, 3); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if dbRows == nil || len(dbRows) != 3 {
		t.Fatalf("%s failed: expected %#v row(s) but received %#v", name, 3, dbRows)
	} else {
		for _, row := range dbRows {
			fetchedUser := dao.toUser(row)
			_compareUsers(t, name, usermap[fetchedUser.Id], fetchedUser)
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
		t.Fatalf("%s failed: %s", name+"/prepareAwsDynamodbTable", err)
	}

	usermap := make(map[string]*UserBoDynamodb)
	for i := 0; i < 10; i++ {
		user := &UserBoDynamodb{
			Id:       strconv.Itoa(i),
			Username: "user" + strconv.Itoa(9-i),
			Name:     "Thanh",
			Version:  int(time.Now().Unix()),
			Active:   i%3 == 0,
			Created:  time.Now(),
			Subject:  "Subject" + strconv.Itoa(i%4),
			Level:    i,
		}
		_, err = dao.GdaoCreate(dao.tableName, dao.toGbo(user))
		if err != nil {
			t.Fatalf("%s failed: %s", name+"/GdaoCreate", err)
		}
		usermap[user.Id] = user
	}

	filter := (&godal.FilterOptAnd{}).
		Add(&godal.FilterOptFieldOpValue{FieldName: fieldSubject, Operator: godal.FilterOpEqual, Value: "Subject1"}).
		Add(&godal.FilterOptFieldOpValue{FieldName: fieldLevel, Operator: godal.FilterOpGreaterOrEqual, Value: 5})
	// the "@" prefix instructs that GdaoFetchMany should use "query" instead of "scan"
	if dbRows, err := dao.GdaoFetchMany("@"+dao.tableName, filter, nil, 0, 3); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if dbRows == nil || len(dbRows) != 2 {
		t.Fatalf("%s failed: expected %#v row(s) but received %#v", name, 3, len(dbRows))
	} else {
		for _, row := range dbRows {
			fetchedUser := dao.toUser(row)
			_compareUsers(t, name, usermap[fetchedUser.Id], fetchedUser)
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
		t.Fatalf("%s failed: %s", name+"/prepareAwsDynamodbTable", err)
	}

	usermap := make(map[string]*UserBoDynamodb)
	for i := 0; i < 10; i++ {
		user := &UserBoDynamodb{
			Id:       strconv.Itoa(i),
			Username: "user" + strconv.Itoa(9-i),
			Name:     "Thanh",
			Version:  int(time.Now().Unix()),
			Active:   i%3 == 0,
			Created:  time.Now(),
			Subject:  "Subject" + strconv.Itoa(i%4),
			Level:    i,
		}
		_, err = dao.GdaoCreate(dao.tableName, dao.toGbo(user))
		if err != nil {
			t.Fatalf("%s failed: %s", name+"/GdaoCreate", err)
		}
		usermap[user.Id] = user
	}

	gsiName := "gsi_" + dao.tableName + "_" + fieldUsername

	filter := &godal.FilterOptFieldOpValue{FieldName: fieldId, Operator: godal.FilterOpGreaterOrEqual, Value: "5"}
	// format @<table-name>:<gsi-name>:<false> indicate that:
	// - @: use 'query' instead of 'scan'
	// - gsi-name: filter against GSI
	// - 'false': do not re-fetch (the returned rows may not contain all fields!)
	if dbRows, err := dao.GdaoFetchMany(dao.tableName+":"+gsiName+":false", filter, nil, 0, 3); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if dbRows == nil || len(dbRows) != 3 {
		t.Fatalf("%s failed: expected %#v row(s) but received %#v", name, 3, dbRows)
	} else {
		for _, row := range dbRows {
			fetchedUser := dao.toUser(row)
			if fetchedUser.Id != usermap[fetchedUser.Id].Id {
				t.Fatalf("%s failed: expected %#v but received %#v", name, usermap[fetchedUser.Id].Id, fetchedUser.Id)
			}
		}
	}

	filter = &godal.FilterOptFieldOpValue{FieldName: fieldUsername, Operator: godal.FilterOpGreaterOrEqual, Value: "user5"}
	// format @<table-name>:<gsi-name>:<true> indicate that:
	// - @: use 'query' instead of 'scan'
	// - gsi-name: filter against GSI
	// - 'true': re-fetch from main table as GSI does not contain all fields
	if dbRows, err := dao.GdaoFetchMany(dao.tableName+":"+gsiName+":true", filter, nil, 0, 3); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if dbRows == nil || len(dbRows) != 3 {
		t.Fatalf("%s failed: expected %#v row(s) but received %#v", name, 3, dbRows)
	} else {
		for _, row := range dbRows {
			fetchedUser := dao.toUser(row)
			_compareUsers(t, name, usermap[fetchedUser.Id], fetchedUser)
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
		t.Fatalf("%s failed: %s", name+"/prepareAwsDynamodbTable", err)
	}

	usermap := make(map[string]*UserBoDynamodb)
	for i := 0; i < 10; i++ {
		user := &UserBoDynamodb{
			Id:       strconv.Itoa(i),
			Username: "user" + strconv.Itoa(9-i),
			Name:     "Thanh",
			Version:  int(time.Now().Unix()),
			Active:   i%3 == 0,
			Created:  time.Now(),
			Subject:  "Subject" + strconv.Itoa(i%4),
			Level:    i,
		}
		_, err = dao.GdaoCreate(dao.tableName, dao.toGbo(user))
		if err != nil {
			t.Fatalf("%s failed: %s", name+"/GdaoCreate", err)
		}
		usermap[user.Id] = user
	}

	gsiName := "gsi_" + dao.tableName + "_" + fieldSubject + "_" + fieldLevel
	filter := (&godal.FilterOptAnd{}).
		Add(&godal.FilterOptFieldOpValue{FieldName: fieldSubject, Operator: godal.FilterOpEqual, Value: "Subject1"}).
		Add(&godal.FilterOptFieldOpValue{FieldName: fieldLevel, Operator: godal.FilterOpGreaterOrEqual, Value: 5})
	// format @<table-name>:<gsi-name>:<true> indicate that:
	// - @: use 'query' instead of 'scan'
	// - gsi-name: filter against GSI
	// - 'true': re-fetch from main table as GSI does not contain all fields
	if dbRows, err := dao.GdaoFetchMany("@"+dao.tableName+":"+gsiName+":true", filter, nil, 0, 3); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if dbRows == nil || len(dbRows) != 2 {
		t.Fatalf("%s failed: expected %#v row(s) but received %#v", name, 3, len(dbRows))
	} else {
		for _, row := range dbRows {
			fetchedUser := dao.toUser(row)
			_compareUsers(t, name, usermap[fetchedUser.Id], fetchedUser)
		}
	}

	// format @<table-name>:<gsi-name>:<false> indicate that:
	// - @: use 'query' instead of 'scan'
	// - gsi-name: filter against GSI
	// - 'false': do not re-fetch (the returned rows may not contain all fields!)
	if dbRows, err := dao.GdaoFetchMany("@"+dao.tableName+":"+gsiName+":false", filter, nil, 0, 3); err != nil {
		t.Fatalf("%s failed: %s", name, err)
	} else if dbRows == nil || len(dbRows) != 2 {
		t.Fatalf("%s failed: expected %#v row(s) but received %#v", name, 3, len(dbRows))
	} else {
		for _, row := range dbRows {
			fetchedUser := dao.toUser(row)
			if fetchedUser.Id != usermap[fetchedUser.Id].Id {
				t.Fatalf("%s failed: expected %#v but received %#v", name, usermap[fetchedUser.Id].Id, fetchedUser.Id)
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
		t.Fatalf("%s failed: %s", name+"/prepareAwsDynamodbTable", err)
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
		t.Fatalf("%s failed: %s", name, err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name, 1, numRows)
	}

	filter := dao.GdaoCreateFilter(dao.tableName, dao.toGbo(&UserBoDynamodb{Id: "1"}))
	if gbo, err := dao.GdaoFetchOne(dao.tableName, filter); err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoFetchOne", err)
	} else if gbo == nil {
		t.Fatalf("%s failed: nil", name+"/GdaoFetchOne")
	} else {
		fetchedUser := dao.toUser(gbo)
		_compareUsers(t, name, user, fetchedUser)
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
		t.Fatalf("%s failed: %s", name+"/prepareAwsDynamodbTable", err)
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
		t.Fatalf("%s failed: %s", name, err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name, 1, numRows)
	}
	clone := *user
	clone.Username = "thanhn"
	if numRows, err := dao.GdaoCreate(dao.tableName, dao.toGbo(&clone)); err != godal.ErrGdaoDuplicatedEntry || numRows != 0 {
		t.Fatalf("%s failed: num rows %#v / error: %s", name, numRows, err)
	}

	filter := dao.GdaoCreateFilter(dao.tableName, dao.toGbo(&UserBoDynamodb{Subject: "English", Level: 1}))
	if gbo, err := dao.GdaoFetchOne(dao.tableName, filter); err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoFetchOne", err)
	} else if gbo == nil {
		t.Fatalf("%s failed: nil", name+"/GdaoFetchOne")
	} else {
		fetchedUser := dao.toUser(gbo)
		_compareUsers(t, name, user, fetchedUser)
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
		t.Fatalf("%s failed: %s", name+"/prepareAwsDynamodbTable", err)
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
	// GdaoUpdate should be successful but no row affected
	if numRows, err := dao.GdaoUpdate(dao.tableName, dao.toGbo(user)); err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoUpdate", err)
	} else if numRows != 0 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoUpdate", 0, numRows)
	}
	if numRows, err := dao.GdaoCreate(dao.tableName, dao.toGbo(user)); err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoCreate", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoCreate", 1, numRows)
	}

	user.Username = "thanhn"
	if numRows, err := dao.GdaoUpdate(dao.tableName, dao.toGbo(user)); err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoUpdate", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoUpdate", 1, numRows)
	}

	filter := dao.GdaoCreateFilter(dao.tableName, dao.toGbo(&UserBoDynamodb{Id: "1"}))
	if gbo, err := dao.GdaoFetchOne(dao.tableName, filter); err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoFetchOne", err)
	} else if gbo == nil {
		t.Fatalf("%s failed: nil", name+"/GdaoFetchOne")
	} else {
		fetchedUser := dao.toUser(gbo)
		_compareUsers(t, name, user, fetchedUser)
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
// 		t.Fatalf("%s failed: %s", name+"/prepareAwsDynamodbTable", err)
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
// 		t.Fatalf("%s failed: %s", name+"/GdaoCreate", err)
// 	} else if numRows != 1 {
// 		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoCreate", 1, numRows)
// 	}
// 	if numRows, err := dao.GdaoCreate(dao.tableName, dao.toGbo(user2)); err != nil {
// 		t.Fatalf("%s failed: %s", name+"/GdaoCreate", err)
// 	} else if numRows != 1 {
// 		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoCreate", 1, numRows)
// 	}
//
// 	user1.Level = 2
// 	if numRows, err := dao.GdaoUpdate(dao.tableName, dao.toGbo(user1)); err != godal.ErrGdaoDuplicatedEntry || numRows != 0 {
// 		t.Fatalf("%s failed: num rows %#v / error: %s", name, numRows, err)
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
		t.Fatalf("%s failed: %s", name+"/prepareAwsDynamodbTable", err)
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
		t.Fatalf("%s failed: %s", name+"/GdaoSave", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoSave", 1, numRows)
	}

	user.Username = "thanhn"
	if numRows, err := dao.GdaoSave(dao.tableName, dao.toGbo(user)); err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoSave", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoSave", 1, numRows)
	}

	filter := dao.GdaoCreateFilter(dao.tableName, dao.toGbo(&UserBoDynamodb{Id: "1"}))
	if gbo, err := dao.GdaoFetchOne(dao.tableName, filter); err != nil {
		t.Fatalf("%s failed: %s", name+"/GdaoFetchOne", err)
	} else if gbo == nil {
		t.Fatalf("%s failed: nil", name+"/GdaoFetchOne")
	} else {
		fetchedUser := dao.toUser(gbo)
		_compareUsers(t, name, user, fetchedUser)
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
		t.Fatalf("%s failed: %s", name+"/prepareAwsDynamodbTable", err)
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
		t.Fatalf("%s failed: %s", name+"/GdaoSave", err)
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
		t.Fatalf("%s failed: %s", name+"/GdaoSave", err)
	} else if numRows != 1 {
		t.Fatalf("%s failed: expected %#v row(s) inserted but received %#v", name+"/GdaoSave", 1, numRows)
	}

	filter := godal.FilterOptFieldOpValue{FieldName: fieldId, Operator: godal.FilterOpEqual, Value: "1"}
	if gbo, err := dao.GdaoFetchOne(dao.tableName, filter); err != nil {
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
