/*
Package dynamodb provides a generic AWS DynamoDB implementation of godal.IGenericDao.

General guideline:

	- Dao must implement IGenericDao.GdaoCreateFilter(string, IGenericBo) interface{}.
	- Row-mapper's function 'ColumnsList(table string) []string' must return all attribute names of specified table's primary key.

Guideline: Use GenericDaoDynamodb (and godal.IGenericBo) directly

	- Define a dao struct that implements IGenericDao.GdaoCreateFilter(string, IGenericBo) interface{}.
	- Use a row-mapper whose 'ColumnsList(table string) []string' must return all attribute names of specified table's primary key.
	- Optionally, create a helper function to create dao instances.

	import (
		//"github.com/aws/aws-sdk-go/aws"
		//"github.com/aws/aws-sdk-go/aws/credentials"
		//"github.com/aws/aws-sdk-go/service/dynamodb"
		//"github.com/aws/aws-sdk-go/service/dynamodb/expression"

		"github.com/btnguyen2k/consu/reddo"
		"github.com/btnguyen2k/godal"
		gdaodynamodb "github.com/btnguyen2k/godal/dynamodb"
		"github.com/btnguyen2k/prom"
	)

	type myGenericDaoDynamodb struct {
		*gdaodynamodb.GenericDaoDynamodb
	}

	// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter.
	func (dao *myGenericDaoDynamodb) GdaoCreateFilter(table string, bo godal.IGenericBo) interface{} {
		return map[string]interface{}{fieldId: bo.GboGetAttrUnsafe(fieldId, reddo.TypeString)}
	}

	// newGenericDaoDynamodb is convenient method to create myGenericDaoDynamodb instances.
	func newGenericDaoDynamodb(adc *prom.AwsDynamodbConnect, tableName string) godal.IGenericDao {
		dao := &myGenericDaoDynamodb{}
		dao.GenericDaoDynamodb = gdaodynamodb.NewGenericDaoDynamodb(adc, godal.NewAbstractGenericDao(dao))
		dao.SetRowMapper(&gdaodynamodb.GenericRowMapperDynamodb{ColumnsListMap: map[string][]string{tableName: {fieldId}}})
		return dao
	}

	Since AWS DynamoDB is schema-less, GenericRowMapperDynamodb should be sufficient. However, it must be configured so that
	its function 'ColumnsList(table string) []string' returns all attribute names of specified table's primary key.

Guideline: Implement custom AWS DynamoDB business dao and bo

	- Define and implement the business dao (Note: dao must implement IGenericDao.GdaoCreateFilter(string, IGenericBo) interface{}).
	- Optionally, create a helper function to create dao instances.
	- Define functions to transform godal.IGenericBo to business bo and vice versa.

	import (
		//"github.com/aws/aws-sdk-go/aws"
		//"github.com/aws/aws-sdk-go/aws/credentials"
		//"github.com/aws/aws-sdk-go/service/dynamodb"
		//"github.com/aws/aws-sdk-go/service/dynamodb/expression"

		"github.com/btnguyen2k/consu/reddo"
		"github.com/btnguyen2k/godal"
		gdaodynamodb "github.com/btnguyen2k/godal/dynamodb"
		"github.com/btnguyen2k/prom"
	)

	// BoApp defines business object app
	type BoApp struct {
		Id            string                 `json:"id"`
		Description   string                 `json:"desc"`
		Value         int                    `json:"val"`
	}

	func (app *BoApp) ToGbo() godal.IGenericBo {
		gbo := godal.NewGenericBo()

		// method 1: populate attributes one by one
		gbo.GboSetAttr("id"  , app.Id)
		gbo.GboSetAttr("desc", app.Description)
		gbo.GboSetAttr("val" , app.Value)

		// method 2: transfer all attributes at once
		if err := gbo.GboImportViaJson(app); err!=nil {
			panic(err)
		}

		return gbo
	}

	func NewBoAppFromGbo(gbo godal.IGenericBo) *BoApp {
		app := BoApp{}

		// method 1: populate attributes one by one
		app.Id          = gbo.GboGetAttrUnsafe("id", reddo.TypeString).(string)
		app.Description = gbo.GboGetAttrUnsafe("desc", reddo.TypeString).(string)
		app.Value       = int(gbo.GboGetAttrUnsafe("val", reddo.TypeInt).(int64))

		// method 2: transfer all attributes at once
		if err := gbo.GboTransferViaJson(&app); err!=nil {
			panic(err)
		}

		return &app
	}

	// DaoAppDynamodb is AWS DynamoDB-implementation of business dao
	type DaoAppDynamodb struct {
		*gdaodynamod.GenericDaoDynamodb
		tableName string
	}

	// NewDaoAppDynamodb is convenient method to create DaoAppDynamodb instances.
	func NewDaoAppDynamodb(adc *prom.AwsDynamodbConnect, tableName string) *NewDaoAppDynamodb {
		dao := &DaoAppDynamodb{tableName: tableName}
		dao.GenericDaoDynamodb = gdaodynamod.NewGenericDaoDynamodb(adc, godal.NewAbstractGenericDao(dao))
		dao.SetRowMapper(&gdaodynamod.GenericRowMapperDynamodb{ColumnsListMap: map[string][]string{tableName: {"id"}}})
		return dao
	}

	Since AWS DynamoDB is schema-less, GenericRowMapperDynamodb should be sufficient. However, it must be configured so that
	its function 'ColumnsList(table string) []string' must return all attribute names of specified table's primary key.

See more examples in 'examples' directory on project's GitHub: https://github.com/btnguyen2k/godal/tree/master/examples

To create prom.AwsDynamodbConnect, see package github.com/btnguyen2k/prom
*/
package dynamodb

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/prom"

	"github.com/btnguyen2k/godal"
)

// GenericRowMapperDynamodb is a generic implementation of godal.IRowMapper for AWS DynamoDB.
//
// Implementation rules:
// 	 - ToRow        : transform godal.IGenericBo "as-is" to map[string]interface{}.
// 	 - ToBo         : expect input is a map[string]interface{}, or JSON data (string or array/slice of bytes), transforms input to godal.IGenericBo via JSON unmarshalling.
// 	 - ColumnsList  : look up column-list from a 'columns-list map' (AWS DynamoDB is schema-free but key-attributes are significant) and returns it.
//   - ToDbColName  : return the input field name "as-is".
//   - ToBoFieldName: return the input column name "as-is".
//
// Available: since v0.2.0
type GenericRowMapperDynamodb struct {
	// ColumnsListMap holds mappings of {table-name:[list of key attribute names]}
	ColumnsListMap map[string][]string
}

// ToRow implements godal.IRowMapper.ToRow.
//
// This function transforms godal.IGenericBo to map[string]interface{}. Field names are kept intact.
func (mapper *GenericRowMapperDynamodb) ToRow(_ string, bo godal.IGenericBo) (interface{}, error) {
	if bo == nil {
		return nil, nil
	}
	result := make(map[string]interface{})
	return result, bo.GboTransferViaJson(&result)
}

// ToBo implements godal.IRowMapper.ToBo.
//
// This function expects input to be a map[string]interface{}, or JSON data (string or array/slice of bytes), transforms it to godal.IGenericBo via JSON unmarshalling. Field names are kept intact.
func (mapper *GenericRowMapperDynamodb) ToBo(table string, row interface{}) (godal.IGenericBo, error) {
	if row == nil {
		return nil, nil
	}
	switch row.(type) {
	case map[string]interface{}:
		bo := godal.NewGenericBo()
		for k, v := range row.(map[string]interface{}) {
			bo.GboSetAttr(k, v)
		}
		return bo, nil
	case string:
		bo := godal.NewGenericBo()
		return bo, bo.GboFromJson([]byte(row.(string)))
	case *string:
		if row.(*string) == nil {
			return nil, nil
		}
		bo := godal.NewGenericBo()
		return bo, bo.GboFromJson([]byte(*row.(*string)))
	case []byte:
		if row.([]byte) == nil {
			return nil, nil
		}
		bo := godal.NewGenericBo()
		return bo, bo.GboFromJson(row.([]byte))
	case *[]byte:
		if row.(*[]byte) == nil {
			return nil, nil
		}
		return mapper.ToBo(table, *row.(*[]byte))
	}

	v := reflect.ValueOf(row)
	for ; v.Kind() == reflect.Ptr; v = v.Elem() {
	}
	switch v.Kind() {
	case reflect.Map:
		if v.IsNil() {
			return nil, nil
		}
		bo := godal.NewGenericBo()
		for iter := v.MapRange(); iter.Next(); {
			key, _ := reddo.ToString(iter.Key().Interface())
			bo.GboSetAttr(key, iter.Value().Interface())
		}
		return bo, nil
	case reflect.String:
		bo := godal.NewGenericBo()
		return bo, bo.GboFromJson([]byte(v.Interface().(string)))
	case reflect.Slice, reflect.Array:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			// input is []byte
			zero := make([]byte, 0)
			arr, err := reddo.ToSlice(v.Interface(), reflect.TypeOf(zero))
			if err != nil || arr.([]byte) == nil || len(arr.([]byte)) == 0 {
				return nil, err
			}
			bo := godal.NewGenericBo()
			return bo, bo.GboFromJson(arr.([]byte))
		}
	case reflect.Interface:
		return mapper.ToBo(table, v.Interface())
	case reflect.Invalid:
		return nil, nil
	}
	return nil, fmt.Errorf("cannot construct godal.IGenericBo from input %v", row)
}

// ColumnsList implements godal.IRowMapper.ColumnsList.
//
// This function looks up column-list from a 'columns-list map' (AWS DynamoDB is schema-free but key-attributes are significant) and returns it.
func (mapper *GenericRowMapperDynamodb) ColumnsList(table string) []string {
	if result, ok := mapper.ColumnsListMap[table]; ok {
		return result
	}
	return nil
}

// ToDbColName implements godal.IRowMapper.ToDbColName.
//
// This function returns the input field name "as-is".
func (mapper *GenericRowMapperDynamodb) ToDbColName(_, fieldName string) string {
	return fieldName
}

// ToBoFieldName implements godal.IRowMapper.ToBoFieldName.
//
// This function returns the input column name "as-is".
func (mapper *GenericRowMapperDynamodb) ToBoFieldName(_, colName string) string {
	return colName
}

var (
	// GenericRowMapperDynamodbInstance is a pre-created instance of GenericRowMapperDynamodb that is ready to use.
	GenericRowMapperDynamodbInstance godal.IRowMapper = &GenericRowMapperDynamodb{}
)

/*--------------------------------------------------------------------------------*/

// NewGenericDaoDynamodb constructs a new AWS DynamoDB implementation of 'godal.IGenericDao'.
func NewGenericDaoDynamodb(dynamodbConnect *prom.AwsDynamodbConnect, agdao *godal.AbstractGenericDao) *GenericDaoDynamodb {
	dao := &GenericDaoDynamodb{AbstractGenericDao: agdao, dynamodbConnect: dynamodbConnect}
	if dao.GetRowMapper() == nil {
		dao.SetRowMapper(GenericRowMapperDynamodbInstance)
	}
	return dao
}

// GenericDaoDynamodb is AWS DynamoDB implementation of godal.IGenericDao.
//
// Function implementations (n = No, y = Yes, i = inherited):
// 	 - (n) GdaoCreateFilter(storageId string, bo godal.IGenericBo) interface{}
// 	 - (y) GdaoDelete(storageId string, bo godal.IGenericBo) (int, error)
// 	 - (y) GdaoDeleteMany(storageId string, filter interface{}) (int, error)
// 	 - (y) GdaoFetchOne(storageId string, filter interface{}) (godal.IGenericBo, error)
// 	 - (y) GdaoFetchMany(storageId string, filter interface{}, sorting interface{}, startOffset, numItems int) ([]godal.IGenericBo, error)
// 	 - (y) GdaoCreate(storageId string, bo godal.IGenericBo) (int, error)
// 	 - (y) GdaoUpdate(storageId string, bo godal.IGenericBo) (int, error)
// 	 - (y) GdaoSave(storageId string, bo godal.IGenericBo) (int, error)
//
// Available: since v0.2.0
type GenericDaoDynamodb struct {
	*godal.AbstractGenericDao
	dynamodbConnect *prom.AwsDynamodbConnect
}

// GetAwsDynamodbConnect returns the '*prom.AwsDynamodbConnect' instance attached to this DAO.
func (dao *GenericDaoDynamodb) GetAwsDynamodbConnect() *prom.AwsDynamodbConnect {
	return dao.dynamodbConnect
}

// SetAwsDynamodbConnect attaches a '*prom.AwsDynamodbConnect' instance to this DAO.
func (dao *GenericDaoDynamodb) SetAwsDynamodbConnect(adc *prom.AwsDynamodbConnect) *GenericDaoDynamodb {
	dao.dynamodbConnect = adc
	return dao
}

func (dao *GenericDaoDynamodb) extractKeysAttributes(table string, item prom.AwsDynamodbItem) map[string]interface{} {
	cols := dao.GetRowMapper().ColumnsList(table)
	if cols == nil {
		return nil
	}
	keysAttrs := make(map[string]interface{})
	for _, k := range cols {
		keysAttrs[k] = item[k]
	}
	return keysAttrs
}

// toConditionBuilder builds a ConditionBuilder from input.
//
//   - if input is expression.ConditionBuilder or *expression.ConditionBuilder: return it as *expression.ConditionBuilder.
// 	 - if input is string, slice/array of bytes: assume input is a map in JSON, convert it to map to build ConditionBuilder.
// 	 - if input is a map: build an "and" condition connecting sub-conditions where each sub-condition is an "equal" condition built from map entry.
func toConditionBuilder(input interface{}) (*expression.ConditionBuilder, error) {
	if input == nil {
		return nil, nil
	}
	switch input.(type) {
	case expression.ConditionBuilder:
		result := input.(expression.ConditionBuilder)
		return &result, nil
	case *expression.ConditionBuilder:
		return input.(*expression.ConditionBuilder), nil
	}
	v := reflect.ValueOf(input)
	for ; v.Kind() == reflect.Ptr; v = v.Elem() {
	}
	switch v.Kind() {
	case reflect.String:
		// expect input to be a map in JSON
		result := make(map[string]interface{})
		if err := json.Unmarshal([]byte(v.Interface().(string)), &result); err != nil {
			return nil, err
		}
		return toConditionBuilder(result)
	case reflect.Array, reflect.Slice:
		// expect input to be a map in JSON
		t, err := reddo.ToSlice(v.Interface(), reflect.TypeOf(byte(0)))
		if err != nil {
			return nil, err
		}
		result := make(map[string]interface{})
		if err := json.Unmarshal(t.([]byte), &result); err != nil {
			return nil, err
		}
		return toConditionBuilder(result)
	case reflect.Map:
		m, err := reddo.ToMap(v.Interface(), reflect.TypeOf(make(map[string]interface{})))
		if err != nil {
			return nil, err
		}
		var result *expression.ConditionBuilder = nil
		for k, v := range m.(map[string]interface{}) {
			if result == nil {
				t := expression.Name(k).Equal(expression.Value(v))
				result = &t
			} else {
				t := result.And(expression.Name(k).Equal(expression.Value(v)))
				result = &t
			}
		}
		return result, err
	}
	return nil, fmt.Errorf("cannot convert %v to *expression.ConditionBuilder", input)
}

func toMap(input interface{}) (map[string]interface{}, error) {
	if input == nil {
		return nil, nil
	}
	switch input.(type) {
	case map[string]interface{}:
		return input.(map[string]interface{}), nil
	case *map[string]interface{}:
		return *input.(*map[string]interface{}), nil
	}
	v := reflect.ValueOf(input)
	for ; v.Kind() == reflect.Ptr; v = v.Elem() {
	}
	switch v.Kind() {
	case reflect.String:
		// expect input to be a map in JSON
		result := make(map[string]interface{})
		err := json.Unmarshal([]byte(v.Interface().(string)), &result)
		return result, err
	case reflect.Array, reflect.Slice:
		// expect input to be a map in JSON
		t, err := reddo.ToSlice(v.Interface(), reflect.TypeOf(byte(0)))
		if err != nil {
			return nil, err
		}
		result := make(map[string]interface{})
		return result, json.Unmarshal(t.([]byte), &result)
	case reflect.Map:
		result, err := reddo.ToMap(v.Interface(), reflect.TypeOf(make(map[string]interface{})))
		return result.(map[string]interface{}), err

	}
	return nil, fmt.Errorf("cannot convert %v to map[string]interface{}", input)
}

/*----------------------------------------------------------------------*/

// GdaoDelete implements godal.IGenericDao.GdaoDelete.
func (dao *GenericDaoDynamodb) GdaoDelete(table string, bo godal.IGenericBo) (int, error) {
	return dao.GdaoDeleteWithContext(nil, table, bo)
}

// GdaoDeleteWithContext is AWS DynamoDB variant of GdaoDelete.
func (dao *GenericDaoDynamodb) GdaoDeleteWithContext(ctx aws.Context, table string, bo godal.IGenericBo) (int, error) {
	keyFilter, err := toMap(dao.GdaoCreateFilter(table, bo))
	if err != nil {
		return 0, err
	}
	deleteInput, err := dao.dynamodbConnect.BuildDeleteItemInput(table, keyFilter, nil)
	if err != nil {
		return 0, err
	}
	deleteResult, err := dao.dynamodbConnect.DeleteItemWithInput(ctx, deleteInput.SetReturnValues("ALL_OLD"))
	numRows := 0
	if deleteResult != nil && deleteResult.Attributes != nil {
		numRows = 1
	}
	return numRows, err
}

// GdaoDeleteMany implements godal.IGenericDao.GdaoDeleteMany.
//
//   - table name format: <table_name>[:<index_name>]:
//       - table_name: name of the table to delete rows from.
//       - index_name: (optional) name of the table's index (local or global) to search for rows.
//   - filter can be a expression.ConditionBuilder (or pointer to it) or a map[string]interface{} (it can be a string/[]byte representing map[string]interface{} in JSON).
//     If filter is a map[string]interface{}, it is used to build an "and" condition connecting sub-conditions where each sub-condition is an "equal" condition built from map entry.
//     nil filter means "match all".
//
// This function uses "scan" operation by default, which is expensive! To force "query" operation, prefix the table name with character @.
func (dao *GenericDaoDynamodb) GdaoDeleteMany(table string, filter interface{}) (int, error) {
	return dao.GdaoDeleteManyWithContext(nil, table, filter)
}

// GdaoDeleteManyWithContext is is AWS DynamoDB variant of GdaoDeleteMany.
func (dao *GenericDaoDynamodb) GdaoDeleteManyWithContext(ctx aws.Context, table string, filter interface{}) (int, error) {
	f, err := toConditionBuilder(filter)
	if err != nil {
		return 0, err
	}
	tokens := strings.Split(table, ":")
	tableName := tokens[0]
	useScan := true
	if strings.HasPrefix(tableName, "@") {
		useScan = false
		tableName = strings.TrimPrefix(tableName, "@")
	}
	indexName := ""
	if len(tokens) > 1 {
		indexName = tokens[1]
	}
	count := 0
	callbackFunc := func(item prom.AwsDynamodbItem, lastEvaluatedKey map[string]*dynamodb.AttributeValue) (b bool, e error) {
		keyFilter := dao.extractKeysAttributes(tableName, item)
		_, err := dao.dynamodbConnect.DeleteItem(ctx, tableName, keyFilter, nil)
		if err == nil {
			count++
		}
		return true, err
	}
	if useScan {
		err = dao.dynamodbConnect.ScanItemsWithCallback(ctx, tableName, f, indexName, nil, callbackFunc)
	} else {
		err = dao.dynamodbConnect.QueryItemsWithCallback(ctx, tableName, f, nil, indexName, nil, callbackFunc)
	}
	return count, err
}

// GdaoFetchOne implements godal.IGenericDao.GdaoFetchOne.
//
// 'keyFilter' should be a map[string]interface{}, or it can be a string/[]byte representing map[string]interface{} in JSON, then it is unmarshalled to map[string]interface{}.
func (dao *GenericDaoDynamodb) GdaoFetchOne(table string, keyFilter interface{}) (godal.IGenericBo, error) {
	return dao.GdaoFetchOneWithContext(nil, table, keyFilter)
}

// GdaoFetchOneWithContext is is AWS DynamoDB variant of GdaoFetchOne.
func (dao *GenericDaoDynamodb) GdaoFetchOneWithContext(ctx aws.Context, table string, keyFilter interface{}) (godal.IGenericBo, error) {
	if f, err := toMap(keyFilter); err != nil {
		return nil, err
	} else if item, err := dao.dynamodbConnect.GetItem(ctx, table, f); err != nil {
		return nil, err
	} else {
		return dao.GetRowMapper().ToBo(table, item)
	}
}

// GdaoFetchMany implements godal.IGenericDao.GdaoFetchMany.
//
//   - table name format: <table_name>[:<index_name>[:<refetch-from-table:true/false>]]:
//       - table_name: name of the table to fetch data from.
//       - index_name: (optional) name of the table's index (local or global) to fetch data from.
//       - refetch-from-table: (optional) true/false - default: false; when fetching data from index, if 'false' only projected fields are returned,.
//         if 'true' another read is made to fetch the whole item from table (additional read capacity is consumed!)
//   - filter can be a expression.ConditionBuilder (or pointer to it) or a map[string]interface{} (it can be a string/[]byte representing map[string]interface{} in JSON).
//     If filter is a map[string]interface{}, it is used to build an "and" condition connecting sub-conditions where each sub-condition is an "equal" condition built from map entry.
//     nil filter means "match all".
//   - sorting will not be used as DynamoDB does not currently support custom sorting of queried items.
//
// This function uses "scan" operation by default, which is expensive! To force "query" operation, prefix the table name with character @.
func (dao *GenericDaoDynamodb) GdaoFetchMany(table string, filter interface{}, sorting interface{}, startOffset, numItems int) ([]godal.IGenericBo, error) {
	return dao.GdaoFetchManyWithContext(nil, table, filter, sorting, startOffset, numItems)
}

// GdaoFetchManyWithContext is is AWS DynamoDB variant of GdaoFetchMany.
func (dao *GenericDaoDynamodb) GdaoFetchManyWithContext(ctx aws.Context, table string, filter interface{}, sorting interface{}, startOffset, numItems int) ([]godal.IGenericBo, error) {
	f, err := toConditionBuilder(filter)
	if err != nil {
		return nil, err
	}
	result := make([]godal.IGenericBo, 0)
	tokens := strings.Split(table, ":")
	tableName := tokens[0]
	useScan := true
	if strings.HasPrefix(tableName, "@") {
		useScan = false
		tableName = strings.TrimPrefix(tableName, "@")
	}
	indexName := ""
	if len(tokens) > 1 {
		indexName = tokens[1]
	}
	refetchFromTable := false
	if len(tokens) > 2 {
		if refetchFromTable, err = strconv.ParseBool(tokens[2]); err != nil {
			refetchFromTable = false
		}
	}

	myOffset := -1
	myCounter := 0
	callbackFunc := func(item prom.AwsDynamodbItem, lastEvaluatedKey map[string]*dynamodb.AttributeValue) (b bool, e error) {
		myOffset++
		if myOffset < startOffset {
			return true, nil
		}
		if refetchFromTable {
			pkAttrs := dao.extractKeysAttributes(tableName, item)
			if item, err = dao.dynamodbConnect.GetItem(ctx, tableName, pkAttrs); err != nil {
				return false, err
			}
		}
		gbo, err := dao.GetRowMapper().ToBo(table, item)
		if err != nil {
			return false, err
		}
		result = append(result, gbo)
		myCounter++
		if numItems > 0 && myCounter >= numItems {
			return false, nil
		}
		return true, nil
	}
	if useScan {
		err = dao.dynamodbConnect.ScanItemsWithCallback(ctx, tableName, f, indexName, nil, callbackFunc)
	} else {
		err = dao.dynamodbConnect.QueryItemsWithCallback(ctx, tableName, f, nil, indexName, nil, callbackFunc)
	}

	return result, err
}

// GdaoCreate implements godal.IGenericDao.GdaoCreate.
func (dao *GenericDaoDynamodb) GdaoCreate(table string, bo godal.IGenericBo) (int, error) {
	return dao.GdaoCreateWithContext(nil, table, bo)
}

// GdaoCreateWithContext is is AWS DynamoDB variant of GdaoCreate.
func (dao *GenericDaoDynamodb) GdaoCreateWithContext(ctx aws.Context, table string, bo godal.IGenericBo) (int, error) {
	pkAttrs := dao.GetRowMapper().ColumnsList(table)
	if pkAttrs == nil || len(pkAttrs) == 0 {
		return 0, fmt.Errorf("cannot find primary-key attribute list for table [%s]", table)
	}
	item, err := dao.GetRowMapper().ToRow(table, bo)
	if err != nil {
		return 0, err
	}
	createResult, err := dao.dynamodbConnect.PutItemIfNotExist(ctx, table, item, pkAttrs)
	if prom.IsAwsError(err, dynamodb.ErrCodeConditionalCheckFailedException) || createResult == nil {
		return 0, godal.GdaoErrorDuplicatedEntry
	}
	return 1, err
}

// GdaoUpdate implements godal.IGenericDao.GdaoUpdate.
//
// Note: due to the nature of AWS DynamoDB, this function does not return godal.GdaoErrorDuplicatedEntry.
func (dao *GenericDaoDynamodb) GdaoUpdate(table string, bo godal.IGenericBo) (int, error) {
	return dao.GdaoUpdateWithContext(nil, table, bo)
}

// GdaoUpdateWithContext is is AWS DynamoDB variant of GdaoUpdate.
func (dao *GenericDaoDynamodb) GdaoUpdateWithContext(ctx aws.Context, table string, bo godal.IGenericBo) (int, error) {
	var keyFilter, itemMap map[string]interface{}
	var err error
	if keyFilter, err = toMap(dao.GdaoCreateFilter(table, bo)); err != nil {
		return 0, err
	}
	var item interface{}
	if item, err = dao.GetRowMapper().ToRow(table, bo); err != nil {
		return 0, err
	} else if itemMap, err = toMap(item); err != nil {
		return 0, err
	}

	pkAttrs := dao.GetRowMapper().ColumnsList(table)
	if pkAttrs == nil || len(pkAttrs) == 0 {
		return 0, fmt.Errorf("cannot find primary-key attribute list for table [%s]", table)
	}
	for _, pk := range pkAttrs {
		// remove primary key attributes from update list
		delete(itemMap, pk)
	}
	condition := prom.AwsDynamodbExistsAllBuilder(pkAttrs)
	if _, err = dao.dynamodbConnect.UpdateItem(ctx, table, keyFilter, condition, nil, itemMap, nil, nil); err != nil {
		err = prom.AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeConditionalCheckFailedException)
		return 0, err
	}
	return 1, nil
}

// GdaoSave implements godal.IGenericDao.GdaoSave.
//
// Note: due to the nature of AWS DynamoDB, this function does not return godal.GdaoErrorDuplicatedEntry.
func (dao *GenericDaoDynamodb) GdaoSave(table string, bo godal.IGenericBo) (int, error) {
	return dao.GdaoSaveWithContext(nil, table, bo)
}

// GdaoSaveWithContext is is AWS DynamoDB variant of GdaoSave.
func (dao *GenericDaoDynamodb) GdaoSaveWithContext(ctx aws.Context, table string, bo godal.IGenericBo) (int, error) {
	pkAttrs := dao.GetRowMapper().ColumnsList(table)
	if pkAttrs == nil || len(pkAttrs) == 0 {
		return 0, fmt.Errorf("cannot find primary-key attribute list for table [%s]", table)
	}
	item, err := dao.GetRowMapper().ToRow(table, bo)
	if err != nil {
		return 0, err
	}
	_, err = dao.dynamodbConnect.PutItem(ctx, table, item, nil)
	return 1, err
}
