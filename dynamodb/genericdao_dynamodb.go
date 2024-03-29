/*
Package dynamodb provides a generic AWS DynamoDB implementation of godal.IGenericDao.

General guideline:

	- DAOs must implement IGenericDao.GdaoCreateFilter(string, IGenericBo) FilterOpt.
	- Row-mapper's function 'ColumnsList(table string) []string' must return all attribute names of specified table's primary key.

Guideline: Use GenericDaoDynamodb (and godal.IGenericBo) directly

	- Define a DAO struct that implements IGenericDao.GdaoCreateFilter(string, IGenericBo) FilterOpt.
	- Use a row-mapper whose 'ColumnsList(table string) []string' returns all attribute names of specified table's primary key.
	- Optionally, create a helper function to create DAO instances.

	import (
		//"github.com/aws/aws-sdk-go/aws"
		//"github.com/aws/aws-sdk-go/aws/credentials"
		//awsdynamodb "github.com/aws/aws-sdk-go/service/dynamodb"
		//"github.com/aws/aws-sdk-go/service/dynamodb/expression"

		"github.com/btnguyen2k/consu/reddo"
		"github.com/btnguyen2k/godal"
		godaldynamodb "github.com/btnguyen2k/godal/dynamodb"
		promdynamodb "github.com/btnguyen2k/prom/dynamodb"
	)

	type myGenericDaoDynamodb struct {
		*godaldynamodb.GenericDaoDynamodb
	}

	// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter.
	func (dao *myGenericDaoDynamodb) GdaoCreateFilter(table string, bo godal.IGenericBo) godal.FilterOpt {
		id := bo.GboGetAttrUnsafe(fieldId, reddo.TypeString)
		return &godal.FilterOptFieldOpValue{FieldName: fieldId, Operator: godal.FilterOpEqual, Value: id}
	}

	// newGenericDaoDynamodb is convenient method to create myGenericDaoDynamodb instances.
	func newGenericDaoDynamodb(adc *promdynamodb.AwsDynamodbConnect, tableName string) godal.IGenericDao {
		dao := &myGenericDaoDynamodb{}
		dao.GenericDaoDynamodb = godaldynamodb.NewGenericDaoDynamodb(adc, godal.NewAbstractGenericDao(dao))
		dao.SetRowMapper(&godaldynamodb.GenericRowMapperDynamodb{ColumnsListMap: map[string][]string{tableName: {fieldId}}})
		return dao
	}

	Since AWS DynamoDB is schema-less, GenericRowMapperDynamodb should be sufficient. However, it must be configured so that
	its function 'ColumnsList(table string) []string' returns all attribute names of specified table's primary key.

Guideline: Implement custom AWS DynamoDB business DAOs and BOs

	- Define and implement the business dao (Note: DAOs must implement IGenericDao.GdaoCreateFilter(string, IGenericBo) FilterOpt).
	- Optionally, create a helper function to create DAO instances.
	- Define functions to transform godal.IGenericBo to business BO and vice versa.

	import (
		//"github.com/aws/aws-sdk-go/aws"
		//"github.com/aws/aws-sdk-go/aws/credentials"
		//awsdynamodb "github.com/aws/aws-sdk-go/service/dynamodb"
		//"github.com/aws/aws-sdk-go/service/dynamodb/expression"

		"github.com/btnguyen2k/consu/reddo"
		"github.com/btnguyen2k/godal"
		godaldynamodb "github.com/btnguyen2k/godal/dynamodb"
		promdynamodb "github.com/btnguyen2k/prom/dynamodb"
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
		*godaldynamodb.GenericDaoDynamodb
		tableName string
	}

	// NewDaoAppDynamodb is convenient method to create DaoAppDynamodb instances.
	func NewDaoAppDynamodb(adc *promdynamodb.AwsDynamodbConnect, tableName string) *NewDaoAppDynamodb {
		dao := &DaoAppDynamodb{tableName: tableName}
		dao.GenericDaoDynamodb = godaldynamodb.NewGenericDaoDynamodb(adc, godal.NewAbstractGenericDao(dao))
		dao.SetRowMapper(&godaldynamodb.GenericRowMapperDynamodb{ColumnsListMap: map[string][]string{tableName: {"id"}}})
		return dao
	}

	Since AWS DynamoDB is schema-less, GenericRowMapperDynamodb should be sufficient. However, it must be configured so that
	its function 'ColumnsList(table string) []string' must return all attribute names of specified table's primary key.

See more examples in 'examples' directory on project's GitHub: https://github.com/btnguyen2k/godal/tree/master/examples

To create AwsDynamodbConnect instances, see package github.com/btnguyen2k/prom/dynamodb
*/
package dynamodb

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	awsdynamodb "github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/prom/dynamodb"
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
	case *map[string]interface{}:
		// unwrap if pointer
		m := row.(*map[string]interface{})
		if m == nil {
			return nil, nil
		}
		return mapper.ToBo(table, *m)
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
		// unwrap if pointer
		s := row.(*string)
		if s == nil {
			return nil, nil
		}
		return mapper.ToBo(table, *s)
	case []byte:
		if row.([]byte) == nil {
			return nil, nil
		}
		bo := godal.NewGenericBo()
		return bo, bo.GboFromJson(row.([]byte))
	case *[]byte:
		// unwrap if pointer
		ba := row.(*[]byte)
		if ba == nil {
			return nil, nil
		}
		return mapper.ToBo(table, *ba)
	}

	v := reflect.ValueOf(row)
	for ; v.Kind() == reflect.Ptr; v = v.Elem() {
		// unwrap if pointer
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
func NewGenericDaoDynamodb(dynamodbConnect *dynamodb.AwsDynamodbConnect, agdao *godal.AbstractGenericDao) *GenericDaoDynamodb {
	dao := &GenericDaoDynamodb{AbstractGenericDao: agdao, dynamodbConnect: dynamodbConnect}
	if dao.GetRowMapper() == nil {
		dao.SetRowMapper(GenericRowMapperDynamodbInstance)
	}
	return dao
}

// GenericDaoDynamodb is AWS DynamoDB implementation of godal.IGenericDao.
//
// Function implementations (n = No, y = Yes, i = inherited):
// 	 - (n) GdaoCreateFilter(tableName string, bo godal.IGenericBo) godal.FilterOpt
// 	 - (y) GdaoDelete(tableName string, bo godal.IGenericBo) (int, error)
// 	 - (y) GdaoDeleteMany(tableName string, filter godal.FilterOpt) (int, error)
// 	 - (y) GdaoFetchOne(tableName string, filter godal.FilterOpt) (godal.IGenericBo, error)
// 	 - (y) GdaoFetchMany(tableName string, filter godal.FilterOpt, sorting *godal.SortingOpt, startOffset, numItems int) ([]godal.IGenericBo, error)
// 	 - (y) GdaoCreate(tableName string, bo godal.IGenericBo) (int, error)
// 	 - (y) GdaoUpdate(tableName string, bo godal.IGenericBo) (int, error)
// 	 - (y) GdaoSave(tableName string, bo godal.IGenericBo) (int, error)
//
// Available: since v0.2.0
type GenericDaoDynamodb struct {
	*godal.AbstractGenericDao
	dynamodbConnect *dynamodb.AwsDynamodbConnect
}

// GetAwsDynamodbConnect returns the '*dynamodb.AwsDynamodbConnect' instance attached to this DAO.
func (dao *GenericDaoDynamodb) GetAwsDynamodbConnect() *dynamodb.AwsDynamodbConnect {
	return dao.dynamodbConnect
}

// SetAwsDynamodbConnect attaches a '*dynamodb.AwsDynamodbConnect' instance to this DAO.
func (dao *GenericDaoDynamodb) SetAwsDynamodbConnect(adc *dynamodb.AwsDynamodbConnect) *GenericDaoDynamodb {
	dao.dynamodbConnect = adc
	return dao
}

func (dao *GenericDaoDynamodb) extractKeysAttributes(table string, item dynamodb.AwsDynamodbItem) map[string]interface{} {
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

// toFilterMap translates a godal.FilterOpt to DynamoDB-compatible filter map.
func toFilterMap(filter godal.FilterOpt) (map[string]interface{}, error) {
	if filter == nil {
		return nil, nil
	}
	switch filter.(type) {
	case godal.FilterOptFieldOpValue:
		f := filter.(godal.FilterOptFieldOpValue)
		return toFilterMap(&f)
	case *godal.FilterOptFieldOpValue:
		f := filter.(*godal.FilterOptFieldOpValue)
		if f.Operator != godal.FilterOpEqual {
			return nil, fmt.Errorf("invalid operator \"%#v\", only accept FilterOptFieldOpValue with operator FilterOpEqual", f.Operator)
		}
		return map[string]interface{}{f.FieldName: f.Value}, nil
	case godal.FilterOptFieldIsNull:
		f := filter.(godal.FilterOptFieldIsNull)
		return toFilterMap(&f)
	case *godal.FilterOptFieldIsNull:
		f := filter.(*godal.FilterOptFieldIsNull)
		return map[string]interface{}{f.FieldName: nil}, nil
	case godal.FilterOptAnd:
		f := filter.(godal.FilterOptAnd)
		return toFilterMap(&f)
	case *godal.FilterOptAnd:
		f := filter.(*godal.FilterOptAnd)
		result := make(map[string]interface{})
		for _, inner := range f.Filters {
			innerF, err := toFilterMap(inner)
			if err != nil {
				return nil, err
			}
			for k, v := range innerF {
				result[k] = v
			}
		}
		return result, nil
	}
	return nil, fmt.Errorf("cannot build filter map from %T", filter)
}

/*----------------------------------------------------------------------*/

// GdaoDelete implements godal.IGenericDao.GdaoDelete.
func (dao *GenericDaoDynamodb) GdaoDelete(table string, bo godal.IGenericBo) (int, error) {
	return dao.GdaoDeleteWithContext(nil, table, bo)
}

// GdaoDeleteWithContext is AWS DynamoDB variant of GdaoDelete.
func (dao *GenericDaoDynamodb) GdaoDeleteWithContext(ctx aws.Context, table string, bo godal.IGenericBo) (int, error) {
	keyFilter, err := toFilterMap(dao.GdaoCreateFilter(table, bo))
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
//   - table name format: <table_name>[:<index_name>]:
//       - table_name: name of the table to delete rows from.
//       - index_name: (optional) name of the table's index (local or global) to search for rows.
//
// This function uses "scan" operation by default, which is expensive! To force "query" operation, prefix the table name with character '@'.
func (dao *GenericDaoDynamodb) GdaoDeleteMany(table string, filter godal.FilterOpt) (int, error) {
	return dao.GdaoDeleteManyWithContext(nil, table, filter)
}

// BuildConditionBuilder transforms a godal.FilterOpt to expression.ConditionBuilder.
//
// Available since v0.5.1
func (dao *GenericDaoDynamodb) BuildConditionBuilder(tableName string, filter godal.FilterOpt) (*expression.ConditionBuilder, error) {
	if filter == nil {
		return nil, nil
	}
	rm := dao.GetRowMapper()
	if rm == nil {
		return nil, errors.New("row-mapper is required to build ConditionBuilder")
	}

	switch filter.(type) {
	case godal.FilterOptFieldOpValue:
		f := filter.(godal.FilterOptFieldOpValue)
		return dao.BuildConditionBuilder(tableName, &f)
	case *godal.FilterOptFieldOpValue:
		f := filter.(*godal.FilterOptFieldOpValue)
		exp := expression.Name(rm.ToDbColName(tableName, f.FieldName))
		switch f.Operator {
		case godal.FilterOpEqual:
			t := exp.Equal(expression.Value(f.Value))
			return &t, nil
		case godal.FilterOpNotEqual:
			t := exp.NotEqual(expression.Value(f.Value))
			return &t, nil
		case godal.FilterOpGreater:
			t := exp.GreaterThan(expression.Value(f.Value))
			return &t, nil
		case godal.FilterOpGreaterOrEqual:
			t := exp.GreaterThanEqual(expression.Value(f.Value))
			return &t, nil
		case godal.FilterOpLess:
			t := exp.LessThan(expression.Value(f.Value))
			return &t, nil
		case godal.FilterOpLessOrEqual:
			t := exp.LessThanEqual(expression.Value(f.Value))
			return &t, nil
		}
		return nil, fmt.Errorf("unknown filter operator: %#v", f.Operator)
	case godal.FilterOptFieldIsNull:
		f := filter.(godal.FilterOptFieldIsNull)
		return dao.BuildConditionBuilder(tableName, &f)
	case *godal.FilterOptFieldIsNull:
		f := filter.(*godal.FilterOptFieldIsNull)
		t := expression.Name(rm.ToDbColName(tableName, f.FieldName)).AttributeNotExists()
		return &t, nil
	case godal.FilterOptFieldIsNotNull:
		f := filter.(godal.FilterOptFieldIsNotNull)
		return dao.BuildConditionBuilder(tableName, &f)
	case *godal.FilterOptFieldIsNotNull:
		f := filter.(*godal.FilterOptFieldIsNotNull)
		t := expression.Name(rm.ToDbColName(tableName, f.FieldName)).AttributeExists()
		return &t, nil
	case godal.FilterOptAnd:
		f := filter.(godal.FilterOptAnd)
		return dao.BuildConditionBuilder(tableName, &f)
	case *godal.FilterOptAnd:
		f := filter.(*godal.FilterOptAnd)
		var result *expression.ConditionBuilder = nil
		for _, innerF := range f.Filters {
			innerResult, err := dao.BuildConditionBuilder(tableName, innerF)
			if err != nil {
				return nil, err
			}
			if result == nil {
				result = innerResult
			} else {
				t := result.And(*innerResult)
				result = &t
			}
		}
		return result, nil
	case godal.FilterOptOr:
		f := filter.(godal.FilterOptOr)
		return dao.BuildConditionBuilder(tableName, &f)
	case *godal.FilterOptOr:
		f := filter.(*godal.FilterOptOr)
		var result *expression.ConditionBuilder = nil
		for _, innerF := range f.Filters {
			innerResult, err := dao.BuildConditionBuilder(tableName, innerF)
			if err != nil {
				return nil, err
			}
			if result == nil {
				result = innerResult
			} else {
				t := result.Or(*innerResult)
				result = &t
			}
		}
		return result, nil
	}
	return nil, fmt.Errorf("cannot build filter from %T", filter)
}

// GdaoDeleteManyWithContext is is AWS DynamoDB variant of GdaoDeleteMany.
func (dao *GenericDaoDynamodb) GdaoDeleteManyWithContext(ctx aws.Context, table string, filter godal.FilterOpt) (int, error) {
	f, err := dao.BuildConditionBuilder(table, filter)
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
	callbackFunc := func(item dynamodb.AwsDynamodbItem, lastEvaluatedKey map[string]*awsdynamodb.AttributeValue) (b bool, e error) {
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
//   - keyFilter: filter that matches exactly one item by key.
func (dao *GenericDaoDynamodb) GdaoFetchOne(table string, keyFilter godal.FilterOpt) (godal.IGenericBo, error) {
	return dao.GdaoFetchOneWithContext(nil, table, keyFilter)
}

// GdaoFetchOneWithContext is is AWS DynamoDB variant of GdaoFetchOne.
func (dao *GenericDaoDynamodb) GdaoFetchOneWithContext(ctx aws.Context, table string, keyFilter godal.FilterOpt) (godal.IGenericBo, error) {
	if f, err := toFilterMap(keyFilter); err != nil {
		return nil, err
	} else if item, err := dao.dynamodbConnect.GetItem(ctx, table, f); err != nil {
		return nil, err
	} else {
		return dao.GetRowMapper().ToBo(table, item)
	}
}

// GdaoFetchMany implements godal.IGenericDao.GdaoFetchMany.
//   - table name format: <table_name>[:<index_name>[:<refetch-from-table:true/false>]]:
//       - table_name: name of the table to fetch data from.
//       - index_name: (optional) name of the table's index (local or global) to fetch data from.
//       - refetch-from-table: (optional) true/false - default: false; when fetching data from index, if 'false' only projected fields are returned,.
//         if 'true' another read is made to fetch the whole item from table (additional read capacity is consumed!)
//   - sorting will not be used as DynamoDB does not currently support custom sorting of queried items.
//
// This function uses "scan" operation by default, which is expensive! To force "query" operation, prefix the table name with character @.
// (Available since v0.5.2) Furthermore, prefix the table name with character ! to query "backward" instead of the default "forward" mode ("query" mode must be used).
func (dao *GenericDaoDynamodb) GdaoFetchMany(table string, filter godal.FilterOpt, sorting *godal.SortingOpt, startOffset, numItems int) ([]godal.IGenericBo, error) {
	return dao.GdaoFetchManyWithContext(nil, table, filter, sorting, startOffset, numItems)
}

var reTablePrefixDirectives = regexp.MustCompile(`^\W+`)
var reUseQuery = regexp.MustCompile(`^\W*@\W*\w+`)
var reQueryBackward = regexp.MustCompile(`^\W*!\W*\w+`)

// GdaoFetchManyWithContext is AWS DynamoDB variant of GdaoFetchMany.
func (dao *GenericDaoDynamodb) GdaoFetchManyWithContext(ctx aws.Context, table string, filter godal.FilterOpt, _ *godal.SortingOpt, startOffset, numItems int) ([]godal.IGenericBo, error) {
	f, err := dao.BuildConditionBuilder(table, filter)
	if err != nil {
		return nil, err
	}
	result := make([]godal.IGenericBo, 0)
	tokens := strings.Split(table, ":")
	tableName := tokens[0]
	useQuery := reUseQuery.FindString(table) != ""
	queryBackward := reQueryBackward.FindString(table) != ""
	tableName = reTablePrefixDirectives.ReplaceAllString(tableName, "")
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
	callbackFunc := func(item dynamodb.AwsDynamodbItem, lastEvaluatedKey map[string]*awsdynamodb.AttributeValue) (b bool, e error) {
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
	if useQuery {
		err = dao.dynamodbConnect.QueryItemsWithCallback(ctx, tableName, f, nil, indexName, nil, callbackFunc, dynamodb.AwsQueryOpt{ScanIndexBackward: aws.Bool(queryBackward)})
	} else {
		err = dao.dynamodbConnect.ScanItemsWithCallback(ctx, tableName, f, indexName, nil, callbackFunc)
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
	if createResult == nil && dynamodb.AwsIgnoreErrorIfMatched(err, awsdynamodb.ErrCodeConditionalCheckFailedException) == nil {
		return 0, godal.ErrGdaoDuplicatedEntry
	}
	return 1, err
}

// GdaoUpdate implements godal.IGenericDao.GdaoUpdate.
//
// Note: due to the nature of AWS DynamoDB, this function does not return godal.ErrGdaoDuplicatedEntry.
func (dao *GenericDaoDynamodb) GdaoUpdate(table string, bo godal.IGenericBo) (int, error) {
	return dao.GdaoUpdateWithContext(nil, table, bo)
}

// GdaoUpdateWithContext is is AWS DynamoDB variant of GdaoUpdate.
func (dao *GenericDaoDynamodb) GdaoUpdateWithContext(ctx aws.Context, table string, bo godal.IGenericBo) (int, error) {
	var keyFilter, itemMap map[string]interface{}
	var err error
	if keyFilter, err = toFilterMap(dao.GdaoCreateFilter(table, bo)); err != nil {
		return 0, err
	}
	var item interface{}
	var ok bool
	if item, err = dao.GetRowMapper().ToRow(table, bo); err != nil {
		return 0, err
	} else if itemMap, ok = item.(map[string]interface{}); !ok {
		return 0, fmt.Errorf("expected map[string]interface{} but received %T", item)
	}

	pkAttrs := dao.GetRowMapper().ColumnsList(table)
	if pkAttrs == nil || len(pkAttrs) == 0 {
		return 0, fmt.Errorf("cannot find primary-key attribute list for table [%s]", table)
	}
	for _, pk := range pkAttrs {
		// remove primary key attributes from update list
		delete(itemMap, pk)
	}
	condition := dynamodb.AwsDynamodbExistsAllBuilder(pkAttrs)
	if _, err = dao.dynamodbConnect.UpdateItem(ctx, table, keyFilter, condition, nil, itemMap, nil, nil); err != nil {
		err = dynamodb.AwsIgnoreErrorIfMatched(err, awsdynamodb.ErrCodeConditionalCheckFailedException)
		return 0, err
	}
	return 1, nil
}

// GdaoSave implements godal.IGenericDao.GdaoSave.
//
// Note: due to the nature of AWS DynamoDB, this function does not return godal.ErrGdaoDuplicatedEntry.
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
