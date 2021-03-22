package sql

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/btnguyen2k/consu/reddo"

	"github.com/btnguyen2k/godal"
)

// NameTransformation specifies how table column name is transformed.
type NameTransformation int

// Predefined name transformations.
const (
	// NameTransfIntact specifies that table column & field names are kept intact.
	NameTransfIntact NameTransformation = iota

	// NameTransfUpperCase specifies that table column & field names should be upper-cased.
	NameTransfUpperCase

	// NameTransfLowerCase specifies that table column & field names should be lower-cased.
	NameTransfLowerCase
)

// GenericRowMapperSql is a generic implementation of godal.IRowMapper for 'database/sql'.
//
// Implementation rules:
//   - ToRow: transform godal.IGenericBo to map[string]interface{}.
//     - Only top level fields are converted. Column/Field names are transformed according to 'NameTransformation' setting and 'GboFieldToColNameTranslator'
// 	   - If field is bool or string or time.Time: its value is converted as-is
// 	   - If field is int (int8 to int64): its value is converted to int64
// 	   - If field is uint (uint8 to uint64): its value is converted to uint64
// 	   - If field is float32 or float64: its value is converted to float64
// 	   - Field is one of other types: its value is converted to JSON string
//   - ToBo: expect input is a map[string]interface{}, transform it to godal.IGenericBo. Column/Field names are transformed according to 'NameTransformation' setting and 'ColNameToGboFieldTranslator'
//   - ColumnsList: lookup column-list from a 'columns-list map', returns []string{"*"} if not found
type GenericRowMapperSql struct {
	// NameTransformation specifies how field/column names are transformed. Default value: NameTransfIntact
	NameTransformation NameTransformation

	// GboFieldToColNameTranslator holds mappings of {table-name:{gbo-field:column-name}}. gbo-field is post-transformed according to 'NameTransformation'.
	GboFieldToColNameTranslator map[string]map[string]interface{}

	// GboFieldToColNameTranslator holds mappings of {table-name:{column-name:gbo-field}}. column-name is post-transformed according to 'NameTransformation'.
	ColNameToGboFieldTranslator map[string]map[string]interface{}

	// ColumnsListMap holds mappings of {table-name:[list of column names]}
	ColumnsListMap map[string][]string
}

var (
	typeTime      = reflect.TypeOf(time.Time{})
	nullableTypes = map[reflect.Kind]bool{
		reflect.Chan:          true,
		reflect.Func:          true,
		reflect.Map:           true,
		reflect.Ptr:           true,
		reflect.UnsafePointer: true,
		reflect.Interface:     true,
		reflect.Slice:         true,
	}
)

func (mapper *GenericRowMapperSql) transformName(name string) string {
	if mapper.NameTransformation == NameTransfLowerCase {
		return strings.ToLower(name)
	} else if mapper.NameTransformation == NameTransfUpperCase {
		return strings.ToUpper(name)
	}
	return name
}

func (mapper *GenericRowMapperSql) translateGboFieldToColName(storageId, fieldName string) string {
	mapping, ok := mapper.GboFieldToColNameTranslator[storageId]
	if !ok || mapping == nil {
		mapping, ok = mapper.GboFieldToColNameTranslator["*"]
	}
	if ok && mapping != nil {
		if t, ok := mapping[fieldName]; ok && t != nil {
			switch t.(type) {
			case string:
				fieldName = t.(string)
			case *string:
				fieldName = *t.(*string)
			case func(string, string) string:
				fieldName = t.(func(string, string) string)(storageId, fieldName)
			}
		}
	}
	return fieldName
}

func (mapper *GenericRowMapperSql) translateColNameToGboField(storageId, colName string) string {
	mapping, ok := mapper.ColNameToGboFieldTranslator[storageId]
	if !ok || mapping == nil {
		mapping, ok = mapper.ColNameToGboFieldTranslator["*"]
	}
	if ok && mapping != nil {
		if t, ok := mapping[colName]; ok && t != nil {
			switch t.(type) {
			case string:
				colName = t.(string)
			case *string:
				colName = *t.(*string)
			case func(string, string) string:
				colName = t.(func(string, string) string)(storageId, colName)
			}
		}
	}
	return colName
}

// ToRow implements godal.IRowMapper.ToRow
func (mapper *GenericRowMapperSql) ToRow(storageId string, gbo godal.IGenericBo) (interface{}, error) {
	if gbo == nil {
		return nil, nil
	}
	var row = make(map[string]interface{})
	var err error
	gbo.GboIterate(func(_ reflect.Kind, field interface{}, value interface{}) {
		if err != nil {
			return
		}
		var colName string
		if colName, err = reddo.ToString(field); err != nil {
			return
		}
		colName = mapper.translateGboFieldToColName(storageId, mapper.transformName(colName))
		v := reflect.ValueOf(value)
		if value == nil || (v.Kind() == reflect.Ptr && v.IsNil()) {
			row[colName] = nil
			return
		}
		for ; v.Kind() == reflect.Ptr; v = v.Elem() {
		}
		k := v.Kind()
		switch k {
		case reflect.Bool:
			row[colName] = v.Bool()
		case reflect.String:
			row[colName] = v.String()
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			row[colName] = v.Int()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			row[colName] = v.Uint()
		case reflect.Float32, reflect.Float64:
			row[colName] = v.Float()
		default:
			if nullableTypes[k] && v.IsNil() {
				row[colName] = nil
			} else if v.Type() == typeTime {
				row[colName] = v.Interface().(time.Time)
			} else {
				if js, e := json.Marshal(v.Interface()); e != nil {
					err = e
				} else {
					row[colName] = string(js)
				}
			}
		}
	})
	return row, err
}

// ToBo implements godal.IRowMapper.ToBo.
//
// This function expects input to be a map[string]interface{}, or JSON data (string or array/slice of bytes), transforms it to godal.IGenericBo. Field names are transform according to 'NameTransformation' setting.
func (mapper *GenericRowMapperSql) ToBo(table string, row interface{}) (godal.IGenericBo, error) {
	if row == nil {
		return nil, nil
	}
	switch row.(type) {
	case map[string]interface{}:
		bo := godal.NewGenericBo()
		for k, v := range row.(map[string]interface{}) {
			bo.GboSetAttr(mapper.translateColNameToGboField(table, mapper.transformName(k)), v)
		}
		return bo, nil
	case string:
		var data interface{}
		json.Unmarshal([]byte(row.(string)), &data)
		return mapper.ToBo(table, data)
	case *string:
		if row.(*string) == nil {
			return nil, nil
		}
		var data interface{}
		json.Unmarshal([]byte(*row.(*string)), &data)
		return mapper.ToBo(table, data)
	case []byte:
		if row.([]byte) == nil {
			return nil, nil
		}
		var data interface{}
		json.Unmarshal(row.([]byte), &data)
		return mapper.ToBo(table, data)
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
		bo := godal.NewGenericBo()
		for iter := v.MapRange(); iter.Next(); {
			key, _ := reddo.ToString(iter.Key().Interface())
			bo.GboSetAttr(mapper.translateColNameToGboField(table, mapper.transformName(key)), iter.Value().Interface())
		}
		return bo, nil
	case reflect.String:
		var data interface{}
		json.Unmarshal([]byte(v.Interface().(string)), &data)
		return mapper.ToBo(table, data)
	case reflect.Slice, reflect.Array:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			// input is []byte
			zero := make([]byte, 0)
			arr, err := reddo.ToSlice(v.Interface(), reflect.TypeOf(zero))
			if err != nil || arr.([]byte) == nil || len(arr.([]byte)) == 0 {
				return nil, err
			}
			var data interface{}
			json.Unmarshal(arr.([]byte), &data)
			return mapper.ToBo(table, data)
		}
	case reflect.Interface:
		return mapper.ToBo(table, v.Interface())
	case reflect.Invalid:
		return nil, nil
	}
	return nil, fmt.Errorf("cannot construct godal.IGenericBo from input %v", row)
}

var allColumns = []string{"*"}

// ColumnsList implements godal.IRowMapper.ColumnsList.
//
// This function lookups column-list from a 'columns-list map', returns []string{"*"} if not found
func (mapper *GenericRowMapperSql) ColumnsList(storageId string) []string {
	if result, ok := mapper.ColumnsListMap[storageId]; ok {
		return result
	}
	if result, ok := mapper.ColumnsListMap["*"]; ok {
		return result
	}
	return allColumns
}
