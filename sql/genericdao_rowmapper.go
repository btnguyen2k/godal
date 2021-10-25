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
//     - Only top level fields are converted. Column/Field name transformation: see below.
// 	   - If field is bool or string or time.Time: its value is converted as-is
// 	   - If field is int (int8 to int64): its value is converted to int64
// 	   - If field is uint (uint8 to uint64): its value is converted to uint64
// 	   - If field is float32 or float64: its value is converted to float64
// 	   - Field is one of other types: its value is converted to JSON string
//   - ToBo: expect input is a map[string]interface{}, or JSON data (string or array/slice of bytes), transform it to godal.IGenericBo. Column/Field name transformation: see below.
//   - ColumnsList: lookup column-list from a 'columns-list map', returns []string{"*"} if not found
//   - ToDbColName: field-name is transformed to database-column-name per the following rule:
//     - firstly, field-name is transformed to new-field-name based on NameTransformation setting
//     - then, new-field-name is feed to GboFieldToColNameTranslator to look up for the database-column-name
//   - ToBoFieldName: database-column-name is transformed to field-name per the following rule:
//     - firstly, database-column-name is transform to new-database-column-name based on NameTransformation setting
//     - then, new database-column-name is feed to ColNameToGboFieldTranslator to look up for the field-name
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

// transformName transforms a name to new one per NameTransformation setting.
func (mapper *GenericRowMapperSql) transformName(name string) string {
	if mapper.NameTransformation == NameTransfLowerCase {
		return strings.ToLower(name)
	} else if mapper.NameTransformation == NameTransfUpperCase {
		return strings.ToUpper(name)
	}
	return name
}

// translateGboFieldToColName uses GboFieldToColNameTranslator to look up for db-column-name from the input field-name.
func (mapper *GenericRowMapperSql) translateGboFieldToColName(tableName, fieldName string) string {
	mapping, ok := mapper.GboFieldToColNameTranslator[tableName]
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
				fieldName = t.(func(string, string) string)(tableName, fieldName)
			}
		}
	}
	return fieldName
}

// translateColNameToGboField uses ColNameToGboFieldTranslator to look up for field-name from the input db-column-name.
func (mapper *GenericRowMapperSql) translateColNameToGboField(tableName, colName string) string {
	mapping, ok := mapper.ColNameToGboFieldTranslator[tableName]
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
				colName = t.(func(string, string) string)(tableName, colName)
			}
		}
	}
	return colName
}

// ToRow implements godal.IRowMapper.ToRow
func (mapper *GenericRowMapperSql) ToRow(tableName string, gbo godal.IGenericBo) (interface{}, error) {
	if gbo == nil {
		return nil, nil
	}
	var row = make(map[string]interface{})
	var err error
	gbo.GboIterate(func(_ reflect.Kind, field interface{}, value interface{}) {
		if err != nil {
			return
		}
		var fieldName string
		if fieldName, err = reddo.ToString(field); err != nil {
			return
		}
		colName := mapper.translateGboFieldToColName(tableName, mapper.transformName(fieldName))
		v := reflect.ValueOf(value)
		if value == nil || (v.Kind() == reflect.Ptr && v.IsNil()) {
			row[colName] = nil
			return
		}
		for ; v.Kind() == reflect.Ptr; v = v.Elem() {
			// unwrap if pointer
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
// This function expects input to be a map[string]interface{}, or JSON data (string or array/slice of bytes), transforms it to godal.IGenericBo.
func (mapper *GenericRowMapperSql) ToBo(tableName string, row interface{}) (godal.IGenericBo, error) {
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
		return mapper.ToBo(tableName, *m)
	case map[string]interface{}:
		bo := godal.NewGenericBo()
		for colName, v := range row.(map[string]interface{}) {
			bo.GboSetAttr(mapper.translateColNameToGboField(tableName, mapper.transformName(colName)), v)
		}
		return bo, nil
	case string:
		var data interface{}
		json.Unmarshal([]byte(row.(string)), &data)
		return mapper.ToBo(tableName, data)
	case *string:
		// unwrap if pointer
		s := row.(*string)
		if s == nil {
			return nil, nil
		}
		return mapper.ToBo(tableName, *s)
	case []byte:
		if row.([]byte) == nil {
			return nil, nil
		}
		var data interface{}
		json.Unmarshal(row.([]byte), &data)
		return mapper.ToBo(tableName, data)
	case *[]byte:
		// unwrap if pointer
		ba := row.(*[]byte)
		if ba == nil {
			return nil, nil
		}
		return mapper.ToBo(tableName, *ba)
	}

	v := reflect.ValueOf(row)
	for ; v.Kind() == reflect.Ptr; v = v.Elem() {
		// unwrap if pointer
	}
	switch v.Kind() {
	case reflect.Map:
		bo := godal.NewGenericBo()
		for iter := v.MapRange(); iter.Next(); {
			colName, _ := reddo.ToString(iter.Key().Interface())
			bo.GboSetAttr(mapper.translateColNameToGboField(tableName, mapper.transformName(colName)), iter.Value().Interface())
		}
		return bo, nil
	case reflect.String:
		var data interface{}
		json.Unmarshal([]byte(v.Interface().(string)), &data)
		return mapper.ToBo(tableName, data)
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
			return mapper.ToBo(tableName, data)
		}
	case reflect.Interface:
		return mapper.ToBo(tableName, v.Interface())
	case reflect.Invalid:
		return nil, nil
	}
	return nil, fmt.Errorf("cannot construct godal.IGenericBo from input %v", row)
}

var allColumns = []string{"*"}

// ColumnsList implements godal.IRowMapper.ColumnsList.
//
// This function lookups column-list from a 'columns-list map', returns []string{"*"} if not found
func (mapper *GenericRowMapperSql) ColumnsList(tableName string) []string {
	if result, ok := mapper.ColumnsListMap[tableName]; ok {
		return result
	}
	if result, ok := mapper.ColumnsListMap["*"]; ok {
		return result
	}
	return allColumns
}

// ToDbColName implements godal.IRowMapper.ToDbColName.
//   - firstly, field-name is transformed to new-field-name based on NameTransformation setting
//   - then, new-field-name is feed to GboFieldToColNameTranslator to look up for the database-column-name
func (mapper *GenericRowMapperSql) ToDbColName(tableName, fieldName string) string {
	return mapper.translateGboFieldToColName(tableName, mapper.transformName(fieldName))
}

// ToBoFieldName implements godal.IRowMapper.ToBoFieldName.
//   - firstly, database-column-name is transform to new-database-column-name based on NameTransformation setting
//   - then, new database-column-name is feed to ColNameToGboFieldTranslator to look up for the field-name
func (mapper *GenericRowMapperSql) ToBoFieldName(tableName, colName string) string {
	return mapper.translateColNameToGboField(tableName, mapper.transformName(colName))
}
