package godal

import (
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/btnguyen2k/consu/checksum"
	"github.com/btnguyen2k/consu/semita"
)

// Available: since v0.2.4
type nilValue []byte

// NilValue represents a nil/null value.
//
// Available: since v0.2.4
var NilValue nilValue = nil

// IGenericBo defines API interface of a generic business object.
//
// The API interface assumes bo's data is stored in a hierarchy structure.
// A bo's attribute value is at the leaf node, and located via a 'path', for example "options.workhour[0].value".
//
// Sample usage: see GenericBo.
type IGenericBo interface {
	// GboIterate iterates over all bo's top level fields.
	//   - If the underlying data is a map, 'callback' is called for each map's entry with reflect.Map is passed as 'kind' parameter.
	//   - If the underlying data is a slice or array, 'callback' is called for each entry with reflect.Slice is passed as 'kind' parameter.
	//
	// Available: since v0.0.2
	GboIterate(callback func(kind reflect.Kind, field interface{}, value interface{}))

	// GboGetAttr retrieves a bo's attribute.
	//   - If 'typ' is not nil, the attribute value is converted to the specified type upon being returned.
	//   - Otherwise, the attribute value is returned as-is.
	GboGetAttr(path string, typ reflect.Type) (interface{}, error)

	// GboGetAttrUnsafe retrieves a bo's attribute as GboGetAttr does, but error is ignored.
	//
	// Available: since v0.0.2
	GboGetAttrUnsafe(path string, typ reflect.Type) interface{}

	// GboGetAttrUnmarshalJson retrieves a bo's attribute, treats attribute value as JSON-encoded and parses it back to Go type.
	//
	// Available: since v0.2.0
	GboGetAttrUnmarshalJson(path string) (interface{}, error)

	// GboGetTimeWithLayout retrieves a bo's attribute as 'time.Time'.
	//
	// If value at 'path' is a datetime represented as a string, this function calls 'time.Parse(...)' to convert the value to 'time.Time' using 'layout'.
	GboGetTimeWithLayout(path, layout string) (time.Time, error)

	// GboSetAttr sets a bo's attribute.
	GboSetAttr(path string, value interface{}) error

	// GboToJson serializes bo's data to JSON string.
	GboToJson() ([]byte, error)

	// GboToJsonUnsafe serializes bo's data to JSON string, ignoring error if any.
	//
	// Available: since v0.1.1
	GboToJsonUnsafe() []byte

	// GboFromJson imports bo's data from a JSON string.
	GboFromJson(js []byte) error

	// GboTransferViaJson copies bo's data to the destination using JSON transformation.
	// Firstly, bo's data is marshaled to JSON data. Then the JSON data is unmarshaled to 'dest'.
	//
	// Note: 'dest' must be a pointer because passing value does not work.
	GboTransferViaJson(dest interface{}) error

	// GboImportViaJson imports bo's data from an external source using JSON transformation.
	// Firstly, src is marshaled to JSON data. Then the JSON data is unmarshaled/imported to bo's attributes.
	GboImportViaJson(src interface{}) error
}

// NewGenericBo constructs a new 'IGenericBo' instance.
func NewGenericBo() IGenericBo {
	bo := &GenericBo{}
	s := semita.NewSemita(bo.data)
	bo.s = s
	return bo
}

/*
GenericBo is a generic implementation of business-object.

Sample usage:

	gbo := NewGenericBo()
	gbo.GboSetAttr("name.first", "Thanh")
	gbo.GboSetAttr("name.last", "Nguyen")

	// reflect.TypeOf("any string") is equivalent to reddo.TypeString
	firstName, err := gbo.GboGetAttr("name.first", reddo.TypeString)    // firstName = "Thanh"
	lastName, err := gbo.GboGetAttr("name.last", reflect.TypeOf(""))    // lastName = "Nguyen"

	gbo.GboSetAttr("age", 123)
	age, err := gbo.GboGetAttr("age", reddo.TypeInt)    // age = 123

	var m interface{}
	err = gbo.GboTransferViaJson(&m)
	// m = map[string]interface{}{
	//   "age" : 123,
	//   "name": map[string]interface{}{
	//     "first": "Thanh",
	//     "last" : "Nguyen",
	//   },
	// }

	ext := map[string]interface{}{"year":2019, "month":"3", "active":true}
	err = gbo.GboImportViaJson(ext)
	year, err  := gbo.GboGetAttr("year", reddo.TypeString)    // year = "2019", because we want a string-result
	month, err := gbo.GboGetAttr("month", reddo.TypeInt)      // month = 3, because we want an int-result and "3" is convertible to int
	name, err := gbbo.GboGetAttr("name", nil)                 // name is nil because GboImportViaJson clear existing data upon importing
*/
type GenericBo struct {
	data interface{}
	s    *semita.Semita
	m    sync.RWMutex
}

// Checksum returns checksum value of the BO.
//
// Available: since v0.0.4
func (bo *GenericBo) Checksum() []byte {
	return checksum.Md5Checksum(bo.data)
}

// GboIterate implements IGenericBo.GboIterate.
//
//   - If the underlying data is a map, 'callback' is called for each map's entry with reflect.Map is passed as 'kind' parameter.
//   - If the underlying data is a slice or array, 'callback' is called for each entry with reflect.Slice is passed as 'kind' parameter.
//   - This function of type GenericBo does not support iterating on other data types.
func (bo *GenericBo) GboIterate(callback func(kind reflect.Kind, field interface{}, value interface{})) {
	bo.m.RLock()
	defer bo.m.RUnlock()
	v := reflect.ValueOf(bo.data)
	if v.Kind() == reflect.Map {
		for iter := v.MapRange(); iter.Next(); callback(reflect.Map, iter.Key().Interface(), iter.Value().Interface()) {
		}
	}
	if v.Kind() == reflect.Array || v.Kind() == reflect.Slice {
		for i, n := 0, v.Len(); i < n; i++ {
			callback(reflect.Slice, i, v.Index(i).Interface())
		}
	}
}

// GboGetAttr implements IGenericBo.GboGetAttr.
//
// 'type' can be nil. In that case, this function returns the result of type 'interface{}'.
func (bo *GenericBo) GboGetAttr(path string, typ reflect.Type) (interface{}, error) {
	bo.m.RLock()
	defer bo.m.RUnlock()
	if bo.s == nil {
		return nil, nil
	}
	return bo.s.GetValueOfType(path, typ)
}

// GboGetAttrUnsafe implements IGenericBo.GboGetAttrUnsafe.
//
// 'type' can be nil. In that case, this function returns the result of type 'interface{}'.
func (bo *GenericBo) GboGetAttrUnsafe(path string, typ reflect.Type) interface{} {
	v, _ := bo.GboGetAttr(path, typ)
	return v
}

// GboGetAttrUnmarshalJson implements IGenericBo.GboGetAttrUnmarshalJson.
func (bo *GenericBo) GboGetAttrUnmarshalJson(path string) (interface{}, error) {
	v, e := bo.GboGetAttr(path, nil)
	if e != nil || v == nil {
		return nil, e
	}
	var vj interface{}
	switch v.(type) {
	case []byte:
		return vj, json.Unmarshal(v.([]byte), &vj)
	case *[]byte:
		return vj, json.Unmarshal(*v.(*[]byte), &vj)
	case string:
		return vj, json.Unmarshal([]byte(v.(string)), &vj)
	case *string:
		return vj, json.Unmarshal([]byte(*v.(*string)), &vj)
	}
	return nil, nil
}

// GboGetTimeWithLayout implements IGenericBo.GboGetTimeWithLayout.
//
// If value does not exist at 'path', this function returns 'zero' time (e.g. time.Time{}).
func (bo *GenericBo) GboGetTimeWithLayout(path, layout string) (time.Time, error) {
	bo.m.RLock()
	defer bo.m.RUnlock()
	if bo.s == nil {
		return time.Time{}, nil
	}
	return bo.s.GetTimeWithLayout(path, layout)
}

// GboSetAttr implements IGenericBo.GboSetAttr.
//
// Intermediate nodes along the path are automatically created.
func (bo *GenericBo) GboSetAttr(path string, value interface{}) error {
	bo.m.Lock()
	defer bo.m.Unlock()
	if bo.s == nil {
		var data interface{}
		if strings.HasPrefix(path, "[") {
			data = make([]interface{}, 0)
		} else {
			data = make(map[string]interface{})
		}
		bo.data = data
		bo.s = semita.NewSemita(bo.data)
	}
	return bo.s.SetValue(path, value)
}

// GboToJson implements IGenericBo.GboToJson.
func (bo *GenericBo) GboToJson() ([]byte, error) {
	bo.m.RLock()
	defer bo.m.RUnlock()
	return json.Marshal(bo.data)
}

// GboToJsonUnsafe implements IGenericBo.GboToJsonUnsafe.
func (bo *GenericBo) GboToJsonUnsafe() []byte {
	js, _ := bo.GboToJson()
	return js
}

// GboFromJson implements IGenericBo.GboFromJson.
//
//   - If error occurs, existing BO data is intact.
//   - If successful, existing data is replaced.
func (bo *GenericBo) GboFromJson(js []byte) error {
	bo.m.Lock()
	defer bo.m.Unlock()
	var data interface{}
	if err := json.Unmarshal(js, &data); err != nil {
		return err
	}
	if data == nil {
		data = map[string]interface{}{}
	}
	bo.data = data
	bo.s = semita.NewSemita(bo.data)
	return nil
}

// GboTransferViaJson implements IGenericBo.GboTransferViaJson.
//
// Passing by value won't work, so 'dest' must be a pointer.
func (bo *GenericBo) GboTransferViaJson(dest interface{}) error {
	js, _ := bo.GboToJson()
	// if err != nil {
	// 	return err
	// }
	return json.Unmarshal(js, dest)
}

// GboImportViaJson implements IGenericBo.GboImportViaJson.
//
// Existing data is removed upon importing.
func (bo *GenericBo) GboImportViaJson(src interface{}) error {
	js, _ := json.Marshal(src)
	// if err != nil {
	// 	return err
	// }
	return bo.GboFromJson(js)
}
