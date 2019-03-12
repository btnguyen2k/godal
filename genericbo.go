package godal

import (
	"encoding/json"
	"github.com/btnguyen2k/consu/semita"
	"reflect"
	"sync"
	"time"
)

/*
IGenericBo defines API interface of a generic business object.

The API interface assume bo's data is stored in a hierarchy structure.
A bo's attribute value is at the leaf node, and located via a 'path', for example "options.workhour[0].value"

Sample usage: see #GenericBo
*/
type IGenericBo interface {
	/*
	GboGetAttr retrieves an attribute of the bo
	*/
	GboGetAttr(path string, typ reflect.Type) (interface{}, error)

	/*
	GboGetTimeWithLayout retrieves an attribute of the bo as 'time.Time'

	If value at 'path' is a datetime represented as a string, this function calls 'time.Parse(...)' to convert the value to 'time.Time' using 'layout'.
	*/
	GboGetTimeWithLayout(path, layout string) (time.Time, error)

	/*
	GboSetAttr sets a attribute of the bo
	*/
	GboSetAttr(path string, value interface{}) error

	/*
	GboToJson serializes bo's data to JSON string
	*/
	GboToJson() ([]byte, error)

	/*
	GboFromJson imports bo's data from a JSON string
	*/
	GboFromJson(js []byte) error

	/*
	GboTransferViaJson transfers bo's attributes to the destination using JSON transformation.
	Firstly, bo's data is marshaled to JSON data. Then the JSON data is unmarshaled to 'dest'.

	Note: 'dest' must be a pointer because passing value does not work.
	*/
	GboTransferViaJson(dest interface{}) error

	/*
	GboImportViaJson imports bo's data from an external source using JSON transformation.
	Firstly, src is marshaled to JSON data. Then the JSON data is unmarshaled/imported to bo's attributes.
	*/
	GboImportViaJson(src interface{}) error
}

/*
NewGenericBo constructs a new 'IGenericBo' instance.
*/
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

	firstName, err := gbo.GboGetAttr("name.first", reddo.TypeString)    // firstName = "Thanh"
	lastName, err := gbo.GboGetAttr("name.last", reflect.TypeOf(""))    // lastName = "Nguyen"

	gbo.GboSetAttr("age", 123)
	age, err := gbo.GboGetAttr("age", reddo.TypeInt)    // age = 123

	var m interface{}
	err = gbo.GboTransferViaJson(&m)
	// m = map[string]interface{}{
	// 	"age" : 123,
	// 	"name": map[string]interface{}{
	// 		"first": "Thanh",
	// 		"last" : "Nguyen",
	// 	},
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

/*
GboGetAttr implements IGenericBo.GboGetAttr

	- 'type' can be nil. In that case, this function returns the result of type 'interface{}'
*/
func (bo *GenericBo) GboGetAttr(path string, typ reflect.Type) (interface{}, error) {
	bo.m.RLock()
	defer bo.m.RUnlock()
	if bo.s == nil {
		return nil, nil
	}
	return bo.s.GetValueOfType(path, typ)
}

/*
GboGetTimeWithLayout implements IGenericBo.GboGetTimeWithLayout

	- If value does not exist at 'path', this function returns 'zero' time (e.g. time.Time{})
*/
func (bo *GenericBo) GboGetTimeWithLayout(path, layout string) (time.Time, error) {
	bo.m.RLock()
	defer bo.m.RUnlock()
	if bo.s == nil {
		return time.Time{}, nil
	}
	return bo.s.GetTimeWithLayout(path, layout)
}

/*
GboSetAttr implements IGenericBo.GboSetAttr

	- Intermediate nodes along the path are automatically created.
*/
func (bo *GenericBo) GboSetAttr(path string, value interface{}) error {
	bo.m.Lock()
	defer bo.m.Unlock()
	if bo.s == nil {
		var data = map[string]interface{}{}
		bo.data = data
		bo.s = semita.NewSemita(bo.data)
	}
	return bo.s.SetValue(path, value)
}

/*
GboToJson implements IGenericBo.GboToJson
*/
func (bo *GenericBo) GboToJson() ([]byte, error) {
	bo.m.RLock()
	defer bo.m.RUnlock()
	return json.Marshal(bo.data)
}

/*
GboFromJson implements IGenericBo.GboFromJson

	- Existing data is removed upon importing
*/
func (bo *GenericBo) GboFromJson(js []byte) error {
	bo.m.Lock()
	defer bo.m.Unlock()
	var data interface{}
	err := json.Unmarshal(js, &data)
	if data == nil {
		data = map[string]interface{}{}
	}
	bo.data = data
	bo.s = semita.NewSemita(bo.data)
	return err
}

/*
GboTransferViaJson implements IGenericBo.GboTransferViaJson

	- Passing by value wont work, so 'dest' should be a pointer
*/
func (bo *GenericBo) GboTransferViaJson(dest interface{}) error {
	js, err := bo.GboToJson()
	if err != nil {
		return err
	}
	return json.Unmarshal(js, dest)
}

/*
GboImportViaJson implements IGenericBo.GboImportViaJson

	- Existing data is removed upon importing
*/
func (bo *GenericBo) GboImportViaJson(src interface{}) error {
	js, err := json.Marshal(src)
	if err != nil {
		return err
	}
	return bo.GboFromJson(js)
}
