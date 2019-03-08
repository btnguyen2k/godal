/*
Package godal provides a skeleton to implement generic data-access-layer in Golang.
*/
package godal

import (
	"encoding/json"
	"github.com/btnguyen2k/consu/semita"
	"reflect"
	"sync"
	"time"
)

const (
	// Version of godal
	Version = "0.0.1"
)

/*
IGenericBo defines API interface of a generic business object.

Assuming bo's data is stored in a hierarchy structure.
A bo's attribute value is at the leaf node, and located via a 'path', for example "options.workhour[0].value"
*/
type IGenericBo interface {
	// GetAttribute retrieves an attribute of the bo
	GetAttribute(path string, typ reflect.Type) (interface{}, error)

	// GetTimeWithLayout retrieves an attribute of the bo as 'time.Time'
	//
	//   - If value at 'path' is a datetime represented as a string, this function calls 'time.Parse(...)' to convert the value to 'time.Time' using 'layout'
	GetTimeWithLayout(path, layout string) (time.Time, error)

	// SetAttribute sets a attribute of the bo
	SetAttribute(path string, value interface{}) error

	// ToJson serialize bo's data to JSON string
	ToJson() ([]byte, error)

	// FromJson import bo's data from a JSON string
	FromJson(js []byte) error

	// TransferViaJson transfer bo's attributes to the destination using JSON transformation.
	TransferViaJson(dest interface{}) error
}

// GenericBo is a generic implementation of business-object.
type GenericBo struct {
	data interface{}
	s    *semita.Semita
	m    sync.RWMutex
}

// NewGenericBo constructs a new 'IGenericBo' instance.
func NewGenericBo() IGenericBo {
	bo := &GenericBo{}
	s := semita.NewSemita(bo.data)
	bo.s = s
	return bo
}

// GetAttribute retrieves an attribute of the bo
func (bo *GenericBo) GetAttribute(path string, typ reflect.Type) (interface{}, error) {
	bo.m.RLock()
	defer bo.m.RUnlock()
	if bo.s == nil {
		return nil, nil
	}
	return bo.s.GetValueOfType(path, typ)
}

// GetTimeWithLayout retrieves an attribute of the bo as 'time.Time'
//
//   - If value at 'path' is a datetime represented as a string, this function calls 'time.Parse(...)' to convert the value to 'time.Time' using 'layout'
func (bo *GenericBo) GetTimeWithLayout(path, layout string) (time.Time, error) {
	bo.m.RLock()
	defer bo.m.RUnlock()
	if bo.s == nil {
		return time.Time{}, nil
	}
	return bo.s.GetTimeWithLayout(path, layout)
}

// SetAttribute sets a attribute of the bo
func (bo *GenericBo) SetAttribute(path string, value interface{}) error {
	bo.m.Lock()
	defer bo.m.Unlock()
	if bo.s == nil {
		var data = map[string]interface{}{}
		bo.data = data
		bo.s = semita.NewSemita(bo.data)
	}
	return bo.s.SetValue(path, value)
}

// TransferViaJson transfer bo's attributes to the destination using JSON transformation.
func (bo *GenericBo) TransferViaJson(dest interface{}) error {
	js, err := bo.ToJson()
	if err != nil {
		return err
	}
	return json.Unmarshal(js, dest)
}

// ToJson serialize bo's data to JSON string
func (bo *GenericBo) ToJson() ([]byte, error) {
	bo.m.RLock()
	defer bo.m.RUnlock()
	return json.Marshal(bo.data)
}

// FromJson import bo's data from a JSON string
func (bo *GenericBo) FromJson(js []byte) error {
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
