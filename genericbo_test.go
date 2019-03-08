package godal

import (
	"package/github.com/btnguyen2k/consu/reddo"
	"reflect"
	"testing"
)

var jsonData = []byte(`
{
    "Name": "Standard",
    "Fruit": [
        "Apple",
        "Banana",
        "Orange"
    ],
    "ref": 999,
    "Created": "2018-04-09T23:00:00Z"
}`)

func TestNewGenericBo(t *testing.T) {
	name := "TestNewGenericBo"

	bo := NewGenericBo()
	if bo == nil {
		t.Errorf("%s failed", name)
	}

	if bo.(*GenericBo).data != nil {
		t.Errorf("%s failed", name)
	}

}

func TestGenericBo_FromJson(t *testing.T) {
	name := "TestGenericBo_FromJson"

	bo := NewGenericBo()
	err := bo.FromJson(jsonData)
	if err != nil {
		t.Errorf("%s failed", name)
	}
	js, err := bo.ToJson()
	if err != nil || string(js) == "null" {
		t.Errorf("%s failed", name)
	}
}

func TestGenericBo_ToJson(t *testing.T) {
	name := "TestGenericBo_ToJson"

	bo := NewGenericBo()
	js, err := bo.ToJson()
	if err != nil || string(js) != "null" {
		t.Errorf("%s failed", name)
	}

	jsonData := []byte(`{"a":1,"b":"a string","c":true}`)
	err = bo.FromJson(jsonData)
	if err != nil {
		t.Errorf("%s failed", name)
	}
	js, err = bo.ToJson()
	if err != nil || string(js) != string(jsonData) {
		t.Errorf("%s failed", name)
	}
}

func TestGenericBo_GetAttribute(t *testing.T) {
	name := "TestGenericBo_GetAttribute"

	bo := NewGenericBo()
	err := bo.FromJson(jsonData)
	if err != nil {
		t.Errorf("%s failed", name)
	}

	{
		p := "not_exists"
		_, err := bo.GetAttribute(p, reddo.TypeString)
		if err != nil {
			t.Errorf("%s failed for path %s", name, p)
		}
	}

	{
		p := "Name"
		expected := "Standard"
		v, err := bo.GetAttribute(p, reddo.TypeString)
		if err != nil || v == nil || v.(string) != expected {
			t.Errorf("%s failed for path %s", name, p)
		}
	}

	{
		p := "ref"
		expected := int64(999)
		v, err := bo.GetAttribute(p, reddo.TypeInt)
		if err != nil || v == nil || v.(int64) != expected {
			t.Errorf("%s failed for path %s", name, p)
		}
	}
}

func TestGenericBo_GetTimeWithLayout(t *testing.T) {
	name := "TestGenericBo_GetTimeWithLayout"

	bo := NewGenericBo()
	err := bo.FromJson(jsonData)
	if err != nil {
		t.Errorf("%s failed", name)
	}

	layout := "2006-01-02T15:04:05Z"
	expected := "2018-04-09T23:00:00Z"
	p := "Created"
	v, err := bo.GetTimeWithLayout(p, layout)
	if err != nil {
		t.Errorf("%s failed for path %s", name, p)
	} else {
		s := v.Format(layout)
		if s != expected {
			t.Errorf("%s failed for path %s", name, p)
		}
	}
}

func TestGenericBo_SetAttribute(t *testing.T) {
	name := "TestGenericBo_SetAttribute"

	bo := NewGenericBo()
	err := bo.FromJson(jsonData)
	if err != nil {
		t.Errorf("%s failed", name)
	}

	p := "a.b.c[].d"
	v, err := bo.GetAttribute(p, nil)
	if v != nil || err != nil {
		t.Errorf("%s failed", name)
	}
	err = bo.SetAttribute(p, 1)
	if err != nil {
		t.Errorf("%s failed", name)
	}
	p = "a.b.c[0].d"
	v, err = bo.GetAttribute(p, reddo.TypeString)
	if err != nil || v == nil || v.(string) != "1" {
		t.Errorf("%s failed", name)
	}
}

func TestGenericBo_SetAttribute2(t *testing.T) {
	name := "TestGenericBo_SetAttribute"

	bo := NewGenericBo()

	p := "a.b.c[].d"
	v, err := bo.GetAttribute(p, nil)
	if v != nil || err != nil {
		t.Errorf("%s failed", name)
	}
	err = bo.SetAttribute(p, 1)
	if err != nil {
		t.Errorf("%s failed", name)
	}
	p = "a.b.c[0].d"
	v, err = bo.GetAttribute(p, reddo.TypeString)
	if err != nil || v == nil || v.(string) != "1" {
		t.Errorf("%s failed", name)
	}
}

func TestGenericBo_TransferViaJson(t *testing.T) {
	name := "TestGenericBo_TransferViaJson"

	bo := NewGenericBo()
	js, err := bo.ToJson()
	if err != nil || string(js) != "null" {
		t.Errorf("%s failed", name)
	}

	jsonData := []byte(`{"a":1,"b":"a string","c":true}`)
	err = bo.FromJson(jsonData)
	if err != nil {
		t.Errorf("%s failed", name)
	}

	var dest interface{}
	err = bo.TransferViaJson(&dest)
	if err != nil {
		t.Errorf("%s failed", name)
	}
	vDest := reflect.ValueOf(dest)
	if vDest.Kind() != reflect.Map {
		t.Errorf("%s failed", name)
	} else {
		mDest := dest.(map[string]interface{})
		if mDest["a"].(float64) != float64(1) || mDest["b"].(string) != "a string" || mDest["c"].(bool) != true {
			t.Errorf("%s failed", name)
		}
	}
}

func TestGenericBo_TransferViaJson2(t *testing.T) {
	name := "TestGenericBo_TransferViaJson2"

	bo := NewGenericBo()
	js, err := bo.ToJson()
	if err != nil || string(js) != "null" {
		t.Errorf("%s failed", name)
	}

	jsonData := []byte(`{"A":1,"B":"a string","C":true}`)
	err = bo.FromJson(jsonData)
	if err != nil {
		t.Errorf("%s failed", name)
	}

	type MyStruct struct {
		A      int
		FieldB string `json:"b"`
	}
	dest := MyStruct{}
	err = bo.TransferViaJson(&dest)
	if err != nil {
		t.Errorf("%s failed", name)
	}
	if dest.A != 1 && dest.FieldB != "a string" {
		t.Errorf("%s failed", name)
	}
}
