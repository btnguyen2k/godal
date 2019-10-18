package godal

import (
	"encoding/json"
	"fmt"
	"github.com/btnguyen2k/consu/checksum"
	"github.com/btnguyen2k/consu/reddo"
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
		t.Fatalf("%s failed", name)
	}

	if bo.(*GenericBo).data != nil {
		t.Fatalf("%s failed", name)
	}
}

func TestGenericBo_FromJson(t *testing.T) {
	name := "TestGenericBo_FromJson"

	bo := NewGenericBo()
	err := bo.GboFromJson(jsonData)
	if err != nil {
		t.Fatalf("%s failed: %e", name, err)
	}

	m := make(map[string]interface{})
	json.Unmarshal(jsonData, &m)
	checksum1 := fmt.Sprintf("%x", checksum.Md5Checksum(m))
	checksum2 := fmt.Sprintf("%x", checksum.Md5Checksum(bo.(*GenericBo).data))
	if checksum1 != checksum2 {
		t.Fatalf("%s failed - checksums mismatch [%s] vs [%s]", name, checksum1, checksum2)
	}
}

func TestGenericBo_ToJson(t *testing.T) {
	name := "TestGenericBo_ToJson"

	bo := NewGenericBo()
	err := bo.GboFromJson(jsonData)
	if err != nil {
		t.Fatalf("%s failed", name)
	}
	m1 := make(map[string]interface{})
	json.Unmarshal(jsonData, &m1)
	checksum1 := fmt.Sprintf("%x", checksum.Md5Checksum(m1))

	js, err := bo.GboToJson()
	if err != nil {
		t.Fatalf("%s failed", name)
	}
	m2 := make(map[string]interface{})
	json.Unmarshal(js, &m2)
	checksum2 := fmt.Sprintf("%x", checksum.Md5Checksum(m2))

	if checksum1 != checksum2 {
		t.Fatalf("%s failed - checksums mismatch [%s] vs [%s]", name, checksum1, checksum2)
	}
}

func TestGenericBo_GetAttribute(t *testing.T) {
	name := "TestGenericBo_GetAttribute"

	bo := NewGenericBo()
	err := bo.GboFromJson(jsonData)
	if err != nil {
		t.Fatalf("%s failed", name)
	}

	{
		p := "not_exists"
		_, err := bo.GboGetAttr(p, nil)
		if err != nil {
			t.Fatalf("%s failed for path %s", name, p)
		}
	}

	{
		p := "Name"
		expected := "Standard"
		v, err := bo.GboGetAttr(p, reddo.TypeString)
		if err != nil || v == nil || v.(string) != expected {
			t.Fatalf("%s failed for path %s", name, p)
		}
	}

	{
		p := "ref"
		expected := int64(999)
		v, err := bo.GboGetAttr(p, reddo.TypeInt)
		if err != nil || v == nil || v.(int64) != expected {
			t.Fatalf("%s failed for path %s", name, p)
		}
	}
}

func TestGenericBo_GetTimeWithLayout(t *testing.T) {
	name := "TestGenericBo_GetTimeWithLayout"

	bo := NewGenericBo()
	err := bo.GboFromJson(jsonData)
	if err != nil {
		t.Fatalf("%s failed", name)
	}

	layout := "2006-01-02T15:04:05Z"
	expected := "2018-04-09T23:00:00Z"
	p := "Created"
	v, err := bo.GboGetTimeWithLayout(p, layout)
	if err != nil {
		t.Fatalf("%s failed for path %s", name, p)
	} else {
		s := v.Format(layout)
		if s != expected {
			t.Fatalf("%s failed for path %s", name, p)
		}
	}
}

func TestGenericBo_SetAttribute(t *testing.T) {
	name := "TestGenericBo_SetAttribute"

	bo := NewGenericBo()
	err := bo.GboFromJson(jsonData)
	if err != nil {
		t.Fatalf("%s failed", name)
	}

	p := "a.b.c[].d"
	v, err := bo.GboGetAttr(p, nil)
	if v != nil || err != nil {
		t.Fatalf("%s failed", name)
	}
	err = bo.GboSetAttr(p, 1)
	if err != nil {
		t.Fatalf("%s failed", name)
	}
	p = "a.b.c[0].d"
	v, err = bo.GboGetAttr(p, reddo.TypeString)
	if err != nil || v == nil || v.(string) != "1" {
		t.Fatalf("%s failed", name)
	}
}

func TestGenericBo_SetAttribute2(t *testing.T) {
	name := "TestGenericBo_SetAttribute2"

	bo := NewGenericBo()

	p := "a.b.c[].d"
	v, err := bo.GboGetAttr(p, nil)
	if v != nil || err != nil {
		t.Fatalf("%s failed", name)
	}
	err = bo.GboSetAttr(p, 1)
	if err != nil {
		t.Fatalf("%s failed", name)
	}
	p = "a.b.c[0].d"
	v, err = bo.GboGetAttr(p, reddo.TypeString)
	if err != nil || v == nil || v.(string) != "1" {
		t.Fatalf("%s failed", name)
	}
}

func TestGenericBo_TransferViaJson(t *testing.T) {
	name := "TestGenericBo_TransferViaJson"

	bo := NewGenericBo()
	err := bo.GboFromJson(jsonData)
	if err != nil {
		t.Fatalf("%s failed", name)
	}

	var dest interface{}
	err = bo.GboTransferViaJson(&dest)
	if err != nil {
		t.Fatalf("%s failed", name)
	}

	checksum1 := fmt.Sprintf("%x", checksum.Md5Checksum(bo.(*GenericBo).data))
	checksum2 := fmt.Sprintf("%x", checksum.Md5Checksum(dest))
	if checksum1 != checksum2 {
		t.Fatalf("%s failed - checksums mismatch [%s] vs [%s]", name, checksum1, checksum2)
	}
}

func TestGenericBo_TransferViaJson2(t *testing.T) {
	name := "TestGenericBo_TransferViaJson2"

	bo := NewGenericBo()
	jsonData := []byte(`{"A":1,"B":"a string","C":true}`)
	err := bo.GboFromJson(jsonData)
	if err != nil {
		t.Fatalf("%s failed", name)
	}

	type MyStruct struct {
		A      int
		FieldB string `json:"b"`
	}
	dest := MyStruct{}
	err = bo.GboTransferViaJson(&dest)
	if err != nil {
		t.Fatalf("%s failed", name)
	}
	if dest.A != 1 && dest.FieldB != "a string" {
		t.Fatalf("%s failed", name)
	}
}

func TestGenericBo_ImportViaJson(t *testing.T) {
	name := "TestGenericBo_ImportViaJson"

	bo := NewGenericBo()
	src := map[string]interface{}{"a": float32(1), "b": "a string", "c": true}
	err := bo.GboImportViaJson(src)
	if err != nil {
		t.Fatalf("%s failed", name)
	}

	checksum1 := fmt.Sprintf("%x", checksum.Md5Checksum(bo.(*GenericBo).data))
	checksum2 := fmt.Sprintf("%x", checksum.Md5Checksum(src))
	if checksum1 != checksum2 {
		t.Fatalf("%s failed - checksums mismatch [%s] vs [%s]", name, checksum1, checksum2)
	}
}
