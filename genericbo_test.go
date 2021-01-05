package godal

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/btnguyen2k/consu/checksum"
	"github.com/btnguyen2k/consu/reddo"
)

func TestNewGenericBo(t *testing.T) {
	name := "TestNewGenericBo"

	bo := NewGenericBo()
	if bo == nil {
		t.Fatalf("%s failed", name)
	}

	if bo.(*GenericBo).data != nil {
		t.Fatalf("%s failed: BO's data should be nil, but actually is %#v", name, bo.(*GenericBo).data)
	}
}

var genericBoJsonDataSlice = []byte(`
[
  "thanhn",
  "Thanh Nguyen",
  {
    "WorkDays": ["Monday","Tuesday","Wednesday","Thursday","Friday"],
    "work_hours": [9,10,11,12,13,14,15,16]
  },
  true,
  ["Apple","Banana","Orange"],
  103,
  "2020-03-01T04:29:18Z",
  "\"[1,true,{\\\"key\\\":\\\"value\\\"}]\""
]
`)
var genericBoDataSlice = make([]interface{}, 0)
var _ = json.Unmarshal(genericBoJsonDataSlice, &genericBoDataSlice)

var genericBoJsonDataMap = []byte(`
{
  "id": "thanhn",
  "Name": "Thanh Nguyen",
  "Options": {
    "WorkDays": ["Monday","Tuesday","Wednesday","Thursday","Friday"],
    "work_hours": [9,10,11,12,13,14,15,16]
  },
  "active": true,
  "Favourites": ["Apple","Banana","Orange"],
  "depId": 103,
  "Created": "2020-03-01T04:29:18Z",
  "JSON": "\"[1,true,{\\\"key\\\":\\\"value\\\"}]\""
}`)
var genericBoTopFieldsList = []string{"id", "Name", "Options", "active", "Favourites", "depId", "Created", "JSON"}
var genericBoTopFieldsMap = map[string]bool{"id": true, "Name": true, "Options": true, "active": true, "Favourites": true, "depId": true, "Created": true, "JSON": true}
var genericBoDataMap = make(map[string]interface{})
var _ = json.Unmarshal(genericBoJsonDataMap, &genericBoDataMap)

func initGenericBoDataMap() (*GenericBo, error) {
	bo := NewGenericBo()
	return bo.(*GenericBo), bo.GboFromJson(genericBoJsonDataMap)
}

func initGenericBoDataSlice() (*GenericBo, error) {
	bo := NewGenericBo()
	return bo.(*GenericBo), bo.GboFromJson(genericBoJsonDataSlice)
}

func TestGenericBo_Checksum(t *testing.T) {
	name := "TestNewGenericBo"

	bo1, err := initGenericBoDataMap()
	if err != nil {
		t.Fatalf("%s failed: %e", name, err)
	}
	checksum1 := bo1.Checksum()
	bo2, err := initGenericBoDataMap()
	if err != nil {
		t.Fatalf("%s failed: %e", name, err)
	}
	checksum2 := bo2.Checksum()
	if !reflect.DeepEqual(checksum1, checksum2) {
		t.Fatalf("%s failed: expect checksum1 %#v to be equal to checksum2 %#v", name, checksum1, checksum2)
	}

	bo2.GboSetAttr("key", "a value")
	checksum2 = bo2.Checksum()
	if reflect.DeepEqual(checksum1, checksum2) {
		t.Fatalf("%s failed: expect checksum1 %#v to be different from checksum2 %#v", name, checksum1, checksum2)
	}
}

func TestGenericBo_GboIterate_Nil(t *testing.T) {
	name := "TestGenericBo_GboIterate_Map"
	bo := &GenericBo{}
	var err error
	bo.GboIterate(func(kind reflect.Kind, field interface{}, value interface{}) {
		if err != nil {
			return
		}
		err = errors.New("should not reach here")
	})
	if err != nil {
		t.Fatalf("%s failed: %e", name, err)
	}
}

func TestGenericBo_GboIterate_Map(t *testing.T) {
	name := "TestGenericBo_GboIterate_Map"

	bo, err := initGenericBoDataMap()
	if err != nil {
		t.Fatalf("%s failed: %e", name, err)
	}
	visited := make(map[string]bool)
	visitedValues := make(map[string]interface{})
	bo.GboIterate(func(kind reflect.Kind, field interface{}, value interface{}) {
		if err != nil {
			return
		}
		if kind != reflect.Map {
			err = errors.New("expect data to be a map")
		}
		visited[field.(string)] = true
		visitedValues[field.(string)] = value
	})
	if err != nil {
		t.Fatalf("%s failed: %e", name, err)
	}
	if !reflect.DeepEqual(visited, genericBoTopFieldsMap) {
		t.Fatalf("%s failed: expected to visit %#v but received %#v", name, genericBoTopFieldsMap, visited)
	}
	if !reflect.DeepEqual(visitedValues, genericBoDataMap) {
		t.Fatalf("%s failed: expected to visit %#v but received %#v", name, genericBoDataMap, visitedValues)
	}
}

func TestGenericBo_GboIterate_Slice(t *testing.T) {
	name := "TestGenericBo_GboIterate_Slice"

	bo, err := initGenericBoDataSlice()
	if err != nil {
		t.Fatalf("%s failed: %e", name, err)
	}
	visitedValues := make([]interface{}, 0)
	bo.GboIterate(func(kind reflect.Kind, field interface{}, value interface{}) {
		if err != nil {
			return
		}
		if kind != reflect.Slice {
			err = errors.New("expect data to be a slice")
		}
		visitedValues = append(visitedValues, value)
	})
	if err != nil {
		t.Fatalf("%s failed: %e", name, err)
	}
	if !reflect.DeepEqual(visitedValues, genericBoDataSlice) {
		t.Fatalf("%s failed: expected to visit %#v but received %#v", name, genericBoDataSlice, visitedValues)
	}
}

func TestGenericBo_GboGetAttr_Map(t *testing.T) {
	name := "TestGenericBo_GboGetAttr_Map"

	bo, err := initGenericBoDataMap()
	if err != nil {
		t.Fatalf("%s failed: %e", name, err)
	}
	for _, k := range genericBoTopFieldsList {
		bov, err := bo.GboGetAttr(k, nil)
		if err != nil {
			t.Fatalf("%s failed: %e", name+"/GboGetAttr", err)
		}
		bovUnsafe := bo.GboGetAttrUnsafe(k, nil)
		if !reflect.DeepEqual(bovUnsafe, bov) {
			t.Fatalf("%s failed: expected %#v but received %#v", name+"/attr["+k+"]", bov, bovUnsafe)
		}

		v := genericBoDataMap[k]
		if !reflect.DeepEqual(bovUnsafe, v) {
			t.Fatalf("%s failed: expected %#v but received %#v", name+"/attr["+k+"]", v, bovUnsafe)
		}
	}
}

func TestGenericBo_GboGetAttr_Slice(t *testing.T) {
	name := "TestGenericBo_GboGetAttr_Slice"

	bo, err := initGenericBoDataSlice()
	if err != nil {
		t.Fatalf("%s failed: %e", name, err)
	}
	for i, v := range genericBoDataSlice {
		bov, err := bo.GboGetAttr("["+strconv.Itoa(i)+"]", nil)
		if err != nil {
			t.Fatalf("%s failed: %e", name+"/GboGetAttr", err)
		}
		bovUnsafe := bo.GboGetAttrUnsafe("["+strconv.Itoa(i)+"]", nil)
		if !reflect.DeepEqual(bovUnsafe, bov) {
			t.Fatalf("%s failed: expected %#v but received %#v", name+"/attr["+strconv.Itoa(i)+"]", bov, bovUnsafe)
		}
		if !reflect.DeepEqual(bovUnsafe, v) {
			t.Fatalf("%s failed: expected %#v but received %#v", name+"/attr["+strconv.Itoa(i)+"]", v, bovUnsafe)
		}
	}
}

func TestGenericBo_GboGetAttrUnmarshalJson(t *testing.T) {
	name := "TestGenericBo_GboGetAttrUnmarshalJson"

	bo := NewGenericBo()
	data := map[string]interface{}{"str": "a string", "b": true}
	jsBytes, _ := json.Marshal(data)
	jsString := string(jsBytes)
	bo.GboSetAttr("jsonstr", jsString)
	bo.GboSetAttr("pjsonstr", &jsString)
	bo.GboSetAttr("jsonbytes", jsBytes)
	bo.GboSetAttr("pjsonbytes", &jsBytes)

	if v, err := bo.GboGetAttrUnmarshalJson("not_found"); err != nil || v != nil {
		t.Fatalf("%s failed: there should be no data at path [not_found] - %s / %#v", name, err, v)
	}

	bo.GboSetAttr("val_int", 1)
	if v, err := bo.GboGetAttrUnmarshalJson("val_int"); err != nil || v != nil {
		t.Fatalf("%s failed: there should be no valid data at path [val_int] - %s / %#v", name, err, v)
	}

	for _, p := range []string{"jsonstr", "pjsonstr", "jsonbytes", "pjsonbytes"} {
		if v, err := bo.GboGetAttrUnmarshalJson(p); err != nil {
			t.Fatalf("%s failed: %s", name, err)
		} else if !reflect.DeepEqual(data, v) {
			t.Fatalf("%s failed: expected %#v but received %#v", name+"/"+p, data, v)
		}
	}
}

func TestGenericBo_GboGetAttrUnmarshalJson_Map(t *testing.T) {
	name := "TestGenericBo_GboGetAttr_Map"

	bo, err := initGenericBoDataMap()
	if err != nil {
		t.Fatalf("%s failed: %e", name, err)
	}
	bov, err := bo.GboGetAttrUnmarshalJson("JSON")
	if err != nil {
		t.Fatalf("%s failed: %e", name, err)
	}
	bojs, _ := json.Marshal(bov)
	vjson := genericBoDataMap["JSON"]
	if !reflect.DeepEqual(vjson, string(bojs)) {
		t.Fatalf("%s failed: expected %#v but received %#v", name+"/GboGetAttrUnmarshalJson", vjson, string(bojs))
	}
}

func TestGenericBo_GboGetAttrUnmarshalJson_Slice(t *testing.T) {
	name := "TestGenericBo_GboGetAttrUnmarshalJson_Slice"

	bo, err := initGenericBoDataSlice()
	if err != nil {
		t.Fatalf("%s failed: %e", name, err)
	}
	i := len(genericBoDataSlice) - 1
	bov, err := bo.GboGetAttrUnmarshalJson("[" + strconv.Itoa(i) + "]")
	if err != nil {
		t.Fatalf("%s failed: %e", name, err)
	}
	bojs, _ := json.Marshal(bov)
	vjson := genericBoDataSlice[i]
	if !reflect.DeepEqual(vjson, string(bojs)) {
		t.Fatalf("%s failed: expected %#v but received %#v", name+"/GboGetAttrUnmarshalJson", vjson, string(bojs))
	}
}

func TestGenericBo_GboGetTimeWithLayout_Zero(t *testing.T) {
	name := "TestGenericBo_GboGetTimeWithLayout_Zero"

	zero := time.Time{}
	bo := &GenericBo{}
	v, err := bo.GboGetTimeWithLayout("path", time.RFC3339)
	if err != nil {
		t.Fatalf("%s failed", name)
	} else if v.Format(time.RFC3339) != zero.Format(time.RFC3339) {
		t.Fatalf("%s failed: expected %#v but received %#v", name, zero.Format(time.RFC3339), v.Format(time.RFC3339))
	}
}

func TestGenericBo_GboGetTimeWithLayout_Map(t *testing.T) {
	name := "TestGenericBo_GboGetTimeWithLayout_Map"

	bo, err := initGenericBoDataMap()
	if err != nil {
		t.Fatalf("%s failed: %e", name, err)
	}

	layout := "2006-01-02T15:04:05Z"
	p := "Created"
	v, err := bo.GboGetTimeWithLayout(p, layout)
	if err != nil {
		t.Fatalf("%s failed", name+"/GboGetTimeWithLayout")
	} else {
		s := v.Format(layout)
		if !reflect.DeepEqual(s, genericBoDataMap[p]) {
			t.Fatalf("%s failed: expected %#v but received %#v", name+"/GboGetTimeWithLayout", genericBoDataMap[p], s)
		}
	}
}

func TestGenericBo_GboGetTimeWithLayout_Slice(t *testing.T) {
	name := "TestGenericBo_GboGetTimeWithLayout_Slice"

	bo, err := initGenericBoDataSlice()
	if err != nil {
		t.Fatalf("%s failed: %e", name, err)
	}

	layout := "2006-01-02T15:04:05Z"
	p := len(genericBoTopFieldsList) - 2
	v, err := bo.GboGetTimeWithLayout("["+strconv.Itoa(p)+"]", layout)
	if err != nil {
		t.Fatalf("%s failed", name+"/GboGetTimeWithLayout")
	} else {
		s := v.Format(layout)
		if !reflect.DeepEqual(s, genericBoDataSlice[p]) {
			t.Fatalf("%s failed: expected %#v but received %#v", name+"/GboGetTimeWithLayout", genericBoDataSlice[p], s)
		}
	}
}

func TestGenericBo_GboSetAttr_Map(t *testing.T) {
	name := "TestGenericBo_GboSetAttr_Map"

	bo := NewGenericBo()

	p := "a.b.c[].d"
	v, err := bo.GboGetAttr(p, nil)
	if v != nil || err != nil {
		t.Fatalf("%s failed", name+"/GboGetAttr")
	}
	err = bo.GboSetAttr(p, 1)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/GboSetAttr", err)
	}
	p = "a.b.c[0].d"
	v, err = bo.GboGetAttr(p, reddo.TypeString)
	if err != nil || v == nil || v.(string) != "1" {
		t.Fatalf("%s failed", name+"/GboGetAttr")
	}
}

func TestGenericBo_GboSetAttr_Slice(t *testing.T) {
	name := "TestGenericBo_GboSetAttr_Slice"

	bo := NewGenericBo()

	p := "[].a.b.c.d"
	v, err := bo.GboGetAttr(p, nil)
	if v != nil || err != nil {
		t.Fatalf("%s failed", name+"/GboGetAttr")
	}
	err = bo.GboSetAttr(p, 1)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/GboSetAttr", err)
	}
	p = "[0].a.b.c.d"
	v, err = bo.GboGetAttr(p, reddo.TypeString)
	if err != nil || v == nil || v.(string) != "1" {
		t.Fatalf("%s failed", name+"/GboGetAttr")
	}
}

func TestGenericBo_GboToJson_Map(t *testing.T) {
	name := "TestGenericBo_GboToJson_Map"

	bo, err := initGenericBoDataMap()
	if err != nil {
		t.Fatalf("%s failed: %e", name, err)
	}
	m1 := make(map[string]interface{})
	json.Unmarshal(genericBoJsonDataMap, &m1)

	js, err := bo.GboToJson()
	if err != nil {
		t.Fatalf("%s failed: %e", name, err)
	}
	m2 := make(map[string]interface{})
	json.Unmarshal(js, &m2)

	js = bo.GboToJsonUnsafe()
	m3 := make(map[string]interface{})
	json.Unmarshal(js, &m3)

	if !reflect.DeepEqual(m1, m2) {
		t.Fatalf("%s failed: expected %#v but received %#v", name, m1, m2)
	}
	if !reflect.DeepEqual(m1, m3) {
		t.Fatalf("%s failed: expected %#v but received %#v", name, m1, m3)
	}
}

func TestGenericBo_GboToJson_Slice(t *testing.T) {
	name := "TestGenericBo_GboToJson_Slice"

	bo, err := initGenericBoDataSlice()
	if err != nil {
		t.Fatalf("%s failed: %e", name, err)
	}
	m1 := make([]interface{}, 0)
	json.Unmarshal(genericBoJsonDataSlice, &m1)

	js, err := bo.GboToJson()
	if err != nil {
		t.Fatalf("%s failed: %e", name, err)
	}
	m2 := make([]interface{}, 0)
	json.Unmarshal(js, &m2)

	js = bo.GboToJsonUnsafe()
	m3 := make([]interface{}, 0)
	json.Unmarshal(js, &m3)

	if !reflect.DeepEqual(m1, m2) {
		t.Fatalf("%s failed: expected %#v but received %#v", name, m1, m2)
	}
	if !reflect.DeepEqual(m1, m3) {
		t.Fatalf("%s failed: expected %#v but received %#v", name, m1, m3)
	}
}

func TestGenericBo_GboFromJson_Error(t *testing.T) {
	name := "TestGenericBo_GboFromJson_Error"

	bo := NewGenericBo()
	err := bo.GboFromJson([]byte(`error`))
	if err == nil {
		t.Fatalf("%s failed", name)
	}
}

func TestGenericBo_GboFromJson_Nil(t *testing.T) {
	name := "TestGenericBo_GboFromJson_Nil"

	bo := NewGenericBo()
	err := bo.GboFromJson([]byte(`null`))
	if err != nil {
		t.Fatalf("%s failed: %e", name, err)
	}
}

func TestGenericBo_GboFromJson_Map(t *testing.T) {
	name := "TestGenericBo_GboFromJson_Map"

	bo := NewGenericBo()
	err := bo.GboFromJson(genericBoJsonDataMap)
	if err != nil {
		t.Fatalf("%s failed: %e", name, err)
	}

	m := make(map[string]interface{})
	json.Unmarshal(genericBoJsonDataMap, &m)
	checksum1 := fmt.Sprintf("%x", checksum.Md5Checksum(m))
	checksum2 := fmt.Sprintf("%x", checksum.Md5Checksum(bo.(*GenericBo).data))
	if checksum1 != checksum2 {
		t.Fatalf("%s failed - checksums mismatch [%s] vs [%s]", name, checksum1, checksum2)
	}
}

func TestGenericBo_GboFromJson_Slice(t *testing.T) {
	name := "TestGenericBo_GboFromJson_Slice"

	bo := NewGenericBo()
	err := bo.GboFromJson(genericBoJsonDataSlice)
	if err != nil {
		t.Fatalf("%s failed: %e", name, err)
	}

	m := make([]interface{}, 0)
	json.Unmarshal(genericBoJsonDataSlice, &m)
	checksum1 := fmt.Sprintf("%x", checksum.Md5Checksum(m))
	checksum2 := fmt.Sprintf("%x", checksum.Md5Checksum(bo.(*GenericBo).data))
	if checksum1 != checksum2 {
		t.Fatalf("%s failed - checksums mismatch [%s] vs [%s]", name, checksum1, checksum2)
	}
}

func TestGenericBo_GboTransferViaJson_Map(t *testing.T) {
	name := "TestGenericBo_GboTransferViaJson_Map"

	bo, err := initGenericBoDataMap()
	if err != nil {
		t.Fatalf("%s failed: %e", name, err)
	}

	var dest interface{}
	err = bo.GboTransferViaJson(&dest)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/GboTransferViaJson", err)
	}

	if !reflect.DeepEqual(dest, bo.data) {
		t.Fatalf("%s failed: expected %#v but received %#v", name, bo.data, dest)
	}
}

func TestGenericBo_GboTransferViaJson_Slice(t *testing.T) {
	name := "TestGenericBo_GboTransferViaJson_Slice"

	bo, err := initGenericBoDataSlice()
	if err != nil {
		t.Fatalf("%s failed: %e", name, err)
	}

	var dest interface{}
	err = bo.GboTransferViaJson(&dest)
	if err != nil {
		t.Fatalf("%s failed: %e", name+"/GboTransferViaJson", err)
	}

	if !reflect.DeepEqual(dest, bo.data) {
		t.Fatalf("%s failed: expected %#v but received %#v", name, bo.data, dest)
	}
}

func TestGenericBo_GboTransferViaJson_Struct(t *testing.T) {
	name := "TestGenericBo_GboTransferViaJson_Struct"

	bo := NewGenericBo()
	jsonData := []byte(`{"A":1,"B":"a string","C":true}`)
	err := bo.GboFromJson(jsonData)
	if err != nil {
		t.Fatalf("%s failed: %e", name, err)
	}

	type MyStruct struct {
		A      int
		FieldB string `json:"b"`
	}
	dest := MyStruct{}
	err = bo.GboTransferViaJson(&dest)
	if err != nil {
		t.Fatalf("%s failed: %e", name, err)
	}
	if dest.A != 1 && dest.FieldB != "a string" {
		t.Fatalf("%s failed", name)
	}
}

func TestGenericBo_GboImportViaJson_Map(t *testing.T) {
	name := "TestGenericBo_GboImportViaJson_Map"

	bo := NewGenericBo()
	src := make(map[string]interface{})
	json.Unmarshal(genericBoJsonDataMap, &src)
	err := bo.GboImportViaJson(src)
	if err != nil {
		t.Fatalf("%s failed: %e", name, err)
	}
	if !reflect.DeepEqual(src, bo.(*GenericBo).data) {
		t.Fatalf("%s failed: expected %#v but received %#v", name, src, bo.(*GenericBo).data)
	}
}

func TestGenericBo_GboImportViaJson_Slice(t *testing.T) {
	name := "TestGenericBo_GboImportViaJson_Slice"

	bo := NewGenericBo()
	src := make([]interface{}, 0)
	json.Unmarshal(genericBoJsonDataSlice, &src)
	err := bo.GboImportViaJson(src)
	if err != nil {
		t.Fatalf("%s failed: %e", name, err)
	}
	if !reflect.DeepEqual(src, bo.(*GenericBo).data) {
		t.Fatalf("%s failed: expected %#v but received %#v", name, src, bo.(*GenericBo).data)
	}
}

func TestGenericBo_GboImportViaJson_Struct(t *testing.T) {
	name := "TestGenericBo_GboImportViaJson_Struct"

	type MyStruct struct {
		A      int
		FieldB string `json:"b"`
	}

	bo := NewGenericBo()
	src := MyStruct{A: 1, FieldB: "a string"}
	err := bo.GboImportViaJson(src)
	if err != nil {
		t.Fatalf("%s failed: %e", name, err)
	}
	if !reflect.DeepEqual(int64(src.A), bo.GboGetAttrUnsafe("A", reddo.TypeInt)) {
		t.Fatalf("%s failed: expected %#v but received %#v", name, src.A, bo.GboGetAttrUnsafe("A", nil))
	}
	if !reflect.DeepEqual(src.FieldB, bo.GboGetAttrUnsafe("b", nil)) {
		t.Fatalf("%s failed: expected %#v but received %#v", name, src.FieldB, bo.GboGetAttrUnsafe("b", nil))
	}
}
