package main

import (
	"fmt"

	"github.com/btnguyen2k/godal"
)

func main() {
	js := []byte(`{
    "profile": {
        "name": "Thanh Nguyen",
        "dept_id": 103,
        "start_date": "Tuesday, 19 May 2020, 3:34:05 PM GMT"
    },
    "options": {
        "work_hours": [9,10,11,12,13,14,15,16]
    },
	"config": "{\"active\":true, \"level\":1, \"alias\":\"myid\"}"
}`)
	bo := godal.NewGenericBo()
	bo.GboFromJson(js)

	{
		type MyStruct struct {
			Profile map[string]interface{}
			Options map[string]interface{} `json:"-"`
		}
		mystr := MyStruct{
			Profile: map[string]interface{}{
				"name": map[string]interface{}{
					"first": "Thanh",
					"last":  "Nguyen",
				},
				"active": true,
			},
			Options: map[string]interface{}{
				"work_hours": []int{9, 10, 11, 12, 13, 14, 15, 16},
			},
		}
		bo.GboImportViaJson(mystr)
		fmt.Printf("Value: %s\n", bo.GboToJsonUnsafe())
	}
}
