package sql

import (
	"encoding/json"
	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/consu/semita"
	"github.com/btnguyen2k/godal"
)

const timeZone = "Asia/Ho_Chi_Minh"

type MyBo struct {
	Id      string `json:"id"`
	Name    string `json:"name"`
	Version int    `json:"version"`
}

func (bo *MyBo) ToGbo() godal.IGenericBo {
	js, _ := json.Marshal(bo)
	gbo := godal.NewGenericBo()
	if err := gbo.GboImportViaJson(map[string]interface{}{fieldGboId: bo.Id, fieldGboData: string(js)}); err != nil {
		panic(err)
	}
	return gbo
}

func fromGbo(gbo godal.IGenericBo) *MyBo {
	js := gbo.GboGetAttrUnsafe(fieldGboData, reddo.TypeString).(string)
	bo := MyBo{}
	if err := json.Unmarshal([]byte(js), &bo); err != nil {
		panic(err)
	}
	return &bo
}

const (
	tableName = "test"
	colId     = "id"
	colData   = "data"

	fieldGboId   = "id"
	fieldGboData = "data"
)

type MyRowMapperSql struct {
}

func (mapper *MyRowMapperSql) ColumnsList(storageId string) []string {
	return []string{colId, colData}
}

func (mapper *MyRowMapperSql) ToRow(storageId string, gbo godal.IGenericBo) (interface{}, error) {
	if gbo == nil {
		return nil, nil
	}
	return map[string]interface{}{
		colId:   gbo.GboGetAttrUnsafe(fieldGboId, reddo.TypeString),
		colData: gbo.GboGetAttrUnsafe(fieldGboData, reddo.TypeString),
	}, nil
}

func (mapper *MyRowMapperSql) ToBo(storageId string, row interface{}) (godal.IGenericBo, error) {
	if row == nil {
		return nil, nil
	}
	s := semita.NewSemita(row)
	id, _ := s.GetValueOfType(colId, reddo.TypeString)
	data, _ := s.GetValueOfType(colData, reddo.TypeString)
	bo := godal.NewGenericBo()
	bo.GboSetAttr(fieldGboId, id)
	bo.GboSetAttr(fieldGboData, data)
	return bo, nil
}
