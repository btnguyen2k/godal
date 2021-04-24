package godal

import (
	"testing"
)

func TestNewAbstractGenericDao(t *testing.T) {
	name := "TestNewAbstractGenericDao"
	dao := NewAbstractGenericDao(nil)
	if dao == nil {
		t.Fatalf("%s failed: nil", name)
	}
}

type mockRowMapper struct {
}

func (m *mockRowMapper) ToDbColName(storageId, fieldName string) string {
	panic("implement me")
}

func (m *mockRowMapper) ToBoFieldName(storageId, colName string) string {
	panic("implement me")
}

func (m *mockRowMapper) ToRow(storageId string, bo IGenericBo) (interface{}, error) {
	panic("implement me")
}

func (m *mockRowMapper) ToBo(storageId string, row interface{}) (IGenericBo, error) {
	panic("implement me")
}

func (m *mockRowMapper) ColumnsList(storageId string) []string {
	panic("implement me")
}

func TestAbstractGenericDao_GetSetRowMapper(t *testing.T) {
	name := "TestNewAbstractGenericDao"
	dao := NewAbstractGenericDao(nil)
	rowMapper := &mockRowMapper{}
	dao2 := dao.SetRowMapper(rowMapper)
	if dao2 == nil {
		t.Fatalf("%s failed: nil", name)
	}
	if v := dao.GetRowMapper(); v != rowMapper {
		t.Fatalf("%s failed: expected %p but received %p", name, rowMapper, v)
	}
}
