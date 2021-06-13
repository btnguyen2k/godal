// Base code for SQL-dao example. This is not a standalone script!
package main

import (
	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/godal/examples/common"

	"github.com/btnguyen2k/godal"
	"github.com/btnguyen2k/godal/sql"
)

var colsSql = []string{"id", "val_desc", "val_bool", "val_int", "val_float", "val_string",
	"val_time", "val_timez", "val_date", "val_datez", "val_datetime", "val_datetimez", "val_timestamp", "val_timestampz",
	"val_list", "val_map"}

/*----------------------------------------------------------------------*/

// toGenericBo transforms BoApp to godal.IGenericBo
func toGenericBo(bo *common.BoApp) (godal.IGenericBo, error) {
	if bo == nil {
		return nil, nil
	}
	return bo.toGenericBo(), nil
}

// toBoApp transforms godal.IGenericBo to BoApp
func toBoApp(gbo godal.IGenericBo) (*common.BoApp, error) {
	if gbo == nil {
		return nil, nil
	}
	bo := &common.BoApp{}
	return bo.fromGenericBo(gbo), nil
}

/*----------------------------------------------------------------------*/

// DaoAppSql is SQL-based DAO implementation.
type DaoAppSql struct {
	// *sql.GenericDaoSql
	sql.IGenericDaoSql
	tableName string
}

// GdaoCreateFilter implements godal.IGenericDao.GdaoCreateFilter.
func (dao *DaoAppSql) GdaoCreateFilter(_ string, bo godal.IGenericBo) godal.FilterOpt {
	id, _ := bo.GboGetAttr("id", reddo.TypeString)
	return &godal.FilterOptFieldOpValue{FieldName: "id", Operator: godal.FilterOpEqual, Value: id}
}

// EnableTxMode implements IDaoApp.EnableTxMode
func (dao *DaoAppSql) EnableTxMode(txMode bool) {
	dao.SetTxModeOnWrite(txMode)
}

// Delete implements IDaoApp.Delete
func (dao *DaoAppSql) Delete(bo *common.BoApp) (bool, error) {
	gbo, err := toGenericBo(bo)
	if err != nil {
		return false, err
	}
	numRows, err := dao.GdaoDelete(dao.tableName, gbo)
	return numRows > 0, err
}

// Create implements IDaoApp.Create
func (dao *DaoAppSql) Create(bo *common.BoApp) (bool, error) {
	gbo, err := toGenericBo(bo)
	if err != nil {
		return false, err
	}
	numRows, err := dao.GdaoCreate(dao.tableName, gbo)
	return numRows > 0, err
}

// Get implements IDaoApp.Get
func (dao *DaoAppSql) Get(id string) (*common.BoApp, error) {
	filter := &godal.FilterOptFieldOpValue{FieldName: "id", Operator: godal.FilterOpEqual, Value: id}
	gbo, err := dao.GdaoFetchOne(dao.tableName, filter)
	if err != nil || gbo == nil {
		return nil, err
	}
	return toBoApp(gbo)
}

// GetAll implements IDaoApp.GetAll
func (dao *DaoAppSql) GetAll() ([]*common.BoApp, error) {
	sorting := (&godal.SortingOpt{}).Add(&godal.SortingField{FieldName: "val_time"})
	rows, err := dao.GdaoFetchMany(dao.tableName, nil, sorting, 0, 0)
	if err != nil {
		return nil, err
	}
	var result []*common.BoApp
	for _, row := range rows {
		bo, err := toBoApp(row)
		if err != nil {
			return nil, err
		}
		result = append(result, bo)
	}
	return result, nil
}

// GetN implements IDaoApp.GetN
func (dao *DaoAppSql) GetN(startOffset, numRows int) ([]*common.BoApp, error) {
	// sorting := map[string]int{"id": 1} // sort by "id" attribute, ascending
	rows, err := dao.GdaoFetchMany(dao.tableName, nil, nil, startOffset, numRows)
	if err != nil {
		return nil, err
	}
	var result []*common.BoApp
	for _, row := range rows {
		bo, err := toBoApp(row)
		if err != nil {
			return nil, err
		}
		result = append(result, bo)
	}
	return result, nil
}

// Update implements IDaoApp.Update
func (dao *DaoAppSql) Update(bo *common.BoApp) (bool, error) {
	gbo, err := toGenericBo(bo)
	if err != nil {
		return false, err
	}
	numRows, err := dao.GdaoUpdate(dao.tableName, gbo)
	return numRows > 0, err
}

// Upsert implements IDaoApp.Upsert
func (dao *DaoAppSql) Upsert(bo *common.BoApp) (bool, error) {
	gbo, err := toGenericBo(bo)
	if err != nil {
		return false, err
	}
	numRows, err := dao.GdaoSave(dao.tableName, gbo)
	return numRows > 0, err
}
