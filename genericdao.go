package godal

import "errors"

// IRowMapper transforms a database row to IGenericBo and vice versa.
//
// Available since v0.0.2
type IRowMapper interface {
	// ToRow transforms a IGenericBo to a row data suitable for persisting to database store.
	ToRow(storageId string, bo IGenericBo) (interface{}, error)

	// ToBo transforms a database row to IGenericBo.
	ToBo(storageId string, row interface{}) (IGenericBo, error)

	// ColumnsList returns list of a column names corresponding to a database store.
	ColumnsList(storageId string) []string

	// ToDbColName maps a IGenericBo field name to the corresponding column name for the database store.
	//
	// Available since v0.5.0
	ToDbColName(storageId, fieldName string) string

	// ToBoFieldName maps a database store's column name to the corresponding IGenericBo field name.
	//
	// Available since v0.5.0
	ToBoFieldName(storageId, colName string) string
}

/*----------------------------------------------------------------------*/

var (
	// ErrGdaoDuplicatedEntry indicates that the write operation failed because of data integrity violation: entry/key duplicated.
	ErrGdaoDuplicatedEntry = errors.New("data integrity violation: duplicated entry/key")
)

// IGenericDao defines API interface of a generic data-access-object.
//
// Sample usage: see AbstractGenericDao for an abstract implementation of IGenericDao.
type IGenericDao interface {
	// GdaoCreateFilter creates a filter object to match exactly one specific BO.
	GdaoCreateFilter(storageId string, bo IGenericBo) FilterOpt

	// GdaoDelete removes the specified BO from database store and returns the number of effected items.
	//
	// Upon successful call, this function returns 1 if the BO is removed, and 0 if the BO does not exist.
	GdaoDelete(storageId string, bo IGenericBo) (int, error)

	// GdaoDeleteMany removes many BOs from database store at once and returns the number of effected items.
	//
	// Upon successful call, this function may return 0 if no BO matches the filter.
	GdaoDeleteMany(storageId string, filter FilterOpt) (int, error)

	// GdaoFetchOne fetches one BO from database store.
	//
	// Filter should match exactly one BO. If there are more than one BO matching the filter, only the first one is returned.
	GdaoFetchOne(storageId string, filter FilterOpt) (IGenericBo, error)

	// GdaoFetchMany fetches many BOs from database store and returns them as a list.
	//
	// startOffset (0-based) and numItems are for paging. numItems <= 0 means no limit. Be noted that some databases do not support startOffset nor paging at all.
	GdaoFetchMany(storageId string, filter FilterOpt, sorting *SortingOpt, startOffset, numItems int) ([]IGenericBo, error)

	// GdaoCreate persists one BO to database store and returns the number of saved items.
	//
	// If the BO already existed, this function does not modify the existing one and should return (0, ErrGdaoDuplicatedEntry).
	GdaoCreate(storageId string, bo IGenericBo) (int, error)

	// GdaoUpdate updates one existing BO and returns the number of updated items.
	//
	// If the BO does not exist, this function does not create new BO and should return (0, nil).
	// If update causes data integrity violation, this function should return (0, ErrGdaoDuplicatedEntry).
	GdaoUpdate(storageId string, bo IGenericBo) (int, error)

	// GdaoSave persists one BO to database store and returns the number of saved items.
	//
	// If the BO already existed, this function replaces the existing one; otherwise new BO is created.
	// If data integrity violation occurs, this function should return (0, ErrGdaoDuplicatedEntry)
	GdaoSave(storageId string, bo IGenericBo) (int, error)

	// GetRowMapper returns the IRowMapper associated with the DAO.
	//
	// Available since v0.3.0.
	GetRowMapper() IRowMapper
}

// NewAbstractGenericDao constructs a new 'AbstractGenericDao' instance.
func NewAbstractGenericDao(gdao IGenericDao) *AbstractGenericDao {
	return &AbstractGenericDao{IGenericDao: gdao}
}

// AbstractGenericDao is an abstract implementation of IGenericDao.
//
// Function implementations (n = No, y = Yes, i = inherited):
//   - (n) GdaoCreateFilter(storageId string, bo IGenericBo) FilterOpt
//   - (n) GdaoDelete(storageId string, bo IGenericBo) (int, error)
//   - (n) GdaoDeleteMany(storageId string, filter FilterOpt) (int, error)
//   - (n) GdaoFetchOne(storageId string, filter FilterOpt) (IGenericBo, error)
//   - (n) GdaoFetchMany(storageId string, filter FilterOpt, sorting *SortingOpt, startOffset, numItems int) ([]IGenericBo, error)
//   - (n) GdaoCreate(storageId string, bo IGenericBo) (int, error)
//   - (n) GdaoUpdate(storageId string, bo IGenericBo) (int, error)
//   - (n) GdaoSave(storageId string, bo IGenericBo) (int, error)
type AbstractGenericDao struct {
	IGenericDao
	rowMapper IRowMapper
}

// GetRowMapper implements IGenericDao.GetRowMapper.
//
// Available since v0.0.2.
func (dao *AbstractGenericDao) GetRowMapper() IRowMapper {
	return dao.rowMapper
}

// SetRowMapper attaches an IRowMapper to the DAO for latter use.
//
// Available since v0.0.2.
func (dao *AbstractGenericDao) SetRowMapper(rowMapper IRowMapper) *AbstractGenericDao {
	dao.rowMapper = rowMapper
	return dao
}
