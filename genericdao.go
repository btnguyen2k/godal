package godal

/*
IGenericDao defines API interface of a generic data-access-object.

Sample usage: see #AbstractGenericDao for an abstract implementation of IGenericDao, and see samples of concrete implementations in folder examples/
*/
type IGenericDao interface {
	/*
	GdaoCreateFilter creates a filter to match a specific BO.
	*/
	GdaoCreateFilter(storageId string, bo IGenericBo) interface{}

	/*
	GdaoDelete removes the specified BO from data storage and returns the number of effected items.

	Upon successful call, this function returns 1 if the BO is removed, and 0 if the BO does not exist.
	*/
	GdaoDelete(storageId string, bo IGenericBo) (int, error)

	/*
	GdaoDeleteMany removes many items from data storage at once and returns the number of effected items.

	Upon successful call, this function may return 0 if no BO matches the filter.
	*/
	GdaoDeleteMany(storageId string, filter interface{}) (int, error)

	/*
	GdaoFetchOne fetches one BO from data storage.

	If there are more than one BO matches the filter, only the first one is returned.
	*/
	GdaoFetchOne(storageId string, filter interface{}) (IGenericBo, error)

	/*
	GdaoFetchOne fetches many BOs from data storage at once.
	*/
	GdaoFetchMany(storageId string, filter interface{}, ordering interface{}) ([]IGenericBo, error)

	/*
	GdaoCreate persists one BO to data storage and returns the number of saved items.

	If the BO already existed, this function does not modify the existing one and should return 0.
	*/
	GdaoCreate(storageId string, bo IGenericBo) (int, error)

	/*
	GdaoUpdate updates one existing BO and returns the number of updated items.

	If the BO does not exist, this function does not create new BO and should return 0.
	*/
	GdaoUpdate(storageId string, bo IGenericBo) (int, error)

	/*
	GdaoSave persists one BO to data storage and returns the number of saved items.

	If the BO already existed, this function replace the existing one; otherwise new BO is created in data storage.
	*/
	GdaoSave(storageId string, bo IGenericBo) (int, error)
}

/*
NewAbstractGenericDao constructs a new 'AbstractGenericDao' instance.
*/
func NewAbstractGenericDao(gdao IGenericDao) *AbstractGenericDao {
	return &AbstractGenericDao{gdao}
}

/*
AbstractGenericDao is an abstract implementation of IGenericDao.

Function implementations (n = No, y = Yes, i = inherited):

	(n) GdaoCreateFilter(storageId string, bo IGenericBo) interface{}
	(y) GdaoDelete(storageId string, bo IGenericBo) (int, error)
	(n) GdaoDeleteMany(storageId string, filter interface{}) (int, error)
	(n) GdaoFetchOne(storageId string, filter interface{}) (IGenericBo, error)
	(n) GdaoFetchMany(storageId string, filter interface{}, ordering interface{}) ([]IGenericBo, error)
	(n) GdaoCreate(storageId string, bo IGenericBo) (int, error)
	(n) GdaoUpdate(storageId string, bo IGenericBo) (int, error)
	(n) GdaoSave(storageId string, bo IGenericBo) (int, error)
*/
type AbstractGenericDao struct {
	IGenericDao
}

/*
GdaoDelete implements IGenericDao.GdaoDelete.

	- This function calls 'GdaoCreateFilter' and 'GdaoDeleteMany', sub-class must implement these functions.
*/
func (dao *AbstractGenericDao) GdaoDelete(storageId string, bo IGenericBo) (int, error) {
	filter := dao.GdaoCreateFilter(storageId, bo)
	return dao.GdaoDeleteMany(storageId, filter)
}
