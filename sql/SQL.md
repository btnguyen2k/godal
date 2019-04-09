# godal/mongo

Generic [MongoDB](https://www.mongodb.com) DAO implementation.

Examples: see directory [examples](../examples/).

Guideline:

- Must implement method `godal.IGenericDao.GdaoCreateFilter(storageId string, bo godal.IGenericBo) interface{}`
- If application uses its own BOs instead of `godal.IGenericBo`, it is recommended to implement a utility method to transform `godal.IGenericBo` to application's BO and vice versa.
