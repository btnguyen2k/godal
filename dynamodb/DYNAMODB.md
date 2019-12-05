# godal/dynamodb

[![GoDoc](https://godoc.org/github.com/btnguyen2k/godal/dynamodb?status.svg)](https://godoc.org/github.com/btnguyen2k/godal/dynamodb)

Generic [AWS DynamoDB](https://aws.amazon.com/dynamodb/) DAO implementation.

## Guideline

- Dao must implement `IGenericDao.GdaoCreateFilter(string, IGenericBo) interface{}.`
- Row-mapper's `ColumnsList(table string) []string` must return all attribute names of specified table's primary key.
- Use `GenericDaoMongo` (and `godal.IGenericBo`) directly:
  - Define a dao struct that implements `IGenericDao.GdaoCreateFilter(string, IGenericBo) interface{}`.
  - Use a row-mapper whose `ColumnsList(table string) []string` must return all attribute names of specified table's primary key.
- Implement custom DynamoDB business dao and bo:
  - Define and implement the business dao (Note: dao must implement `IGenericDao.GdaoCreateFilter(string, IGenericBo) interface{}`
  and its row-mapper's `ColumnsList(table string) []string` function must return all attribute names of specified table's primary key).
  - Define functions to transform `godal.IGenericBo` to business bo and vice versa.
- Optionally, create a helper function to create dao instances.

**Examples**: see directory [examples](../examples/).
