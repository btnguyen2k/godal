# godal/cosmosdbsql

[![PkgGoDev](https://pkg.go.dev/badge/github.com/btnguyen2k/godal/cosmosdbsql)](https://pkg.go.dev/github.com/btnguyen2k/godal/cosmosdbsql)
[![codecov](https://codecov.io/gh/btnguyen2k/godal/branch/cosmosdb/graph/badge.svg?token=0L23UTJHOZ)](https://app.codecov.io/gh/btnguyen2k/godal/branch/cosmosdb)

Generic [Azure Cosmos DB](https://docs.microsoft.com/en-us/azure/cosmos-db/) DAO implementation using `database/sql` interface.

## Guideline

**General**

- Dao must implement `IGenericDao.GdaoCreateFilter(string, IGenericBo) interface{}.`
- Row-mapper's `ColumnsList(table string) []string` must return all attribute names of specified table's primary key.

**Use `GenericDaoMongo` (and `godal.IGenericBo`) directly**

- Define a dao struct that implements `IGenericDao.GdaoCreateFilter(string, IGenericBo) interface{}`.
- Use a row-mapper whose `ColumnsList(table string) []string` must return all attribute names of specified table's primary key.

**Implement custom DynamoDB business dao and bo**

- Define and implement the business dao (Note: dao must implement `IGenericDao.GdaoCreateFilter(string, IGenericBo) interface{}`
  and its row-mapper's `ColumnsList(table string) []string` function must return all attribute names of specified table's primary key).
- Define functions to transform `godal.IGenericBo` to business bo and vice versa.

> Optionally, create a helper function to create dao instances.

**Examples**: see [examples](../examples/) and [examples_sta](../examples_sta/).

> This package uses [github.com/aws/aws-sdk-go](https://github.com/aws/aws-sdk-go) to access AWS DynamoDB.
