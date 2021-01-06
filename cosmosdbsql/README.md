# godal/cosmosdbsql

[![PkgGoDev](https://pkg.go.dev/badge/github.com/btnguyen2k/godal/cosmosdbsql)](https://pkg.go.dev/github.com/btnguyen2k/godal/cosmosdbsql)
[![codecov](https://codecov.io/gh/btnguyen2k/godal/branch/cosmosdb/graph/badge.svg?token=0L23UTJHOZ)](https://app.codecov.io/gh/btnguyen2k/godal/branch/cosmosdb)

Generic [Azure Cosmos DB](https://docs.microsoft.com/en-us/azure/cosmos-db/) DAO implementation using `database/sql` interface.

## Guideline

**General**

- Dao must implement `IGenericDao.GdaoCreateFilter(string, IGenericBo) interface{}.`

**Use `GenericDaoCosmosdb` (and `godal.IGenericBo`) directly**

- Define a dao struct that extends `GenericDaoCosmosdb` and implements `IGenericDao.GdaoCreateFilter(string, IGenericBo) interface{}`.

**Implement custom `database/sql` business dao and bo**

- Define and implement the business dao (Note: dao must implement `IGenericDao.GdaoCreateFilter(string, IGenericBo) interface{}`).
- Define functions to transform `godal.IGenericBo` to business bo and vice versa.

> Optionally, create a helper function to create dao instances.

**Examples**: see [examples](../examples/) and [examples_sta](../examples_sta/).

> This package explicitly uses [github.com/btnguyen2k/gocosmos](https://github.com/btnguyen2k/gocosmos) as the SQL driver for Azure Cosmos DB.
