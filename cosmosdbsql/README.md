# godal/cosmosdbsql

[![PkgGoDev](https://pkg.go.dev/badge/github.com/btnguyen2k/godal/cosmosdbsql)](https://pkg.go.dev/github.com/btnguyen2k/godal/cosmosdbsql)
[![codecov](https://codecov.io/gh/btnguyen2k/godal/branch/cosmosdb/graph/badge.svg?token=0L23UTJHOZ)](https://app.codecov.io/gh/btnguyen2k/godal/branch/cosmosdb)

Generic [Azure Cosmos DB](https://docs.microsoft.com/en-us/azure/cosmos-db/) DAO implementation using `database/sql` interface.

## Guideline

**General**

- Dao must implement `IGenericDao.GdaoCreateFilter(string, IGenericBo) FilterOpt`.

**Use `GenericDaoCosmosdb` (and `godal.IGenericBo`) directly**

- Define a dao struct that extends `GenericDaoCosmosdb` and implements `IGenericDao.GdaoCreateFilter(string, IGenericBo) FilterOpt`.
- Configure either `{collection-name:path-to-fetch-partition_key-value-from-genericbo}` via `GenericDaoCosmosdb.CosmosSetPkGboMapPath`
  or `{collection-name:path-to-fetch-partition_key-value-from-dbrow}` via `GenericDaoCosmosdb.CosmosSetPkRowMapPath`.
- Optionally, configure `{collection-name:path-to-fetch-id-value-from-genericbo}` via `GenericDaoCosmosdb.CosmosSetIdGboMapPath`.
- Optionally, create a helper function to create dao instances.

**Implement custom `database/sql` business dao and bo**

- Define and implement the business dao (Note: dao must implement `IGenericDao.GdaoCreateFilter(string, IGenericBo) FilterOpt`).
- Define functions to transform `godal.IGenericBo` to business bo and vice versa.
- Optionally, create a helper function to create dao instances.

> Partition key (PK) is crucial to CosmosDB. PK value is needed in almost all document related operations. Hence, it's
> important to be able to extract PK value from BO. If using or extending `GenericDaoCosmosdb`, configure either
> `{collection-name:path-to-fetch-partition_key-value-from-genericbo}` via `GenericDaoCosmosdb.CosmosSetPkGboMapPath`
> or `{collection-name:path-to-fetch-partition_key-value-from-dbrow}` via `GenericDaoCosmosdb.CosmosSetPkRowMapPath`.

**Examples**: see [examples](../examples/) and [examples_sta](../examples_sta/).

> This package explicitly uses [github.com/btnguyen2k/gocosmos](https://github.com/btnguyen2k/gocosmos) as the SQL driver for Azure Cosmos DB.
