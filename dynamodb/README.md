# godal/dynamodb

[![PkgGoDev](https://pkg.go.dev/badge/github.com/btnguyen2k/godal/dynamodb)](https://pkg.go.dev/github.com/btnguyen2k/godal/dynamodb)
[![codecov](https://codecov.io/gh/btnguyen2k/godal/branch/dynamodb/graph/badge.svg?token=0L23UTJHOZ)](https://app.codecov.io/gh/btnguyen2k/godal/branch/dynamodb)

Generic [AWS DynamoDB](https://aws.amazon.com/dynamodb/) DAO implementation.

## Guideline

**General**

- DAO must implement `IGenericDao.GdaoCreateFilter(string, IGenericBo) FilterOpt.`
- Row-mapper's `ColumnsList(table string) []string` must return all attribute names of specified table's primary key.

**Use `GenericDaoDynamodb` (and `godal.IGenericBo`) directly**

- Define a DAO struct that implements `IGenericDao.GdaoCreateFilter(string, IGenericBo) FilterOpt`.
- Use a row-mapper whose `ColumnsList(table string) []string` must return all attribute names of specified table's primary key.

**Implement custom DynamoDB business DAO and BO**

- Define and implement the business DAO (Note: DAO must implement `IGenericDao.GdaoCreateFilter(string, IGenericBo) FilterOpt`
  and its row-mapper's `ColumnsList(table string) []string` function must return all attribute names of specified table's primary key).
- Define functions to transform `godal.IGenericBo` to business BO and vice versa.

> Optionally, create a helper function to create DAO instances.

**Examples**: see [examples](../examples/) and [examples_sta](../examples_sta/).

> This package uses [github.com/aws/aws-sdk-go](https://github.com/aws/aws-sdk-go) to interact with AWS DynamoDB service.
