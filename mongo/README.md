# godal/mongo

[![PkgGoDev](https://pkg.go.dev/badge/github.com/btnguyen2k/godal/mongo)](https://pkg.go.dev/github.com/btnguyen2k/godal/mongo)
[![codecov](https://codecov.io/gh/btnguyen2k/godal/branch/mongo/graph/badge.svg?token=0L23UTJHOZ)](https://app.codecov.io/gh/btnguyen2k/godal/branch/mongo)

Generic [MongoDB](https://www.mongodb.com) DAO implementation.

## Guideline

**General**

- Dao must implement `IGenericDao.GdaoCreateFilter(string, IGenericBo) FilterOpt`.

**Use `GenericDaoMongo` (and `godal.IGenericBo`) directly**

- Define a dao struct that implements `IGenericDao.GdaoCreateFilter(string, IGenericBo) FilterOpt`.

**Implement custom MongoDB business dao and bo**

- Define and implement the business dao (Note: dao must implement `IGenericDao.GdaoCreateFilter(string, IGenericBo) FilterOpt`).
- Define functions to transform `godal.IGenericBo` to business bo and vice versa.

> Optionally, create a helper function to create dao instances.

**Examples**: see [examples](../examples/) and [examples_sta](../examples_sta/).

> This package uses [go.mongodb.org/mongo-driver/mongo](https://go.mongodb.org/mongo-driver/mongo) as MongoDB driver.
