# godal/mongo

[![PkgGoDev](https://pkg.go.dev/badge/github.com/btnguyen2k/godal/mongo)](https://pkg.go.dev/github.com/btnguyen2k/godal/mongo)
[![codecov](https://codecov.io/gh/btnguyen2k/godal/branch/mongo/graph/badge.svg?token=0L23UTJHOZ)](https://app.codecov.io/gh/btnguyen2k/godal/branch/mongo)

Generic [MongoDB](https://www.mongodb.com) DAO implementation.

## Guideline

**General**

- DAOs must implement `IGenericDao.GdaoCreateFilter(string, IGenericBo) FilterOpt`.

**Use `GenericDaoMongo` (and `godal.IGenericBo`) directly**

- Define a DAO struct that implements `IGenericDao.GdaoCreateFilter(string, IGenericBo) FilterOpt`.

**Implement custom MongoDB business DAOs and BOs**

- Define and implement the business DAO (Note: DAOs must implement `IGenericDao.GdaoCreateFilter(string, IGenericBo) FilterOpt`).
- Define functions to transform `godal.IGenericBo` to business BO and vice versa.

> Optionally, create a helper function to create DAO instances.

**Examples**: see [examples](../examples/) and [examples_sta](../examples_sta/).

> This package uses [go.mongodb.org/mongo-driver/mongo](https://go.mongodb.org/mongo-driver/mongo) as MongoDB driver.
