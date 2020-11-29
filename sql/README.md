# godal/sql

[![PkgGoDev](https://pkg.go.dev/badge/github.com/btnguyen2k/godal/sql)](https://pkg.go.dev/github.com/btnguyen2k/godal/sql)
[![codecov](https://codecov.io/gh/btnguyen2k/godal/branch/sql/graph/badge.svg?token=0L23UTJHOZ)](https://app.codecov.io/gh/btnguyen2k/godal/branch/sql)

Generic [`database/sql`](https://golang.org/pkg/database/sql/) DAO implementation.

## Guideline

**General**

- Dao must implement `IGenericDao.GdaoCreateFilter(string, IGenericBo) interface{}.`

**Use `GenericDaoSql` (and `godal.IGenericBo`) directly**

- Define a dao struct that implements `IGenericDao.GdaoCreateFilter(string, IGenericBo) interface{}`.

**Implement custom `database/sql` business dao and bo**

- Define and implement the business dao (Note: dao must implement `IGenericDao.GdaoCreateFilter(string, IGenericBo) interface{}`).
- Define functions to transform `godal.IGenericBo` to business bo and vice versa.

> Optionally, create a helper function to create dao instances.

**Examples**: see [examples](../examples/) and [examples_sta](../examples_sta/).

> While this package does not use a specific [SQL driver](https://github.com/golang/go/wiki/SQLDrivers), it is highly recommended to use the following SQL drivers with `godal/sql`:
  - MySQL: github.com/go-sql-driver/mysql
  - MSSQL: github.com/denisenkom/go-mssqldb
  - Oracle: github.com/godror/godror
  - PostgreSQL: github.com/jackc/pgx
  - SQLite3: github.com/mattn/go-sqlite3
