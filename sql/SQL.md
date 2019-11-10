# godal/sql

[![GoDoc](https://godoc.org/github.com/btnguyen2k/godal/sql?status.svg)](https://godoc.org/github.com/btnguyen2k/godal/sql)

Generic [`database/sql`](https://golang.org/pkg/database/sql/) DAO implementation.

## Guideline

- Dao must implement `IGenericDao.GdaoCreateFilter(string, IGenericBo) interface{}.`
- Use `GenericDaoSql` (and `godal.IGenericBo`) directly:
  - Define a dao struct that implements `IGenericDao.GdaoCreateFilter(string, IGenericBo) interface{}`.
- Implement custom `database/sql` business dao and bo:
  - Define and implement the business dao (Note: dao must implement `IGenericDao.GdaoCreateFilter(string, IGenericBo) interface{}`).
  - Define functions to transform `godal.IGenericBo` to business bo and vice versa.
- Optionally, create a helper function to create dao instances.

**Examples**: see directory [examples](../examples/).
