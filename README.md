# godal

[![Go Report Card](https://goreportcard.com/badge/github.com/btnguyen2k/godal)](https://goreportcard.com/report/github.com/btnguyen2k/godal)
[![GoDoc](https://godoc.org/github.com/btnguyen2k/godal?status.svg)](https://godoc.org/github.com/btnguyen2k/godal)

Generic Database Access Layer implementation in Golang.

## Documentation

- [GoDoc](https://godoc.org/github.com/btnguyen2k/godal)
- [Examples](examples/)
- [Generic MongoDB DAO](mongo/MONGO.md)
- [Generic database/sql DAO](sql/SQL.md)


## History

### 2019-11-03 - v0.1.0

- Breaking changes:
  - Move `AbstractGenericDao.GdaoDelete(...)` to sub-classes (`GenericDaoMongo` and `GenericDaoSql`)
  - `IGenericDao`: `GdaoCreate`, `GdaoUpdate` and `GdaoSave` return `(0, GdaoErrorDuplicatedEntry)` if written row violate data integrity (duplicated key or unique index)
- Add transaction-supported functions to `GenericDaoMongo` and `GenericDaoSql`.
- `GenericDaoSql`:
  - New method `WrapTransaction(ctx context.Context, func(ctx context.Context, tx *sql.Tx) error) error`  
- More tests
- Update dependency libs
- Other fixes & enhancements


### 2019-10-25 - v0.0.4

- `GenericBo`: new function `Checksum() []byte`
- `GenericDaoMongo`:
  - New method `WrapTransaction(ctx context.Context, txFunc func(sctx mongo.SessionContext) error) error`
  - Add tests
  - Fixes & Enhancements
- `GenericDaoSql`:
  - Add tests
  - Fixes & Enhancements
- Update dependency libs
- Other fixes & enhancements


### 2019-09-14 - v0.0.3

- Upgrade dependency libs.


### 2019-04-09 - v0.0.2

- Migrate to Go modular design.
- Generic [database/sql](https://golang.org/pkg/database/sql/) DAO implementation: `GenericDaoSql`


### 2019-03-12 - v0.0.1

- Generic business object:
  - Interface `IGenericBo` & implementation `GenericBo`
- Generic data access object:
  - Interface `IGenericDao` & abstract implementation `AbstractGenericDao`
- Generic [MongoDB](https://www.mongodb.com) DAO implementation: `GenericDaoMongo`


## License

MIT - see [LICENSE.md](LICENSE.md).
