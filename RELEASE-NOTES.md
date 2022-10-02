# godal release notes

## 2022-10-02 - v0.6.0

- Migrated to `btnguyen2k/prom v0.4.x`.

## 2021-10-25 - v0.5.2

- `GenericDaoDynamodb.GdaoFetchMany`: prefix table name with character `!` to query 'backward' instead of the normal 'forward' mode (used only in "query" operation).

## 2021-07-01 - v0.5.1

- (BREAKING CHANGE) `dynamodb` package: implement `FilterOpt` following `IGenericDao` interface.
- Other fixes and enhancements.

## 2021-04-24 - v0.5.0

- (BREAKING CHANGE) `IRowMapper` has 2 new functions `ToDbColName(storageId, fieldName string) string` and `ToBoFieldName(storageId, colName string) string`.
- (BREAKING CHANGE) Introduce `SortingOpt` to abstract sorting of query's result. `IGenericDao` changes accordingly.
- (BREAKING CHANGE) Introduce `FilterOpt` to abstract filtering of query's result. `IGenericDao` changes accordingly.
- (BREAKING CHANGE) Rename `GdaoErrorDuplicatedEntry` to `ErrGdaoDuplicatedEntry`.
- Other BREAKING CHANGES:
  - `IGenericDaoSql/GenericDaoSql.SqlUpdateEx`: make `context.Context` the first parameter.
  - `IGenericDaoSql/GenericDaoSql.SqlSelectEx`: make `context.Context` the first parameter.
  - `IGenericDaoSql/GenericDaoSql.SqlInsertEx`: make `context.Context` the first parameter.
  - `IGenericDaoSql/GenericDaoSql.SqlDeleteEx`: make `context.Context` the first parameter.
- Other fixes and enhancements.

## 2021-03-21 - v0.4.0

- `IGenericBo`: new function `GboImportViaMap(src map[string]interface{}) error`
- Package `sql`: new filters
  - `FilterBetween`: represents single filter `<field> BETWEEN <value1> AND <value2>`.
  - `FilterIsNull`: represents single filter `<field> IS NULL`.
  - `FilterIsNotNull`: represents single filter `<field> IS NOT NULL`.
- Bug fixes, enhancements and refactoring.

## 2021-01-15 - v0.3.0

- `GenericDaoMongo`:
  - Fix: Azure CosmosDB's MongoDB API returns `ConflictingOperationInProgress` error instead of error code `E11000`.
- New package [`cosmosdbsql`](cosmosdbsql/): provides a generic Azure Cosmos DB implementation of `godal.IGenericDao` using `database/sql` interface.
- Other bug fixes and enhancements.

## 2020-10-28 - v0.2.5

- `GenericDaoDynamodb`:
  - `GdaoDeleteMany`: support "query" operation in addition to "scan". Also support filter rows on index.
  - `GdaoFetchMany` support "query" operation in addition to "scan".
- `GenericDaoSql`: add SQLite flavor
- More tests.
- Bug fixes & Enhancements.
- Update docs & dependency libs.

## 2020-06-15 - v0.2.4

- More tests.
- Bug fixes & Enhancements.
- Update docs & dependency libs.
- Removed deprecated functions.

## 2019-12-08 - v0.2.3

- Bug fixes.

## 2019-12-04 - v0.2.1

- More tests.
- Bug fixes & enhancements.
- Update docs & dependency libs.

## 2019-11-25 - v0.2.0

- New package [`dynamodb`](dynamodb/): provides a generic AWS DynamoDB implementation of `godal.IGenericDao`.
- `GenericDaoSql`:
  - Fix nil-pointer bug when passing `nil` filter/sorting.
  - `SqlExecute()` and `SqlQuery()`: always use prepared statement (also fix a bug that int/float columns are returned as []byte)
- `GenericRowMapperSql`:
  - Add `GboFieldToColNameTranslator` and `ColNameToGboFieldTranslator`: define rules to translate column names to field names and vice versa.
- *Breaking changes:*
  - `ColummNameTransformation` renamed to `NameTransformation`
  - Constants `ColNameTransIntact`, `ColNameTransUpperCase` and `ColNameTransLowerCase` renamed to `NameTransfIntact`, `NameTransfUpperCase` and `NameTransfLowerCase`.
  - `GenericRowMapperSql.ColNameTrans` renamed to `NameTransformation`.
- Update docs & dependency libs.
- Removed deprecated functions.
- Other fixes & enhancements.

## 2019-11-03 - v0.1.0

- Breaking changes:
  - Move `AbstractGenericDao.GdaoDelete(...)` to sub-classes (`GenericDaoMongo` and `GenericDaoSql`)
  - `IGenericDao`: `GdaoCreate`, `GdaoUpdate` and `GdaoSave` return `(0, GdaoErrorDuplicatedEntry)` if written row violate data integrity (duplicated key or unique index)
- Add transaction-supported functions to `GenericDaoMongo` and `GenericDaoSql`.
- `GenericDaoSql`:
  - New method `WrapTransaction(ctx context.Context, func(ctx context.Context, tx *sql.Tx) error) error`  
- More tests
- Update dependency libs
- Other fixes & enhancements

## 2019-10-25 - v0.0.4

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

## 2019-09-14 - v0.0.3

- Upgrade dependency libs.

## 2019-04-09 - v0.0.2

- Migrate to Go modular design.
- Generic [database/sql](https://golang.org/pkg/database/sql/) DAO implementation: `GenericDaoSql`

## 2019-03-12 - v0.0.1

- Generic business object:
  - Interface `IGenericBo` & implementation `GenericBo`
- Generic data access object:
  - Interface `IGenericDao` & abstract implementation `AbstractGenericDao`

- Generic [MongoDB](https://www.mongodb.com) DAO implementation: `GenericDaoMongo`