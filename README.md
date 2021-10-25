# godal

[![Go Report Card](https://goreportcard.com/badge/github.com/btnguyen2k/godal)](https://goreportcard.com/report/github.com/btnguyen2k/godal)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/btnguyen2k/godal)](https://pkg.go.dev/github.com/btnguyen2k/godal)
[![Actions Status](https://github.com/btnguyen2k/godal/workflows/godal/badge.svg)](https://github.com/btnguyen2k/godal/actions)
[![codecov](https://codecov.io/gh/btnguyen2k/godal/branch/master/graph/badge.svg?token=0L23UTJHOZ)](https://codecov.io/gh/btnguyen2k/godal)
[![Release](https://img.shields.io/github/release/btnguyen2k/godal.svg?style=flat-square)](RELEASE-NOTES.md)

Generic Database Access Layer library for Go (Golang).

## Feature overview

- Interface for generic business object (BO) and data access object (DAO).
- Generic BO implementation.
- [Generic DAO implementation](./dynamodb/) for [AWS DynamoDB](https://aws.amazon.com/dynamodb/).
- Generic DAO implementation for [Azure Cosmos DB](https://docs.microsoft.com/en-us/azure/cosmos-db/).
  - [`database/sql` implementation](./cosmosdbsql/).
- [Generic DAO implementation](./mongo/) for [MongoDB](https://www.mongodb.com/).
- [Generic DAO implementation](./sql/) for [`database/sql`](https://golang.org/pkg/database/sql/). Ready-to-use implementations:
  - MSSQL
  - MySQL
  - Oracle
  - PostgreSQL
  - SQLite3

## Installation

```go
go get github.com/btnguyen2k/godal
```

## Usage & Documentation

- [![PkgGoDev](https://pkg.go.dev/badge/github.com/btnguyen2k/godal)](https://pkg.go.dev/github.com/btnguyen2k/godal)
- Samples: see [examples](./examples/) and [examples_sta](./examples_sta/)
- [Wiki](https://github.com/btnguyen2k/godal/wiki)

## Contributing

Use [Github issues](https://github.com/btnguyen2k/godal/issues) for bug reports and feature requests.

Contribute by Pull Request:

1. Fork `Godal` on github (https://help.github.com/articles/fork-a-repo/)
2. Create a topic branch (`git checkout -b my_branch`)
3. Implement your change
4. Push to your branch (`git push origin my_branch`)
5. Post a pull request on github (https://help.github.com/articles/creating-a-pull-request/)

## License

MIT - see [LICENSE.md](LICENSE.md).
