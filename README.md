# godal

[![Go Report Card](https://goreportcard.com/badge/github.com/btnguyen2k/godal)](https://goreportcard.com/report/github.com/btnguyen2k/godal)
[![GoDoc](https://godoc.org/github.com/btnguyen2k/godal?status.svg)](https://godoc.org/github.com/btnguyen2k/godal)

Generic Database Access Layer implementation in Golang.

Latest release [v0.2.3](RELEASE-NOTES.md).

## Feature overview

- Interface for generic business object (BO) and data access object (DAO).
- Generic implementation of BO.
- Generic implementation of [MongoDB](https://www.mongodb.com/) DAO.
- Generic implementation of [AWS DynamoDB](https://aws.amazon.com/dynamodb/) DAO.
- Generic implementation of [`database/sql`](https://golang.org/pkg/database/sql/) DAO:
  - MSSQL
  - MySQL
  - Oracle
  - PostgreSQL

## Installation

```go
go get github.com/btnguyen2k/godal
```

## Documentation

- [GoDoc](https://godoc.org/github.com/btnguyen2k/godal)
- [Examples](examples/)
- [Generic AWS DynamoDB DAO](dynamodb/DYNAMODB.md)
- [Generic MongoDB DAO](mongo/MONGO.md)
- [Generic database/sql DAO](sql/SQL.md)


## Contributing

Use [Github issues](./issues) for bug reports and feature requests.

Contribute by Pull Request:

1. Fork `Godal` on github (https://help.github.com/articles/fork-a-repo/)
2. Create a topic branch (`git checkout -b my_branch`)
3. Implement your change
4. Push to your branch (`git push origin my_branch`)
5. Post a pull request on github (https://help.github.com/articles/creating-a-pull-request/)


## License

MIT - see [LICENSE.md](LICENSE.md).
