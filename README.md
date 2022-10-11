# PSQL wire protocol 🔌

[![CI](https://github.com/jeroenrinzema/psql-wire/actions/workflows/build.yaml/badge.svg)](https://github.com/jeroenrinzema/psql-wire/actions/workflows/build.yaml)
[![Go Reference](https://pkg.go.dev/badge/github.com/jeroenrinzema/psql-wire.svg)](https://pkg.go.dev/github.com/jeroenrinzema/psql-wire) [![Latest release](https://img.shields.io/github/release/jeroenrinzema/psql-wire.svg)](https://github.com/jeroenrinzema/psql-wire/releases) [![Go Report Card](https://goreportcard.com/badge/github.com/jeroenrinzema/psql-wire)](https://goreportcard.com/report/github.com/jeroenrinzema/psql-wire)

A pure Go [PostgreSQL](https://www.postgresql.org/) server wire protocol implementation.
Build your own PostgreSQL server with 15 lines of code.
This project attempts to make it as straight forward as possible to set-up and configure your own PSQL server.
Feel free to check out the [examples](https://github.com/jeroenrinzema/psql-wire/tree/main/examples) directory for various ways on how to configure/set-up your own server.

> 🚧 This project does not include a PSQL parser. Please check out other projects such as [auxten/postgresql-parser](https://github.com/auxten/postgresql-parser) to parse PSQL SQL queries.

```go
package main

import (
	"context"
	"fmt"

	wire "github.com/jeroenrinzema/psql-wire"
)

func main() {
	wire.ListenAndServe("127.0.0.1:5432", func(ctx context.Context, query string, writer wire.DataWriter) error {
		fmt.Println(query)
		return writer.Complete("OK")
	})
}
```

---

## Contributing

Thank you for your interest in contributing to psql-wire!
Check out the open projects and/or issues and feel free to join any ongoing discussion.

Everyone is welcome to contribute, whether it's in the form of code, documentation, bug reports, feature requests, or anything else. We encourage you to experiment with the project and make contributions to help evolve it to meet your needs!

See the [contributing guide](https://github.com/jeroenrinzema/psql-wire/blob/main/CONTRIBUTING.md) for more details.
