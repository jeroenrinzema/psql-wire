# PSQL wire protocol 🔌

[![CI](https://github.com/jeroenrinzema/psql-wire/actions/workflows/build.yaml/badge.svg)](https://github.com/jeroenrinzema/psql-wire/actions/workflows/build.yaml)
[![Go Reference](https://pkg.go.dev/badge/github.com/jeroenrinzema/psql-wire.svg)](https://pkg.go.dev/github.com/jeroenrinzema/psql-wire) [![Latest release](https://img.shields.io/github/release/jeroenrinzema/psql-wire.svg)](https://github.com/jeroenrinzema/psql-wire/releases) [![Go Report Card](https://goreportcard.com/badge/github.com/jeroenrinzema/psql-wire)](https://goreportcard.com/report/github.com/jeroenrinzema/psql-wire)

A pure Go [PostgreSQL](https://www.postgresql.org/) server wire protocol implementation.
Build your own PostgreSQL server within a few lines of code.
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
	wire.ListenAndServe("127.0.0.1:5432", handler)
}

func handler(ctx context.Context, query string) (wire.PreparedStatements, error) {
	return wire.Prepared(wire.NewStatement(func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
		fmt.Println(query)
		return writer.Complete("OK")
	})), nil
}
```

---

> 🚧 When wanting to debug issues and or inspect the PostgreSQL wire protocol please check out the [psql-proxy](https://github.com/cloudproud/psql-proxy) cli

## Sponsor

<img src="https://github.com/cloudproud/lab/assets/3440116/9a92844f-15a6-45a1-9f75-5f26b56b8ee8" width="150px" alt="Cloud Proud" /><br>

Sponsored by [Cloud Proud](https://cloudproud.nl). A single access point for all your data.
Query all your data sources as one large virtual database using the PostgreSQL protocol and SQL.

```sh
$ docker run -p 5432:5432 registry.cloudproud.nl/lab/kit
$ # The web interface is up and running at: http://localhost:5432
$ # You could login using the default username and password kit:kitpw
```

## Support

Feel free to join the Cloud Proud [Slack](https://join.slack.com/t/cloudproud/shared_invite/zt-23094hi83-MnbKFknPmPsnqnMtXUYfUg) workspace to discuss feature requests or issues.

## Contributing

Thank you for your interest in contributing to psql-wire!
Check out the open projects and/or issues and feel free to join any ongoing discussion.
Feel free to checkout the [open TODO's](https://github.com/jeroenrinzema/psql-wire/issues?q=is%3Aissue+is%3Aopen+label%3Atodo) within the project.

Everyone is welcome to contribute, whether it's in the form of code, documentation, bug reports, feature requests, or anything else. We encourage you to experiment with the project and make contributions to help evolve it to meet your needs!

See the [contributing guide](https://github.com/jeroenrinzema/psql-wire/blob/main/CONTRIBUTING.md) for more details.
