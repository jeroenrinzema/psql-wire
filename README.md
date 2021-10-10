# PSQL wire protocol 🔌

A pure Go [PostgreSQL](https://www.postgresql.org/) server wire protocol implementation.
Build your own PostgreSQL server with 15 lines of code.
This project attempts to make it as straigt forward as possible to set-up and configure your own PSQL server.

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