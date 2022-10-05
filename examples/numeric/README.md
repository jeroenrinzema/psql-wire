# Numeric support

This is a small example displaying how to extend the underlying connection to support numeric types.

The Go language does not have a standard decimal type. pgx supports the PostgreSQL numeric type out of the box. However, in the absence of a proper Go type it can only be used when translated to a `float64` or `string`. This is obviously not ideal.

The recommended solution is to use the [github.com/shopspring/decimal](https://github.com/shopspring/decimal) package.