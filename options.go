package wire

// OptionFn options pattern used to define and set options for the given
// PostgreSQL server.
type OptionFn func(*Server)

// SimpleQuery sets the simple query handle inside the given server instance.
func SimpleQuery(fn SimpleQueryFn) OptionFn {
	return func(srv *Server) {
		srv.SimpleQuery = fn
	}
}
