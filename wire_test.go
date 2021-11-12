package wire

import (
	"net"
	"testing"
)

// TListenAndServe will open a new TCP listener on a unallocated port inside
// the local network. The newly created listner is passed to the given server to
// start serving PostgreSQL connections. The full listener address is returned
// for clients to interact with the newly created server.
func TListenAndServe(t *testing.T, server *Server) string {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		err := server.Close()
		if err != nil {
			t.Fatal(err)
		}
	})

	go server.Serve(listener)
	return listener.Addr().String()
}
