package wire

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/mock"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tlsStateRecorder records the TLS connection state observed by the
// authentication strategy and the query handler of a single connection.
type tlsStateRecorder struct {
	auth    chan *tls.ConnectionState
	handler chan *tls.ConnectionState
	authed  atomic.Bool
}

func newTLSStateRecorder() *tlsStateRecorder {
	return &tlsStateRecorder{
		auth:    make(chan *tls.ConnectionState, 1),
		handler: make(chan *tls.ConnectionState, 1),
	}
}

func (r *tlsStateRecorder) options() []OptionFn {
	strategy := func(ctx context.Context, writer *buffer.Writer, reader *buffer.Reader) (context.Context, error) {
		r.authed.Store(true)
		r.auth <- TLSConnectionState(ctx)
		return ctx, writeAuthType(writer, authOK)
	}

	return []OptionFn{SessionAuthStrategy(strategy)}
}

func (r *tlsStateRecorder) handle(ctx context.Context, query Query) (PreparedStatements, error) {
	statement := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
		r.handler <- TLSConnectionState(ctx)
		return writer.Complete("OK")
	})

	return Prepared(statement), nil
}

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()

	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for value")
		var zero T
		return zero
	}
}

// generateTestCertWithUsage generates a self-signed certificate valid for the
// given extended key usage.
func generateTestCertWithUsage(t *testing.T, cn string, usage x509.ExtKeyUsage) (tls.Certificate, *x509.Certificate) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	leaf, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, leaf
}

// dialTLS dials the given address, sends a SSLRequest and upgrades the
// connection to TLS using the given client configuration.
func dialTLS(t *testing.T, address *net.TCPAddr, config *tls.Config) *tls.Conn {
	t.Helper()

	conn, err := net.Dial("tcp", address.String())
	require.NoError(t, err)

	_, err = conn.Write(sslRequestMessage())
	require.NoError(t, err)

	answer := make([]byte, 1)
	_, err = io.ReadFull(conn, answer)
	require.NoError(t, err)
	require.Equal(t, []byte(sslSupported), answer)

	tlsConn := tls.Client(conn, config)
	require.NoError(t, tlsConn.Handshake())
	return tlsConn
}

func sslRequestMessage() []byte {
	msg := make([]byte, 8)
	binary.BigEndian.PutUint32(msg[0:4], 8)
	binary.BigEndian.PutUint32(msg[4:8], uint32(types.VersionSSLRequest))
	return msg
}

func cancelRequestMessage(processID, secretKey int32) []byte {
	msg := make([]byte, 16)
	binary.BigEndian.PutUint32(msg[0:4], 16)
	binary.BigEndian.PutUint32(msg[4:8], uint32(types.VersionCancel))
	binary.BigEndian.PutUint32(msg[8:12], uint32(processID))
	binary.BigEndian.PutUint32(msg[12:16], uint32(secretKey))
	return msg
}

func startupMessage() []byte {
	params := []byte("user\x00test\x00\x00")
	msg := make([]byte, 8, 8+len(params))
	binary.BigEndian.PutUint32(msg[0:4], uint32(8+len(params)))
	binary.BigEndian.PutUint32(msg[4:8], uint32(types.Version30))
	return append(msg, params...)
}

// assertClosedWithoutAuth asserts that the server closes the given connection
// without ever announcing a successful authentication.
func assertClosedWithoutAuth(t *testing.T, conn net.Conn) {
	t.Helper()

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	received, err := io.ReadAll(conn)
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		t.Fatal("server did not close the connection")
	}

	authOKMsg := []byte{byte(types.ServerAuth), 0, 0, 0, 8, 0, 0, 0, 0}
	assert.NotContains(t, string(received), string(authOKMsg), "server authenticated the connection")
}

func TestTLSCertificateReload(t *testing.T) {
	t.Parallel()

	first, firstLeaf := generateTestCertWithUsage(t, "first", x509.ExtKeyUsageServerAuth)
	second, secondLeaf := generateTestCertWithUsage(t, "second", x509.ExtKeyUsageServerAuth)

	configs := map[string]func(current *atomic.Pointer[tls.Certificate]) *tls.Config{
		"GetCertificate": func(current *atomic.Pointer[tls.Certificate]) *tls.Config {
			return &tls.Config{
				GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
					return current.Load(), nil
				},
			}
		},
		"GetConfigForClient": func(current *atomic.Pointer[tls.Certificate]) *tls.Config {
			return &tls.Config{
				GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
					return &tls.Config{Certificates: []tls.Certificate{*current.Load()}}, nil
				},
			}
		},
	}

	for name, config := range configs {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var current atomic.Pointer[tls.Certificate]
			current.Store(&first)

			recorder := newTLSStateRecorder()
			options := append(recorder.options(), Logger(slogt.New(t)), TLSConfig(config(&current)))

			server, err := NewServer(recorder.handle, options...)
			require.NoError(t, err)

			address := TListenAndServe(t, server)

			connstr := fmt.Sprintf("postgres://%s:%d?sslmode=require", address.IP, address.Port)
			conn, err := pgx.Connect(context.Background(), connstr)
			require.NoError(t, err)
			defer conn.Close(context.Background()) //nolint:errcheck

			_, err = conn.Exec(context.Background(), "SELECT 1;")
			require.NoError(t, err)

			for _, state := range []*tls.ConnectionState{receive(t, recorder.auth), receive(t, recorder.handler)} {
				require.NotNil(t, state, "TLS connection state not available")
				assert.True(t, state.HandshakeComplete)
			}

			// The certificate is resolved for every connection, so a rotated
			// certificate is served to new connections.
			rotations := []struct {
				cert *tls.Certificate
				leaf *x509.Certificate
			}{{&first, firstLeaf}, {&second, secondLeaf}}

			for _, rotation := range rotations {
				current.Store(rotation.cert)
				expected := rotation.leaf

				tlsConn := dialTLS(t, address, &tls.Config{InsecureSkipVerify: true})
				served := tlsConn.ConnectionState().PeerCertificates
				require.NoError(t, tlsConn.Close())

				require.NotEmpty(t, served)
				assert.True(t, expected.Equal(served[0]), "server presented %q, expected %q", served[0].Subject.CommonName, expected.Subject.CommonName)
			}
		})
	}
}

func TestTLSClientCertificate(t *testing.T) {
	t.Parallel()

	serverCert, _ := generateTestCertWithUsage(t, "server", x509.ExtKeyUsageServerAuth)
	clientCert, clientLeaf := generateTestCertWithUsage(t, "client", x509.ExtKeyUsageClientAuth)

	roots := x509.NewCertPool()
	roots.AddCert(clientLeaf)

	var verified atomic.Int32
	config := &tls.Config{
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return &serverCert, nil
		},
		ClientAuth: tls.VerifyClientCertIfGiven,
		ClientCAs:  roots,
		VerifyConnection: func(state tls.ConnectionState) error {
			verified.Add(1)
			return nil
		},
	}

	recorder := newTLSStateRecorder()
	options := append(recorder.options(), Logger(slogt.New(t)), TLSConfig(config))

	server, err := NewServer(recorder.handle, options...)
	require.NoError(t, err)

	address := TListenAndServe(t, server)

	t.Run("with certificate", func(t *testing.T) {
		conn := dialTLS(t, address, &tls.Config{
			InsecureSkipVerify: true,
			Certificates:       []tls.Certificate{clientCert},
		})

		client := mock.NewClient(t, conn)
		client.Handshake(t)
		client.Authenticate(t)
		client.ReadyForQuery(t, types.ServerIdle)
		client.Close(t)

		state := receive(t, recorder.auth)
		require.NotNil(t, state)
		require.Len(t, state.PeerCertificates, 1)
		assert.Equal(t, "client", state.PeerCertificates[0].Subject.CommonName)
		require.Len(t, state.VerifiedChains, 1)
	})

	t.Run("without certificate", func(t *testing.T) {
		conn := dialTLS(t, address, &tls.Config{InsecureSkipVerify: true})

		client := mock.NewClient(t, conn)
		client.Handshake(t)
		client.Authenticate(t)
		client.ReadyForQuery(t, types.ServerIdle)
		client.Close(t)

		state := receive(t, recorder.auth)
		require.NotNil(t, state)
		assert.Empty(t, state.PeerCertificates)
	})

	t.Run("plaintext", func(t *testing.T) {
		conn, err := net.Dial("tcp", address.String())
		require.NoError(t, err)

		client := mock.NewClient(t, conn)
		client.Handshake(t)
		client.Authenticate(t)
		client.ReadyForQuery(t, types.ServerIdle)
		client.Close(t)

		assert.Nil(t, receive(t, recorder.auth))
	})

	t.Run("rejected by VerifyConnection", func(t *testing.T) {
		rejecting := config.Clone()
		rejecting.VerifyConnection = func(tls.ConnectionState) error {
			return errors.New("rejected")
		}

		server, err := NewServer(recorder.handle, append(recorder.options(), Logger(slogt.New(t)), TLSConfig(rejecting))...)
		require.NoError(t, err)

		address := TListenAndServe(t, server)

		conn, err := net.Dial("tcp", address.String())
		require.NoError(t, err)
		defer conn.Close() //nolint:errcheck

		_, err = conn.Write(sslRequestMessage())
		require.NoError(t, err)

		answer := make([]byte, 1)
		_, err = io.ReadFull(conn, answer)
		require.NoError(t, err)
		require.Equal(t, []byte(sslSupported), answer)

		tlsConn := tls.Client(conn, &tls.Config{InsecureSkipVerify: true})
		_ = tlsConn.Handshake()
		assertClosedWithoutAuth(t, tlsConn)

		select {
		case <-recorder.auth:
			t.Fatal("authentication strategy called for a rejected connection")
		default:
		}
	})

	assert.Equal(t, int32(2), verified.Load())
}

// countingConn counts the bytes written through it and whether it has been
// closed.
type countingConn struct {
	net.Conn
	written atomic.Int64
	closed  atomic.Bool
}

func (c *countingConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	c.written.Add(int64(n))
	return n, err
}

func (c *countingConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

func TestWrapConn(t *testing.T) {
	t.Parallel()

	cert, _ := generateTestCertWithUsage(t, "server", x509.ExtKeyUsageServerAuth)

	type wrapped struct {
		conn       *countingConn
		isTLS      bool
		state      *tls.ConnectionState
		authBefore bool
	}

	recorder := newTLSStateRecorder()
	// Buffered beyond a single connection so that a duplicate call fails the
	// test instead of blocking the server.
	conns := make(chan wrapped, 4)
	wrap := func(ctx context.Context, conn net.Conn) (net.Conn, error) {
		_, isTLS := conn.(*tls.Conn)
		counting := &countingConn{Conn: conn}
		conns <- wrapped{
			conn:       counting,
			isTLS:      isTLS,
			state:      TLSConnectionState(ctx),
			authBefore: recorder.authed.Load(),
		}
		return counting, nil
	}

	cancels := make(chan struct{}, 1)
	cancelRequest := func(ctx context.Context, processID, secretKey int32) error {
		cancels <- struct{}{}
		return nil
	}

	options := append(recorder.options(),
		Logger(slogt.New(t)),
		TLSConfig(&tls.Config{Certificates: []tls.Certificate{cert}}),
		WrapConn(wrap),
		CancelRequest(cancelRequest),
	)

	server, err := NewServer(recorder.handle, options...)
	require.NoError(t, err)

	address := TListenAndServe(t, server)

	run := func(t *testing.T, conn net.Conn) wrapped {
		recorder.authed.Store(false)

		client := mock.NewClient(t, conn)
		client.Handshake(t)
		client.Authenticate(t)
		client.ReadyForQuery(t, types.ServerIdle)
		client.Close(t)

		receive(t, recorder.auth)
		result := receive(t, conns)
		assert.False(t, result.authBefore, "connection wrapped after authentication")
		assert.Empty(t, conns, "connection wrapped more than once")
		assert.Eventually(t, result.conn.closed.Load, 5*time.Second, 10*time.Millisecond, "wrapped connection not closed")
		assert.Positive(t, result.conn.written.Load(), "server did not write through the wrapped connection")
		return result
	}

	t.Run("tls", func(t *testing.T) {
		result := run(t, dialTLS(t, address, &tls.Config{InsecureSkipVerify: true}))
		assert.True(t, result.isTLS, "expected the wrapped connection to be a TLS connection")
		require.NotNil(t, result.state)
		assert.True(t, result.state.HandshakeComplete)
	})

	t.Run("plaintext", func(t *testing.T) {
		conn, err := net.Dial("tcp", address.String())
		require.NoError(t, err)

		result := run(t, conn)
		assert.False(t, result.isTLS)
		assert.Nil(t, result.state)
	})

	t.Run("cancel request", func(t *testing.T) {
		conn, err := net.Dial("tcp", address.String())
		require.NoError(t, err)
		defer conn.Close() //nolint:errcheck

		_, err = conn.Write(cancelRequestMessage(42, 1234))
		require.NoError(t, err)

		receive(t, cancels)
		assertClosedWithoutAuth(t, conn)
		select {
		case <-conns:
			t.Fatal("cancel request connection wrapped")
		default:
		}
	})
}

func TestWrapConnError(t *testing.T) {
	t.Parallel()

	recorder := newTLSStateRecorder()
	wrap := func(ctx context.Context, conn net.Conn) (net.Conn, error) {
		return nil, errors.New("rejected")
	}

	server, err := NewServer(recorder.handle, append(recorder.options(), Logger(slogt.New(t)), WrapConn(wrap))...)
	require.NoError(t, err)

	address := TListenAndServe(t, server)

	conn, err := net.Dial("tcp", address.String())
	require.NoError(t, err)
	defer conn.Close() //nolint:errcheck

	_, err = conn.Write(startupMessage())
	require.NoError(t, err)

	assertClosedWithoutAuth(t, conn)
	select {
	case <-recorder.auth:
		t.Fatal("authentication strategy called after WrapConn failed")
	default:
	}
}

func TestTLSMandatedPlaintextCancelRequest(t *testing.T) {
	t.Parallel()

	cert, _ := generateTestCertWithUsage(t, "server", x509.ExtKeyUsageServerAuth)

	type cancel struct{ processID, secretKey int32 }
	cancels := make(chan cancel, 1)
	cancelRequest := func(ctx context.Context, processID, secretKey int32) error {
		cancels <- cancel{processID, secretKey}
		return nil
	}

	recorder := newTLSStateRecorder()
	options := append(recorder.options(),
		Logger(slogt.New(t)),
		TLSConfig(&tls.Config{Certificates: []tls.Certificate{cert}}),
		ClientAuth(tls.RequireAndVerifyClientCert),
		CancelRequest(cancelRequest),
	)

	server, err := NewServer(recorder.handle, options...)
	require.NoError(t, err)

	address := TListenAndServe(t, server)

	t.Run("cancel request", func(t *testing.T) {
		conn, err := net.Dial("tcp", address.String())
		require.NoError(t, err)
		defer conn.Close() //nolint:errcheck

		_, err = conn.Write(cancelRequestMessage(42, 1234))
		require.NoError(t, err)

		assert.Equal(t, cancel{42, 1234}, receive(t, cancels))
	})

	t.Run("startup", func(t *testing.T) {
		conn, err := net.Dial("tcp", address.String())
		require.NoError(t, err)
		defer conn.Close() //nolint:errcheck

		_, err = conn.Write(startupMessage())
		require.NoError(t, err)

		assertClosedWithoutAuth(t, conn)
		select {
		case <-recorder.auth:
			t.Fatal("plaintext connection authenticated while TLS is mandated")
		default:
		}
	})
}

func TestSSLRequestInjectedPlaintext(t *testing.T) {
	t.Parallel()

	cert, _ := generateTestCertWithUsage(t, "server", x509.ExtKeyUsageServerAuth)
	config := &tls.Config{Certificates: []tls.Certificate{cert}}

	t.Run("buffered with the SSLRequest", func(t *testing.T) {
		t.Parallel()

		server, err := NewServer(nil, Logger(slogt.New(t)), TLSConfig(config))
		require.NoError(t, err)

		// net.Pipe delivers a single write in a single read, which ensures
		// the injected startup message is buffered together with the
		// SSLRequest.
		client, conn := net.Pipe()
		t.Cleanup(func() { _ = client.Close() })

		errs := make(chan error, 1)
		go func() {
			_, _, _, err := server.Handshake(conn)
			_ = conn.Close()
			errs <- err
		}()

		require.NoError(t, client.SetDeadline(time.Now().Add(5*time.Second)))
		_, err = client.Write(append(sslRequestMessage(), startupMessage()...))
		require.NoError(t, err)

		received, _ := io.ReadAll(client)
		require.NoError(t, client.Close())

		assert.Empty(t, received, "server answered the SSLRequest")
		assert.EqualError(t, receive(t, errs), "received unencrypted data after SSL request")
	})

	t.Run("sent after the SSLRequest answer", func(t *testing.T) {
		t.Parallel()

		recorder := newTLSStateRecorder()
		options := append(recorder.options(), Logger(slogt.New(t)), TLSConfig(config))

		server, err := NewServer(recorder.handle, options...)
		require.NoError(t, err)

		address := TListenAndServe(t, server)

		conn, err := net.Dial("tcp", address.String())
		require.NoError(t, err)
		defer conn.Close() //nolint:errcheck

		_, err = conn.Write(sslRequestMessage())
		require.NoError(t, err)

		answer := make([]byte, 1)
		_, err = io.ReadFull(conn, answer)
		require.NoError(t, err)
		require.Equal(t, []byte(sslSupported), answer)

		_, err = conn.Write(startupMessage())
		require.NoError(t, err)

		assertClosedWithoutAuth(t, conn)
		select {
		case <-recorder.auth:
			t.Fatal("plaintext startup message accepted after SSLRequest")
		default:
		}
	})
}
