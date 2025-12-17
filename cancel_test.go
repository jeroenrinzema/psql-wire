package wire

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/pem"
	"fmt"
	"math/big"
	mathrand "math/rand"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	_ "github.com/lib/pq"
)

type testServer struct {
	server   *Server
	sessions map[int32]*testSession
	mutex    sync.RWMutex
	rowChan  chan struct{}
}

type testSession struct {
	ProcessID int32
	SecretKey int32
	Cancel    context.CancelFunc
	Addr      net.Addr
}

func newTestServer(tlsConfig *tls.Config) (*testServer, error) {
	ts := &testServer{
		sessions: make(map[int32]*testSession),
		rowChan:  make(chan struct{}, 1),
	}

	server, err := NewServer(ts.handler, BackendKeyData(ts.backendKeyData),
		CancelRequest(ts.cancelRequest), TLSConfig(tlsConfig))
	if err != nil {
		return nil, err
	}
	ts.server = server
	return ts, nil
}

func (ts *testServer) backendKeyData(ctx context.Context) (int32, int32) {
	rng := mathrand.New(mathrand.NewSource(time.Now().UnixNano()))
	processID, secretKey := rng.Int31(), rng.Int31()

	ts.mutex.Lock()
	ts.sessions[processID] = &testSession{
		ProcessID: processID,
		SecretKey: secretKey,
		Addr:      RemoteAddress(ctx),
	}
	ts.mutex.Unlock()

	return processID, secretKey
}

func (ts *testServer) cancelRequest(ctx context.Context, processID, secretKey int32) error {
	ts.mutex.RLock()
	session, exists := ts.sessions[processID]
	ts.mutex.RUnlock()

	if !exists || session.SecretKey != secretKey {
		return nil
	}

	if session.Cancel != nil {
		session.Cancel()
	}
	return nil
}

func (ts *testServer) handler(ctx context.Context, query string) (PreparedStatements, error) {
	handle := func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
		queryCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		// Store cancel function in session
		addr := RemoteAddress(ctx)
		ts.mutex.Lock()
		for _, session := range ts.sessions {
			if session.Addr == addr {
				session.Cancel = cancel
				break
			}
		}
		ts.mutex.Unlock()

		// The handler will always try to return 3 rows before completing, the rows
		// are controlled by the test.
		for i := range 3 {
			select {
			case <-queryCtx.Done():
				return queryCtx.Err()
			case <-ts.rowChan:
				_ = writer.Row([]any{i})
			}
		}

		return writer.Complete("SELECT 3")
	}

	cols := Columns{
		Column{Name: "1", Oid: pgtype.Int4OID, Width: 4},
	}

	return Prepared(NewStatement(handle, WithColumns(cols))), nil
}

func generateTestCert() (tls.Certificate, error) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return tls.Certificate{}, err
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{Organization: []string{"Test"}},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})

	return tls.X509KeyPair(certPEM, keyPEM)
}

func startTestServer(t *testing.T, withTLS bool) (int, *testServer) {
	var tlsConfig *tls.Config
	if withTLS {
		cert, err := generateTestCert()
		if err != nil {
			t.Fatal(err)
		}
		tlsConfig = &tls.Config{Certificates: []tls.Certificate{cert}}
	}

	server, err := newTestServer(tlsConfig)
	if err != nil {
		t.Fatal(err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		if err := server.server.Serve(listener); err != nil {
			t.Errorf("Server failed: %v", err)
		}
	}()

	return listener.Addr().(*net.TCPAddr).Port, server
}

func testCancellation(t *testing.T, port int, server *testServer, sslMode string) {
	connStr := fmt.Sprintf("host=127.0.0.1 port=%d dbname=test user=test sslmode=%s", port, sslMode)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("Error closing database: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// pre-send signal to allow one row to be sent
	server.rowChan <- struct{}{}

	rows, err := db.QueryContext(ctx, "SELECT 1")

	// Check for immediate error
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
		return
	}

	if rows == nil {
		t.Error("Expected rows but got nil")
		return
	}

	defer func() {
		if err := rows.Close(); err != nil {
			t.Logf("Error closing rows: %v", err)
		}
	}()

	// Get the first row
	if !rows.Next() {
		t.Error("Expected one row")
		return
	}

	cancel()

	if rows.Next() {
		t.Error("Expected rows.Next() to return false after cancellation")
		return
	}

	// Check for cancellation error
	if err := rows.Err(); err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "canceled") {
			return // Success - got cancellation error
		}
		t.Errorf("Expected cancellation error but got: %v", err)
		return
	}

	t.Error("Expected cancellation error but no error was returned")
}

func TestCancellationWithoutTLS(t *testing.T) {
	port, server := startTestServer(t, false)
	testCancellation(t, port, server, "disable")
}

func TestCancellationWithTLS(t *testing.T) {
	port, server := startTestServer(t, true)
	testCancellation(t, port, server, "require")
}
