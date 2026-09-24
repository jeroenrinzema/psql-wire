package wire

import (
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"hash"
	"net"
)

// sslIdentifier represents the bytes identifying whether the given connection
// supports SSL.
type sslIdentifier []byte

var (
	sslSupported   sslIdentifier = []byte{'S'}
	sslUnsupported sslIdentifier = []byte{'N'}
)

type tlsCertificateSelection struct {
	certificate *tls.Certificate
}

// tlsServerConn retains the local certificate selected for this connection.
// Go versions before 1.27 do not expose it through tls.ConnectionState.
type tlsServerConn struct {
	*tls.Conn
	selection *tlsCertificateSelection
}

func newTLSServerConn(conn net.Conn, config *tls.Config) *tlsServerConn {
	selection := &tlsCertificateSelection{}
	clone := config.Clone()

	// A single static certificate is unambiguous. GetConfigForClient may
	// replace the entire configuration, so do not guess in that case.
	if clone.GetConfigForClient == nil && len(clone.Certificates) == 1 {
		selection.certificate = &clone.Certificates[0]
	}
	if clone.GetConfigForClient == nil && clone.GetCertificate != nil {
		getCertificate := clone.GetCertificate
		clone.GetCertificate = func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
			certificate, err := getCertificate(hello)
			if err == nil && certificate != nil {
				selection.certificate = certificate
			}
			return certificate, err
		}
	}

	return &tlsServerConn{
		Conn:      tls.Server(conn, clone),
		selection: selection,
	}
}

type scramChannelBindingKey struct{}

func withSCRAMChannelBinding(ctx context.Context, conn net.Conn) context.Context {
	tlsConn, ok := conn.(*tlsServerConn)
	if !ok || tlsConn.selection.certificate == nil {
		return ctx
	}

	binding, err := tlsServerEndPoint(tlsConn.selection.certificate)
	if err != nil {
		return ctx
	}
	return context.WithValue(ctx, scramChannelBindingKey{}, binding)
}

func scramChannelBinding(ctx context.Context) ([]byte, bool) {
	binding, ok := ctx.Value(scramChannelBindingKey{}).([]byte)
	return binding, ok && len(binding) != 0
}

// tlsServerEndPoint implements the tls-server-end-point channel binding from
// RFC 5929 section 4 using the leaf certificate sent by this server.
func tlsServerEndPoint(certificate *tls.Certificate) ([]byte, error) {
	if certificate == nil || len(certificate.Certificate) == 0 {
		return nil, errors.New("TLS certificate chain is empty")
	}
	leaf := certificate.Leaf
	if leaf == nil {
		var err error
		leaf, err = x509.ParseCertificate(certificate.Certificate[0])
		if err != nil {
			return nil, err
		}
	}

	var digest hash.Hash
	switch leaf.SignatureAlgorithm {
	case x509.MD5WithRSA, x509.SHA1WithRSA, x509.DSAWithSHA1, x509.ECDSAWithSHA1,
		x509.SHA256WithRSA, x509.SHA256WithRSAPSS, x509.DSAWithSHA256, x509.ECDSAWithSHA256:
		digest = sha256.New()
	case x509.SHA384WithRSA, x509.SHA384WithRSAPSS, x509.ECDSAWithSHA384:
		digest = sha512.New384()
	case x509.SHA512WithRSA, x509.SHA512WithRSAPSS, x509.ECDSAWithSHA512:
		digest = sha512.New()
	default:
		return nil, errors.New("TLS certificate signature algorithm has no supported channel-binding hash")
	}

	_, _ = digest.Write(certificate.Certificate[0])
	return digest.Sum(nil), nil
}
