package wire

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
	scramlib "github.com/xdg-go/scram"
	"github.com/xdg-go/stringprep"
)

const (
	scramSHA256Mechanism     = "SCRAM-SHA-256"
	scramSHA256PlusMechanism = "SCRAM-SHA-256-PLUS"
	scramDefaultIterations   = 4096
	scramSaltLength          = 16
	scramKeyLength           = sha256.Size
)

// NewSCRAMSHA256Verifier returns a PostgreSQL-compatible SCRAM-SHA-256
// verifier. It uses PostgreSQL's default iteration count and a random 16-byte
// salt.
func NewSCRAMSHA256Verifier(password string) (string, error) {
	var salt [scramSaltLength]byte
	if _, err := rand.Read(salt[:]); err != nil {
		return "", err
	}
	return newSCRAMSHA256Verifier(password, salt[:], scramDefaultIterations)
}

// SCRAMSHA256 authenticates clients using SCRAM-SHA-256. On TLS connections
// where tls-server-end-point channel-binding data is available it also
// advertises SCRAM-SHA-256-PLUS, in preferred order.
//
// AuthenticationFn must return a verifier created by
// [NewSCRAMSHA256Verifier] or an equivalent PostgreSQL implementation.
func SCRAMSHA256(authenticate AuthenticationFn) AuthStrategy {
	var fakeSeed [sha256.Size]byte
	var fakeSeedErr error
	var fakeSeedOnce sync.Once

	return func(ctx context.Context, writer *buffer.Writer, reader *buffer.Reader) (context.Context, error) {
		params := ClientParameters(ctx)
		database := params[ParamDatabase]
		username := params[ParamUsername]
		next, verifier, found, err := authenticate(ctx, database, username)
		if err != nil {
			return ctx, err
		}
		ctx = next

		credentials, err := parseSCRAMSHA256Verifier(verifier)
		doomed := !found || err != nil
		if doomed {
			fakeSeedOnce.Do(func() {
				_, fakeSeedErr = rand.Read(fakeSeed[:])
			})
			if fakeSeedErr != nil {
				return ctx, fakeSeedErr
			}
			credentials = fakeSCRAMCredentials(fakeSeed[:], database, username)
		}

		bindingData, hasChannelBinding := scramChannelBinding(ctx)
		mechanisms := []string{scramSHA256Mechanism}
		if hasChannelBinding {
			mechanisms = []string{scramSHA256PlusMechanism, scramSHA256Mechanism}
		}
		if err := writeAuthRequest(writer, authSASL, encodeSASLMechanisms(mechanisms)); err != nil {
			return ctx, err
		}

		mechanism, initial, hasInitial, err := readSASLInitialResponse(reader)
		if err != nil {
			return ctx, authenticationProtocolFailed(writer, err)
		}
		if mechanism != scramSHA256Mechanism && mechanism != scramSHA256PlusMechanism {
			return ctx, authenticationProtocolFailed(writer, fmt.Errorf("unsupported SASL mechanism %q", mechanism))
		}
		if mechanism == scramSHA256PlusMechanism && !hasChannelBinding {
			return ctx, authenticationProtocolFailed(writer, errors.New("SCRAM-SHA-256-PLUS selected without channel binding"))
		}

		server, err := scramlib.SHA256.NewServer(func(string) (scramlib.StoredCredentials, error) {
			return credentials, nil
		})
		if err != nil {
			return ctx, err
		}

		var conversation *scramlib.ServerConversation
		if hasChannelBinding {
			binding := scramlib.ChannelBinding{
				Type: scramlib.ChannelBindingTLSServerEndpoint,
				Data: bindingData,
			}
			if mechanism == scramSHA256PlusMechanism {
				conversation = server.NewConversationWithChannelBindingRequired(binding)
			} else {
				conversation = server.NewConversationWithChannelBinding(binding)
			}
		} else {
			conversation = server.NewConversation()
		}

		if !hasInitial {
			if err := writeAuthRequest(writer, authSASLContinue, nil); err != nil {
				return ctx, err
			}
			initial, err = readSASLResponse(reader)
			if err != nil {
				return ctx, authenticationProtocolFailed(writer, err)
			}
		}

		if err := validateSCRAMMechanismBinding(mechanism, initial); err != nil {
			return ctx, authenticationProtocolFailed(writer, err)
		}

		serverFirst, err := conversation.Step(string(initial))
		if err != nil {
			return ctx, authenticationProtocolFailed(writer, err)
		}
		if conversation.AuthzID() != "" {
			return ctx, authenticationProtocolFailed(writer, errors.New("SCRAM authorization identity is not supported"))
		}
		if err := writeAuthRequest(writer, authSASLContinue, []byte(serverFirst)); err != nil {
			return ctx, err
		}

		clientFinal, err := readSASLResponse(reader)
		if err != nil {
			return ctx, authenticationProtocolFailed(writer, err)
		}
		serverFinal, err := conversation.Step(string(clientFinal))
		if err != nil {
			if serverFinal == scramlib.ErrInvalidProof {
				return ctx, authenticationFailed(writer)
			}
			return ctx, authenticationProtocolFailed(writer, err)
		}
		if doomed || !conversation.Valid() {
			return ctx, authenticationFailed(writer)
		}

		if err := writeAuthRequest(writer, authSASLFinal, []byte(serverFinal)); err != nil {
			return ctx, err
		}
		return ctx, writeAuthType(writer, authOK)
	}
}

func newSCRAMSHA256Verifier(password string, salt []byte, iterations int) (string, error) {
	prepared, err := stringprep.SASLprep.Prepare(password)
	if err != nil {
		prepared = password
	}
	client, err := scramlib.SHA256.NewClientUnprepped("", prepared, "")
	if err != nil {
		return "", err
	}
	credentials, err := client.GetStoredCredentialsWithError(scramlib.KeyFactors{
		Salt:  string(salt),
		Iters: iterations,
	})
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(
		"%s$%d:%s$%s:%s",
		scramSHA256Mechanism,
		iterations,
		base64.StdEncoding.EncodeToString(salt),
		base64.StdEncoding.EncodeToString(credentials.StoredKey),
		base64.StdEncoding.EncodeToString(credentials.ServerKey),
	), nil
}

func parseSCRAMSHA256Verifier(verifier string) (scramlib.StoredCredentials, error) {
	var credentials scramlib.StoredCredentials
	prefix := scramSHA256Mechanism + "$"
	if !strings.HasPrefix(verifier, prefix) {
		return credentials, errors.New("invalid PostgreSQL SCRAM-SHA-256 verifier")
	}

	sections := strings.Split(strings.TrimPrefix(verifier, prefix), "$")
	if len(sections) != 2 {
		return credentials, errors.New("invalid PostgreSQL SCRAM-SHA-256 verifier")
	}
	factors := strings.Split(sections[0], ":")
	keys := strings.Split(sections[1], ":")
	if len(factors) != 2 || len(keys) != 2 {
		return credentials, errors.New("invalid PostgreSQL SCRAM-SHA-256 verifier")
	}

	iterations, err := strconv.Atoi(factors[0])
	if err != nil || iterations <= 0 {
		return credentials, errors.New("invalid PostgreSQL SCRAM-SHA-256 iteration count")
	}
	salt, err := decodeCanonicalBase64(factors[1])
	if err != nil || len(salt) == 0 {
		return credentials, errors.New("invalid PostgreSQL SCRAM-SHA-256 salt")
	}
	storedKey, err := decodeCanonicalBase64(keys[0])
	if err != nil || len(storedKey) != scramKeyLength {
		return credentials, errors.New("invalid PostgreSQL SCRAM-SHA-256 stored key")
	}
	serverKey, err := decodeCanonicalBase64(keys[1])
	if err != nil || len(serverKey) != scramKeyLength {
		return credentials, errors.New("invalid PostgreSQL SCRAM-SHA-256 server key")
	}

	return scramlib.StoredCredentials{
		KeyFactors: scramlib.KeyFactors{Salt: string(salt), Iters: iterations},
		StoredKey:  storedKey,
		ServerKey:  serverKey,
	}, nil
}

func decodeCanonicalBase64(value string) ([]byte, error) {
	decoded, err := base64.StdEncoding.Strict().DecodeString(value)
	if err != nil || base64.StdEncoding.EncodeToString(decoded) != value {
		return nil, errors.New("invalid base64")
	}
	return decoded, nil
}

func fakeSCRAMCredentials(seed []byte, database, username string) scramlib.StoredCredentials {
	identity := database + "\x00" + username
	derive := func(label string) []byte {
		mac := hmac.New(sha256.New, seed)
		_, _ = mac.Write([]byte(label))
		_, _ = mac.Write([]byte{0})
		_, _ = mac.Write([]byte(identity))
		return mac.Sum(nil)
	}

	return scramlib.StoredCredentials{
		KeyFactors: scramlib.KeyFactors{
			Salt:  string(derive("salt")[:scramSaltLength]),
			Iters: scramDefaultIterations,
		},
		StoredKey: derive("stored-key"),
		ServerKey: derive("server-key"),
	}
}

func encodeSASLMechanisms(mechanisms []string) []byte {
	size := 1
	for _, mechanism := range mechanisms {
		size += len(mechanism) + 1
	}
	encoded := make([]byte, 0, size)
	for _, mechanism := range mechanisms {
		encoded = append(encoded, mechanism...)
		encoded = append(encoded, 0)
	}
	return append(encoded, 0)
}

func validateSCRAMMechanismBinding(mechanism string, initial []byte) error {
	end := bytes.IndexByte(initial, ',')
	if end < 0 {
		return errors.New("malformed SCRAM client-first-message")
	}
	flag := string(initial[:end])
	switch mechanism {
	case scramSHA256PlusMechanism:
		if flag != "p=tls-server-end-point" {
			return errors.New("SCRAM-SHA-256-PLUS requires tls-server-end-point channel binding")
		}
	case scramSHA256Mechanism:
		if strings.HasPrefix(flag, "p=") {
			return errors.New("SCRAM-SHA-256 does not permit channel binding")
		}
	}
	return nil
}

func readSASLInitialResponse(reader *buffer.Reader) (mechanism string, payload []byte, present bool, err error) {
	t, _, err := reader.ReadTypedMsg()
	if err != nil {
		return "", nil, false, err
	}
	if t != types.ClientPassword {
		return "", nil, false, errors.New("unexpected SASL initial response message")
	}

	mechanism, err = reader.GetString()
	if err != nil {
		return "", nil, false, err
	}
	length, err := reader.GetInt32()
	if err != nil {
		return "", nil, false, err
	}
	if length == -1 {
		if len(reader.Msg) != 0 {
			return "", nil, false, errors.New("unexpected data after absent SASL initial response")
		}
		return mechanism, nil, false, nil
	}
	if length < 0 || int64(length) != int64(len(reader.Msg)) {
		return "", nil, false, errors.New("invalid SASL initial response length")
	}
	payload, err = reader.GetBytes(int(length))
	if err != nil {
		return "", nil, false, err
	}
	return mechanism, payload, true, nil
}

func readSASLResponse(reader *buffer.Reader) ([]byte, error) {
	t, _, err := reader.ReadTypedMsg()
	if err != nil {
		return nil, err
	}
	if t != types.ClientPassword {
		return nil, errors.New("unexpected SASL response message")
	}
	return reader.GetBytes(len(reader.Msg))
}
