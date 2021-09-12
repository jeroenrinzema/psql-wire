package wire

type SSLIdentifier []byte

var (
	SSLSupported   SSLIdentifier = []byte{'Y'}
	SSLUnsupported SSLIdentifier = []byte{'N'}
)
