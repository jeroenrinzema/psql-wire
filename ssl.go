package wire

type SSLIdentifier []byte

var (
	SSLSupported   SSLIdentifier = []byte{'S'}
	SSLUnsupported SSLIdentifier = []byte{'N'}
)
