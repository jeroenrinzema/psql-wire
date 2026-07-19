package types

// Version represents a connection version presented inside the connection
// header. A version encodes a major and minor number as (major << 16) | minor.
type Version uint32

// NewVersion constructs a Version from its major and minor numbers, matching
// the on-the-wire encoding of (major << 16) | minor.
func NewVersion(major, minor uint16) Version {
	return Version(uint32(major)<<16 | uint32(minor))
}

// The below versions can occur during the first message a client sends to the
// server. There are two categories: protocol versions and request codes.
// Protocol versions carry the major and minor number directly. Request codes
// reuse the same header with a reserved major number of 1234 followed by
// 5678 + N, where N started at 0 and is increased by 1 for every new request
// code added, which happens rarely during major or minor Postgres releases.
//
// See: https://www.postgresql.org/docs/current/protocol-message-formats.html
var (
	Version30         = NewVersion(3, 0)
	Version32         = NewVersion(3, 2)
	VersionCancel     = NewVersion(1234, 5678)
	VersionSSLRequest = NewVersion(1234, 5679)
	VersionGSSENC     = NewVersion(1234, 5680)

	// VersionLatest is the highest protocol version implemented by this
	// library. Clients requesting a newer minor version are negotiated back
	// down to this version through a NegotiateProtocolVersion message.
	VersionLatest = Version30
)

// Major returns the major protocol version number ((major << 16) | minor).
func (v Version) Major() uint16 {
	return uint16(v >> 16)
}

// Minor returns the minor protocol version number ((major << 16) | minor).
func (v Version) Minor() uint16 {
	return uint16(v & 0x0000ffff)
}
