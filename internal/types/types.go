package types

//ClientMessage represents a client pgwire message.
type ClientMessage byte

//ServerMessage represents a server pgwire message.
type ServerMessage byte

// http://www.postgresql.org/docs/9.4/static/protocol-message-formats.html
const (
	ClientBind        ClientMessage = 'B'
	ClientClose       ClientMessage = 'C'
	ClientCopyData    ClientMessage = 'd'
	ClientCopyDone    ClientMessage = 'c'
	ClientCopyFail    ClientMessage = 'f'
	ClientDescribe    ClientMessage = 'D'
	ClientExecute     ClientMessage = 'E'
	ClientFlush       ClientMessage = 'H'
	ClientParse       ClientMessage = 'P'
	ClientPassword    ClientMessage = 'p'
	ClientSimpleQuery ClientMessage = 'Q'
	ClientSync        ClientMessage = 'S'
	ClientTerminate   ClientMessage = 'X'

	ServerAuth                 ServerMessage = 'R'
	ServerBindComplete         ServerMessage = '2'
	ServerCommandComplete      ServerMessage = 'C'
	ServerCloseComplete        ServerMessage = '3'
	ServerCopyInResponse       ServerMessage = 'G'
	ServerDataRow              ServerMessage = 'D'
	ServerEmptyQuery           ServerMessage = 'I'
	ServerErrorResponse        ServerMessage = 'E'
	ServerNoticeResponse       ServerMessage = 'N'
	ServerNoData               ServerMessage = 'n'
	ServerParameterDescription ServerMessage = 't'
	ServerParameterStatus      ServerMessage = 'S'
	ServerParseComplete        ServerMessage = '1'
	ServerPortalSuspended      ServerMessage = 's'
	ServerReady                ServerMessage = 'Z'
	ServerRowDescription       ServerMessage = 'T'
)
