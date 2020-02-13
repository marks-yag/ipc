namespace java com.github.yag.ipc

struct ConnectRequest {
1: string version
2: optional i64 requestTimeoutMs
3: optional map<string, string> headers
4: optional binary body
}

struct ConnectionAccepted {
1: required string connectionId
2: optional map<string, string> headers
3: optional binary body
}

struct ConnectionRejected {
1: optional i32 code
2: string message
}

union ConnectionResponse {
1: ConnectionAccepted accepted
2: ConnectionRejected rejected
}

struct RequestHeader {
1: required i64 callId
2: required string callType
3: required i32 contentLength
}


enum StatusCode {
OK = 0
PARTIAL_CONTENT = 1

CONNECTION_ERROR = -1
TIMEOUT = -2
NOT_FOUND = -3
INTERNAL_ERROR = -4
}

struct ResponseHeader {
1: required i64 callId
2: required StatusCode statusCode
3: required i32 contentLength
}