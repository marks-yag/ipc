namespace java com.github.yag.ipc

struct ConnectRequest {
1: string version
2: optional i64 requestTimeoutMs
3: optional map<string, string> headers
}

struct ConnectionAccepted {
1: required string connectionId
2: optional map<string, string> headers
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
OK = 100
PARTIAL_CONTENT = 101

BAD_REQUEST = 200
NOT_FOUND = 201
UNAUTHORIZED = 202
FORBIDDEN = 203

INTERNAL_ERROR = 300

TIMEOUT = 400

CONNECTION_ERROR = 500

}

struct ResponseHeader {
1: required i64 callId
2: required StatusCode statusCode
3: required i32 contentLength
}