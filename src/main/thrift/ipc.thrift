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

struct Heartbeat {
1: required i64 timestamp
}

union RequestPacket {
1: Heartbeat heartbeat
2: list<Request> requests
}

struct Content {
1: optional string type
2: required binary body
}

struct Request {
1: required i64 callId
2: required string callType
3: optional Content content
4: optional map<string, string> headers
}

union ResponsePacket {
1: Heartbeat heartbeat
2: Response response
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

struct Response {
1: required i64 callId
2: required StatusCode statusCode
3: optional Content content
4: optional map<string, string> headers
}