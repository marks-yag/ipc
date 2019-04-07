package com.github.yag.ipc.server

import com.github.yag.ipc.Request
import com.github.yag.ipc.Response
import com.github.yag.ipc.StatusCode
import com.github.yag.ipc.status
import java.util.concurrent.ConcurrentHashMap

class RootRequestHandler : RequestHandler, AutoCloseable {

    private val handlers = ConcurrentHashMap<String, RequestHandler>()

    fun set(requestType: String, handler: RequestHandler) {
        handlers[requestType] = handler
    }

    fun set(contentType: String, handler: (Connection, Request, (Response) -> Unit) -> Unit) {
        set(contentType, object: RequestHandler {
            override fun handle(connection: Connection, request: Request, echo: (Response) -> Unit) {
                handler(connection, request, echo)
            }
        })
    }

    fun map(contentType: String, map: (Request) -> Response) {
        set(contentType) { _, request, echo ->
            echo(map(request))
        }
    }

    override fun handle(connection: Connection, request: Request, echo: (Response) -> Unit) {
        val handler = handlers[request.callType]
        if (handler != null) {
            handler.handle(connection, request, echo)
        } else {
            echo(request.status(StatusCode.NOT_FOUND))
        }
    }

    override fun close() {
        handlers.forEach { _, handler ->
            if (handler is AutoCloseable) {
                handler.close()
            }
        }
    }
}