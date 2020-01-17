package com.github.yag.ipc.server

import com.github.yag.ipc.*
import io.netty.buffer.Unpooled
import java.lang.IllegalArgumentException
import java.util.concurrent.ConcurrentHashMap

class RootRequestHandler : RequestHandler, AutoCloseable {

    private val handlers = ConcurrentHashMap<String, RequestHandler>()

    fun set(requestType: String, handler: RequestHandler) {
        handlers[requestType] = handler
    }

    fun set(contentType: String, handler: (Connection, RequestPacket, (ResponsePacket) -> Unit) -> Unit) {
        set(contentType, object: RequestHandler {
            override fun handle(connection: Connection, request: RequestPacket, echo: (ResponsePacket) -> Unit) {
                handler(connection, request, echo)
            }
        })
    }

    fun map(contentType: String, map: (RequestPacket) -> ResponsePacket) {
        set(contentType) { _, request, echo ->
            echo(map(request))
        }
    }

    override fun handle(connection: Connection, request: RequestPacket, echo: (ResponsePacket) -> Unit) {
        val handler = handlers[request.header.callType]
        if (handler != null) {
            try {
                handler.handle(connection, request, echo)
            } catch (e: IllegalArgumentException) {
                echo(ResponsePacket(ResponseHeader(request.header.callId, StatusCode.BAD_REQUEST, 0), Unpooled.EMPTY_BUFFER))
            } catch (e: Throwable) {
                echo(ResponsePacket(ResponseHeader(request.header.callId, StatusCode.INTERNAL_ERROR, 0), Unpooled.EMPTY_BUFFER))
            }
        } else {
            echo(ResponsePacket(ResponseHeader(request.header.callId, StatusCode.NOT_FOUND, 0), Unpooled.EMPTY_BUFFER))
        }
    }

    override fun close() {
        handlers.forEach { (_, handler) ->
            if (handler is AutoCloseable) {
                handler.close()
            }
        }
    }
}