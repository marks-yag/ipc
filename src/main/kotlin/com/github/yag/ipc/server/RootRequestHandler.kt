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

    fun set(contentType: String, handler: (Connection, Packet<RequestHeader>, (Packet<ResponseHeader>) -> Unit) -> Unit) {
        set(contentType, object: RequestHandler {
            override fun handle(connection: Connection, request: Packet<RequestHeader>, echo: (Packet<ResponseHeader>) -> Unit) {
                handler(connection, request, echo)
            }
        })
    }

    fun map(contentType: String, map: (Packet<RequestHeader>) -> Packet<ResponseHeader>) {
        set(contentType) { _, request, echo ->
            echo(map(request))
        }
    }

    override fun handle(connection: Connection, request: Packet<RequestHeader>, echo: (Packet<ResponseHeader>) -> Unit) {
        val handler = handlers[request.header.thrift.callType]
        if (handler != null) {
            try {
                handler.handle(connection, request, echo)
            } catch (e: IllegalArgumentException) {
                echo(Packet(ResponsePacketHeader(ResponseHeader(request.header.thrift.callId, StatusCode.BAD_REQUEST, 0)), Unpooled.EMPTY_BUFFER))
            } catch (e: Throwable) {
                echo(Packet(ResponsePacketHeader(ResponseHeader(request.header.thrift.callId, StatusCode.INTERNAL_ERROR, 0)), Unpooled.EMPTY_BUFFER))
            }
        } else {
            echo(Packet(ResponsePacketHeader(ResponseHeader(request.header.thrift.callId, StatusCode.NOT_FOUND, 0)), Unpooled.EMPTY_BUFFER))
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