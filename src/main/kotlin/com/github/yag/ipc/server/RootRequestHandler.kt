package com.github.yag.ipc.server

import com.github.yag.ipc.Packet
import com.github.yag.ipc.RequestHeader
import com.github.yag.ipc.ResponseHeader
import com.github.yag.ipc.ResponsePacketHeader
import com.github.yag.ipc.StatusCode
import io.netty.buffer.Unpooled
import java.util.concurrent.ConcurrentHashMap

class RootRequestHandler : RequestHandler, AutoCloseable {

    private val handlers = ConcurrentHashMap<String, RequestHandler>()

    fun set(requestType: String, handler: RequestHandler) {
        handlers[requestType] = handler
    }

    fun set(
        callType: String,
        handler: (Connection, Packet<RequestHeader>, (Packet<ResponseHeader>) -> Unit) -> Unit
    ) {
        set(callType, object : RequestHandler {
            override fun handle(
                connection: Connection,
                request: Packet<RequestHeader>,
                echo: (Packet<ResponseHeader>) -> Unit
            ) {
                handler(connection, request, echo)
            }
        })
    }

    fun map(callType: String, map: (Packet<RequestHeader>) -> Packet<ResponseHeader>) {
        set(callType) { _, request, echo ->
            echo(map(request))
        }
    }

    override fun handle(
        connection: Connection,
        request: Packet<RequestHeader>,
        echo: (Packet<ResponseHeader>) -> Unit
    ) {
        val handler = handlers[request.header.thrift.callType]
        if (handler != null) {
            try {
                handler.handle(connection, request, echo)
            } catch (e: Throwable) {
                echo(
                    Packet(
                        ResponsePacketHeader(
                            ResponseHeader(
                                request.header.thrift.callId,
                                StatusCode.INTERNAL_ERROR,
                                0
                            )
                        ), Unpooled.EMPTY_BUFFER
                    )
                )
            } finally {
                request.body.release()
            }
        } else {
            echo(
                Packet(
                    ResponsePacketHeader(ResponseHeader(request.header.thrift.callId, StatusCode.NOT_FOUND, 0)),
                    Unpooled.EMPTY_BUFFER
                )
            )
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