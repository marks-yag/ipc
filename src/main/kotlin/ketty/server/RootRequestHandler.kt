/*
 * Copyright 2018-2020 marks.yag@gmail.com
 *
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package ketty.server

import ketty.common.Packet
import ketty.common.PlainBody
import ketty.common.ResponsePacketHeader
import ketty.protocol.RequestHeader
import ketty.protocol.ResponseHeader
import ketty.protocol.StatusCode
import io.netty.buffer.Unpooled
import java.util.concurrent.ConcurrentHashMap

class RootRequestHandler<T : Any> : RequestHandler, AutoCloseable {

    private val handlers = ConcurrentHashMap<String, RequestHandler>()

    fun set(requestType: T, handler: RequestHandler) {
        handlers[requestType.toString()] = handler
    }

    fun set(
        callType: T,
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

    fun map(callType: T, map: (Packet<RequestHeader>) -> Packet<ResponseHeader>) {
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
                        ), PlainBody(Unpooled.EMPTY_BUFFER)
                    )
                )
            } finally {
                request.close()
            }
        } else {
            request.close()
            echo(
                Packet(
                    ResponsePacketHeader(ResponseHeader(request.header.thrift.callId, StatusCode.NOT_FOUND, 0)),
                    PlainBody(Unpooled.EMPTY_BUFFER)
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
