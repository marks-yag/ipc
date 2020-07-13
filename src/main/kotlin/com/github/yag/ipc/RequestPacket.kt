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

package com.github.yag.ipc

import com.github.yag.ipc.client.PlainBody
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled

class RequestPacketHeader(thrift: RequestHeader = RequestHeader()) : PacketHeader<RequestHeader>(thrift, {
    it.contentLength
}, {
    it.callId == -1L
})

fun Packet<RequestHeader>.ok(data: ByteBuf = Unpooled.EMPTY_BUFFER): Packet<ResponseHeader> {
    return status(StatusCode.OK, data)
}

fun Packet<RequestHeader>.status(code: StatusCode, data: ByteBuf = Unpooled.EMPTY_BUFFER): Packet<ResponseHeader> {
    return status(header.thrift.callId, code, data)
}

fun status(callId: Long, code: StatusCode, data: ByteBuf = Unpooled.EMPTY_BUFFER): Packet<ResponseHeader> {
    return Packet(ResponsePacketHeader(ResponseHeader(callId, code, data.readableBytes())), PlainBody(data))
}
