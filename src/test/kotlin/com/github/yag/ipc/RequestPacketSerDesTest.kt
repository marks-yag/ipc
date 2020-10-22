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

import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.Unpooled
import kotlin.test.Test
import kotlin.test.assertEquals

class RequestPacketSerDesTest {

    @Test
    fun test() {
        val data = Unpooled.wrappedBuffer("hello".toByteArray())

        val packet = Packet(RequestPacketHeader(RequestHeader(1, "foo", data.readableBytes())), PlainBody(data))

        val buf = PacketCodec.encode(packet, ByteBufAllocator.DEFAULT)

        val newPacket = PacketCodec.decode(buf, RequestPacketHeader())
        assertEquals(1, newPacket.header.thrift.callId)
        assertEquals("foo", newPacket.header.thrift.callType)
        assertEquals(5, newPacket.header.thrift.contentLength)

        buf.release()
        data.release()
    }
}