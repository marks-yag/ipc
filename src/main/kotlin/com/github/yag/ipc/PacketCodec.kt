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

import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.Unpooled
import org.apache.thrift.TSerializable
import org.apache.thrift.protocol.TBinaryProtocol
import org.slf4j.LoggerFactory

object PacketCodec {

    fun <T : TSerializable> encode(packet: Packet<T>, allocator: ByteBufAllocator): ByteBuf {
        if (LOG.isTraceEnabled) {
            LOG.trace("Encode: ${packet.header}")
        }
        val buf = allocator.ioBuffer()
        val protocol = TBinaryProtocol(
            TByteBufTransport(buf)
        )
        packet.header.thrift.write(protocol)

        check(packet.header.length.invoke(packet.header.thrift) == packet.body.data().readableBytes())

        return Unpooled.wrappedBuffer(buf, packet.body.data().retain())
    }

    fun <T : TSerializable> decode(buf: ByteBuf, header: PacketHeader<T>): Packet<T> {
        if (LOG.isTraceEnabled) {
            LOG.debug("Decode: ${header.javaClass}")
        }
        TDecoder.decode(header.thrift, buf)
        check(buf.readableBytes() == header.length.invoke(header.thrift))

        val body = buf.slice(buf.readerIndex(), buf.readableBytes())
        buf.skipBytes(buf.readableBytes())
        return header.packet(body.retain())
    }

    private val LOG = LoggerFactory.getLogger(PacketCodec::class.java)

}