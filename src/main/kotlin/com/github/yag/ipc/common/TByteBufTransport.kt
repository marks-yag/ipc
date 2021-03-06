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

package com.github.yag.ipc.common

import io.netty.buffer.ByteBuf
import org.apache.thrift.TConfiguration
import org.apache.thrift.transport.TEndpointTransport
import org.apache.thrift.transport.TTransportException


internal class TByteBufTransport(private val bf: ByteBuf, conf: TConfiguration = TConfiguration()) : TEndpointTransport(conf) {

    override fun close() {}

    override fun isOpen(): Boolean {
        return true
    }

    override fun open() {}

    override fun read(buf: ByteArray, off: Int, len: Int): Int {
        val rd = minOf(bf.readableBytes(), len)
        if (rd > 0) {
            bf.readBytes(buf, off, rd)
        }
        return rd
    }

    @Throws(TTransportException::class)
    override fun readAll(buf: ByteArray, off: Int, len: Int): Int {
        check(bf.readableBytes() >= len) {
            throw TTransportException("Unexpected end of frame")
        }
        bf.readBytes(buf, off, len)
        return len
    }

    override fun write(buf: ByteArray, off: Int, len: Int) {
        bf.writeBytes(buf, off, len)
    }

    override fun getBuffer(): ByteArray? {
        return if (bf.hasArray()) {
            bf.array()
        } else {
            null
        }
    }

    override fun getBufferPosition(): Int {
        return if (bf.hasArray()) {
            bf.arrayOffset() + bf.readerIndex()
        } else {
            0
        }
    }

    override fun getBytesRemainingInBuffer(): Int {
        return if (bf.hasArray()) {
            bf.readableBytes()
        } else {
            -1
        }
    }

    override fun consumeBuffer(len: Int) {
        bf.skipBytes(len)
    }
}
