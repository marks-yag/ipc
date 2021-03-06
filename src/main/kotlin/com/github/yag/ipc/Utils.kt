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

import io.netty.bootstrap.AbstractBootstrap
import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelOption
import io.netty.channel.WriteBufferWaterMark

internal fun <T : AbstractBootstrap<*, *>> T.applyChannelConfig(config: ChannelConfig): T {
    option(ChannelOption.CONNECT_TIMEOUT_MILLIS, minOf(config.connectionTimeoutMs, Int.MAX_VALUE.toLong()).toInt())
    if (this is Bootstrap) {
        option(ChannelOption.TCP_NODELAY, config.tcpNoDelay)
        option(ChannelOption.SO_SNDBUF, config.sendBufSize)
    }
    option(ChannelOption.SO_RCVBUF, config.recvBufSize)
    option(ChannelOption.WRITE_BUFFER_WATER_MARK, WriteBufferWaterMark(config.watermarkLow, config.watermarkHigh))
    return this
}

fun StatusCode.isSuccessful(): Boolean {
    return value >= 0
}

fun <T> ByteBuf.use(body: (ByteBuf) -> T): T {
    return try {
        body(this)
    } finally {
        release()
    }
}

fun ByteBuf.readArray() : ByteArray {
    return ByteArray(readableBytes()).also {
        readBytes(it)
    }
}
