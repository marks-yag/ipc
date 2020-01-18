package com.github.yag.ipc

import io.netty.bootstrap.AbstractBootstrap
import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelOption
import io.netty.channel.WriteBufferWaterMark
import java.nio.ByteBuffer
import java.util.concurrent.locks.Lock

fun <T : AbstractBootstrap<*, *>> T.applyChannelConfig(config: ChannelConfig): T {
    option(ChannelOption.CONNECT_TIMEOUT_MILLIS, minOf(config.connectionTimeoutMs, Int.MAX_VALUE.toLong()).toInt())
    if (this is Bootstrap) {
        option(ChannelOption.TCP_NODELAY, config.tcpNoDelay)
        option(ChannelOption.SO_SNDBUF, config.sendBufSize)
    }
    option(ChannelOption.SO_RCVBUF, config.recvBufSize)
    option(ChannelOption.WRITE_BUFFER_WATER_MARK, WriteBufferWaterMark(config.waterMarkLow, config.waterMarkHigh))
    return this
}

fun StatusCode.isSuccessful(): Boolean {
    return (100..199).contains(value)
}

fun <T> addThreadName(postfix: String, body: () -> T): T {
    val thread = Thread.currentThread()
    val oldName = thread.name
    if (!oldName.endsWith("-$postfix")) {
        thread.name = "$oldName-$postfix"
    }
    try {
        return body()
    } finally {
        thread.name = oldName
    }
}
