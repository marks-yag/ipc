package com.github.yag.ipc

import com.github.yag.config.Value

class ChannelConfig {

    @Value
    var connectionTimeoutMs: Long = 20_000L

    @Value
    var tcpNoDelay: Boolean = false

    @Value
    var recvBufSize: Int = 1024 * 1024

    @Value
    var sendBufSize: Int = 1024 * 1024

    @Value
    var waterMarkHigh: Int = 1024 * 1024 * 16

    @Value
    var waterMarkLow: Int = 1024 * 1024 * 8

}