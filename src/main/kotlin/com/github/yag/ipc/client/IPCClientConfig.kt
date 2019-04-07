package com.github.yag.ipc.client

import com.github.yag.config.Value
import com.github.yag.ipc.ChannelConfig
import java.net.InetSocketAddress
import java.util.TreeMap

class IPCClientConfig {

    @Value(required = true)
    lateinit var endpoint: InetSocketAddress

    @Value
    var threads: Int = 4

    @Value
    var maxResponsePacketSize: Int = 1024 * 1024 * 10

    @Value
    var heartbeatIntervalMs: Long = 1000

    @Value
    var requestTimeoutMs: Long = Long.MAX_VALUE

    @Value
    var heartbeatTimeoutMs: Long = 10_000

    @Value
    var maxParallelCalls: Int = 1_000_000

    @Value
    var maxParallelRequestContentSize: Int = 1024 * 1024 * 100

    @Value
    var maxWriteBatchSize: Int = 8192

    @Value
    var channelConfig = ChannelConfig()

    @Value
    var headers = TreeMap<String, String>()

    @Value
    var reconnectDelayMs = -1L

}