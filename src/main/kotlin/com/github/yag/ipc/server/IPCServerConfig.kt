package com.github.yag.ipc.server

import com.github.yag.config.Value
import com.github.yag.ipc.ChannelConfig

class IPCServerConfig {

    @Value(required = true)
    var host: String = "127.0.0.1"

    @Value(required = true)
    var port: Int = 0

    @Value
    var maxReqeustPacketSize = 1024 * 1024 * 10

    @Value
    var maxIdleTimeMs: Long = 30 * 1000

    @Value
    var parentThreads: Int = 0

    @Value
    var childThreads: Int = 0

    @Value
    var channelConfig = ChannelConfig()

}