package com.github.yag.ipc.smoke

import com.github.yag.config.Value
import com.github.yag.ipc.server.IPCServerConfig

class IPCSmokeServerConfig {

    @Value
    var restartIntervalMs = 10000

    @Value
    val responseBodySize = 1024

    @Value
    val ipc = IPCServerConfig()

}