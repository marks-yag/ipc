package com.github.yag.ipc.smoke

import com.github.yag.config.Value
import com.github.yag.ipc.server.IPCServerConfig

class IPCSmokeServerConfig {

    @Value
    var minResponseBodySize = 0

    @Value
    var maxResponseBodySize = 1024 * 1024

    @Value
    val ipc = IPCServerConfig()

}