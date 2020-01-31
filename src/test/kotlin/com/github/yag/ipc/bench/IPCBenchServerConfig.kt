package com.github.yag.ipc.bench

import com.github.yag.config.Value
import com.github.yag.ipc.server.IPCServerConfig

class IPCBenchServerConfig {

    @Value
    val responseBodySize = 1024

    @Value
    val ipc = IPCServerConfig()

}