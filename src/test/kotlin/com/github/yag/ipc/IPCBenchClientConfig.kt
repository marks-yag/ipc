package com.github.yag.ipc

import com.github.yag.config.Value
import com.github.yag.ipc.client.IPCClientConfig

class IPCBenchClientConfig {

    @Value
    var clients = 1

    @Value
    var requests = 1000000

    @Value
    var requestBodySize = 1024

    @Value
    val ipc = IPCClientConfig()

}