package com.github.yag.ipc.smoke

import com.github.yag.config.Value
import com.github.yag.ipc.client.IPCClientConfig

class IPCSmokeClientConfig {

    @Value
    var clients = 1

    @Value
    var restartIntervalMs = 10000

    @Value
    var requests = 1_000_000L

    @Value
    var requestBodySize = 1024

    @Value
    val ipc = IPCClientConfig()

}