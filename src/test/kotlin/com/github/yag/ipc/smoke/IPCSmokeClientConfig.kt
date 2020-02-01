package com.github.yag.ipc.smoke

import com.github.yag.config.Value
import com.github.yag.ipc.client.IPCClientConfig

class IPCSmokeClientConfig {

    @Value
    var clients = 8

    @Value
    var minAliveMs = 0L

    @Value
    var maxAliveMs = 3600 * 24 * 1000L

    @Value
    var minRequestBodySize = 0

    @Value
    var maxRequestBodySize = 1024 * 1024

    @Value
    val ipc = IPCClientConfig()

}