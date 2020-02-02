package com.github.yag.ipc.server

import com.codahale.metrics.MetricRegistry
import java.util.UUID

class IPCServerBuilder<T : Any>(
    private var ipcServerConfig: IPCServerConfig,
    private val metric: MetricRegistry,
    val id: String
) {

    private val rootHandler = RootRequestHandler<T>()

    private val connectionHandler = ChainConnectionHandler()

    fun config(init: IPCServerConfig.() -> Unit) {
        ipcServerConfig.init()
    }

    fun connection(init: ChainConnectionHandler.() -> Unit) {
        connectionHandler.init()
    }

    fun request(init: RootRequestHandler<T>.() -> Unit) {
        rootHandler.init()
    }

    fun build(): IPCServer {
        return IPCServer(ipcServerConfig, rootHandler, connectionHandler, metric, id)
    }
}


fun server(
    config: IPCServerConfig = IPCServerConfig(),
    metric: MetricRegistry = MetricRegistry(),
    id: String = UUID.randomUUID().toString(),
    init: IPCServerBuilder<String>.() -> Unit
) = tserver(config, metric, id, init)

fun <T : Any> tserver(
    config: IPCServerConfig = IPCServerConfig(),
    metric: MetricRegistry = MetricRegistry(),
    id: String = UUID.randomUUID().toString(),
    init: IPCServerBuilder<T>.() -> Unit
): IPCServer {
    val builder = IPCServerBuilder<T>(config, metric, id)
    builder.init()
    return builder.build()
}
