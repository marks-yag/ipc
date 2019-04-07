package com.github.yag.ipc.server

import java.util.UUID

class IPCServerBuilder(var ipcServerConfig: IPCServerConfig, val id: String) {

    private val rootHandler = RootRequestHandler()

    private val connectionHandler = ChainConnectionHandler()

    fun config(init: IPCServerConfig.() -> Unit) {
        ipcServerConfig.init()
    }

    fun connection(init: ChainConnectionHandler.() -> Unit) {
        connectionHandler.init()
    }

    fun request(init: RootRequestHandler.() -> Unit) {
        rootHandler.init()
    }

    fun build(): IPCServer {
        return IPCServer(ipcServerConfig, rootHandler, connectionHandler, id)
    }
}

fun server(
        config: IPCServerConfig = IPCServerConfig(),
        id: String = UUID.randomUUID().toString(),
        init: IPCServerBuilder.() -> Unit
): IPCServer {
    val builder = IPCServerBuilder(config, id)
    builder.init()
    return builder.build()
}