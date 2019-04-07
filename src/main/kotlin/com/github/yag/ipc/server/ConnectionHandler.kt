package com.github.yag.ipc.server

import com.github.yag.ipc.ConnectionRejectException
import com.github.yag.ipc.ConnectionRequest

interface ConnectionHandler {

    @Throws(ConnectionRejectException::class)
    fun handle(connection: Connection, request: ConnectionRequest)

}

class ChainConnectionHandler : ConnectionHandler {

    private val chain = ArrayList<ConnectionHandler>()

    fun add(handler: ConnectionHandler) {
        chain.add(handler)
    }

    fun add(handler: (Connection, ConnectionRequest) -> Unit) {
        add(object : ConnectionHandler {
            override fun handle(connection: Connection, request: ConnectionRequest) {
                handler(connection, request)
            }
        })
    }

    fun remove(handler: ConnectionHandler) {
        chain.remove(handler)
    }

    @Throws(ConnectionRejectException::class)
    override fun handle(connection: Connection, request: ConnectionRequest) {
        return chain.forEach { it.handle(connection, request) }
    }

}