package com.github.yag.ipc.server

import com.github.yag.ipc.ConnectionRejectException

class ChainConnectionHandler : ConnectionHandler {

    private val chain = ArrayList<ConnectionHandler>()

    fun add(handler: ConnectionHandler) {
        chain.add(handler)
    }

    fun add(handler: (Connection) -> Unit) {
        add(object : ConnectionHandler {
            override fun handle(connection: Connection) {
                handler(connection)
            }
        })
    }

    fun remove(handler: ConnectionHandler) {
        chain.remove(handler)
    }

    @Throws(ConnectionRejectException::class)
    override fun handle(connection: Connection) {
        return chain.forEach { it.handle(connection) }
    }

}