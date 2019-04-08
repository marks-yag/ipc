package com.github.yag.ipc.server

import com.github.yag.ipc.ConnectionRejectException

interface ConnectionHandler {

    @Throws(ConnectionRejectException::class)
    fun handle(connection: Connection)

}

