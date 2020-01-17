package com.github.yag.ipc.server

import com.github.yag.ipc.*

interface RequestHandler {

    fun handle(connection: Connection, request: Packet<RequestHeader>, echo: (Packet<ResponseHeader>) -> Unit)

}