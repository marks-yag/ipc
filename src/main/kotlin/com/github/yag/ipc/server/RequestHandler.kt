package com.github.yag.ipc.server

import com.github.yag.ipc.Packet
import com.github.yag.ipc.RequestHeader
import com.github.yag.ipc.ResponseHeader

interface RequestHandler {

    fun handle(connection: Connection, request: Packet<RequestHeader>, echo: (Packet<ResponseHeader>) -> Unit)

}