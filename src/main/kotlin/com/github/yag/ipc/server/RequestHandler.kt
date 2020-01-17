package com.github.yag.ipc.server

import com.github.yag.ipc.Request
import com.github.yag.ipc.RequestPacket
import com.github.yag.ipc.Response
import com.github.yag.ipc.ResponsePacket

interface RequestHandler {

    fun handle(connection: Connection, request: RequestPacket, echo: (ResponsePacket) -> Unit)

}