package com.github.yag.ipc.server

import com.github.yag.ipc.Request
import com.github.yag.ipc.Response

interface RequestHandler {

    fun handle(connection: Connection, request: Request, echo: (Response) -> Unit)

}