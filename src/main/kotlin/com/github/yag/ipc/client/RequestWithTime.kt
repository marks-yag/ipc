package com.github.yag.ipc.client

import com.github.yag.ipc.Packet
import com.github.yag.ipc.RequestHeader

data class RequestWithTime(val request: Packet<RequestHeader>, val timestamp: Long)