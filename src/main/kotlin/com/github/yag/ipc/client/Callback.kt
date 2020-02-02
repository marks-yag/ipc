package com.github.yag.ipc.client

import com.github.yag.ipc.Packet
import com.github.yag.ipc.ResponseHeader

data class Callback(var lastContactTimestamp: Long, val func: (Packet<ResponseHeader>) -> Any?)