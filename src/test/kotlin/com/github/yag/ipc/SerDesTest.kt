package com.github.yag.ipc

import io.netty.buffer.Unpooled
import kotlin.test.Test
import kotlin.test.assertEquals

class SerDesTest {

    @Test
    fun test() {
        val request = ConnectRequest().apply {
            setVersion("v1")
            setRequestTimeoutMs(1000)
        }

        val encoded = TEncoder.encode(request, Unpooled.buffer())
        val got = TDecoder.decode(ConnectRequest(), encoded)
        assertEquals(request, got)
    }

}