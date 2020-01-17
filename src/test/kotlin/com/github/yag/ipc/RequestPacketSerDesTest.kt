package com.github.yag.ipc

import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.Unpooled
import kotlin.test.Test
import kotlin.test.assertEquals

class RequestPacketSerDesTest {

    @Test
    fun test() {
        val data = Unpooled.wrappedBuffer("hello".toByteArray())

        val packet = RequestPacket(RequestHeader(1, "foo", data.readableBytes()), data)

        val buf = encodeRequestPacket(packet, ByteBufAllocator.DEFAULT)

        val newPacket = decodeRequestPacket(buf)
        assertEquals(1, newPacket.header.callId)
        assertEquals("foo", newPacket.header.callType)
        assertEquals(5, newPacket.header.contentLength)
    }
}