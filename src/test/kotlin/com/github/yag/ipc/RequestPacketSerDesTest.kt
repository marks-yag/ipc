package com.github.yag.ipc

import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.Unpooled
import kotlin.test.Test
import kotlin.test.assertEquals

class RequestPacketSerDesTest {

    @Test
    fun test() {
        val data = Unpooled.wrappedBuffer("hello".toByteArray())

        val packet = Packet(RequestPacketHeader(RequestHeader(1, "foo", data.readableBytes())), data)

        val buf = PacketCodec.encode(packet, ByteBufAllocator.DEFAULT)

        val newPacket = PacketCodec.decode(buf, RequestPacketHeader())
        assertEquals(1, newPacket.header.thrift.callId)
        assertEquals("foo", newPacket.header.thrift.callType)
        assertEquals(5, newPacket.header.thrift.contentLength)
    }
}