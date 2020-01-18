package com.github.yag.ipc

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.util.ReferenceCounted
import org.apache.thrift.TSerializable

class Packet<T : TSerializable>(val header: PacketHeader<T>, body: ByteBuf) {

    val body = body.retain()

    fun isHeartbeat() = header.isHeartbeat(header.thrift)

    companion object {

        val requestHeartbeat = Packet(RequestPacketHeader(RequestHeader(-1, "", 0)), Unpooled.EMPTY_BUFFER)

        val responseHeartbeat = status(-1, StatusCode.OK)

    }

}




