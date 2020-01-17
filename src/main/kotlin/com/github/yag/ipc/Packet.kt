package com.github.yag.ipc

import io.netty.buffer.ByteBuf
import org.apache.thrift.TSerializable

class Packet<T: TSerializable>(val header: PacketHeader<T>, val body: ByteBuf)

open class PacketHeader<T: TSerializable>(val thrift: T, val length: (T) -> Int, val isHeartbeat: (T) -> Boolean) {

    fun packet(body: ByteBuf) : Packet<T> {
        return Packet(this, body)
    }

}


