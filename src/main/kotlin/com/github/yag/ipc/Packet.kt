package com.github.yag.ipc

import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.Unpooled
import org.apache.thrift.TSerializable
import org.apache.thrift.protocol.TBinaryProtocol

class Packet<T: TSerializable>(val header: PacketHeader<T>, val body: ByteBuf)

open class PacketHeader<T: TSerializable>(val thrift: T, val length: (T) -> Int) {

    fun packet(body: ByteBuf) : Packet<T> {
        return Packet(this, body)
    }

}


class PacketCodec {

    fun <T: TSerializable> encode(packet: Packet<T>, allocator: ByteBufAllocator) : ByteBuf {
        val buf = allocator.buffer()
        val protocol = TBinaryProtocol(TByteBufTransport(buf))
        packet.header.thrift.write(protocol)

        check(packet.header.length.invoke(packet.header.thrift) == packet.body.readableBytes())

        return Unpooled.wrappedBuffer(buf, packet.body.retain())
    }

    fun <T: TSerializable> decode(buf: ByteBuf, header: PacketHeader<T>) : Packet<T> {
        TDecoder.decode(header.thrift, buf)
        check(buf.readableBytes() == header.length.invoke(header.thrift))

        val body = buf.slice(buf.readerIndex(), buf.readableBytes())
        buf.skipBytes(buf.readableBytes())
        return header.packet(body)
    }


}