package com.github.yag.ipc

import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.Unpooled
import org.apache.thrift.TSerializable
import org.apache.thrift.protocol.TBinaryProtocol
import org.slf4j.LoggerFactory

object PacketCodec {

    fun <T : TSerializable> encode(packet: Packet<T>, allocator: ByteBufAllocator): ByteBuf {
        if (LOG.isTraceEnabled) {
            LOG.trace("Encode: ${packet.header}")
        }
        val buf = allocator.buffer()
        val protocol = TBinaryProtocol(
            TByteBufTransport(buf)
        )
        packet.header.thrift.write(protocol)

        check(packet.header.length.invoke(packet.header.thrift) == packet.body.readableBytes())

        return Unpooled.wrappedBuffer(buf, packet.body.retain())
    }

    fun <T : TSerializable> decode(buf: ByteBuf, header: PacketHeader<T>): Packet<T> {
        if (LOG.isTraceEnabled) {
            LOG.debug("Decode: ${header.javaClass}")
        }
        TDecoder.decode(header.thrift, buf)
        check(buf.readableBytes() == header.length.invoke(header.thrift))

        val body = buf.slice(buf.readerIndex(), buf.readableBytes())
        buf.skipBytes(buf.readableBytes())
        return header.packet(body.retain())
    }

    private val LOG = LoggerFactory.getLogger(PacketCodec::class.java)

}