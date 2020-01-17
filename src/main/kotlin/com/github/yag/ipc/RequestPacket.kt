package com.github.yag.ipc

import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.CompositeByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelOutboundHandlerAdapter
import io.netty.channel.ChannelPromise
import org.apache.thrift.protocol.TBinaryProtocol

data class RequestPacket(val header: RequestHeader, val body: ByteBuf) {


    fun ok(data: ByteBuf = Unpooled.EMPTY_BUFFER) : ResponsePacket {
        return status(StatusCode.OK, data)
    }

    fun status(code: StatusCode, data: ByteBuf = Unpooled.EMPTY_BUFFER) : ResponsePacket {
        return ResponsePacket(ResponseHeader(header.callId, code, data.readableBytes()), data)
    }
}

class RequestPacketEncoder : ChannelOutboundHandlerAdapter() {

    override fun write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise) {
        if (msg is RequestPacket) {
            val buf = encodeRequestPacket(msg, ctx.alloc())
            ctx.write(buf, promise)
        } else {
            ctx.write(msg, promise)
        }
    }

}

fun encodeRequestPacket(packet: RequestPacket, allocator: ByteBufAllocator) : ByteBuf {
    val buf = allocator.buffer()
    val protocol = TBinaryProtocol(TByteBufTransport(buf))
    packet.header.write(protocol)
    check(packet.header.contentLength == packet.body.readableBytes())
    return Unpooled.wrappedBuffer(buf, packet.body.retain())
}

fun decodeRequestPacket(buf: ByteBuf): RequestPacket {
    val header = TDecoder.decode(RequestHeader(), buf)
    check(buf.readableBytes() == header.contentLength) {
        "readable: ${buf.readableBytes()} != contentLength: ${header.contentLength}"
    }

    val body = buf.slice(buf.readerIndex(), header.contentLength)
    buf.skipBytes(header.contentLength)
    return RequestPacket(header, body.retain())
}


fun isHeartbeat(header: RequestHeader) : Boolean {
    return header.callId == -1L
}
