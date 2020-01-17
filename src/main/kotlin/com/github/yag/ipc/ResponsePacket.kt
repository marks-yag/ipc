package com.github.yag.ipc

import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.CompositeByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelOutboundHandlerAdapter
import io.netty.channel.ChannelPromise
import org.apache.thrift.protocol.TBinaryProtocol

class ResponsePacketHeader(header: ResponseHeader) : PacketHeader<ResponseHeader>(header, {
    header.contentLength
})

data class ResponsePacket(val header: ResponseHeader, val body: ByteBuf) {

    constructor(callId: Long, code: StatusCode, body: ByteBuf) : this(ResponseHeader(callId, code, body.readableBytes()), body)


    fun encode(allocator: ByteBufAllocator) : ByteBuf {
        val buf = allocator.buffer()
        val protocol = TBinaryProtocol(TByteBufTransport(buf))
        header.write(protocol)
        check(header.contentLength == body.readableBytes())

        return Unpooled.wrappedBuffer(buf, body.retain())
    }

}

class ResponsePacketEncoder : ChannelOutboundHandlerAdapter() {

    override fun write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise) {
        if (msg is ResponsePacket) {
            val buf = msg.encode(ctx.alloc())
            ctx.write(buf, promise)
        } else {
            ctx.write(msg, promise)
        }
    }

}

fun decodeResponsePacket(buf: ByteBuf) : ResponsePacket {
    val header = TDecoder.decode(ResponseHeader(), buf)
    check(buf.readableBytes() == header.contentLength)

    val body = buf.slice(buf.readerIndex(), header.contentLength)
    buf.skipBytes(header.contentLength)
    return ResponsePacket(header, body.retain())
}

fun isHeartbeat(header: ResponseHeader) : Boolean {
    return header.callId == -1L
}
