package com.github.yag.ipc

import io.netty.buffer.ByteBuf
import io.netty.buffer.CompositeByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelOutboundHandlerAdapter
import io.netty.channel.ChannelPromise
import org.apache.thrift.protocol.TBinaryProtocol

data class RequestPacket(val request: Request, val body: ByteBuf)

class RequestPacketEncoder : ChannelOutboundHandlerAdapter() {

    override fun write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise) {
        if (msg is RequestPacket) {
            val buf = CompositeByteBuf(ctx.alloc(), false, 2)
            try {
                val protocol = TBinaryProtocol(TByteBufTransport(buf))
                msg.request.write(protocol)
                if (msg.request.isSetHeader) {
                    check(msg.request.header.contentLength == msg.body.readableBytes())

                    buf.addComponent(msg.body)
                }

                ctx.write(buf, promise)
            } finally {
                buf.release()
            }
        } else {
            ctx.write(msg, promise)
        }
    }

}

fun decodeRequestPacket(buf: ByteBuf): RequestPacket {
    val request = TDecoder.decode(Request(), buf)
    return if (request.isSetHeader) {
        val header = request.header
        check(buf.readableBytes() == header.contentLength)

        val body = buf.slice(buf.readerIndex(), header.contentLength)
        RequestPacket(request, body)
    } else {
        RequestPacket(request, Unpooled.EMPTY_BUFFER)
    }
}