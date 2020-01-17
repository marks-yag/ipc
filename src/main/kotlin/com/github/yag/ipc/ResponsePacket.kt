package com.github.yag.ipc

import io.netty.buffer.ByteBuf
import io.netty.buffer.CompositeByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelOutboundHandlerAdapter
import io.netty.channel.ChannelPromise
import org.apache.thrift.protocol.TBinaryProtocol

data class ResponsePacket(val response: Response, val body: ByteBuf)

class ResponsePacketEncoder : ChannelOutboundHandlerAdapter() {

    override fun write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise) {
        if (msg is ResponsePacket) {
            val buf = CompositeByteBuf(ctx.alloc(), false, 2)
            try {
                val protocol = TBinaryProtocol(TByteBufTransport(buf))
                msg.response.write(protocol)
                if (msg.response.isSetHeader) {
                    check(msg.response.header.contentLength == msg.body.readableBytes())
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

fun decodeResponsePacket(buf: ByteBuf) : ResponsePacket {
    val response = TDecoder.decode(Response(), buf)
    return if (response.isSetHeader) {
        val header = response.header
        check(buf.readableBytes() == header.contentLength)

        val body = buf.slice(buf.readerIndex(), header.contentLength)
        ResponsePacket(response, body)
    } else {
        ResponsePacket(response, Unpooled.EMPTY_BUFFER)
    }
}

