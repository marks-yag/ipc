package com.github.yag.ipc

import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelOutboundHandlerAdapter
import io.netty.channel.ChannelPromise
import org.slf4j.LoggerFactory

class PacketEncoder : ChannelOutboundHandlerAdapter() {

    override fun write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise) {
        LOG.trace("Write packet: {}", msg)
        if (msg is Packet<*>) {
            val buf = PacketCodec.encode(msg, ctx.alloc())
            ctx.write(buf, promise)
        } else {
            ctx.write(msg, promise)
        }
    }

    companion object {
        private val LOG =
            LoggerFactory.getLogger(PacketEncoder::class.java)
    }

}