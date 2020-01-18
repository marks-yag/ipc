package com.github.yag.ipc

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToMessageDecoder
import org.apache.thrift.TSerializable
import org.apache.thrift.protocol.TBinaryProtocol

class TDecoder(private val newObject: () -> TSerializable) : MessageToMessageDecoder<ByteBuf>() {

    override fun decode(ctx: ChannelHandlerContext, msg: ByteBuf, out: MutableList<Any>) {
        out.add(decode(newObject(), msg))
    }

    companion object {

        @JvmStatic
        fun <T : TSerializable> decode(obj: T, buf: ByteBuf): T {
            obj.read(TBinaryProtocol(TByteBufTransport(buf)))
            return obj
        }

    }

}