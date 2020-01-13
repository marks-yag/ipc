package com.github.yag.ipc

import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufOutputStream
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToByteEncoder
import org.apache.thrift.TSerializable
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TIOStreamTransport

class TEncoder<T: TSerializable>(clazz: Class<T>) : MessageToByteEncoder<T>(clazz) {

    override fun encode(ctx: ChannelHandlerContext, msg: T, out: ByteBuf) {
        encode(msg, out)
    }

    companion object {

        @JvmStatic
        fun <T: TSerializable> encode(obj: T, buf: ByteBuf) : ByteBuf {
            val protocol = TBinaryProtocol(TByteBufTransport(buf))
            obj.write(protocol)
            protocol.transport.flush()
            return buf
        }

    }

}