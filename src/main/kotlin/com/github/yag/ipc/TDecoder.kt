package com.github.yag.ipc

import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufInputStream
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToMessageDecoder
import org.apache.thrift.TSerializable
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TIOStreamTransport

class TDecoder(private val newObject: () -> TSerializable) : MessageToMessageDecoder<ByteBuf>() {

    override fun decode(ctx: ChannelHandlerContext, msg: ByteBuf, out: MutableList<Any>) {
        val protocol = TBinaryProtocol(TIOStreamTransport(ByteBufInputStream(msg)))
        val obj = newObject().apply { read(protocol) }
        out.add(obj)
    }

}