package com.github.yag.ipc

import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.CompositeByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelOutboundHandlerAdapter
import io.netty.channel.ChannelPromise
import org.apache.thrift.protocol.TBinaryProtocol

class RequestPacketHeader(thrift: RequestHeader = RequestHeader()) : PacketHeader<RequestHeader>(thrift, {
    it.contentLength
}, {
    it.callId == -1L
})

fun Packet<RequestHeader>.ok(data: ByteBuf = Unpooled.EMPTY_BUFFER) : Packet<ResponseHeader> {
    return status(StatusCode.OK, data)
}

fun Packet<RequestHeader>.status(code: StatusCode, data: ByteBuf = Unpooled.EMPTY_BUFFER) : Packet<ResponseHeader> {
    return status(header.thrift.callId, code, data)
}

fun status(callId: Long, code: StatusCode, data: ByteBuf = Unpooled.EMPTY_BUFFER) : Packet<ResponseHeader> {
    return Packet(ResponsePacketHeader(ResponseHeader(callId, code, data.readableBytes())), data)
}
