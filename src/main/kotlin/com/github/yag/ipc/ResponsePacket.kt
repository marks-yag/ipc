package com.github.yag.ipc

class ResponsePacketHeader(thrift: ResponseHeader = ResponseHeader()) : PacketHeader<ResponseHeader>(thrift, {
    it.contentLength
}, {
    it.callId == -1L
})

fun Packet<ResponseHeader>.status() = header.thrift.statusCode