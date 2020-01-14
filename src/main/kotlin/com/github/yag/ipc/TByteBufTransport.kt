package com.github.yag.ipc

import io.netty.buffer.ByteBuf
import org.apache.thrift.transport.TTransport
import org.apache.thrift.transport.TTransportException


class TByteBufTransport(private val bf: ByteBuf) : TTransport() {

    override fun close() {}

    override fun isOpen(): Boolean {
        return true
    }

    override fun open() {}

    override fun read(buf: ByteArray, off: Int, len: Int): Int {
        val rd = minOf(bf.readableBytes(), len)
        if (rd > 0) {
            bf.readBytes(buf, off, rd)
        }
        return rd
    }

    @Throws(TTransportException::class)
    override fun readAll(buf: ByteArray, off: Int, len: Int): Int {
        check (bf.readableBytes() >= len) {
            throw TTransportException("Unexpected end of frame")
        }
        bf.readBytes(buf, off, len)
        return len
    }

    override fun write(buf: ByteArray, off: Int, len: Int) {
        bf.writeBytes(buf, off, len)
    }

    override fun getBuffer(): ByteArray? {
        return if (bf.hasArray()) {
            bf.array()
        } else {
            null
        }
    }

    override fun getBufferPosition(): Int {
        return if (bf.hasArray()) {
            bf.arrayOffset() + bf.readerIndex()
        } else {
            0
        }
    }

    override fun getBytesRemainingInBuffer(): Int {
        return if (bf.hasArray()) {
            bf.readableBytes()
        } else {
            -1
        }
    }

    override fun consumeBuffer(len: Int) {
        bf.skipBytes(len)
    }
}