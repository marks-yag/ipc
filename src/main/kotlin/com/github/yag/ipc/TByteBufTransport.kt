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
        val bytesRemaining = bf.readableBytes()
        val amtToRead = if (len > bytesRemaining) bytesRemaining else len
        if (amtToRead > 0) {
            bf.readBytes(buf, off, amtToRead)
        }
        return amtToRead
    }

    @Throws(TTransportException::class)
    override fun readAll(buf: ByteArray, off: Int, len: Int): Int {
        val bytesRemaining = bf.readableBytes()
        if (len > bytesRemaining) {
            throw TTransportException("unexpected end of frame")
        }
        bf.readBytes(buf, off, len)
        return len
    }

    override fun write(buf: ByteArray, off: Int, len: Int) {
        bf.writeBytes(buf, off, len)
    }

    override fun getBuffer(): ByteArray? {
        return if (!bf.hasArray()) {
            null
        } else {
            bf.array()
        }
    }

    override fun getBufferPosition(): Int {
        return if (bf.hasArray()) {
            0
        } else {
            bf.arrayOffset() + bf.readerIndex()
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