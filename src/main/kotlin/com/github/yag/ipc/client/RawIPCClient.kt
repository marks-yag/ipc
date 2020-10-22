/*
 * Copyright 2018-2020 marks.yag@gmail.com
 *
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.github.yag.ipc.client

import com.codahale.metrics.MetricRegistry
import com.github.yag.ipc.Body
import com.github.yag.ipc.ConnectRequest
import com.github.yag.ipc.ConnectionAccepted
import com.github.yag.ipc.ConnectionRejectException
import com.github.yag.ipc.ConnectionResponse
import com.github.yag.ipc.Daemon
import com.github.yag.ipc.Packet
import com.github.yag.ipc.PacketCodec
import com.github.yag.ipc.PacketEncoder
import com.github.yag.ipc.Prompt
import com.github.yag.ipc.RequestHeader
import com.github.yag.ipc.RequestPacketHeader
import com.github.yag.ipc.ResponseHeader
import com.github.yag.ipc.ResponsePacketHeader
import com.github.yag.ipc.StatusCode
import com.github.yag.ipc.TEncoder
import com.github.yag.ipc.addThreadName
import com.github.yag.ipc.applyChannelConfig
import com.github.yag.ipc.daemon
import com.github.yag.ipc.status
import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufInputStream
import io.netty.channel.Channel
import io.netty.channel.ChannelFutureListener
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelInitializer
import io.netty.channel.epoll.Epoll
import io.netty.channel.epoll.EpollSocketChannel
import io.netty.channel.kqueue.KQueue
import io.netty.channel.kqueue.KQueueSocketChannel
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.ByteToMessageDecoder
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.handler.codec.LengthFieldPrepender
import io.netty.handler.timeout.IdleState
import io.netty.handler.timeout.IdleStateEvent
import io.netty.handler.timeout.IdleStateHandler
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TIOStreamTransport
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.net.ConnectException
import java.net.InetSocketAddress
import java.net.SocketException
import java.nio.ByteBuffer
import java.util.Timer
import java.util.TimerTask
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.ExecutionException
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.system.measureTimeMillis

internal class RawIPCClient<T : Any>(
    private var endpoint: InetSocketAddress,
    private val config: IPCClientConfig,
    private val promptHandler: (Prompt) -> ByteArray,
    private val sessionId: String?,
    private val currentCallId: AtomicLong,
    metric: MetricRegistry,
    private val id: String,
    private val timer: Timer,
    private val channelInactive: () -> Unit
) : AutoCloseable {

    private val bootstrap: Bootstrap

    private val prompt: Prompt

    private val promptFuture: CompletableFuture<Prompt>

    internal val connection: ConnectionAccepted

    private val connectFuture: CompletableFuture<ConnectionAccepted>

    private val channel: Channel

    private val queue = LinkedBlockingQueue<Request<T>>()

    private val flusher: Daemon<*>

    private val parallelCalls = Semaphore(config.maxParallelCalls)

    private val parallelRequestContentSize = Semaphore(config.maxParallelRequestContentSize)

    private val onTheFly = ConcurrentSkipListMap<Long, OnTheFly<T>>()

    private val uncompleted = ArrayList<OnTheFly<T>>()

    private var lastContact: Long = 0L

    private val closed = AtomicBoolean()

    private val lock = ReentrantLock()

    private val cbLock = ReentrantLock()

    private val blockTime = metric.histogram("ipc-client-request-block-time")

    private val batchSize = metric.histogram("ipc-client-batch-size")

    private val sendTime = metric.histogram("ipc-client-send-time")

    init {
        bootstrap = Bootstrap().apply {
            channel(CHANNEL_CLASS)
            group(PlatformEventLoopGroup(config.threads).instance)
            applyChannelConfig(config.channel)
            handler(ChildChannelHandler())
        }
        closed.set(false)

        promptFuture = CompletableFuture()
        connectFuture = CompletableFuture()

        var succ = false
        try {
            channel = bootstrap.connect(endpoint).sync().channel().also {
                prompt = promptFuture.get()

                val connectionRequest = ConnectRequest("V1", ByteBuffer.wrap(promptHandler(prompt)))
                if (sessionId != null) {
                    connectionRequest.setSessionId(sessionId)
                }
                if (config.headers.isNotEmpty()) {
                    connectionRequest.setHeaders(config.headers)
                }
                it.writeAndFlush(connectionRequest)
                lastContact = System.currentTimeMillis()
            }
            succ = true
        } catch (e: InterruptedException) {
            throw InterruptedException("Connect to ipc server timeout and interrupted.")
        } catch (e: ConnectException) {
            throw ConnectException(e.message) //make stack clear
        } catch (e: SocketException) {
            throw SocketException(e.message)
        } finally {
            LOG.debug("Cleanup bootstrap threads.")
            if (!succ) {
                bootstrap.config().group().shutdownGracefully().sync()
            }
            LOG.debug("Cleanup bootstrap threads done.")
        }

        LOG.debug("New ipc client created.")
        try {
            connection = connectFuture.get()
            LOG.debug("New ipc client connection accepted: {}.", connection.connectionId)
        } catch (e: ExecutionException) {
            throw e.cause ?: e
        }

        flusher = daemon("flusher-$id") { shouldStop ->
            while (!shouldStop.get()) {
                try {
                    val list = poll()
                    batchSize.update(list.size)

                    val start = System.currentTimeMillis()

                    list.forEach { packet ->
                        val call = onTheFly[packet.header.thrift.callId]
                        checkNotNull(call)
                        call.sent = true

                        channel.write(packet).addListener {
                            sendTime.update(System.currentTimeMillis() - start)
                            parallelRequestContentSize.release(packet.header.thrift.contentLength)
                            if (LOG.isTraceEnabled) {
                                LOG.trace(
                                    "Released {} then {}.",
                                    packet.header.thrift.contentLength,
                                    parallelRequestContentSize.availablePermits()
                                )
                            }
                        }
                    }

                    channel.flush()
                } catch (e: InterruptedException) {
                    //:~
                }
            }
        }.apply { start() }
    }

    fun send(type: RequestType<T>, body: Body, callback: (Packet<ResponseHeader>) -> Any?) {
        lock.withLock {
            val name = type.getName()
            val buf = body.data()
            val header = RequestHeader(currentCallId.incrementAndGet(), name.toString(), buf.readableBytes())
            buf.retain()
            val request = Packet(RequestPacketHeader(header), body)

            send(type, request, callback)

            (body as? AutoCloseable)?.close()
        }
    }

    internal fun send(
        type: RequestType<T>,
        packet: Packet<RequestHeader>,
        callback: (Packet<ResponseHeader>) -> Any?
    ) {
        val timestamp = System.currentTimeMillis()
        val request = Request(type, packet, timestamp)
        val header = packet.header.thrift

        blockTime.update(measureTimeMillis {
            parallelCalls.acquire()
            parallelRequestContentSize.acquire(header.contentLength)
        })

        val callId = header.callId
        onTheFly[callId] = OnTheFly(request, Callback(timestamp, callback))

        queue.offer(request)

        val timeoutMs = type.timeoutMs() ?: config.requestTimeoutMs

        LOG.trace("Queued: {}, timeout: {}.", request, timeoutMs)

        if (timeoutMs > 0) {
            timer.schedule(object : TimerTask() {
                override fun run() {
                    timeout(callId)
                }
            }, timeoutMs)
        }
    }

    private fun timeout(callId: Long) {
        onTheFly.remove(callId)?.let {
            LOG.debug("Timeout: {}.", callId)
            it.doResponse(status(callId, StatusCode.TIMEOUT))
        }
    }

    private fun poll(): ArrayList<Packet<RequestHeader>> {
        val list = ArrayList<Packet<RequestHeader>>()
        var length = 0L

        var packet = queue.take().packet
        list.add(packet)
        length += packet.body.data().readableBytes()

        while (true) {
            val request = queue.poll()
            if (request != null) {
                packet = request.packet
                list.add(packet)
                length += packet.body.data().readableBytes()
                if (length >= config.maxWriteBatchSize) {
                    break
                }
            } else {
                break
            }
        }
        return list
    }

    override fun close() {
        if (closed.compareAndSet(false, true)) {
            addThreadName(id) {
                LOG.info("IPC client closing...")
                flusher.close()
                try {
                    channel.close().sync()
                } catch (e: Exception) {
                    LOG.warn("Close channel failed.")
                }
                channel.eventLoop().shutdownGracefully()

                LOG.info("IPC client closed, make all pending requests timeout.")
                handlePendingRequests()
            }
        }
    }

    private fun handlePendingRequests() {
        cbLock.withLock {
            onTheFly.keys.forEach { key ->
                onTheFly[key]?.let { call ->
                    parallelCalls.release()
                    if (call.request.type.isIdempotent() || !call.sent) {
                        uncompleted.add(call)
                    } else {
                        onTheFly.remove(key)?.let {
                            it.doResponse(status(key, StatusCode.CONNECTION_ERROR))
                            it.request.packet.close()
                        }
                    }
                }
            }
        }
    }

    internal fun getUncompletedRequests() : List<OnTheFly<T>> {
        return uncompleted
    }

    inner class ResponseDecoder : ByteToMessageDecoder() {

        @Volatile
        private var prompted = false

        @Volatile
        private var connected = false

        override fun decode(ctx: ChannelHandlerContext, buf: ByteBuf, out: MutableList<Any>) {
            if (!prompted) {
                val protocol = TBinaryProtocol(TIOStreamTransport(ByteBufInputStream(buf)))
                val prompt = Prompt().apply { read(protocol) }
                prompted = true
                promptFuture.complete(prompt)
            } else if (!connected) {
                val protocol = TBinaryProtocol(TIOStreamTransport(ByteBufInputStream(buf)))
                val connectionResponse = ConnectionResponse().apply { read(protocol) }
                connected = connectionResponse.isSetAccepted
                if (connected) {
                    connectFuture.complete(connectionResponse.accepted)
                } else {
                    connectFuture.completeExceptionally(ConnectionRejectException(connectionResponse.rejected.message))
                    ctx.close()
                }
            } else {
                val packet = PacketCodec.decode(buf, ResponsePacketHeader())
                if (!packet.isHeartbeat()) {
                    out.add(packet)
                } else {
                    LOG.debug("Received heartbeat ack.")
                    lastContact = System.currentTimeMillis()
                    buf.release()
                }
            }
        }
    }

    inner class ResponseHandler : ChannelInboundHandlerAdapter() {
        override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
            @Suppress("UNCHECKED_CAST")
            val packet = msg as Packet<ResponseHeader>

            val header = packet.header
            LOG.trace(
                "Received response, connectionId: {}, requestId: {}.",
                connection.connectionId,
                header.thrift.callId
            )
            doCallback(packet)
        }

        private fun doCallback(packet: Packet<ResponseHeader>) {
            val header = packet.header
            onTheFly[header.thrift.callId]?.let {
                if (header.thrift.statusCode != StatusCode.PARTIAL_CONTENT) {
                    it.request.packet.close()
                    onTheFly.remove(header.thrift.callId)
                    parallelCalls.release()
                } else {
                    it.callback.lastContactTimestamp = System.currentTimeMillis()
                    LOG.trace(
                        "Continue, connectionId: {}, requestId: {}.",
                        connection.connectionId,
                        header.thrift.callId
                    )
                }
                it.callback.func(packet)
            }
            packet.close()
        }

        override fun channelInactive(ctx: ChannelHandlerContext) {
            super.channelInactive(ctx)
            LOG.debug("Channel inactive: {} {}.", connection?.connectionId, this)
            channelInactive()
        }

        override fun userEventTriggered(ctx: ChannelHandlerContext, event: Any) {
            super.userEventTriggered(ctx, event)
            if (event is IdleStateEvent) {
                @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
                when (event.state()) {
                    IdleState.READER_IDLE, IdleState.ALL_IDLE -> {
                        LOG.debug("Channel heartbeat timeout.")
                        ctx.channel().close()
                    }
                    IdleState.WRITER_IDLE -> {
                        LOG.debug("Send heartbeat.")
                        ctx.channel().writeAndFlush(
                            Packet.requestHeartbeat
                        ).addListener { ChannelFutureListener.CLOSE_ON_FAILURE }
                    }
                }
            }
        }

        @Suppress("DEPRECATION", "OverridingDeprecatedMember")
        override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
            super.exceptionCaught(ctx, cause)
            LOG.error("Unknown exception.", cause)
        }
    }

    inner class ChildChannelHandler : ChannelInitializer<SocketChannel>() {
        override fun initChannel(channel: SocketChannel) {
            channel.pipeline().apply {
                addLast(LengthFieldBasedFrameDecoder(config.maxResponsePacketSize, 0, 4, 0, 4))
                addLast(LengthFieldPrepender(4, 0))

                addLast(
                    IdleStateHandler(
                        config.heartbeatTimeoutMs,
                        config.heartbeatIntervalMs,
                        config.heartbeatTimeoutMs,
                        TimeUnit.MILLISECONDS
                    )
                )
                addLast(ResponseDecoder())
                addLast(TEncoder(ConnectRequest::class.java))
                addLast(PacketEncoder())

                addLast(ResponseHandler())
            }
        }
    }

    companion object {
        private val LOG: Logger = LoggerFactory.getLogger(RawIPCClient::class.java)

        val CHANNEL_CLASS: Class<out SocketChannel> =
            when {
                Epoll.isAvailable() -> {
                    EpollSocketChannel::class.java
                }
                KQueue.isAvailable() -> {
                    KQueueSocketChannel::class.java
                }
                else -> {
                    NioSocketChannel::class.java
                }
            }
    }

}
