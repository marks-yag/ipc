package com.github.yag.ipc.client

import com.github.yag.ipc.*
import com.google.common.util.concurrent.SettableFuture
import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufInputStream
import io.netty.buffer.Unpooled
import io.netty.channel.Channel
import io.netty.channel.ChannelFutureListener
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelInitializer
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.ByteToMessageDecoder
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.handler.codec.LengthFieldPrepender
import io.netty.handler.timeout.IdleState
import io.netty.handler.timeout.IdleStateEvent
import io.netty.handler.timeout.IdleStateHandler
import io.netty.util.concurrent.DefaultThreadFactory
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TIOStreamTransport
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.net.ConnectException
import java.net.SocketException
import java.net.SocketTimeoutException
import java.util.UUID
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import kotlin.system.measureTimeMillis

class IPCClient(
        private val config: IPCClientConfig,
        private val id: String = UUID.randomUUID().toString()
) : AutoCloseable {

    private lateinit var bootstrap: Bootstrap

    private lateinit var connection: ConnectionAccepted

    private lateinit var connectFuture: SettableFuture<ConnectionAccepted>

    private lateinit var channel: Channel

    private var currentId = 0L

    private val queue = LinkedBlockingQueue<RequestWithTime>()

    private lateinit var flusher: Daemon<*>

    private val parallelCalls = Semaphore(config.maxParallelCalls)

    private val parallelRequestContentSize = Semaphore(config.maxParallelRequestContentSize)

    private val callbacks = ConcurrentSkipListMap<Long, Callback>()

    private lateinit var executor: ScheduledExecutorService

    private var lastContact: Long = 0L

    private var closed = AtomicBoolean()

    private val connected = AtomicBoolean()

    private val lock = ReentrantLock()

    private val cbLock = ReentrantLock()

    private val blockTime = metric.histogram("ipc-client-request-block-time")

    private val queueTime = metric.histogram("ipc-client-queue-time")

    private val batchSize = metric.histogram("ipc-client-batch-size")

    private val sendTime = metric.histogram("ipc-client-send-time")

    init {
        addThreadName(id) {
            initChannel()
        }
    }

    private fun initChannel() {
        locking(lock) {
            executor = Executors.newSingleThreadScheduledExecutor()
            bootstrap = Bootstrap().apply {
                channel(NioSocketChannel::class.java)
                        .group(NioEventLoopGroup(config.threads, DefaultThreadFactory("ipc-client-$id", true)))
                        .applyChannelConfig(config.channelConfig)
                        .handler(ChildChannelHandler())
            }
            closed.set(false)
            currentId = 0L
            connectFuture = SettableFuture.create<ConnectionAccepted>()
            try {
                channel = bootstrap.connect(config.endpoint).sync().channel().also {
                    val connectionRequest = ConnectRequest("V1")
                    if (config.headers.isNotEmpty()) {
                        connectionRequest.setHeaders(config.headers)
                    }
                    it.writeAndFlush(connectionRequest)
                    lastContact = System.currentTimeMillis()
                }
            } catch (e: InterruptedException) {
                throw SocketTimeoutException("Connect to ipc server timeout and interrupted.")
            } catch (e: ConnectException) {
                throw ConnectException(e.message) //make stack clear
            } catch (e: SocketException) {
                throw SocketException(e.message)
            }

            LOG.debug("New ipc client created.")
            try {
                connection = connectFuture.get()
                LOG.debug("New ipc client connection accepted: {}.", connection.connectionId)
            } catch (e: ExecutionException) {
                throw e.cause ?: e
            }

            //TODO try schedule with delay for each call.
            executor.scheduleAtFixedRate({
                val iterator = callbacks.iterator()
                val now = System.currentTimeMillis()
                while (iterator.hasNext()) {
                    val next = iterator.next()
                    if (now - next.value.lastContactTimestramp > config.requestTimeoutMs) {
                        LOG.debug("Handle timeout request, connectionId: {}, requestId: {}.", connection.connectionId, next.key)
                        iterator.remove()
                        parallelCalls.release()
                        try {
                            next.value.func(
                                ResponsePacket(ResponseHeader(next.key, StatusCode.TIMEOUT, 0), Unpooled.EMPTY_BUFFER)
                            )
                        } catch (e: Exception) {
                            LOG.warn("Callback error, connectionId: {}, requestId: {}.", connection.connectionId, next.key, e)
                        }
                    } else {
                        break
                    }
                }
            }, 1, 1, TimeUnit.MILLISECONDS)
            connected.set(true)

            flusher = daemon("flusher-$id") { shouldStop ->
                Runnable {
                    while (!shouldStop.get()) {
                        try {
                            val list = ArrayList<RequestPacket>()
                            var length = 0L
                            var requestWithTime = queue.poll(1, TimeUnit.SECONDS)
                            if (requestWithTime != null) {
                                val qt = measureTimeMillis {
                                    var request = requestWithTime.request
                                    list.add(request)
                                    length += request.body.readableBytes()

                                    while (true) {
                                        requestWithTime = queue.poll()
                                        if (requestWithTime != null) {
                                            request = requestWithTime.request
                                            list.add(request)
                                            length += request.body.readableBytes()
                                            if (length >= config.maxWriteBatchSize) {
                                                break
                                            }
                                        } else {
                                            break
                                        }
                                    }

                                    batchSize.update(list.size)

                                    val start = System.currentTimeMillis()

                                    list.forEach { packet ->
                                        channel.write(packet).addListener {
                                            sendTime.update(System.currentTimeMillis() - start)
                                            parallelRequestContentSize.release(packet.header.contentLength)
                                            if (LOG.isTraceEnabled) {
                                                LOG.trace("Released {} then {}.", packet.header.contentLength, parallelRequestContentSize.availablePermits())
                                            }
                                        }
                                    }

                                    channel.flush()
                                }
                                queueTime.update(qt)
                            }
                        } catch (e: InterruptedException) {
                            //:~
                        }
                    }
                }
            }.apply { start() }
        }
    }

    fun send(type: String, buf: ByteBuf, callback: (ResponsePacket) -> Unit) {
        locking(lock) {
            val request = RequestHeader(++currentId, type, buf.readableBytes())
            val requestPacket = RequestPacket(request, buf)

            if (!connected.get()) {
                callback(ResponsePacket(ResponseHeader(request.callId, StatusCode.CONNECTION_ERROR, 0), Unpooled.EMPTY_BUFFER))
            }

            blockTime.update(measureTimeMillis {
                parallelCalls.acquire()
            })

            val timestamp = System.currentTimeMillis()
            callbacks[request.callId] = Callback(timestamp, callback)

            parallelRequestContentSize.acquire(request.contentLength)
            queue.offer(RequestWithTime(requestPacket, timestamp), Long.MAX_VALUE, TimeUnit.MILLISECONDS)
            LOG.trace("Queued request: {}.", request.callId)
        }
    }

    fun send(type: String, buf: ByteBuf): Future<ResponsePacket> {
        val future = SettableFuture.create<ResponsePacket>()
        send(type, buf) {
            future.set(it)
        }
        return future
    }

    fun sendSync(type: String, data: ByteBuf): ResponsePacket {
        return send(type, data).get()
    }

    override fun close() {
        if (closed.compareAndSet(false, true)) {
            addThreadName(id) {
                LOG.debug("IPC client closing...")
                release()
            }
        }
    }

    private fun release() {
        if (connected.compareAndSet(true, false)) {
            addThreadName(id) {
                flusher.close()
                try {
                    channel.close().sync()
                } catch (e: Exception) {
                    LOG.warn("Close channel failed.")
                }
                channel.eventLoop().shutdownGracefully()
                executor.shutdown()

                LOG.debug("IPC client closed, make all pending requests timeout.")
                handlePendingRequests()
            }
        }
    }

    private fun handlePendingRequests() {
        locking(cbLock) {
            callbacks.keys.forEach { key ->
                callbacks.remove(key)?.let { cb ->
                    parallelCalls.release()
                    cb.func(ResponsePacket(ResponseHeader(key, StatusCode.TIMEOUT, 0), Unpooled.EMPTY_BUFFER))
                }
            }
        }
    }

    fun isConnected(): Boolean {
        return connected.get()
    }

    inner class ResponseDecoder : ByteToMessageDecoder() {

        @Volatile
        private var connected = false

        override fun decode(ctx: ChannelHandlerContext, buf: ByteBuf, out: MutableList<Any>) {
            if (!connected) {
                val protocol = TBinaryProtocol(TIOStreamTransport(ByteBufInputStream(buf)))
                val connectionResponse = ConnectionResponse().apply { read(protocol) }
                connected = connectionResponse.isSetAccepted
                if (connected) {
                    connectFuture.set(connectionResponse.accepted)
                } else {
                    connectFuture.setException(ConnectionRejectException(connectionResponse.rejected.message))
                    ctx.close()
                }
            } else {
                val packet = decodeResponsePacket(buf)
                if (!isHeartbeat(packet.header)) {
                    out.add(packet)
                } else {
                    LOG.debug("Received heartbeat ack.")
                    lastContact = System.currentTimeMillis()
                }
            }
        }
    }

    inner class ResponseHandler : ChannelInboundHandlerAdapter() {
        override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
            super.channelRead(ctx, msg)
            val packet = msg as ResponsePacket

            val header = packet.header
            LOG.trace("Received response, connectionId: {}, requestId: {}.",
                connection.connectionId,
                header.callId)
            doCallback(packet)
        }

        private fun doCallback(packet: ResponsePacket) {
            val header = packet.header
            callbacks[header.callId]?.let {
                if (header.statusCode != StatusCode.PARTIAL_CONTENT) {
                    callbacks.remove(header.callId)
                    parallelCalls.release()
                } else {
                    it.lastContactTimestramp = System.currentTimeMillis()
                    LOG.trace("Continue, connectionId: {}, requestId: {}.", connection.connectionId, header.callId)
                }
                it.func(packet)
            }
        }

        override fun channelInactive(ctx: ChannelHandlerContext) {
            super.channelInactive(ctx)
            LOG.debug("Channel inactive.")
            release()
            config.reconnectDelayMs.let {
                if (it > 0 && !closed.get()) {
                    LOG.info("Will reconnect after {}ms.", it)
                    Thread.sleep(it)
                    LOG.info("Reconnecting...")
                    initChannel()
                }
            }
        }

        override fun userEventTriggered(ctx: ChannelHandlerContext, event: Any) {
            super.userEventTriggered(ctx, event)
            if (event is IdleStateEvent) {
                when (event.state()) {
                    IdleState.READER_IDLE, IdleState.ALL_IDLE -> {
                        LOG.info("Channel heartbeat timeout.")
                        ctx.channel().close()
                    }
                    IdleState.WRITER_IDLE -> {
                        if (connected.get()) {
                            LOG.info("Send heartbeat.")
                            ctx.channel().writeAndFlush(
                                RequestPacket(RequestHeader(-1, "", 0), Unpooled.EMPTY_BUFFER) //TODO create heartbeat fun
                            ).addListener { ChannelFutureListener.CLOSE_ON_FAILURE }
                        }
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

                addLast(IdleStateHandler(config.heartbeatTimeoutMs, config.heartbeatIntervalMs, config.heartbeatTimeoutMs, TimeUnit.MILLISECONDS))
                addLast(ResponseDecoder())
                addLast(TEncoder(ConnectRequest::class.java))
                addLast(RequestPacketEncoder())

                addLast(ResponseHandler())
            }
        }
    }

    companion object {
        private val LOG: Logger = LoggerFactory.getLogger(IPCClient::class.java)
    }

}

data class Callback(var lastContactTimestramp: Long, val func: (ResponsePacket) -> Unit)

data class RequestWithTime(val request: RequestPacket, val timestamp: Long)

fun client(
    config: IPCClientConfig = IPCClientConfig(),
    init: IPCClientConfig.() -> Unit): IPCClient {
    config.init()
    return IPCClient(config)
}
