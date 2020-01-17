package com.github.yag.ipc.server

import com.github.yag.ipc.*
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelFuture
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelInitializer
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.ByteToMessageDecoder
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.handler.codec.LengthFieldPrepender
import io.netty.handler.timeout.ReadTimeoutException
import io.netty.handler.timeout.ReadTimeoutHandler
import io.netty.handler.traffic.GlobalTrafficShapingHandler
import io.netty.util.concurrent.DefaultThreadFactory
import org.jetbrains.annotations.TestOnly
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.net.BindException
import java.net.InetSocketAddress
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class IPCServer internal constructor(
    private val config: IPCServerConfig,
    private val requestHandler: RequestHandler,
    private val connectionHandler: ConnectionHandler = ChainConnectionHandler(),
    private val id: String
) : AutoCloseable {

    private val serverBootstrap: ServerBootstrap

    private val handler = ChildChannelHandler()

    private val channelFuture: ChannelFuture

    private val trafficExecutor = Executors.newSingleThreadScheduledExecutor()

    private val trafficHandler = GlobalTrafficShapingHandler(trafficExecutor, 1000)

    val endpoint: InetSocketAddress

    private val messageReceived = metric.counter("ipc-server-message-received")

    private val channelFlushed = metric.counter("ipc-server-flush-counter")

    private val bytesRead = metric.meter("ipc-server-bytes-read")

    private val byteWrite = metric.meter("ipc-server-bytes-write")

    private val closed = AtomicBoolean()

    internal var ignoreHeartbeat = false
        @TestOnly set


    init {
        addThreadName(id) {
            LOG.info("Start ipc server.")
        }
        serverBootstrap = ServerBootstrap().apply {
            channel(NioServerSocketChannel::class.java)
                .group(
                    NioEventLoopGroup(config.parentThreads, DefaultThreadFactory("ipc-server-parent-$id", true)),
                    NioEventLoopGroup(config.childThreads, DefaultThreadFactory("ipc-server-child-$id", true))
                )
                .applyChannelConfig(config.channelConfig)
                .childHandler(handler)
        }

        try {
            channelFuture = serverBootstrap.bind(config.host, config.port).sync()
            endpoint = channelFuture.channel().localAddress() as InetSocketAddress
            addThreadName(id) {
                LOG.info("IPC server started on: {}.", endpoint)
            }
        } catch (e: BindException) {
            addThreadName(id) {
                LOG.error("Port conflict: {}.", config.port, e)
            }
            throw e
        }
    }


    inner class ChildChannelHandler : ChannelInitializer<SocketChannel>() {

        override fun initChannel(socketChannel: SocketChannel) {
            LOG.debug("New tcp connection arrived.")

            val connection = Connection(UUID.randomUUID().toString())

            socketChannel.pipeline().apply {
                addLast(ReadTimeoutHandler(config.maxIdleTimeMs, TimeUnit.MILLISECONDS))
                addLast(trafficHandler)

                addLast(LengthFieldBasedFrameDecoder(config.maxReqeustPacketSize, 0, 4, 0, 4))
                addLast(LengthFieldPrepender(4, 0))

                addLast(TEncoder(ConnectionResponse::class.java))
                addLast(RequestPacketEncoder())

                addLast(RequestDecoder(connection))

                addLast(RequestDispatcher(connection))
            }
        }

    }

    inner class RequestDecoder(private val connection: Connection) : ByteToMessageDecoder() {

        @Volatile
        private var connected = false

        override fun decode(ctx: ChannelHandlerContext, buf: ByteBuf, out: MutableList<Any>) {
            if (!connected) {
                LOG.debug("Handling incoming connect request from: {}.", ctx.channel().remoteAddress())

                connection.remoteAddress = ctx.channel().remoteAddress() as InetSocketAddress
                connection.localAddress = ctx.channel().localAddress() as InetSocketAddress
                connection.getConnectRequest = {
                    TDecoder.decode(ConnectRequest(), buf)
                }

                try {
                    connectionHandler.handle(connection)
                    connected = true
                    LOG.debug(
                        "Connected, connectionId: {}, remoteAddress: {}.",
                        connection.id,
                        connection.remoteAddress
                    )
                    ctx.writeAndFlush(ConnectionResponse(ConnectionResponse.accepted(ConnectionAccepted(connection.id))))
                } catch (e: Exception) {
                    LOG.debug(
                        "Reject connection, connectionId: {}, remoteAddress: {}.",
                        connection.id,
                        connection.remoteAddress
                    )
                    ctx.writeAndFlush(ConnectionResponse(ConnectionResponse.rejected(ConnectionRejected((e.message)))))
                    ctx.close()
                }
            } else {
                val packet = decodeRequestPacket(buf)
                if (packet.request.isSetHeader) {
                    out.add(packet)
                } else {
                    check(packet.request.isSetHeartbeat)
                    if (!ignoreHeartbeat) {
                        val timestamp = minOf(packet.request.heartbeat.timestamp, System.currentTimeMillis())
                        val heartbeat = ResponsePacket(
                            Response.header(
                                ResponseHeader(
                                    packet.request.heartbeat.timestamp,
                                    StatusCode.OK,
                                    0
                                )
                            ), Unpooled.EMPTY_BUFFER
                        )
                        ctx.writeAndFlush(heartbeat)
                    }
                }
            }
        }
    }

    inner class RequestDispatcher(private val connection: Connection) : ChannelInboundHandlerAdapter() {

        override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
            super.channelRead(ctx, msg)
            val packet = msg as RequestPacket
            LOG.trace("Handle message, id: {}, requestId: {}.", connection.id, packet.request.header.callId)
            requestHandler.handle(connection, packet) {
                check(it.response.header.callId == packet.request.header.callId)

                if (ctx.channel().eventLoop().inEventLoop()) {
                    ctx.write(it, ctx.voidPromise())
                } else {
                    try {
                        ctx.channel().eventLoop().execute {
                            ctx.write(it, ctx.voidPromise())
                        }
                    } catch (e: RejectedExecutionException) {
                        LOG.info("Ignored pending response: {}.", it.response.header.callId)
                    }
                }
                LOG.trace("Send response, id: {}, requestId: {}.", connection.id, it.response.header.callId)
            }
        }

        override fun channelReadComplete(ctx: ChannelHandlerContext) {
            super.channelReadComplete(ctx)
            LOG.trace("Read complete and flush data.")
            ctx.flush()
        }

        @Suppress("deprecation", "OverridingDeprecatedMember")
        override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
            super.exceptionCaught(ctx, cause)
            if (cause is ReadTimeoutException) {
                LOG.info("Connection read timeout, connection: {}.", connection.id)
            } else {
                LOG.warn("Unknown exception.", cause)
            }
        }

        override fun channelInactive(ctx: ChannelHandlerContext) {
            super.channelInactive(ctx)
            LOG.debug("Channel inactive: {}.", connection.id)
        }
    }

    override fun close() {
        if (closed.compareAndSet(false, true)) {
            addThreadName(id) {
                LOG.info("Closing ipc server.")
                trafficExecutor.let {
                    it.shutdown()
                    it.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS)
                }

                if (requestHandler is AutoCloseable) {
                    requestHandler.close()
                }

                LOG.debug("Request handler closed.")

                channelFuture.channel().close().sync()

                LOG.debug("Channel closed.")

                serverBootstrap.config().let {
                    it.group().shutdownGracefully()
                    it.childGroup().shutdownGracefully()
                }

                LOG.info("IPC server closed.")
            }
        }
    }

    companion object {
        private val LOG: Logger = LoggerFactory.getLogger(IPCServer::class.java)
    }
}