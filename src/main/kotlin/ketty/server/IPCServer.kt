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

package ketty.server

import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufUtil
import io.netty.channel.ChannelFuture
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelInitializer
import io.netty.channel.epoll.Epoll
import io.netty.channel.epoll.EpollEventLoopGroup
import io.netty.channel.epoll.EpollServerSocketChannel
import io.netty.channel.kqueue.KQueue
import io.netty.channel.kqueue.KQueueEventLoopGroup
import io.netty.channel.kqueue.KQueueServerSocketChannel
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
import ketty.common.ConnectionRejectException
import ketty.common.Packet
import ketty.common.PacketCodec
import ketty.common.PacketEncoder
import ketty.common.RequestPacketHeader
import ketty.common.TDecoder
import ketty.common.TEncoder
import ketty.common.applyChannelConfig
import ketty.protocol.ConnectRequest
import ketty.protocol.ConnectionAccepted
import ketty.protocol.ConnectionRejected
import ketty.protocol.ConnectionResponse
import ketty.protocol.Prompt
import ketty.protocol.RequestHeader
import org.jetbrains.annotations.TestOnly
import org.slf4j.LoggerFactory
import java.io.IOException
import java.net.BindException
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.ClosedChannelException
import java.security.SecureRandom
import java.util.Base64
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class IPCServer internal constructor(
    private val config: IPCServerConfig,
    private val requestHandler: RequestHandler,
    private val connectionHandler: ConnectionHandler = ChainConnectionHandler(),
    private val promptGenerator: () -> ByteArray,
    private val id: String
) : AutoCloseable {

    private val serverBootstrap: ServerBootstrap

    private val handler = ChildChannelHandler()

    private val channelFuture: ChannelFuture

    private val trafficExecutor = Executors.newSingleThreadScheduledExecutor()

    private val trafficHandler = GlobalTrafficShapingHandler(trafficExecutor, 1000)

    val endpoint: InetSocketAddress

    private val random = SecureRandom()

    private val closed = AtomicBoolean()

    var ignoreHeartbeat = false
        @TestOnly set

    var ignoreNewConnection = false
        @TestOnly set

    var brokenOnRequest = false
        @TestOnly set


    init {
        LOG.info("Start ipc server.")
        serverBootstrap = ServerBootstrap().apply {
            when {
                Epoll.isAvailable() -> {
                    LOG.debug("Using epoll.")
                    channel(EpollServerSocketChannel::class.java)
                        .group(
                            EpollEventLoopGroup(config.parentThreads, DefaultThreadFactory("ipc-server-parent-$id", true)),
                            EpollEventLoopGroup(config.childThreads, DefaultThreadFactory("ipc-server-child-$id", true))
                        )
                }
                KQueue.isAvailable() -> {
                    LOG.debug("Using kqueue.")
                    channel(KQueueServerSocketChannel::class.java)
                        .group(
                            KQueueEventLoopGroup(config.parentThreads, DefaultThreadFactory("ipc-server-parent-$id", true)),
                            KQueueEventLoopGroup(config.childThreads, DefaultThreadFactory("ipc-server-child-$id", true))
                        )
                }
                else -> {
                    LOG.debug("Using nio.")
                    channel(NioServerSocketChannel::class.java)
                        .group(
                            NioEventLoopGroup(config.parentThreads, DefaultThreadFactory("ipc-server-parent-$id", true)),
                            NioEventLoopGroup(config.childThreads, DefaultThreadFactory("ipc-server-child-$id", true))
                        )
                }
            }.applyChannelConfig(config.channelConfig).childHandler(handler)
        }

        try {
            channelFuture = serverBootstrap.bind(config.host, config.port).sync()
            endpoint = channelFuture.channel().localAddress() as InetSocketAddress
            LOG.info("IPC server started on: {}.", endpoint)
        } catch (e: BindException) {
            LOG.error("Port conflict: {}.", config.port, e)
            throw e
        }
    }


    inner class ChildChannelHandler : ChannelInitializer<SocketChannel>() {

        override fun initChannel(socketChannel: SocketChannel) {
            LOG.debug("New tcp connection arrived.")

            if (ignoreNewConnection) {
                socketChannel.close()
                return
            }

            val promptData = promptGenerator()
            val connection = Connection(UUID.randomUUID().toString(), promptData)

            socketChannel.pipeline().apply {
                addLast(ReadTimeoutHandler(config.maxIdleTimeMs, TimeUnit.MILLISECONDS))
                addLast(trafficHandler)

                addLast(LengthFieldBasedFrameDecoder(config.maxRequestPacketSize, 0, 4, 0, 4))
                addLast(LengthFieldPrepender(4, 0))

                addLast(TEncoder(Prompt::class.java))
                addLast(TEncoder(ConnectionResponse::class.java))
                addLast(PacketEncoder())

                addLast(RequestDecoder(connection))

                addLast(RequestDispatcher(connection))
            }

            socketChannel.writeAndFlush(Prompt("V1", ByteBuffer.wrap(promptData)))
        }

    }

    inner class RequestDecoder(private val connection: Connection) : ByteToMessageDecoder() {

        @Volatile
        private var connected = false

        override fun decode(ctx: ChannelHandlerContext, buf: ByteBuf, out: MutableList<Any>) {
            if (LOG.isTraceEnabled) {
                LOG.trace("Decode request: ${buf.readableBytes()}, ${ByteBufUtil.hexDump(buf)}")
            }

            if (!connected) {
                LOG.debug("Handling incoming connect request from: {}.", ctx.channel().remoteAddress())

                connection.remoteAddress = ctx.channel().remoteAddress() as InetSocketAddress
                connection.localAddress = ctx.channel().localAddress() as InetSocketAddress
                val connectRequest = TDecoder.decode(ConnectRequest(), buf)
                connection.version = connectRequest.getVersion()
                connection.headers = if (connectRequest.isSetHeaders) connectRequest.getHeaders() else emptyMap()
                connection.requestBody = connectRequest.getBody()
                connection.requestTimeoutMs = if (connectRequest.isSetRequestTimeoutMs) connectRequest.getRequestTimeoutMs() else 0
                connection.sessionId = if (connectRequest.isSetSessionId) {
                    connectRequest.sessionId
                } else {
                    val id = ByteArray(config.sessionIdLength)
                    random.nextBytes(id)
                    Base64.getEncoder().encodeToString(id)
                }

                try {
                    connectionHandler.handle(connection)
                    connected = true
                    LOG.debug(
                        "Connected, connectionId: {}, remoteAddress: {}.",
                        connection.id,
                        connection.remoteAddress
                    )
                    ctx.writeAndFlush(ConnectionResponse(ConnectionResponse.accepted(ConnectionAccepted(connection.id, connection.sessionId))))
                } catch (e: ConnectionRejectException) {
                    LOG.debug(
                        "Reject connection, connectionId: {}, remoteAddress: {}.",
                        connection.id,
                        connection.remoteAddress
                    )
                    handle(ctx, e)
                } catch (e: Exception) {
                    LOG.warn(
                        "Validate connection failed, connectionId: {}, remoteAddress: {}.",
                        connection.id,
                        connection.remoteAddress,
                        e
                    )
                    handle(ctx, e)
                }
            } else {
                val packet = PacketCodec.decode(buf, RequestPacketHeader())
                connection.lastContactTimestamp = System.currentTimeMillis()
                if (!packet.isHeartbeat()) {
                    out.add(packet)
                } else {
                    buf.release()
                    if (!ignoreHeartbeat) {
                        val heartbeat = Packet.responseHeartbeat
                        ctx.writeAndFlush(heartbeat)
                    }
                }
            }
        }

        private fun handle(ctx: ChannelHandlerContext, e: Exception) {
            ctx.writeAndFlush(ConnectionResponse(ConnectionResponse.rejected(ConnectionRejected((e.message)))))
            ctx.close()
        }
    }

    inner class RequestDispatcher(private val connection: Connection) : ChannelInboundHandlerAdapter() {

        @Suppress("unchecked_cast")
        override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
            val packet = msg as Packet<RequestHeader>
            val header = packet.header
            LOG.trace("Handle message, id: {}, requestId: {}.", connection.id, header.thrift.callId)

            if (brokenOnRequest) {
                LOG.debug("Broken connection.")
                packet.close()
                ctx.channel().close()
                return
            }

            requestHandler.handle(connection, packet) {
                check(it.header.thrift.callId == header.thrift.callId)

                if (ctx.channel().eventLoop().inEventLoop()) {
                    ctx.write(it, ctx.voidPromise())
                    it.close()
                } else {
                    try {
                        ctx.channel().eventLoop().execute {
                            ctx.write(it, ctx.voidPromise())
                            it.close()
                        }
                    } catch (e: RejectedExecutionException) {
                        LOG.info("Ignored pending response: {}.", it.header.thrift.callId)
                    }
                }
                LOG.trace("Send response, id: {}, requestId: {}.", connection.id, it.header.thrift.callId)
            }
        }

        override fun channelReadComplete(ctx: ChannelHandlerContext) {
            super.channelReadComplete(ctx)
            LOG.trace("Read complete and flush data.")
            ctx.flush()
        }

        @Suppress("deprecation", "OverridingDeprecatedMember")
        override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
            when (cause) {
                is ReadTimeoutException -> {
                    LOG.info("Connection read timeout, connection: {}.", connection.id)
                    connectionHandler.timeout(connection)
                }
                is ClosedChannelException -> {
                    if (LOG.isDebugEnabled) {
                        LOG.debug(
                            "Connection closed, connection: {}.",
                            connection.id
                        )
                    }
                }
                is IOException -> {
                    when (cause.message) {
                        "Broken pipe", "Connection reset by peer" -> LOG.debug(
                            "Connection broken, connection: {}, cause: {}.",
                            connection.id,
                            cause.toString()
                        )
                        else -> LOG.debug("Connection I/O error, connection: {}.", connection.id, cause)
                    }
                }
                else -> {
                    LOG.warn("Unknown exception.", cause)
                }
            }
        }

        override fun channelInactive(ctx: ChannelHandlerContext) {
            super.channelInactive(ctx)
            LOG.debug("Channel inactive: {}.", connection.id)
            connectionHandler.inactive(connection)
        }
    }

    override fun close() {
        if (closed.compareAndSet(false, true)) {
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

    companion object {
        private val LOG =  LoggerFactory.getLogger(IPCServer::class.java)
    }

}
