package com.github.yag.ipc.client

import com.codahale.metrics.MetricRegistry
import com.github.yag.ipc.Daemon
import com.github.yag.ipc.Packet
import com.github.yag.ipc.ResponseHeader
import com.github.yag.ipc.daemon
import com.google.common.util.concurrent.SettableFuture
import io.netty.buffer.ByteBuf
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.UUID
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

class IPCClient<T : Any>(
    private val config: IPCClientConfig,
    private val metric: MetricRegistry,
    private val id: String
) : AutoCloseable {

    private var client: RawIPCClient<T>

    inner class Monitor(private val shouldStop: AtomicBoolean) : Runnable {
        override fun run() {
            while (!shouldStop.get()) {
                try {
                    client.channel.closeFuture().await()
                    LOG.warn("Connection broken, will retry after: {}ms.", config.reconnectDelayMs)
                    client.close()

                    Thread.sleep(config.reconnectDelayMs)
                    client = RawIPCClient(config, metric, id)
                } catch (e: InterruptedException) {
                }
            }
        }

    }

    private var monitor: Daemon<Monitor>

    init {
        client = RawIPCClient(config, metric, id)
        monitor = daemon("connection-monitor") {
            Monitor(it)
        }.also { it.start() }
    }

    fun send(type: T, buf: ByteBuf, callback: (Packet<ResponseHeader>) -> Any?) {
        client.send(type, buf, callback)
    }

    fun send(type: T, buf: ByteBuf): Future<Packet<ResponseHeader>> {
        val future = SettableFuture.create<Packet<ResponseHeader>>()
        send(type, buf) {
            future.set(it.also {
                it.body.retain()
            })
        }
        return future
    }

    fun sendSync(type: T, data: ByteBuf): Packet<ResponseHeader> {
        return send(type, data).get()
    }

    override fun close() {
        monitor.close()
        client.close()
    }

    fun isConnected(): Boolean {
        return client.isConnected()
    }

    companion object {
        private val LOG: Logger = LoggerFactory.getLogger(IPCClient::class.java)

        private val counter = AtomicLong(0)
    }

}

fun client(
    config: IPCClientConfig = IPCClientConfig(),
    metric: MetricRegistry = MetricRegistry(),
    id: String = UUID.randomUUID().toString(),
    init: IPCClientConfig.() -> Unit
) = tclient<String>(config, metric, id, init)

fun <T : Any> tclient(
    config: IPCClientConfig = IPCClientConfig(),
    metric: MetricRegistry = MetricRegistry(),
    id: String = UUID.randomUUID().toString(),
    init: IPCClientConfig.() -> Unit
): IPCClient<T> {
    config.init()
    return IPCClient(config, metric, id)
}
