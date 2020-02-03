/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package com.github.yag.ipc.client

import com.codahale.metrics.MetricRegistry
import com.github.yag.ipc.Daemon
import com.github.yag.ipc.Packet
import com.github.yag.ipc.ResponseHeader
import com.github.yag.ipc.daemon
import com.github.yag.retry.DefaultErrorHandler
import com.github.yag.retry.Retry
import com.google.common.util.concurrent.SettableFuture
import io.netty.buffer.ByteBuf
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.UUID
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicBoolean

/**
 * The IPC Client class send messages to IPC server can execute callback when response was received.
 *
 * @param T The call type class, typically string, numbers and enums.
 * @property config client config
 * @property metric metric registry
 * @property id client id, will be used in log and thread name.
 */
class IPCClient<T : Any>(
    private val config: IPCClientConfig,
    private val metric: MetricRegistry,
    private val id: String
) : AutoCloseable {

    private val retry = Retry(config.connectRetry, config.connectBackOff, DefaultErrorHandler())

    private var client: RawIPCClient<T>

    inner class Monitor(private val shouldStop: AtomicBoolean) : Runnable {
        override fun run() {
            while (!shouldStop.get()) {
                try {
                    client.channel.closeFuture().await()
                    LOG.warn("Connection broken.")
                    client.close()

                    Thread.sleep(config.connectBackOff.baseIntervalMs)
                    client = retry.call {
                        RawIPCClient(config, metric, id)
                    }
                } catch (e: InterruptedException) {
                }
            }
        }

    }

    private var monitor: Daemon<Monitor>

    init {
        client = retry.call {
            RawIPCClient(config, metric, id)
        }
        monitor = daemon("connection-monitor") {
            Monitor(it)
        }.also { it.start() }
    }

    /**
     * Send request packet to server.
     * @param type call type
     * @param data request body
     * @param callback code block to handle response packet
     */
    fun send(type: T, data: ByteBuf, callback: (Packet<ResponseHeader>) -> Any?) {
        client.send(type, data, callback)
    }

    /**
     * Send request packet to server.
     * @param type call type
     * @param data request body
     * @return Future object of response packet
     */
    fun send(type: T, data: ByteBuf): Future<Packet<ResponseHeader>> {
        val future = SettableFuture.create<Packet<ResponseHeader>>()
        send(type, data) {
            future.set(it.also {
                it.body.retain()
            })
        }
        return future
    }

    /**
     * Send request packet to server.
     * @param type call type
     * @param data request body
     * @return response packet
     */
    fun sendSync(type: T, data: ByteBuf): Packet<ResponseHeader> {
        return send(type, data).get()
    }

    /**
     * Close IPC client and release all related resources..
     */
    override fun close() {
        monitor.close()
        client.close()
    }

    /**
     * Check connection is well.
     * @return true is connected.
     */
    fun isConnected(): Boolean {
        return client.isConnected()
    }

    companion object {
        private val LOG: Logger = LoggerFactory.getLogger(IPCClient::class.java)
    }

}

/**
 * Create IPC client.
 * @param T call type
 * @param config client config
 * @param metric metric registry
 * @param id client id
 * @param init init block of client config
 * @return created IPC client.
 */
fun <T : Any> client(
    config: IPCClientConfig = IPCClientConfig(),
    metric: MetricRegistry = MetricRegistry(),
    id: String = UUID.randomUUID().toString(),
    init: IPCClientConfig.() -> Unit
): IPCClient<T> {
    config.init()
    return IPCClient(config, metric, id)
}
