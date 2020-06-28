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
import com.github.yag.ipc.Daemon
import com.github.yag.ipc.Packet
import com.github.yag.ipc.Prompt
import com.github.yag.ipc.ResponseHeader
import com.github.yag.ipc.daemon
import com.github.yag.retry.DefaultErrorHandler
import com.github.yag.retry.Retry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
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
    private val promptHandler: (Prompt) -> ByteArray,
    private val metric: MetricRegistry,
    private val id: String
) : AutoCloseable {

    private val retry = Retry(config.connectRetry, config.connectBackOff, DefaultErrorHandler(), config.backOffRandomRange)

    private var client: RawIPCClient<T>

    inner class Monitor(private val shouldStop: AtomicBoolean) : Runnable {
        override fun run() {
            while (!shouldStop.get()) {
                try {
                    client.channel.closeFuture().await()
                    LOG.warn("Connection broken.")
                    client.close()

                    val uncompleted = client.getUncompletedRequests()

                    Thread.sleep(config.connectBackOff.baseIntervalMs)
                    client = retry.call {
                        RawIPCClient(config, promptHandler, metric, id)
                    }

                    for (request in uncompleted) {
                        //client.send(request.request.type, request.request)
                    }
                } catch (e: InterruptedException) {
                }
            }
        }

    }

    private var monitor: Daemon<Monitor>

    init {
        client = retry.call {
            RawIPCClient(config, promptHandler, metric, id)
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
    fun send(type: RequestType<T>, body: Body, callback: (Packet<ResponseHeader>) -> Any?) {
        client.send(type, body, callback)
    }

    /**
     * Send request packet to server.
     * @param type call type
     * @param data request body
     * @return Future object of response packet
     */
    fun send(type: RequestType<T>, body: Body): Future<Packet<ResponseHeader>> {
        val future = CompletableFuture<Packet<ResponseHeader>>()
        send(type, body) {
            future.complete(it.also {
                it.body.getBody().retain()
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
    fun sendSync(type: RequestType<T>, body: Body): Packet<ResponseHeader> {
        return send(type, body).get()
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

