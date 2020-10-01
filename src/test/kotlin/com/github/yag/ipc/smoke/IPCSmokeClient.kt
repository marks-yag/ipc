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

package com.github.yag.ipc.smoke

import com.codahale.metrics.ConsoleReporter
import com.codahale.metrics.MetricRegistry
import com.github.yag.ipc.CallType
import com.github.yag.ipc.Utils
import com.github.yag.ipc.client.NonIdempotentRequest
import com.github.yag.ipc.PlainBody
import com.github.yag.ipc.client.client
import com.github.yag.ipc.isSuccessful
import com.github.yag.retry.DefaultErrorHandler
import com.github.yag.retry.ExponentialBackOffPolicy
import com.github.yag.retry.Retry
import com.github.yag.retry.RetryPolicy
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlin.random.Random

object IPCSmokeClient {

    private const val configFile = "./smoke-client.properties"

    @JvmStatic
    fun main(args: Array<String>) {
        val config = Utils.getConfig(IPCSmokeClientConfig::class.java, configFile, args) ?: return

        val metric = MetricRegistry()
        val callMetric = metric.timer("call")
        val errorMetric = metric.meter("error")
        val clients = metric.meter("clients")
        val reporter = ConsoleReporter.forRegistry(metric).convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS).build()
        reporter.start(1, TimeUnit.SECONDS)

        val clientSemaphore = Semaphore(config.clients)

        val random = Random(System.currentTimeMillis())
        val retry = Retry(object : RetryPolicy {
            override fun allowRetry(retryCount: Int, duration: Duration, error: Throwable): Boolean {
                return true
            }
        }, ExponentialBackOffPolicy(), DefaultErrorHandler())

        while (true) {
            clientSemaphore.acquire()
            thread {
                val aliveMs = random.nextLong(config.minAliveMs, config.maxAliveMs)
                val stopTime = System.currentTimeMillis() + aliveMs

                try {
                    retry.call {
                        client<CallType>(config.ipc) {
                            this.metric = metric
                            this.id = "ipc-client"
                        }
                    }.use { client ->
                        LOG.info("Create new client, alive for {}ms.", aliveMs)
                        clients.mark()
                        while (true) {
                            val startMs = System.currentTimeMillis()
                            if (startMs > stopTime) {
                                break
                            }
                            val buf = Utils.createByteBuf(
                                random.nextInt(
                                    config.minRequestBodySize,
                                    config.maxRequestBodySize
                                )
                            )

                            client.send(NonIdempotentRequest(CallType.values().random()), PlainBody(buf)) {
                                val endMs = System.currentTimeMillis()
                                callMetric.update(endMs - startMs, TimeUnit.MILLISECONDS)

                                if (!it.isSuccessful()) {
                                    errorMetric.mark()
                                }
                                buf.release()
                            }

                        }
                    }
                } finally {
                    clientSemaphore.release()
                }
            }
        }
    }

    private val LOG = LoggerFactory.getLogger(IPCSmokeClient::class.java)
}