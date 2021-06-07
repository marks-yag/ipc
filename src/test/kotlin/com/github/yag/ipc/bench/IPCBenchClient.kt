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

package ketty.bench

import com.codahale.metrics.ConsoleReporter
import com.codahale.metrics.MetricRegistry
import ketty.common.CallType
import ketty.common.PlainBody
import ketty.common.Utils
import ketty.client.NonIdempotentRequest
import ketty.client.client
import ketty.common.isSuccessful
import java.net.InetAddress
import java.net.InetSocketAddress
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

object IPCBenchClient {

    private const val configFile = "./bench-client.properties"

    @JvmStatic
    fun main(args: Array<String>) {
        val config = Utils.getConfig(IPCBenchClientConfig::class.java, configFile, args) ?: return
        val endpoint = InetSocketAddress(InetAddress.getLocalHost(), 9527)

        val buf = Utils.createByteBuf(config.requestBodySize)

        val metric = MetricRegistry()
        val callMetric = metric.timer("call")
        val errorMetric = metric.meter("error")
        val reporter = ConsoleReporter.forRegistry(metric).convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS).build()
        reporter.start(1, TimeUnit.SECONDS)

        val latch = CountDownLatch(config.clients * config.requests)
        repeat(config.clients) {
            thread {
                client<CallType>(endpoint, config.ipc).use { client ->
                    repeat(config.requests) {
                        val startMs = System.currentTimeMillis()
                        client.send(NonIdempotentRequest(CallType.values().random()), PlainBody(buf.slice())) {
                            val endMs = System.currentTimeMillis()
                            callMetric.update(endMs - startMs, TimeUnit.MILLISECONDS)

                            if (!it.isSuccessful()) {
                                errorMetric.mark()
                            }
                            latch.countDown()
                        }

                    }
                }
            }
        }
        latch.await(Long.MAX_VALUE, TimeUnit.MILLISECONDS)

        reporter.close()
    }

}
