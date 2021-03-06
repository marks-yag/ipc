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
import com.github.yag.ipc.ok
import com.github.yag.ipc.server.server
import java.util.concurrent.TimeUnit
import kotlin.random.Random

object IPCSmokeServer {

    private const val configFile = "./smoke-server.properties"

    @JvmStatic
    fun main(args: Array<String>) {
        val config = Utils.getConfig(IPCSmokeServerConfig::class.java, configFile, args) ?: return

        val metric = MetricRegistry()
        val callMetric = metric.meter("call")
        val readMetric = metric.meter("readBytes")
        val writeMetric = metric.meter("writeBytes")
        val reporter = ConsoleReporter.forRegistry(metric).convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS).build()
        reporter.start(1, TimeUnit.SECONDS)

        val random = Random(System.currentTimeMillis())

        while (true) {
            val server = server<CallType>(config.ipc) {
                metric(metric)
                request {
                    CallType.values().forEach { callType ->
                        map(callType) {
                            val buf = Utils.createByteBuf(
                                random.nextInt(
                                    config.minResponseBodySize,
                                    config.maxResponseBodySize
                                )
                            )
                            callMetric.mark()
                            readMetric.mark(it.header.thrift.contentLength.toLong())
                            writeMetric.mark(buf.readableBytes().toLong())
                            it.ok(buf)
                        }
                    }

                }
            }
            Thread.sleep(random.nextLong(config.minAliveMs, config.maxAliveMs))
            server.close()
        }
    }
}