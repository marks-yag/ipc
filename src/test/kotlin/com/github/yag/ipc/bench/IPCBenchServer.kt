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

package com.github.yag.ipc.bench

import com.codahale.metrics.ConsoleReporter
import com.codahale.metrics.MetricRegistry
import com.github.yag.ipc.CallType
import com.github.yag.ipc.Utils
import com.github.yag.ipc.ok
import com.github.yag.ipc.server.tserver
import java.util.concurrent.TimeUnit

object IPCBenchServer {

    private const val configFile = "./bench-server.properties"

    @JvmStatic
    fun main(args: Array<String>) {
        val config = Utils.getConfig(IPCBenchServerConfig::class.java, configFile, args) ?: return

        val buf = Utils.createByteBuf(config.responseBodySize)

        val metric = MetricRegistry()
        val callMetric = metric.meter("call")
        val readMetric = metric.meter("readBytes")
        val writeMetric = metric.meter("writeBytes")
        val reporter = ConsoleReporter.forRegistry(metric).convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS).build()
        reporter.start(1, TimeUnit.SECONDS)



        tserver<CallType>(config.ipc, metric) {
            request {
                CallType.values().forEach { callType ->
                    map(callType) {
                        callMetric.mark()
                        readMetric.mark(it.header.thrift.contentLength.toLong())
                        writeMetric.mark(config.responseBodySize.toLong())
                        it.ok(buf.retain())
                    }
                }
            }
        }
    }
}