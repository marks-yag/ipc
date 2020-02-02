package com.github.yag.ipc.bench

import com.codahale.metrics.ConsoleReporter
import com.codahale.metrics.MetricRegistry
import com.github.yag.ipc.CallType
import com.github.yag.ipc.Utils
import com.github.yag.ipc.client.IPCClient
import com.github.yag.ipc.isSuccessful
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

object IPCBenchClient {

    private const val configFile = "./bench-client.properties"

    @JvmStatic
    fun main(args: Array<String>) {
        val config = Utils.getConfig(IPCBenchClientConfig::class.java, configFile, args) ?: return

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
                IPCClient<CallType>(config.ipc, metric, id = "ipc-client").use { client ->
                    repeat(config.requests) {
                        val startMs = System.currentTimeMillis()
                        client.send(CallType.values().random(), buf) {
                            val endMs = System.currentTimeMillis()
                            callMetric.update(endMs - startMs, TimeUnit.MILLISECONDS)

                            if (!it.header.thrift.statusCode.isSuccessful()) {
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