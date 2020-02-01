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