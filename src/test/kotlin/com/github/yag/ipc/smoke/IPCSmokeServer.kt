package com.github.yag.ipc.smoke

import com.codahale.metrics.ConsoleReporter
import com.codahale.metrics.MetricRegistry
import com.github.yag.ipc.CallType
import com.github.yag.ipc.Utils
import com.github.yag.ipc.ok
import com.github.yag.ipc.server.tserver
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

        tserver<CallType>(config.ipc, metric) {
            request {
                CallType.values().forEach { callType ->
                    map(callType) {
                        val buf = Utils.createByteBuf(random.nextInt(config.minResponseBodySize, config.maxResponseBodySize))
                        callMetric.mark()
                        readMetric.mark(it.header.thrift.contentLength.toLong())
                        writeMetric.mark(buf.readableBytes().toLong())
                        it.ok(buf)
                    }
                }

            }
        }
    }
}