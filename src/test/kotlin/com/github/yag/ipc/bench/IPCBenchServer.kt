package com.github.yag.ipc.bench

import com.codahale.metrics.ConsoleReporter
import com.codahale.metrics.MetricRegistry
import com.github.yag.ipc.Utils
import com.github.yag.ipc.ok
import com.github.yag.ipc.server.server
import io.netty.buffer.ByteBufAllocator
import java.util.concurrent.TimeUnit

object IPCBenchServer {

    private const val configFile = "./bench-server.properties"

    @JvmStatic
    fun main(args: Array<String>) {
        val config = Utils.getConfig(IPCBenchServerConfig::class.java, configFile, args) ?: return

        val buf = ByteBufAllocator.DEFAULT.directBuffer(config.responseBodySize, config.responseBodySize).also {
            it.writerIndex(config.responseBodySize)
        }

        val metric = MetricRegistry()
        val callMetric = metric.meter("call")
        val readMetric = metric.meter("read")
        val writeMetric = metric.meter("write")
        val reporter = ConsoleReporter.forRegistry(metric).convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS).build()
        reporter.start(1, TimeUnit.SECONDS)

        server(config.ipc, metric) {
            request {
                map("req") {
                    callMetric.mark()
                    readMetric.mark(it.header.thrift.contentLength.toLong())
                    writeMetric.mark(buf.readableBytes().toLong())
                    it.ok(buf.retain())
                }
            }
        }
    }
}