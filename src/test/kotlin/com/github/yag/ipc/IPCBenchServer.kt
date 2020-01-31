package com.github.yag.ipc

import com.codahale.metrics.ConsoleReporter
import com.codahale.metrics.MetricRegistry
import com.github.yag.config.ConfigLoader
import com.github.yag.config.config
import com.github.yag.ipc.server.IPCServerConfig
import com.github.yag.ipc.server.server
import io.netty.buffer.ByteBufAllocator
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import java.util.concurrent.TimeUnit

object IPCBenchServer {

    private const val configFile = "./bench-server.properties"

    @JvmStatic
    fun main(args: Array<String>) {
        val options = Options().also {
            it.addOption("h", "help", false, "Show this help message.")
            it.addOption(Option.builder("f").longOpt("config").hasArg().argName("config file").desc("Configuration file path in classpath or absolute").build())
            it.addOption(Option.builder("D").argName("property=value").numberOfArgs(2).valueSeparator('=').desc("Override configuration value").build())
        }

        val cmd = DefaultParser().parse(options, args)
        if (cmd.hasOption("h")) {
            HelpFormatter().printHelp("[options]", "Options:", options, "")
            return
        }

        val config = ConfigLoader.load(
            if (cmd.hasOption("config")) {
                cmd.getOptionValue("config")
            } else {
                configFile
            }
        ).also {
            if (cmd.hasOption("D")) {
                ConfigLoader.override(it, cmd.getOptionProperties("D"))
            }
        }.config(IPCBenchServerConfig::class)

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