package com.github.yag.ipc

import com.codahale.metrics.ConsoleReporter
import com.codahale.metrics.MetricRegistry
import com.github.yag.config.ConfigLoader
import com.github.yag.config.config
import com.github.yag.ipc.client.IPCClient
import com.github.yag.ipc.client.IPCClientConfig
import io.netty.buffer.Unpooled
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

object IPCBenchClient {

    private const val configFile = "./bench-client.properties"

    @JvmStatic
    fun main(args: Array<String>) {
        val options = Options().also {
            it.addOption("h", "help", false, "Show this help message.")
            it.addOption(Option.builder("f").argName("config-file").desc("Configuration file path in classpath or absolute").build())
            it.addOption(Option.builder("D").argName("property=value").numberOfArgs(2).valueSeparator('=').desc("Override configuration value").build())
            it.addOption(Option.builder("n").required(true).hasArg().argName("requests").desc("Number of requests to perform").build())
            it.addOption(Option.builder("c").argName("concurrency").hasArg().desc("Number of multiple requests to make at a time").build())
            it.addOption(Option.builder("s").required(true).hasArg().argName("request-body-size").desc("Size of request body").build())
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
        }.config(IPCClientConfig::class)

        val requests = cmd.getOptionValue("n").toInt()
        val concurrency = if (cmd.hasOption("c")) {
            cmd.getOptionValue("c").toInt()
        } else {
            1
        }
        val requestBodySize = cmd.getOptionValue("s").toInt()

        val metric = MetricRegistry()
        val callMetric = metric.timer("call")
        val errorMetric = metric.meter("error")
        val reporter = ConsoleReporter.forRegistry(metric).convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS).build()
        reporter.start(1, TimeUnit.SECONDS)

        val buf = Unpooled.directBuffer(requestBodySize, requestBodySize)
        buf.writerIndex(requestBodySize)
        check(buf.readableBytes() == requestBodySize)

        val latch = CountDownLatch(concurrency * requests)
        repeat(concurrency) { loop ->
            thread {
                IPCClient<String>(config, metric).use { client ->

                    repeat(requests) {
                        val startMs = System.currentTimeMillis()
                        client.send("req", buf) {
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
