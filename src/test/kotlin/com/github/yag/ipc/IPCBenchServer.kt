package com.github.yag.ipc

import com.github.yag.config.ConfigLoader
import com.github.yag.config.config
import com.github.yag.ipc.server.IPCServerConfig
import com.github.yag.ipc.server.server
import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.Unpooled
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options

object IPCBenchServer {

    private const val configFile = "./bench-server.properties"

    @JvmStatic
    fun main(args: Array<String>) {
        val options = Options().also {
            it.addOption("h", "help", false, "Show this help message.")
            it.addOption(Option.builder("f").longOpt("config").hasArg().argName("config file").desc("Configuration file path in classpath or absolute").build())
            it.addOption(Option.builder("D").argName("property=value").numberOfArgs(2).valueSeparator('=').desc("Override configuration value").build())
            it.addOption(Option.builder("s").required(true).hasArg().argName("response-body-size").desc("Size of response body").build())

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
        }.config(IPCServerConfig::class)

        val responseBodySize = cmd.getOptionValue("s").toInt()
        val buf = ByteBufAllocator.DEFAULT.directBuffer(responseBodySize, responseBodySize).also {
            it.writerIndex(responseBodySize)
        }

        server(config) {
            request {
                map("req") {
                    it.ok(buf)
                }
            }
        }
    }
}