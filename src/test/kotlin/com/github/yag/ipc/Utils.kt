package com.github.yag.ipc

import com.github.yag.config.ConfigLoader
import com.github.yag.config.config
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufAllocator
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options

object Utils {

    fun <T : Any> getConfig(clazz: Class<T>, configFile: String, args: Array<String>): T? {
        val options = Options().also {
            it.addOption("h", "help", false, "Show this help message.")
            it.addOption(Option.builder("f").argName("config-file").desc("Configuration file path in classpath or absolute").build())
            it.addOption(Option.builder("D").argName("property=value").numberOfArgs(2).valueSeparator('=').desc("Override configuration value").build())
        }

        val cmd = DefaultParser().parse(options, args)
        if (cmd.hasOption("h")) {
            HelpFormatter().printHelp("[options]", "Options:", options, "")
            return null
        }

        return ConfigLoader.load(
            if (cmd.hasOption("config")) {
                cmd.getOptionValue("config")
            } else {
                configFile
            }
        ).also {
            if (cmd.hasOption("D")) {
                ConfigLoader.override(it, cmd.getOptionProperties("D"))
            }
        }.config(clazz)
    }

    fun createByteBuf(size: Int): ByteBuf {
        return ByteBufAllocator.DEFAULT.directBuffer(size, size).also {
            it.writerIndex(size)
        }
    }
}