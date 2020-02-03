/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

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