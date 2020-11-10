/*
 * Copyright 2018-2020 marks.yag@gmail.com
 *
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.github.yag.ipc.server

import com.codahale.metrics.MetricRegistry
import java.util.UUID

class IPCServerBuilder<T : Any>(
    private var ipcServerConfig: IPCServerConfig
) {

    private var id: String = UUID.randomUUID().toString()

    private var metric: MetricRegistry = MetricRegistry()

    private val rootHandler = RootRequestHandler<T>()

    private var connectionHandler = ChainConnectionHandler()

    private var promptGenerator = {
        ByteArray(0)
    }

    fun config(init: IPCServerConfig.() -> Unit) = apply {
        ipcServerConfig.init()
    }

    fun config(init: IPCServerConfigurator) = apply {
        init.configure(ipcServerConfig)
    }

    fun connection(init: ChainConnectionHandler.() -> Unit) = apply {
        connectionHandler.init()
    }

    fun connection(init: ChainConnectionHandlerConfigurator) = apply {
        init.configure(connectionHandler)
    }

    fun request(init: RootRequestHandler<T>.() -> Unit) = apply {
        rootHandler.init()
    }

    fun request(init: RootRequestHandlerConfigurator<T>) = apply {
        init.configure(rootHandler)
    }

    fun prompt(promptGenerator: () -> ByteArray) = apply {
        this.promptGenerator = promptGenerator
    }

    fun prompt(promptGenerator: PromptGenerator) = apply {
        this.promptGenerator = promptGenerator::generate
    }

    fun id(id: String) = apply {
        this.id = id
    }

    fun metric(metric: MetricRegistry) = apply {
        this.metric = metric
    }

    fun build(): IPCServer {
        return IPCServer(ipcServerConfig, rootHandler, connectionHandler, promptGenerator, metric, id)
    }
}

fun <T : Any> server(
    config: IPCServerConfig = IPCServerConfig(),
    init: IPCServerBuilder<T>.() -> Unit = {}
): IPCServer {
    val builder = IPCServerBuilder<T>(config)
    builder.init()
    return builder.build()
}
