/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package com.github.yag.ipc.server

import com.codahale.metrics.MetricRegistry
import java.util.UUID

class IPCServerBuilder<T : Any>(
    private var ipcServerConfig: IPCServerConfig,
    private val metric: MetricRegistry,
    val id: String
) {

    private val rootHandler = RootRequestHandler<T>()

    private val connectionHandler = ChainConnectionHandler()

    fun config(init: IPCServerConfig.() -> Unit) {
        ipcServerConfig.init()
    }

    fun connection(init: ChainConnectionHandler.() -> Unit) {
        connectionHandler.init()
    }

    fun request(init: RootRequestHandler<T>.() -> Unit) {
        rootHandler.init()
    }

    fun build(): IPCServer {
        return IPCServer(ipcServerConfig, rootHandler, connectionHandler, metric, id)
    }
}


fun server(
    config: IPCServerConfig = IPCServerConfig(),
    metric: MetricRegistry = MetricRegistry(),
    id: String = UUID.randomUUID().toString(),
    init: IPCServerBuilder<String>.() -> Unit
) = tserver(config, metric, id, init)

fun <T : Any> tserver(
    config: IPCServerConfig = IPCServerConfig(),
    metric: MetricRegistry = MetricRegistry(),
    id: String = UUID.randomUUID().toString(),
    init: IPCServerBuilder<T>.() -> Unit
): IPCServer {
    val builder = IPCServerBuilder<T>(config, metric, id)
    builder.init()
    return builder.build()
}
