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
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import java.util.UUID

class IPCServerBuilder<T : Any>(
    private var ipcServerConfig: IPCServerConfig
) {

    var id: String = UUID.randomUUID().toString()

    var metric: MetricRegistry = MetricRegistry()

    var rootHandler = RootRequestHandler<T>()

    var connectionHandler = ChainConnectionHandler()

    var promptData: () -> ByteArray = { ByteArray(0) }

    fun config(init: IPCServerConfig.() -> Unit) {
        ipcServerConfig.init()
    }

    fun connection(init: ChainConnectionHandler.() -> Unit) {
        connectionHandler.init()
    }

    fun request(init: RootRequestHandler<T>.() -> Unit) {
        rootHandler.init()
    }

    fun prompt(data: () -> ByteArray) {
        promptData = data
    }

    fun build(): IPCServer {
        return IPCServer(ipcServerConfig, rootHandler, connectionHandler, promptData, metric, id)
    }
}

fun <T : Any> server(
    config: IPCServerConfig = IPCServerConfig(),
    init: IPCServerBuilder<T>.() -> Unit
): IPCServer {
    val builder = IPCServerBuilder<T>(config)
    builder.init()
    return builder.build()
}
