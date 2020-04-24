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

package com.github.yag.ipc.client

import com.codahale.metrics.MetricRegistry
import com.github.yag.ipc.Prompt
import java.util.UUID

class IPCClientBuilder<T: Any>(private val config: IPCClientConfig = IPCClientConfig()) {

    var promptHandler: (Prompt) -> ByteArray = {
        ByteArray(0)
    }

    var metric: MetricRegistry = MetricRegistry()

    var id: String = UUID.randomUUID().toString()

    fun config(init: IPCClientConfig.() -> Unit) {
        config.init()
    }

    fun prompt(handler: (Prompt) -> ByteArray) {
        promptHandler = handler
    }

    fun build() : IPCClient<T> {
        return IPCClient(config, promptHandler, metric, id)
    }

}

/**
 * Create IPC client.
 * @param T call type
 * @param init init block of client config
 * @return created IPC client.
 */
fun <T : Any> client(
    config: IPCClientConfig = IPCClientConfig(),
    init: IPCClientBuilder<T>.() -> Unit
): IPCClient<T> {
    val builder = IPCClientBuilder<T>(config)
    builder.init()
    return builder.build()
}
