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
import java.net.InetSocketAddress
import java.util.UUID

class IPCClientBuilder<T: Any>
    @JvmOverloads constructor(private var endpoint: InetSocketAddress, val config: IPCClientConfig = IPCClientConfig()) {

    private var threadContext: ThreadContext? = null

    private var promptHandler: (Prompt) -> ByteArray = {
        ByteArray(0)
    }

    private var metric: MetricRegistry = MetricRegistry()

    private var id: String = UUID.randomUUID().toString()

    fun config(init: IPCClientConfig.() -> Unit) = apply {
        config.init()
    }

    fun config(init: IPCClientConfigurator) = apply {
        init.configure(config)
    }

    fun threadContext(threadContext: ThreadContext?) = apply {
        this.threadContext = threadContext
    }

    fun prompt(handler: (Prompt) -> ByteArray) = apply {
        this.promptHandler = handler
    }

    fun prompt(handler: PromptHandler) = apply {
        prompt(handler::handle)
    }

    fun metric(metric: MetricRegistry) = apply {
        this.metric = metric
    }

    fun id(id: String) = apply {
        this.id = id
    }

    fun build() : IPCClient<T> {
        return IPCClient(endpoint, config, threadContext ?: ThreadContext.getDefault(), promptHandler, metric, id)
    }

}

/**
 * Create IPC client.
 * @param T call type
 * @param init init block of client config
 * @return created IPC client.
 */
fun <T : Any> client(
    endpoint: InetSocketAddress,
    config: IPCClientConfig = IPCClientConfig(),
    init: IPCClientBuilder<T>.() -> Unit = {}
): IPCClient<T> {
    val builder = IPCClientBuilder<T>(endpoint, config)
    builder.init()
    return builder.build()
}
