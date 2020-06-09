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

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.ChannelFuture
import org.slf4j.LoggerFactory
import java.net.SocketAddress

class Endpoint<T : SocketAddress>(val serverBootstrap: ServerBootstrap, val channelFuture: ChannelFuture, val socketAddress: T) : AutoCloseable {

    override fun close() {
        channelFuture.channel().close().sync()

        LOG.debug("Channel closed.")

        serverBootstrap.config().let {
            it.group().shutdownGracefully()
            it.childGroup().shutdownGracefully()
        }
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(Endpoint::class.java)
    }
}
