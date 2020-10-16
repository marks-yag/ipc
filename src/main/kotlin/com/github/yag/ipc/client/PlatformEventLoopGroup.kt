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

import io.netty.channel.EventLoopGroup
import io.netty.channel.epoll.Epoll
import io.netty.channel.epoll.EpollEventLoopGroup
import io.netty.channel.kqueue.KQueue
import io.netty.channel.kqueue.KQueueEventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.util.concurrent.DefaultThreadFactory

class PlatformEventLoopGroup(private val threads: Int) {

    val instance : EventLoopGroup = {
        val threadFactory = DefaultThreadFactory("executor", true)
        when {
            Epoll.isAvailable() -> {
                EpollEventLoopGroup(threads, threadFactory)
            }
            KQueue.isAvailable() -> {
                KQueueEventLoopGroup(threads, threadFactory)
            }
            else -> {
                NioEventLoopGroup(threads, threadFactory)
            }
        }
    }.invoke()

}