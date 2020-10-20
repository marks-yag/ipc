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

import com.github.yag.config.ConfigLoader
import com.github.yag.config.Configuration
import com.github.yag.ipc.Daemon
import com.github.yag.ipc.Packet
import com.github.yag.ipc.RequestHeader
import com.github.yag.ipc.daemon
import io.netty.channel.EventLoopGroup
import java.io.IOException
import java.util.Properties
import java.util.Timer
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.Semaphore
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class ThreadContext private constructor(private val config: ThreadContextConfig, val timer: Timer = Timer(true), val eventLoop: EventLoopGroup = PlatformEventLoopGroup(config.threads).instance) {

    var refCnt = 1
        private set

    internal val queue = LinkedBlockingQueue<Call<*>>()

    internal var left: Call<*>? = null

    internal val flusher: Daemon<*> = daemon("flusher") { shouldStop ->
        while (!shouldStop.get()) {
            try {
                val batch = poll()
                batch.first.writeAndFlush(batch.second)
            } catch (e: InterruptedException) {
                //:~
            }
        }
    }.apply { start() }

    internal val parallelCalls = Semaphore(config.maxParallelCalls)

    internal val parallelRequestContentSize = Semaphore(config.maxParallelRequestContentSize)

    private fun poll(): Pair<IPCClient<*>, ArrayList<Packet<RequestHeader>>> {
        val list = ArrayList<Packet<RequestHeader>>()
        var length = 0L

        val call = left?:queue.take()
        var packet = call.request.packet
        val client = call.client
        list.add(packet)
        length += packet.body.data().readableBytes()

        while (true) {
            val call = queue.poll()
            if (call != null) {
                if (call.client == client) {
                    packet = call.request.packet
                    list.add(packet)
                    length += packet.body.data().readableBytes()
                    if (length >= config.maxWriteBatchSize) {
                        break
                    }
                } else {
                    left = call
                }
            } else {
                break
            }
        }
        return client to list
    }

    fun retain() : ThreadContext {
        return lock.withLock {
            check(refCnt > 0)
            refCnt++
            this
        }
    }

    fun release() : ThreadContext {
        return lock.withLock {
            check(refCnt > 0)
            refCnt--
            if (refCnt == 0) {
                eventLoop.shutdownGracefully()
            }
            flusher.close()
            this
        }
    }

    companion object {

        private val lock = ReentrantLock()

        internal var cache: ThreadContext? = null

        fun getDefault() : ThreadContext {
            return lock.withLock {
                var c = cache
                if (c == null || c.refCnt == 0) {
                    c = ThreadContext(getConfig())
                    cache = c
                } else {
                    c.retain()
                }
                c
            }
        }

        private fun getConfig() : ThreadContextConfig {
            val prop = try {
                ConfigLoader.load("ipc.client.thread.context.conf")
            } catch (e: IOException) {
                Properties()
            }
            return Configuration(prop).get(ThreadContextConfig::class.java)
        }

    }

}
