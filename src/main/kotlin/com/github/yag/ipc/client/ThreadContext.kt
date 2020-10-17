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
import org.slf4j.LoggerFactory
import java.lang.NumberFormatException
import java.util.Timer
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class ThreadContext private constructor(private val threads: Int, val timer: Timer = Timer(true), val eventLoop: EventLoopGroup = PlatformEventLoopGroup(threads).instance) {

    var refCnt = 1
        private set

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
            this
        }
    }

    companion object {

        private val LOG = LoggerFactory.getLogger(ThreadContext::class.java)

        private const val IPC_CLIENT_THREADS = "ipc.client.threads"

        private val lock = ReentrantLock()

        internal var cache: ThreadContext? = null

        fun getDefault() : ThreadContext {
            return lock.withLock {
                var c = cache
                if (c == null || c.refCnt == 0) {
                    c = ThreadContext(getThreads())
                    cache = c
                } else {
                    c.retain()
                }
                c
            }
        }

        private fun getThreads() : Int {
            val threads = try {
                System.getProperty(IPC_CLIENT_THREADS, "${Runtime.getRuntime().availableProcessors()}").toInt()
            } catch (e: NumberFormatException) {
                LOG.warn("Invalid property: {}.", IPC_CLIENT_THREADS, e)
                Runtime.getRuntime().availableProcessors()
            }
            LOG.info("Default ThreadContext threads: {}.", threads)
            return threads
        }

    }

}
