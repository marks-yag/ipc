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

import com.github.yag.ipc.Daemon
import com.github.yag.ipc.daemon
import io.netty.channel.EventLoopGroup
import io.netty.channel.epoll.Epoll
import io.netty.channel.epoll.EpollEventLoopGroup
import io.netty.channel.kqueue.KQueue
import io.netty.channel.kqueue.KQueueEventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.util.concurrent.DefaultThreadFactory
import org.slf4j.LoggerFactory
import java.util.Timer
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class ThreadContext(private val config: ThreadContextConfig) {

    @Volatile
    var refCnt = 1
        private set

    val timer: Timer = Timer(true)

    val eventLoop: EventLoopGroup = createEventLoopGroup(config.eventLoopThreads, config.poolName)

    private val queue = LinkedBlockingQueue<Call<*>>()

    private val reconnectExecutor = Executors.newCachedThreadPool()

    private val flusher: Daemon<*> = daemon("flusher") { shouldStop ->
        while (!shouldStop.get()) {
            try {
                val batch = poll()
                batch.groupBy {
                    it.client
                }.forEach { (t, u) ->
                    t.writeAndFlush(u.map { it.pendingRequest.request.packet })
                }
            } catch (e: InterruptedException) {
                //:<
            } catch (e: Exception) {
                LOG.debug("Write request data failed.", e)
            }
        }
    }.apply { start() }

    internal val parallelCalls = Semaphore(config.maxParallelCalls)

    internal val parallelRequestContentSize = Semaphore(config.maxParallelRequestContentSize)

    internal fun offer(call: Call<*>) {
        check(queue.offer(call))
        LOG.trace("Offer: {}.", call.pendingRequest.request)
    }

    private fun poll(): List<Call<*>> {
        val list = ArrayList<Call<*>>()
        var length = 0L

        val firstCall = queue.take()
        list.add(firstCall)

        var packet = firstCall.pendingRequest.request.packet
        length += packet.body.data().readableBytes()

        while (true) {
            val call = queue.poll()
            if (call != null) {
                packet = call.pendingRequest.request.packet
                list.add(call)
                length += packet.body.data().readableBytes()
                if (length >= config.maxWriteBatchSize) {
                    break
                }
            } else {
                break
            }
        }
        return list
    }

    internal fun execute(callable: Callable<Boolean>) : Future<Boolean> {
        return reconnectExecutor.submit(callable)
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
                LOG.info("Shutdown thread context.")
                eventLoop.shutdownGracefully()
                flusher.close()
                reconnectExecutor.shutdown()
                reconnectExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS)
            }
            this
        }
    }

    companion object {

        private val LOG = LoggerFactory.getLogger(ThreadContext::class.java)

        private val lock = ReentrantLock()

        internal var cache: ThreadContext? = null

        var defaultConfig = ThreadContextConfig()

        fun getDefault() : ThreadContext {
            return lock.withLock {
                var c = cache
                if (c == null || c.refCnt == 0) {
                    c = ThreadContext(defaultConfig)
                    cache = c
                } else {
                    c.retain()
                }
                c
            }
        }

        internal fun createEventLoopGroup(threads: Int, name: String) : EventLoopGroup {
            val threadFactory = DefaultThreadFactory(name, true)
            return when {
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
        }

    }

}
