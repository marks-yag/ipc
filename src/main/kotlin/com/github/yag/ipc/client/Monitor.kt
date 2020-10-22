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

import org.slf4j.LoggerFactory
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class Monitor(private val shouldStop: AtomicBoolean) : Runnable {

    private val queue = LinkedBlockingQueue<IPCClient<*>>()

    fun notifyInactive(client: IPCClient<*>) {
        queue.add(client)
    }

    override fun run() {
        while (!shouldStop.get()) {
            try {
                val client = queue.poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS)
                LOG.warn("Connection broken: {}.", client.getConnection())
                client.recover()
            } catch (e: InterruptedException) {
                //:<
            } catch (e: Exception) {
                LOG.warn("Recovery client failed.", e)
            }
        }
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(Monitor::class.java)
    }

}