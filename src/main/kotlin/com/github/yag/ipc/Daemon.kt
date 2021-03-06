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

package com.github.yag.ipc

import java.util.concurrent.atomic.AtomicBoolean

internal class Daemon<T : Runnable>(
    name: String,
    private val interruptable: Boolean = true,
    val runnable: (AtomicBoolean) -> T
) : Thread(name), AutoCloseable {

    private var shouldShop = AtomicBoolean(false)

    val runner = runnable(shouldShop)

    init {
        isDaemon = true
    }

    override fun run() {
        runner.run()
    }

    override fun close() {
        shouldShop.set(true)
        if (interruptable) {
            interrupt()
        }
        join()
        if (runner is AutoCloseable) {
            runner.close()
        }
    }
}

internal fun <T> daemon(name: String, init: (AtomicBoolean) -> T): Daemon<Runnable> {
    return Daemon(name, true) {
        Runnable {
            init(it)
        }
    }
}
