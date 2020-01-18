package com.github.yag.ipc

import java.util.concurrent.atomic.AtomicBoolean

class Daemon<T : Runnable>(
    private val runnable: (AtomicBoolean) -> T,
    name: String = runnable.javaClass.simpleName,
    val interruptable: Boolean = true
) : Thread(name), AutoCloseable {

    private var shouldShop = AtomicBoolean(false)

    val runner = runnable(shouldShop)

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

fun <T : Runnable> daemon(name: String? = null, init: (AtomicBoolean) -> T): Daemon<T> {
    return if (name == null) {
        Daemon({ init(it) })
    } else {
        Daemon({ init(it) }, name)
    }
}
