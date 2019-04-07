package com.github.yag.ipc.server

import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap

class Connection {

    lateinit var id: String

    lateinit var localAddress: InetSocketAddress

    lateinit var remoteAddress: InetSocketAddress

    var account: String = "default"

    private val data by lazy {
        ConcurrentHashMap<String, Any>()
    }

    fun put(key: String, value: Any) {
        data[key] = value
    }

    fun get(key: String): Any? {
        return data[key]
    }

    fun remove(key: String): Any? {
        return data.remove(key)
    }
}