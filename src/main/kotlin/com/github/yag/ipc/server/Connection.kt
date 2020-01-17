package com.github.yag.ipc.server

import com.github.yag.ipc.ConnectRequest
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap

class Connection(val id: String) {

    lateinit var localAddress: InetSocketAddress
        internal set

    lateinit var remoteAddress: InetSocketAddress
        internal set

    lateinit var connectRequest: ConnectRequest

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