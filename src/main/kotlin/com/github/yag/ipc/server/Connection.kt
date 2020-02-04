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

import com.github.yag.ipc.ConnectRequest
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap

class Connection(val id: String) {

    lateinit var localAddress: InetSocketAddress
        internal set

    lateinit var remoteAddress: InetSocketAddress
        internal set

    lateinit var connectRequest: ConnectRequest

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