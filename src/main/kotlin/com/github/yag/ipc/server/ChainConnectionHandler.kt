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

import com.github.yag.ipc.common.ConnectionRejectException

class ChainConnectionHandler : ConnectionHandler {

    private val chain = ArrayList<ConnectionHandler>()

    fun add(handler: ConnectionHandler) {
        chain.add(handler)
    }

    fun add(handler: (Connection) -> Unit) {
        add(object : ConnectionHandler {
            override fun handle(connection: Connection) {
                handler(connection)
            }
        })
    }

    fun remove(handler: ConnectionHandler) {
        chain.remove(handler)
    }

    @Throws(ConnectionRejectException::class)
    override fun handle(connection: Connection) {
        return chain.forEach { it.handle(connection) }
    }

}
