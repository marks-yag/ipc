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

import com.github.yag.ipc.ConnectionRejectException
import java.net.InetSocketAddress

class RemoteAddressFilter(private val whiteList: Set<String>, private val blackList: Set<String>) : ConnectionHandler {

    override fun handle(connection: Connection) {
        val address = connection.remoteAddress
        check(address is InetSocketAddress)
        val remote = address.hostString
        if (!whiteList.contains(remote) || blackList.contains(remote)) {
            throw ConnectionRejectException("$remote not allowed.")
        }
    }
}