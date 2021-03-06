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

package com.github.yag.ipc.common

import com.github.yag.ipc.protocol.RequestHeader
import com.github.yag.ipc.protocol.StatusCode
import io.netty.buffer.Unpooled
import org.apache.thrift.TSerializable

data class Packet<T : TSerializable>(val header: PacketHeader<T>, val body: Body) : AutoCloseable {

    internal fun isHeartbeat() = header.isHeartbeat(header.thrift)

    override fun close() {
        body.data().release()
    }

    companion object {

        internal val requestHeartbeat = Packet(RequestPacketHeader(RequestHeader(-1, "", 0)), PlainBody(Unpooled.EMPTY_BUFFER))

        internal val responseHeartbeat = status(-1, StatusCode.OK)

    }

}




