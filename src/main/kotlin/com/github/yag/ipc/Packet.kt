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

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import org.apache.thrift.TSerializable

class Packet<T : TSerializable>(val header: PacketHeader<T>, val body: ByteBuf) {

    fun isHeartbeat() = header.isHeartbeat(header.thrift)

    companion object {

        val requestHeartbeat = Packet(RequestPacketHeader(RequestHeader(-1, "", 0)), Unpooled.EMPTY_BUFFER)

        val responseHeartbeat = status(-1, StatusCode.OK)

    }

}




