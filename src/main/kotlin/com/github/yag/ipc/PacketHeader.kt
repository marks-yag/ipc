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
import org.apache.thrift.TSerializable

open class PacketHeader<T : TSerializable>(val thrift: T, val length: (T) -> Int, val isHeartbeat: (T) -> Boolean) {

    fun packet(body: ByteBuf): Packet<T> {
        return Packet(this, PlainBody(body))
    }

    override fun toString(): String {
        return thrift.toString()
    }

}