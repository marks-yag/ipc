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
import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.PooledByteBufAllocator
import org.apache.thrift.TSerializable

class ThriftBody @JvmOverloads constructor(private val obj: TSerializable, allocator: ByteBufAllocator = PooledByteBufAllocator.DEFAULT) : Body, AutoCloseable {

    private val buf = TEncoder.encode(obj, allocator.buffer())

    override fun data(): ByteBuf {
        return buf
    }

    override fun close() {
        buf.release()
    }

    override fun toString(): String {
        return obj.toString()
    }

}