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
import java.nio.ByteBuffer


data class PlainBody(private val body: ByteBuf) : Body {

    constructor(array: ByteArray) : this(Unpooled.wrappedBuffer(array))

    constructor(buf: ByteBuffer) : this(Unpooled.wrappedBuffer(buf))

    override fun data(): ByteBuf {
        return body
    }

    companion object {
        @JvmStatic
        val EMPTY = PlainBody(Unpooled.EMPTY_BUFFER)
    }

}
