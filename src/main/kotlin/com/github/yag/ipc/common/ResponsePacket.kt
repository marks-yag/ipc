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

import com.github.yag.ipc.protocol.ResponseHeader
import com.github.yag.ipc.protocol.StatusCode
import io.netty.buffer.ByteBuf
import java.net.ConnectException
import java.util.concurrent.TimeoutException

class ResponsePacketHeader(thrift: ResponseHeader = ResponseHeader()) : PacketHeader<ResponseHeader>(thrift, {
    it.contentLength
}, {
    it.callId == -1L
})

fun Packet<ResponseHeader>.status(): StatusCode = header.thrift.statusCode

fun Packet<ResponseHeader>.isSuccessful(): Boolean = status().isSuccessful()

fun Packet<ResponseHeader>.body(): ByteBuf {
    val status = status()
    val data = body.data()
    return if (status.isSuccessful()) {
        data
    } else {
        val bodyArray = data.readArray()
        when (status) {
            StatusCode.NOT_FOUND -> throw UnsupportedOperationException()
            StatusCode.TIMEOUT -> throw TimeoutException()
            StatusCode.CONNECTION_ERROR -> throw ConnectException()
            StatusCode.INTERNAL_ERROR -> throw RemoteException(bodyArray)
            else -> throw IllegalStateException("Impossible status: $status")
        }
    }
}
