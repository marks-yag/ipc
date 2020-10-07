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

import com.github.yag.ipc.client.NonIdempotentRequest
import com.github.yag.ipc.client.client
import com.github.yag.ipc.server.server
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import java.net.ConnectException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.random.Random
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class BasicTest {

    @Test
    fun testRequestMapping() {
        server<String> {
            request {
                map("foo") { request ->
                    request.ok(Unpooled.EMPTY_BUFFER)
                }
            }
        }.use { server ->
            client<String> {
                config {
                    endpoint = server.endpoint
                }
            }.use { client ->
                client.sendSync(NonIdempotentRequest("foo"), PlainBody.empty()).use {
                    assertEquals(StatusCode.OK, it.status())
                }
                client.sendSync(NonIdempotentRequest("non-exist"), PlainBody.empty()).use {
                    assertEquals(StatusCode.NOT_FOUND, it.status())
                }
            }
        }
    }

    @Test
    fun testResponseContent() {
        val requestData = Unpooled.directBuffer().writeBytes("Ping".toByteArray())
        val responseData = Unpooled.directBuffer().writeBytes("Pong".toByteArray())
        server<String> {
            request {
                map("any") { request ->
                    request.ok(responseData.retain())
                }
            }
        }.use { server ->
            client<String> {
                config {
                    endpoint = server.endpoint
                }
            }.use { client ->
                assertEquals(1, requestData.refCnt())
                client.sendSync(NonIdempotentRequest("any"), PlainBody(requestData)).use {
                    requestData.release()

                    assertEquals(StatusCode.OK, it.status())
                    val body = it.body()
                    assertEquals(1, body.refCnt())
                    assertEquals(responseData, body)

                    responseData.release()
                }
                assertEquals(0, requestData.refCnt())
                assertEquals(0, requestData.refCnt())
            }
        }
    }

    @Test
    fun testPartialContent() {
        server<String> {
            request {
                set("foo") { _, request, echo ->
                    repeat(10) {
                        echo(request.status(StatusCode.PARTIAL_CONTENT))
                    }
                    echo(request.ok())
                }
            }
        }.use { server ->
            client<String> {
                config {
                    endpoint = server.endpoint
                }
            }.use { client ->
                val queue = LinkedBlockingQueue<Packet<ResponseHeader>>()
                client.send(NonIdempotentRequest("foo"), PlainBody.empty()) {
                    queue.add(it)
                }

                repeat(10) {
                    queue.take().let {
                        assertEquals(StatusCode.PARTIAL_CONTENT, it.status())
                        assertEquals(1, it.header.thrift.callId)
                    }
                }

                queue.take().let {
                    assertEquals(StatusCode.OK, it.status())
                    assertEquals(1, it.header.thrift.callId)
                }
            }
        }
    }

    @Test
    fun testDefaultExceptionMapping() {
        val inputs = listOf(
            StatusCode.INTERNAL_ERROR to RemoteException::class,
            StatusCode.TIMEOUT to TimeoutException::class,
            StatusCode.NOT_FOUND to UnsupportedOperationException::class,
            StatusCode.CONNECTION_ERROR to ConnectException::class
        )

        for (input in inputs) {
            val len = 5
            val data = Unpooled.wrappedBuffer(ByteArray(len))
            val packet = Packet(ResponsePacketHeader(ResponseHeader(1L, input.first, len)), PlainBody(data))
            assertEquals(input.first, packet.status())
            assertFailsWith(input.second) {
                packet.body()
            }
            assertEquals(0, data.refCnt())
        }
    }

    /**
     * Test requests from single client will be processed in sequence by default.
     */
    @Test
    fun testSequence() {
        val queue = LinkedBlockingQueue<Long>()
        server<String> {
            request {
                map("foo") {
                    queue.put(it.header.thrift.callId)
                    it.ok()
                }
            }
        }.use { server ->
            client<String> {
                config {
                    endpoint = server.endpoint
                }
            }.use { client ->
                val latch = CountDownLatch(10000)
                repeat(10000) {
                    client.send(NonIdempotentRequest("foo"), PlainBody.empty()) {
                        latch.countDown()
                    }
                }

                repeat(10000) {
                    assertEquals(it + 1L, queue.take())
                }

                latch.await()
            }
        }
    }

    /**
     * Test in case of server close before response to client, client can:
     * 1. Detect server was closed.
     * 2. Let pending requests timeout.
     */
    @Test
    fun testServerClose() {
        val server = server<String> {
            request {
                set("ignore") { _, _, _ ->
                    //:~
                }
            }
        }
        client<String> {
            config {
                endpoint = server.endpoint
                heartbeatTimeoutMs = Long.MAX_VALUE
                connectRetry.maxRetries = 0
            }
        }.use { client ->
            assertTrue(client.isConnected())

            val resultFuture = client.send(NonIdempotentRequest("ignore"), PlainBody.empty())
            server.close()

            val result = resultFuture.get(3, TimeUnit.SECONDS)
            assertEquals(StatusCode.TIMEOUT, result.use { it.status() })

            assertFalse(client.isConnected())
        }
    }

}
