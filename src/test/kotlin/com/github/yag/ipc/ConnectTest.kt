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

import com.github.yag.ipc.client.IPCClient
import com.github.yag.ipc.client.NonIdempotentRequest
import com.github.yag.ipc.client.client
import com.github.yag.ipc.server.server
import com.google.common.util.concurrent.SettableFuture
import io.netty.buffer.Unpooled
import java.net.ConnectException
import kotlin.concurrent.thread
import kotlin.system.measureTimeMillis
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.fail

class ConnectTest {

    @Test
    fun testConnectionHandler() {
        server<String> {
            connection {
                add {
                    if (it.remoteAddress.hostString != "127.0.0.1") {
                        throw ConnectionRejectException("${it.remoteAddress.hostString} not allowed.")
                    }
                }
                add {
                    if (!it.connectRequest.isSetHeaders || !it.connectRequest.headers.containsKey("token")) {
                        throw ConnectionRejectException("A valid token was required.")
                    }
                }
            }
        }.use { server ->
            try {
                client<String> {
                    config {
                        endpoint = server.endpoint
                        connectRetry.maxRetries = 0
                    }
                }
                fail()
            } catch (e: ConnectionRejectException) {
            }

            client<String> {
                config {
                    endpoint = server.endpoint
                    headers["token"] = "foo"
                }
            }.use { client ->
                client.sendSync(NonIdempotentRequest("foo"), PlainBody(Unpooled.EMPTY_BUFFER)).let {
                    assertEquals(StatusCode.NOT_FOUND, it.status())
                    assertFailsWith(UnsupportedOperationException::class) {
                        it.body()
                    }
                }
            }
        }
    }

    @Test
    fun testPrompt() {
        server<String> {
            prompt {
                "hello".toByteArray(Charsets.UTF_8)
            }
            connection {
                add {
                    assertEquals("hello", it.promptData.toString(Charsets.UTF_8))
                    val body = ByteArray(5)
                    it.connectRequest.body.get(body)
                    assertEquals("world", body.toString(Charsets.UTF_8))
                }
            }
        }.use { server ->
            client<String> {
                prompt {
                    val body = ByteArray(5)
                    it.body.get(body)
                    assertEquals("hello", body.toString(Charsets.UTF_8))
                    "world".toByteArray(Charsets.UTF_8)
                }
                config {
                    endpoint = server.endpoint
                }
            }.use { client ->
                client.sendSync(NonIdempotentRequest("foo"), PlainBody(Unpooled.EMPTY_BUFFER)).let {
                    assertEquals(StatusCode.NOT_FOUND, it.status())
                    assertFailsWith(UnsupportedOperationException::class) {
                        it.body()
                    }
                }
            }
        }
    }

    @Test
    fun testConnectTimeout() {
        val server = server<String> {
        }.also {
            it.close()
        }

        val cost = measureTimeMillis {
            assertFailsWith<ConnectException> {
                client<String> {
                    config {
                        endpoint = server.endpoint
                        channel.connectionTimeoutMs = 1000
                        connectRetry.maxRetries = 1
                        connectRetry.maxTimeElapsedMs = 5000
                    }
                }.close()
            }
        }

        assertTrue(cost < 10000, "Cost is ${cost}ms.")
    }

    @Test
    fun testConnectTimeoutWithInterrupt() {
        val server = server<String> {
        }.also {
            it.close()
        }

        val client = SettableFuture.create<IPCClient<String>>()

        val thread = thread {
            assertFailsWith<ConnectException> {
                client.set(client {
                    config {
                        endpoint = server.endpoint
                    }
                })
            }
        }

        Thread.sleep(1000)
        thread.interrupt()
        thread.join()

        assertFalse(client.isDone)
    }

}