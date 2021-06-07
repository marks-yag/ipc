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

package ketty.common

import ketty.client.IPCClient
import ketty.client.NonIdempotentRequest
import ketty.client.ThreadContext
import ketty.client.client
import ketty.protocol.StatusCode
import ketty.server.server
import io.netty.buffer.Unpooled
import org.slf4j.LoggerFactory
import java.net.ConnectException
import java.util.concurrent.CompletableFuture
import kotlin.concurrent.thread
import kotlin.system.measureTimeMillis
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.fail

class ConnectTest {

    @AfterTest
    fun after() {
        assertEquals(0, ThreadContext.cache?.refCnt?:0)
    }

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
                    if (!it.headers.containsKey("token")) {
                        throw ConnectionRejectException("A valid token was required.")
                    }
                }
            }
        }.use { server ->
            try {
                client<String>(server.endpoint) {
                    config {
                        connectRetry.maxRetries = 0
                    }
                }
                fail()
            } catch (e: ConnectionRejectException) {
            }

            client<String>(server.endpoint) {
                config {
                    headers["token"] = "foo"
                }
            }.use { client ->
                client.sendSync(NonIdempotentRequest("foo"), PlainBody(Unpooled.EMPTY_BUFFER)).use {
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
                    val body = it.requestBody
                    assertEquals("world", body.toString(Charsets.UTF_8))
                }
            }
        }.use { server ->
            client<String>(server.endpoint) {
                prompt {
                    val body = ByteArray(5)
                    it.body.get(body)
                    assertEquals("hello", body.toString(Charsets.UTF_8))
                    "world".toByteArray(Charsets.UTF_8)
                }
            }.use { client ->
                client.sendSync(NonIdempotentRequest("foo"), PlainBody(Unpooled.EMPTY_BUFFER)).use {
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
                client<String>(server.endpoint) {
                    config {
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

        val client = CompletableFuture<IPCClient<String>>()

        val thread = thread {
            client.complete(client(server.endpoint))
        }

        Thread.sleep(5000)
        assertFalse(client.isDone)

        LOG.debug("Try to interrupt client.")
        thread.interrupt()
        thread.join()

        assertFalse(client.isDone)
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(ConnectTest::class.java)
    }

}
