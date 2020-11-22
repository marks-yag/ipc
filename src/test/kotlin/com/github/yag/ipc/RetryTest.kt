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

import com.github.yag.ipc.client.IdempotentRequest
import com.github.yag.ipc.client.NonIdempotentRequest
import com.github.yag.ipc.client.ThreadContext
import com.github.yag.ipc.client.client
import com.github.yag.ipc.server.server
import com.github.yag.punner.core.eventually
import io.netty.buffer.Unpooled
import org.slf4j.LoggerFactory
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class RetryTest {

    @AfterTest
    fun after() {
        assertEquals(0, ThreadContext.cache?.refCnt?:0)
        System.gc()
    }

    /**
     * Test client can reconnect to server and make remote calls.
     */
    @Test
    fun testIdempotentCallRetrySuccessWhenClientReconnect() {
        server<String> {
            request {
                set("foo") { _, request, echo ->
                    echo(request.ok())
                }
            }
        }.use { server ->
            client<String>(server.endpoint) {
                config {
                    requestTimeoutMs = 4000
                }
            }.use { client ->
                server.brokenOnRequest = true
                server.ignoreNewConnection = true

                val initConnection = client.getConnection()

                val idempotentBody = PlainBody(Unpooled.directBuffer())
                val idempotentRequest = client.send(IdempotentRequest("foo"), idempotentBody)

                eventually(1000) {
                    assertFalse(client.isConnected())
                }

                server.brokenOnRequest = false
                server.ignoreNewConnection = false
                LOG.info("Server recovered.")

                eventually(5000) {
                    assertTrue(client.isConnected())
                    assertNotEquals(initConnection, client.getConnection())
                }

                assertEquals(StatusCode.OK, idempotentRequest.get().use {
                    it.status()
                })

                idempotentBody.data().use {
                    assertEquals(1, it.refCnt())
                }
            }
        }
    }

    /**
     * Test client can reconnect to server and make remote calls.
     */
    @Test
    fun testNonIdempotentCallDoNotRetryWhenClientReconnect() {
        server<String> {
            request {
                set("foo") { _, request, echo ->
                    echo(request.ok())
                }
            }
        }.use { server ->
            client<String>(server.endpoint) {
                config {
                    requestTimeoutMs = 4000
                }
            }.use { client ->
                server.brokenOnRequest = true
                server.ignoreNewConnection = true

                val initConnection = client.getConnection()

                val idempotentBody = PlainBody(Unpooled.directBuffer())
                val idempotentRequest = client.send(NonIdempotentRequest("foo"), idempotentBody)

                eventually(1000) {
                    assertFalse(client.isConnected())
                }

                server.brokenOnRequest = false
                server.ignoreNewConnection = false
                LOG.info("Server recovered.")

                eventually(5000) {
                    assertTrue(client.isConnected())
                    assertNotEquals(initConnection, client.getConnection())
                }

                assertEquals(StatusCode.CONNECTION_ERROR, idempotentRequest.get().use {
                    it.status()
                })

                idempotentBody.data().use {
                    assertEquals(1, it.refCnt())
                }
            }
        }
    }

    @Test
    fun testCallTimeoutWhenClientReconnectFailed() {
        server<String> {
            request {
                set("foo") { _, _, _ ->
                }
            }
        }.use { server ->
            client<String>(server.endpoint) {
                config {
                    requestTimeoutMs = 2000
                    connectRetry.maxTimeElapsedMs = 3000
                }
            }.use { client ->
                val idempotentRequest = client.send(IdempotentRequest("foo"), PlainBody(Unpooled.EMPTY_BUFFER))
                server.close()

                assertEquals(StatusCode.TIMEOUT, idempotentRequest.get().use {
                    it.status()
                })
            }
        }
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(RetryTest::class.java)
    }

}