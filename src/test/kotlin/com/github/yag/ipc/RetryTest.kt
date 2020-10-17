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
import com.github.yag.ipc.client.client
import com.github.yag.ipc.server.server
import com.github.yag.punner.core.eventually
import io.netty.buffer.Unpooled
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import kotlin.concurrent.thread
import kotlin.test.Ignore
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class RetryTest {

    /**
     * Test client can reconnect to server and make remote calls.
     */
    @Test
    fun testCallRetrySuccessWhenClientReconnect() {
        var ignore = true
        server<String> {
            request {
                set("foo") { _, request, echo ->
                    if (!ignore) {
                        echo(request.ok())
                    }
                }
            }
        }.use { server ->
            server.ignoreHeartbeat = true

            client<String>(server.endpoint) {
                config {
                    heartbeatIntervalMs = 500
                    heartbeatTimeoutMs = 1000

                    requestTimeoutMs = 4000
                }
            }.use { client ->
                val initConnection = client.getConnection()

                val idempotentRequest = client.send(IdempotentRequest("foo"), PlainBody(Unpooled.EMPTY_BUFFER))
                val nonIdempotentRequest = client.send(NonIdempotentRequest("foo"), PlainBody(Unpooled.EMPTY_BUFFER))

                assertEquals(StatusCode.CONNECTION_ERROR, nonIdempotentRequest.get().status())

                eventually(2000) {
                    assertFalse(client.isConnected())
                }

                server.ignoreHeartbeat = false
                ignore = false

                eventually(2000) {
                    assertTrue(client.isConnected())
                }

                assertNotEquals(initConnection, client.getConnection())

                assertEquals(StatusCode.OK, idempotentRequest.get().status())
            }
        }
    }

    @Test
    fun testCallTimeoutWhenClientReconnectFailed() {
        server<String> {
            request {
                set("foo") { _, request, echo ->
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

                assertEquals(StatusCode.TIMEOUT, idempotentRequest.get().status())
            }
        }
    }

}