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
import com.github.yag.ipc.client.ThreadContext
import com.github.yag.ipc.client.client
import com.github.yag.ipc.server.Connection
import com.github.yag.ipc.server.RequestHandler
import com.github.yag.ipc.server.server
import com.github.yag.punner.core.eventually
import io.netty.buffer.Unpooled
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class SessionTest {

    @AfterTest
    fun after() {
        assertEquals(0, ThreadContext.cache?.refCnt?:0)
    }

    @Test
    fun testSessionId() {
        var sessionId: String? = null
        server<String> {
            connection {
                add {
                    sessionId = it.sessionId
                }
            }
            request {
                set("foo", object : RequestHandler {
                    override fun handle(
                        connection: Connection,
                        request: Packet<RequestHeader>,
                        echo: (Packet<ResponseHeader>) -> Unit
                    ) {
                        if (connection.sessionId == sessionId) {
                            echo(request.ok())
                        } else {
                            echo(request.status(StatusCode.INTERNAL_ERROR))
                        }
                    }
                })
            }
        }.use { server ->
            client<String>(server.endpoint).use { client ->
                assertNotNull(sessionId)
                assertEquals(sessionId, client.sessionId)

                client.sendSync(NonIdempotentRequest("foo"), PlainBody(Unpooled.EMPTY_BUFFER)).use {
                    assertEquals(StatusCode.OK, it.status())
                }
            }
        }
    }

    @Test
    fun testSessionIdWithReconnect() {
        var sessionIdList = ArrayList<String>()
        server<String> {
            connection {
                add {
                    sessionIdList.add(it.sessionId)
                }
            }
        }.use { server ->
            client<String>(server.endpoint) {
                config {
                    heartbeatTimeoutMs = 1000
                }
            }.use { client ->
                val initConnection = client.getConnection()
                server.ignoreHeartbeat = true
                eventually(2000) {
                    assertNotEquals(initConnection, client.getConnection())
                }

                server.ignoreHeartbeat = false
                eventually(2000) {
                    assertTrue(client.isConnected())
                }
                assertNotEquals(initConnection, client.getConnection())

                assertEquals(2, sessionIdList.size)
                assertEquals(sessionIdList.first(), sessionIdList.last())
            }
        }
    }
}