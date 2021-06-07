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

import ketty.client.NonIdempotentRequest
import ketty.client.ThreadContext
import ketty.client.client
import ketty.protocol.RequestHeader
import ketty.protocol.ResponseHeader
import ketty.protocol.StatusCode
import ketty.server.Connection
import ketty.server.RequestHandler
import ketty.server.server
import com.github.yag.retry.Retry
import io.netty.buffer.Unpooled
import java.time.Duration
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
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
        val sessionIdList = ArrayList<String>()
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
                Retry.duration(Duration.ofSeconds(2), Duration.ofMillis(100)).call {
                    assertNotEquals(initConnection, client.getConnection())
                }

                server.ignoreHeartbeat = false
                Retry.duration(Duration.ofSeconds(2), Duration.ofMillis(100)).call {
                    assertTrue(client.isConnected())
                }
                assertNotEquals(initConnection, client.getConnection())

                assertEquals(2, sessionIdList.size)
                assertEquals(sessionIdList.first(), sessionIdList.last())
            }
        }
    }
}
