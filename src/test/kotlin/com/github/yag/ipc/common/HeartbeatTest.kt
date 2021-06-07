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

import ketty.client.IdempotentRequest
import ketty.client.NonIdempotentRequest
import ketty.client.ThreadContext
import ketty.client.client
import ketty.protocol.StatusCode
import ketty.server.server
import com.github.yag.retry.Retry
import io.netty.buffer.Unpooled
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals

class HeartbeatTest {

    @AfterTest
    fun after() {
        assertEquals(0, ThreadContext.cache?.refCnt?:0)
    }

    /**
     * Test in case of client can not write data in time:
     * 1. server can detect read timeout and kick client out.
     * 2. client can detect connection was closed
     */
    @Test
    fun testServerSideHeartbeatTimeout() {
        server<String> {
            config {
                maxIdleTimeMs = 1000
            }
        }.use { server ->
            client<String>(server.endpoint) {
                config {
                    heartbeatIntervalMs = 2000
                    heartbeatTimeoutMs = 10000
                    connectRetry.maxRetries = 0
                }
            }.use { client ->
                val initConnection = client.getConnection()
                Retry.duration(Duration.ofMillis(3000), Duration.ofSeconds(1)).call {
                    assertNotEquals(initConnection, client.getConnection())
                }
            }
        }
    }

    /**
     * Test if client do dot send requests, heartbeat will be send automatically to keep connection alive.
     */
    @Test
    fun testClientSideHeartbeat() {
        server<String> {
            config {
                maxIdleTimeMs = 2000
            }
        }.use { server ->
            client<String>(server.endpoint) {
                config {
                    heartbeatIntervalMs = 500
                    heartbeatTimeoutMs = 1000
                }
            }.use { client ->
                repeat(10) {
                    assertEquals(StatusCode.NOT_FOUND, client.sendSync(NonIdempotentRequest("any"), PlainBody(Unpooled.EMPTY_BUFFER)).use { it.status() })
                    Thread.sleep(200)
                }

                Thread.sleep(3000)
                assertEquals(StatusCode.NOT_FOUND, client.sendSync(NonIdempotentRequest("any"), PlainBody(Unpooled.EMPTY_BUFFER)).use { it.status() })
            }
        }
    }

    /**
     * Test in case of server can not response heartbeat(write data) in time, client can detect heartbeat timeout.
     */
    @Test
    fun testClientSideHeartbeatTimeout() {
        server<String> {
        }.use { server ->
            server.ignoreHeartbeat = true
            client<String>(server.endpoint) {
                config {
                    heartbeatIntervalMs = 500
                    heartbeatTimeoutMs = 2000
                    connectRetry.maxRetries = 0
                }
            }.use { client ->
                val initConnection = client.getConnection()
                Retry.duration(Duration.ofMillis(3000), Duration.ofSeconds(1)).call {
                    assertNotEquals(initConnection, client.getConnection())
                }
            }
        }
    }

    @Test
    fun testHeartbeatBlockedByHandler() {
        var block = true
        server<String> {
            request {
                map("foo") {
                    if (block) {
                        Thread.sleep(Long.MAX_VALUE)
                    }
                    it.ok()
                }
            }
        }.use { server ->
            client<String>(server.endpoint) {
                config {
                    heartbeatIntervalMs = 500
                    heartbeatTimeoutMs = 2000
                    connectRetry.maxRetries = 0
                }
            }.use { client ->
                val initConnection = client.getConnection()
                val data = Unpooled.directBuffer().writeBytes("hello".toByteArray())
                val response = data.use {
                    client.send(IdempotentRequest("foo"), PlainBody(data))
                }

                Retry.duration(Duration.ofMillis(5000), Duration.ofSeconds(1)).call {
                    assertNotEquals(initConnection, client.getConnection())
                }

                assertFalse(response.isDone)

                block = false

                assertEquals(StatusCode.OK, response.get(3, TimeUnit.SECONDS).use {
                    it.status()
                })
            }
        }
    }

}
