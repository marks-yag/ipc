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
import com.github.yag.ipc.client.RequestType
import com.github.yag.ipc.client.client
import com.github.yag.ipc.server.server
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class RequestTimeoutTest {

    enum class Operation(private val timeoutMs: Long) : RequestType<Operation> {
        FOO(2000L),
        BAR(1000L);

        override fun getName(): Operation {
            return this
        }

        override fun timeoutMs(): Long? {
            return timeoutMs
        }
    }

    @Test
    fun testPerRequestTypeTimeout() = server<Operation> {
        request {
            set(Operation.FOO) { _, _, _ ->
            }

            set(Operation.BAR) { _, _, _ ->
            }
        }
    }.use { server ->
        client<Operation>(server.endpoint) {
            config {
                requestTimeoutMs = 100L
            }
        }.use { client ->
            val start = System.currentTimeMillis()
            val first = client.send(Operation.FOO, ThriftBody(User("yag", "123")))
            val second = client.send(Operation.BAR, ThriftBody(User("yag", "456")))

            val secondResult = second.get()
            val secondCost = System.currentTimeMillis() - start
            assertEquals(StatusCode.TIMEOUT, secondResult.status())
            assertTrue(secondCost in 1000L..1200L, "Cost $secondCost")

            val firstResult = first.get()
            val firstCost = System.currentTimeMillis() - start
            assertEquals(StatusCode.TIMEOUT, firstResult.status())
            assertTrue(firstCost in 2000L..2200L, "Cost $firstCost")
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
        client<String>(server.endpoint) {
            config {
                requestTimeoutMs = Long.MAX_VALUE
            }
        }.use { client ->
            assertTrue(client.isConnected())

            val resultFuture = client.send(NonIdempotentRequest("ignore"), PlainBody.EMPTY)
            server.close()

            val result = resultFuture.get(3, TimeUnit.SECONDS)
            assertEquals(StatusCode.CONNECTION_ERROR, result.use { it.status() })

            assertFalse(client.isConnected())
        }
    }

}