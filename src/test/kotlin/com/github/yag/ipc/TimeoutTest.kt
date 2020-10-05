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

import com.github.yag.ipc.client.RequestType
import com.github.yag.ipc.client.client
import com.github.yag.ipc.server.server
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class TimeoutTest {

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
    fun testPerRequestTypeTimeout() {
        server<Operation> {
            request {
                set(Operation.FOO) { connection, packet, function ->
                }

                set(Operation.BAR) { connection, packet, function ->
                }
            }
        }.use { server ->
            client<Operation> {
                config {
                    endpoint = server.endpoint
                    requestTimeoutMs = 100L
                }
            }.use { client ->
                val start = System.currentTimeMillis()
                val first = client.send(Operation.FOO, ThriftBody(User("yag", "123")))
                val second = client.send(Operation.BAR, ThriftBody(User("yag", "456")))
                val third = client.send(Operation.FOO, ThriftBody(User("yag", "456"), 500L))

                val thirdResult = third.get()
                val thirdCost = System.currentTimeMillis() - start
                assertEquals(StatusCode.TIMEOUT, thirdResult.status())
                assertTrue(thirdCost in 500L..600L)

                val secondResult = second.get()
                val secondCost = System.currentTimeMillis() - start
                assertEquals(StatusCode.TIMEOUT, secondResult.status())
                assertTrue(secondCost in 1000L..1100L)

                val firstResult = first.get()
                val firstCost = System.currentTimeMillis() - start
                assertEquals(StatusCode.TIMEOUT, firstResult.status())
                assertTrue(firstCost in 2000L..2100L)
            }
        }
    }

}