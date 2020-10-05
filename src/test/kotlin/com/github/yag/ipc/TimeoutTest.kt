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
import kotlin.test.Test
import kotlin.test.assertEquals

class TimeoutTest {

    @Test
    fun testPerRequestTimeout() {
        server<String> {
            request {
                set("foo") { connection, packet, function ->
                }

                map("bar") { request ->
                    request.ok()
                }
            }
        }.use { server ->
            client<String> {
                config {
                    endpoint = server.endpoint
                }
            }.use { client ->
                client.sendSync(NonIdempotentRequest("foo"), ThriftBody(User("yag", "123"), 1000)).let {
                    assertEquals(StatusCode.TIMEOUT, it.status())
                }
                client.sendSync(NonIdempotentRequest("bar"), ThriftBody(User("yag", "123"), 1000)).let {
                    assertEquals(StatusCode.OK, it.status())
                }
            }
        }
    }

    enum class Operation(private val timeoutMs: Long) : RequestType<Operation> {
        FOO(1000L),
        BAR(2000L);

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

                map(Operation.BAR) { request ->
                    request.ok()
                }
            }
        }.use { server ->
            client<Operation> {
                config {
                    endpoint = server.endpoint
                }
            }.use { client ->
                client.sendSync(Operation.FOO, ThriftBody(User("yag", "123"))).let {
                    assertEquals(StatusCode.TIMEOUT, it.status())
                }
                client.sendSync(Operation.BAR, ThriftBody(User("yag", "123"))).let {
                    assertEquals(StatusCode.OK, it.status())
                }
            }
        }
    }
}