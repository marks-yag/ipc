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
import com.github.yag.ipc.client.ThreadContext
import com.github.yag.ipc.client.client
import com.github.yag.ipc.server.server
import io.netty.buffer.Unpooled
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull

class ThreadContextTest {

    @AfterTest
    fun after() {
        assertEquals(0, ThreadContext.cache?.refCnt?:0)
        System.gc()
    }

    @Test
    fun testReferenceCount() {
        val tc = ThreadContext.getDefault()
        assertEquals(ThreadContext.cache, tc)
        assertEquals(1, tc.refCnt)
        assertEquals(2, tc.retain().refCnt)
        assertEquals(1, tc.release().refCnt)
        assertEquals(0, tc.release().refCnt)

        assertFailsWith<IllegalStateException> {
            tc.retain()
        }

        assertFailsWith<IllegalStateException> {
            tc.release()
        }
    }

    @Test
    fun testCreateClientsAndCloseOneByOne() {
        server<String> {
            request {
                set("foo") { _, _, _ ->
                }
            }
        }.use { server ->
            val results = Array(100) {
                client<String>(server.endpoint) {
                    config {
                        requestTimeoutMs = 2000
                        connectRetry.maxTimeElapsedMs = 3000
                    }
                }.use {
                    it.send(IdempotentRequest("foo"), PlainBody(Unpooled.EMPTY_BUFFER))
                }.also {
                    val tc = ThreadContext.cache
                    assertNotNull(tc)
                    assertEquals(0, tc.refCnt)
                }
            }

            results.forEach {
                assertEquals(StatusCode.TIMEOUT, it.get().use {
                    it.status()
                })
            }
        }
    }

    @Test
    fun testCreateClientsOneByOne() {
        server<String> {
            request {
                set("foo") { _, _, _ ->
                }
            }
        }.use { server ->
            val results = Array(100) {
                client<String>(server.endpoint) {
                    config {
                        requestTimeoutMs = 2000
                        connectRetry.maxTimeElapsedMs = 3000
                    }
                }.let {
                    it to it.send(IdempotentRequest("foo"), PlainBody(Unpooled.EMPTY_BUFFER))
                }
            }

            results.forEach {
                assertEquals(StatusCode.TIMEOUT, it.second.get().use {
                    it.status()
                })
            }

            val tc = ThreadContext.cache
            assertNotNull(tc)
            assertEquals(100, tc.refCnt)

            results.forEach {
                it.first.close()
            }

            assertNotNull(tc)
            assertEquals(0, tc.refCnt)
        }
    }
}