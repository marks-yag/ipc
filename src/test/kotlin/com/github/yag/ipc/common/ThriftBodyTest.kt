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

import ketty.User
import ketty.client.NonIdempotentRequest
import ketty.client.ThreadContext
import ketty.client.client
import ketty.protocol.StatusCode
import ketty.server.server
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals

class ThriftBodyTest {

    @AfterTest
    fun after() {
        assertEquals(0, ThreadContext.cache?.refCnt?:0)
    }

    @Test
    fun test() {
        server<String> {
            request {
                map("foo") { request ->
                    request.ok()
                }
            }
        }.use { server ->
            client<String>(server.endpoint).use { client ->
                val body = ThriftBody(User("yag", "123"))
                client.sendSync(NonIdempotentRequest("foo"), body).let {
                    assertEquals(StatusCode.OK, it.status())
                    it.body().release()
                }
                assertEquals(0, body.data().refCnt())
            }
        }
    }
}
