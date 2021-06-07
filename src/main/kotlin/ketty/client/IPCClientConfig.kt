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

package ketty.client

import com.github.yag.config.Value
import ketty.common.ChannelConfig
import com.github.yag.retry.CountDownRetryPolicy
import com.github.yag.retry.ExponentialBackOffPolicy
import java.util.TreeMap

class IPCClientConfig {

    @Value
    var maxResponsePacketSize: Int = 1024 * 1024 * 10

    @Value
    var heartbeatIntervalMs: Long = 1000

    @Value
    var requestTimeoutMs: Long = 0

    @Value
    var heartbeatTimeoutMs: Long = 10_000

    @Value
    var channel = ChannelConfig()

    @Value
    var headers = TreeMap<String, String>()

    @Value
    var connectRetry = CountDownRetryPolicy()

    @Value
    var connectBackOff = ExponentialBackOffPolicy()

    @Value
    var backOffRandomRange = 0.2

}
