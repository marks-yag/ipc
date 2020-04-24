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

import com.github.yag.config.Value

class ChannelConfig {

    @Value
    var connectionTimeoutMs: Long = 20_000L

    @Value
    var tcpNoDelay: Boolean = false

    @Value
    var recvBufSize: Int = 1024 * 1024

    @Value
    var sendBufSize: Int = 1024 * 1024

    @Value
    var watermarkHigh: Int = 1024 * 1024 * 16

    @Value
    var watermarkLow: Int = 1024 * 1024 * 8

}