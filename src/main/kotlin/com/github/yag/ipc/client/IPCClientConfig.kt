/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.github.yag.ipc.client

import com.github.yag.config.Value
import com.github.yag.ipc.ChannelConfig
import com.github.yag.retry.CountDownRetryPolicy
import com.github.yag.retry.ExponentialBackOffPolicy
import java.net.InetSocketAddress
import java.util.TreeMap

class IPCClientConfig {

    @Value(required = true)
    lateinit var endpoint: InetSocketAddress

    @Value
    var threads: Int = 4

    @Value
    var maxResponsePacketSize: Int = 1024 * 1024 * 10

    @Value
    var heartbeatIntervalMs: Long = 1000

    @Value
    var requestTimeoutMs: Long = Long.MAX_VALUE

    @Value
    var heartbeatTimeoutMs: Long = 10_000

    @Value
    var maxParallelCalls: Int = 128

    @Value
    var maxParallelRequestContentSize: Int = 1024 * 1024 * 16

    @Value
    var maxWriteBatchSize: Int = 8192

    @Value
    var channelConfig = ChannelConfig()

    @Value
    var headers = TreeMap<String, String>()

    @Value
    var connectRetry = CountDownRetryPolicy()

    @Value
    var connectBackOff = ExponentialBackOffPolicy()

}