package com.github.yag.ipc

import com.github.yag.ipc.client.IPCClient
import com.github.yag.ipc.client.client
import com.github.yag.ipc.server.server
import com.github.yag.punner.core.eventually
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.fail

class IPCTest {

    private lateinit var requestData: ByteBuf

    private lateinit var responseData: ByteBuf


    @BeforeTest
    fun before() {
        requestData = Unpooled.directBuffer().writeBytes("Ping".toByteArray())
        responseData = Unpooled.directBuffer().writeBytes("Pong".toByteArray())
    }

    @AfterTest
    fun after() {
        assertEquals(1, requestData.refCnt())
        requestData.release()
        assertEquals(1, responseData.refCnt())
        responseData.release()
    }

    @Test
    fun testConnectionHandler() {
        server {
            connection {
                add {
                    if (it.remoteAddress.hostString != "127.0.0.1") {
                        throw ConnectionRejectException("${it.remoteAddress.hostString} not allowed.")
                    }
                }
                add {
                    if (!it.connectRequest.isSetHeaders || !it.connectRequest.headers.containsKey("token")) {
                        throw ConnectionRejectException("A valid token was required.")
                    }
                }
            }
        }.use { server ->
            try {
                client {
                    endpoint = server.endpoint
                }
                fail()
            } catch (e: ConnectionRejectException) {
            }

            client {
                endpoint = server.endpoint
                headers["token"] = "foo"
            }.use { client ->
                client.sendSync("foo", requestData).let {
                    assertEquals(StatusCode.NOT_FOUND, it.status())
                }
            }
        }
    }

    @Test
    fun testPingPong() {
        server {
            request {
                map("foo") { request ->
                    request.ok(responseData.retain())
                }
            }
        }.use { server ->
            client {
                endpoint = server.endpoint
            }.use { client ->
                doTest(client)
            }
        }
    }

    private fun doTest(client: IPCClient<String>) {
        client.sendSync("foo", requestData).let {
            assertEquals(StatusCode.OK, it.status())
            it.body.release()
        }
        client.sendSync("not-exist", requestData).let {
            assertEquals(StatusCode.NOT_FOUND, it.status())
            it.body.release()
        }
    }

    @Test
    fun testResponseContent() {
        server {
            request {
                map("any") { request ->
                    request.ok(responseData.retain())
                }
            }
        }.use { server ->
            client {
                endpoint = server.endpoint
            }.use { client ->
                assertEquals(1, requestData.refCnt())
                client.sendSync("any", requestData).let {
                    assertEquals(StatusCode.OK, it.status())
                    assertEquals(1, it.body.refCnt())
                    assertEquals(responseData, it.body)
                    it.body.release()
                }
                assertEquals(1, requestData.refCnt())
            }
        }
    }

    @Test
    fun testPartialContent() {
        server {
            request {
                set("foo") { _, request, echo ->
                    repeat(10) {
                        echo(request.status(StatusCode.PARTIAL_CONTENT))
                    }
                    echo(request.ok())
                }
            }
        }.use { server ->
            client {
                endpoint = server.endpoint
            }.use { client ->
                val queue = LinkedBlockingQueue<Packet<ResponseHeader>>()
                client.send("foo", requestData) {
                    queue.add(it)
                }

                repeat(10) {
                    queue.take().let {
                        assertEquals(StatusCode.PARTIAL_CONTENT, it.status())
                        assertEquals(1, it.header.thrift.callId)
                    }
                }

                queue.take().let {
                    assertEquals(StatusCode.OK, it.status())
                    assertEquals(1, it.header.thrift.callId)
                }
            }
        }
    }

    @Test
    fun testDefaultExceptionMapping() {
        server {
            request {
                map("foo") {
                    throw IllegalArgumentException()
                }
                map("bar") {
                    throw NullPointerException()
                }
            }
        }.use { server ->
            client {
                endpoint = server.endpoint
            }.use { client ->
                assertEquals(StatusCode.INTERNAL_ERROR, client.sendSync("foo", requestData).status())
                assertEquals(StatusCode.INTERNAL_ERROR, client.sendSync("bar", requestData).status())
            }
        }
    }

    /**
     * Test requests from single client will be processed in sequence by default.
     */
    @Test
    fun testSequence() {
        val queue = LinkedBlockingQueue<Long>()
        server {
            request {
                map("foo") {
                    queue.put(it.header.thrift.callId)
                    it.ok()
                }
            }
        }.use { server ->
            client {
                endpoint = server.endpoint
            }.use { client ->
                val latch = CountDownLatch(10000)
                repeat(10000) {
                    client.send("foo", requestData) {
                        latch.countDown()
                    }
                }

                repeat(10000) {
                    assertEquals(it + 1L, queue.take())
                }

                latch.await()
            }
        }
    }

    /**
     * Test in case of server close before response to client, client can:
     * 1. Detect server was closed.
     * 2. Let pending requests timeout.
     */
    @Test
    fun testServerClose() {
        val server = server {
            request {
                set("ignore") { _, _, _ ->
                    //:~
                }
            }
        }
        client {
            endpoint = server.endpoint
            heartbeatTimeoutMs = Long.MAX_VALUE
        }.use { client ->
            assertTrue(client.isConnected())

            val resultFuture = client.send("ignore", requestData)
            server.close()

            val result = resultFuture.get(3, TimeUnit.SECONDS)
            assertEquals(StatusCode.TIMEOUT, result.status())

            assertFalse(client.isConnected())
        }
    }

    /**
     * Test in case of server can not response heartbeat(write data) in time, client can detect heartbeat timeout.
     */
    @Test
    fun testClientSideHeartbeatTimeout() {
        server {
        }.use { server ->
            server.ignoreHeartbeat = true
            client {
                endpoint = server.endpoint
                heartbeatIntervalMs = 500
                heartbeatTimeoutMs = 2000
            }.use { client ->
                eventually(3000) {
                    assertFalse(client.isConnected())
                }
            }
        }
    }

    /**
     * Test in case of client can not write data in time:
     * 1. server can detect read timeout and kick client out.
     * 2. client can detect connection was closed
     */
    @Test
    fun testServerSideHeartbeatTimeout() {
        server {
            config {
                maxIdleTimeMs = 1000
            }
        }.use { server ->
            client {
                endpoint = server.endpoint
                heartbeatIntervalMs = 2000
                heartbeatTimeoutMs = 10000
            }.use { client ->
                eventually(3000) {
                    assertFalse(client.isConnected())
                }
            }
        }
    }

    /**
     * Test if client do dot send requests, heartbeat will be send automatically to keep connection alive.
     */
    @Test
    fun testClientSideHeartbeat() {
        server {
            config {
                maxIdleTimeMs = 2000
            }
        }.use { server ->
            client {
                endpoint = server.endpoint
                heartbeatIntervalMs = 500
                heartbeatTimeoutMs = 1000
            }.use { client ->
                repeat(10) {
                    assertEquals(StatusCode.NOT_FOUND, client.sendSync("any", requestData).status())
                    Thread.sleep(200)
                }

                Thread.sleep(3000)
                assertEquals(StatusCode.NOT_FOUND, client.sendSync("any", requestData).status())
            }
        }
    }

    /**
     * Test client can reconnect to server and make remote calls.
     */
    @Test
    fun testClientReconnect() {
        server {
        }.use { server ->
            server.ignoreHeartbeat = true
            thread {
                Thread.sleep(2000)
                server.ignoreHeartbeat = false
            }

            client {
                endpoint = server.endpoint
                heartbeatIntervalMs = 500
                heartbeatTimeoutMs = 1000

                requestTimeoutMs = 2000

                reconnectDelayMs = 3000
            }.use { client ->
                eventually(2000) {
                    assertFalse(client.isConnected())
                }

                assertEquals(StatusCode.CONNECTION_ERROR, client.sendSync("any", requestData).status())
                assertEquals(1, requestData.refCnt())

                eventually(5000) {
                    assertTrue(client.isConnected())
                }

                assertEquals(StatusCode.NOT_FOUND, client.sendSync("any", requestData).status())
                assertEquals(1, requestData.refCnt())

                Thread.sleep(5000)

                assertEquals(StatusCode.NOT_FOUND, client.sendSync("any", requestData).status())
                assertEquals(1, requestData.refCnt())
            }
        }
    }

}
