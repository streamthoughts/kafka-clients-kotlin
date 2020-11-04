/*
 * Copyright 2020 StreamThoughts.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamthoughts.kafka.tests

import org.apache.curator.test.TestingServer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.net.BindException
import kotlin.jvm.Throws

/**
 * Runs an in-memory, "embedded" instance of a ZooKeeper server.
 */
class TestingEmbeddedZookeeper(private val port: Int = -1) {

    companion object {
        private val Log: Logger = LoggerFactory.getLogger(TestingEmbeddedZookeeper::class.java)
    }

    private lateinit var server: TestingServer

    /**
     * Creates and starts a ZooKeeper instance.
     */
    @Throws(Exception::class)
    fun start() {
        Log.debug("Starting embedded ZooKeeper server")
        System.setProperty("zookeeper.admin.enableServer", "false")
        server = createTestingServer()
        Log.debug(
            "Embedded ZooKeeper server at {} uses the temp directory at {}",
            server.connectString, server.tempDirectory
        )
    }

    @Throws(IOException::class)
    fun stop() {
        Log.debug(
            "Stopping down embedded ZooKeeper server at {} ...",
            server.connectString
        )
        server.close()
        Log.debug(
            "Stopping of embedded ZooKeeper server at {} completed",
            server.connectString
        )
    }

    fun connectString(): String {
        return server.connectString
    }

    @Throws(Exception::class)
    private fun createTestingServer(): TestingServer {
        while (true) {
            try {
                return TestingServer(port)
            } catch (e: BindException) {
                // https://issues.apache.org/jira/browse/CURATOR-535
                Log.warn("Failed to create test ZK node due to known race condition" +
                        " while choosing random available ports. Let's retry")
            }
        }
    }
}