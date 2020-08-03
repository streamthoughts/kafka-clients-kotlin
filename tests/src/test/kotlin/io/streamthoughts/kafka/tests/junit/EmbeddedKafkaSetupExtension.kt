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
package io.streamthoughts.kafka.tests.junit

import io.streamthoughts.kafka.tests.TestingEmbeddedKafka
import io.streamthoughts.kafka.tests.TestingEmbeddedZookeeper
import kafka.server.KafkaConfig
import org.junit.jupiter.api.extension.AfterAllCallback
import org.junit.jupiter.api.extension.BeforeAllCallback
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.ParameterContext
import org.junit.jupiter.api.extension.ParameterResolver


class EmbeddedSingleNodeKafkaCluster
    : BeforeAllCallback, AfterAllCallback, ParameterResolver {

    private val kafka: TestingEmbeddedKafka = TestingEmbeddedKafka()

    private val zookeeper: TestingEmbeddedZookeeper = TestingEmbeddedZookeeper()

    override fun beforeAll(extensionContext: ExtensionContext) {
        zookeeper.start()
        kafka.start(mapOf(Pair(KafkaConfig.ZkConnectProp(), zookeeper.connectString())))
    }

    override fun afterAll(extensionContext: ExtensionContext?) {
        kafka.stop()
        zookeeper.stop()
    }

    override fun supportsParameter(parameterContext: ParameterContext,
                                   extensionContext: ExtensionContext): Boolean {
        return parameterContext.parameter.type == TestingEmbeddedKafka::class.java
    }

    override fun resolveParameter(parameterContext: ParameterContext?,
                                  extensionContext: ExtensionContext?): Any {
        return kafka
    }
}