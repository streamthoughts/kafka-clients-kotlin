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
package io.streamthoughts.kafka.clients.consumer

import io.streamthoughts.kafka.clients.Kafka
import io.streamthoughts.kafka.clients.KafkaClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaConsumerConfigsTest {

    private val kafka = Kafka(bootstrapServers = arrayOf("dummy:1234"))
    private val client = KafkaClientConfigs(kafka = kafka).clientId("clientId")

    @Test
    fun should_return_kafka_consumer_config_as_map() {
        val mapConfigs = KafkaConsumerConfigs(client).groupId("test-group")
        Assertions.assertTrue(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG in mapConfigs)
        Assertions.assertTrue(ConsumerConfig.CLIENT_ID_CONFIG in mapConfigs)
        Assertions.assertTrue(ConsumerConfig.GROUP_ID_CONFIG in mapConfigs)
    }

}