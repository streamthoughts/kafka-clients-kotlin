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

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaConsumerConfigsTest {

    @Test
    fun should_return_valid_kafka_consumer_config() {
        val configs: KafkaConsumerConfigs = consumerConfigsOf()
            .client {
                bootstrapServers("dummy:1234")
                clientId("test-id")
            }
            .groupId("test-group")
            .keyDeserializer(StringDeserializer::class.java.name)
            .valueDeserializer(StringDeserializer::class.java.name)

        assertEquals("dummy:1234", configs[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG])
        assertEquals("test-id", configs[ConsumerConfig.CLIENT_ID_CONFIG])
        assertEquals("test-group", configs[ConsumerConfig.GROUP_ID_CONFIG])
        assertEquals(StringDeserializer::class.java.name, configs[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG])
        assertEquals(StringDeserializer::class.java.name, configs[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG])
    }

    @Test
    fun should_load_consumer_config_given_props_file() {
        val configs: KafkaConsumerConfigs = loadConsumerConfigs(configsInputStream())
        assertEquals("localhost:9092", configs[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] as String)
        assertEquals("client-test-id", configs[ConsumerConfig.CLIENT_ID_CONFIG] as String)
    }


    private fun configsInputStream() = this.javaClass.classLoader
        .getResourceAsStream("test-configs.properties")
        ?:throw IllegalArgumentException( "Cannot load properties")

}