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
package io.streamthoughts.kafka.clients.producer

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class KafkaProducerConfigsTest {

    @Test
    fun should_return_valid_kafka_producer_config() {

        val configs: KafkaProducerConfigs = producerConfigsOf()
            .client {
                bootstrapServers("dummy:1234")
                clientId("test-id")
            }
            .acks(Acks.Leader)
            .keySerializer(StringSerializer::class.java.name)
            .valueSerializer(StringSerializer::class.java.name)

        assertEquals("dummy:1234", configs[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG])
        assertEquals("test-id", configs[ProducerConfig.CLIENT_ID_CONFIG])
        assertEquals(Acks.Leader, configs[ProducerConfig.ACKS_CONFIG])
        assertEquals(StringSerializer::class.java.name, configs[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG])
        assertEquals(StringSerializer::class.java.name, configs[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG])
    }

    @Test
    fun should_load_producer_config_given_props_file() {
        val configs: KafkaProducerConfigs = loadProducerConfigs(configsInputStream())
        assertEquals("localhost:9092", configs[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] as String)
        assertEquals("client-test-id", configs[ConsumerConfig.CLIENT_ID_CONFIG] as String)
    }

    private fun configsInputStream() = this.javaClass.classLoader
        .getResourceAsStream("test-configs.properties")
        ?:throw IllegalArgumentException( "Cannot load properties")
}