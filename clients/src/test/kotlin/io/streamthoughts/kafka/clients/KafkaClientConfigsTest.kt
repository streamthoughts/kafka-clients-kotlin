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
package io.streamthoughts.kafka.clients

import io.streamthoughts.kafka.clients.consumer.KafkaConsumerConfigs
import io.streamthoughts.kafka.clients.producer.KafkaProducerConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class KafkaClientConfigsTest {

    private val kafka = Kafka(bootstrapServers = arrayOf("dummy:1234"))

    @Test
    fun should_return_kafka_consumer_config_as_map() {
        val configs = KafkaClientConfigs(kafka = kafka).clientId("dummy-id")
        Assertions.assertEquals("dummy:1234", configs[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] as String)
        Assertions.assertEquals("dummy-id", configs[ConsumerConfig.CLIENT_ID_CONFIG] as String)
    }

    @Test
    fun should_load_client_config_given_props_file() {
        val configs: KafkaClientConfigs = loadClientConfigs(configsInputStream())
        Assertions.assertEquals("localhost:9092", configs[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] as String)
        Assertions.assertEquals("client-test-id", configs[ConsumerConfig.CLIENT_ID_CONFIG] as String)
    }

    @Test
    fun should_load_consumer_config_given_props_file() {
        val configs: KafkaConsumerConfigs = loadConsumerConfigs(configsInputStream())
        Assertions.assertEquals("localhost:9092", configs[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] as String)
        Assertions.assertEquals("client-test-id", configs[ConsumerConfig.CLIENT_ID_CONFIG] as String)
    }

    @Test
    fun should_load_producer_config_given_props_file() {
        val configs: KafkaProducerConfigs = loadProducerConfigs(configsInputStream())
        Assertions.assertEquals("localhost:9092", configs[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] as String)
        Assertions.assertEquals("client-test-id", configs[ConsumerConfig.CLIENT_ID_CONFIG] as String)
    }

    private fun configsInputStream() = this.javaClass.classLoader
        .getResourceAsStream("test-configs.properties")
        ?:throw IllegalArgumentException( "Cannot load properties")

}