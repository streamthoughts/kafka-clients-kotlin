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

import io.streamthoughts.kafka.clients.Kafka
import io.streamthoughts.kafka.clients.KafkaClientConfigs
import io.streamthoughts.kafka.clients.loggerFor
import io.streamthoughts.kafka.tests.junit.EmbeddedSingleNodeKafkaCluster
import io.streamthoughts.kafka.tests.TestingEmbeddedKafka
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.Logger
import java.time.Duration
import java.util.Properties

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(EmbeddedSingleNodeKafkaCluster::class)
class KafkaProducerContainerTest(private val cluster: TestingEmbeddedKafka) {

    companion object {
        private val Log: Logger = loggerFor(KafkaProducerContainerTest::class.java)

        const val DEFAULT_TOPIC = "default-topic"
        const val TEST_TOPIC = "test-topic"
    }
    private lateinit var kafka : Kafka

    private lateinit var configs: KafkaProducerConfigs

    private lateinit var container : ProducerContainer<String, String>

    @BeforeAll
    fun setUp() {
        kafka = Kafka(cluster.bootstrapServers())
        configs = KafkaProducerConfigs(KafkaClientConfigs(kafka))
        createAndInitContainer()
    }

    @AfterAll
    fun tearDown() {
        container.close()
    }

    @BeforeEach
    fun createTopics() {
        val retryOnTopicExistsException = fun (topic: String) {
            while (true) {
                try {
                    cluster.createTopic(topic)
                    break
                } catch (e: TopicExistsException) {
                    Log.warn("Cannot create $topic due to TopicExistsException. Ignore error and retry")
                }
            }
        }
        retryOnTopicExistsException(DEFAULT_TOPIC)
        retryOnTopicExistsException(TEST_TOPIC)
    }



    @AfterEach
    fun deleteTopics() {
        cluster.deleteTopics(DEFAULT_TOPIC, TEST_TOPIC)
    }

    private fun createAndInitContainer() {
        container = KafkaProducerContainer.Builder<String, String>(configs)
            .keySerializer(StringSerializer())
            .valueSerializer(StringSerializer())
            .defaultTopic(DEFAULT_TOPIC)
            .build()
        container.init()
    }

    @Test
    fun should_produce_record_to_specific_topic_given_single_value() {
        container.send(value = "test-value", topic = TEST_TOPIC)
        container.flush()
        val records = consumeRecords(TEST_TOPIC)
        Assertions.assertEquals("test-value", records[0].value())
    }

    @Test
    fun should_produce_record_to_default_topic_given_single_value() {
        container.send(value = "test-value")
        container.flush()
        val records = consumeRecords(DEFAULT_TOPIC)
        Assertions.assertEquals("test-value", records[0].value())
    }

    @Test
    fun should_produce_record_to_specific_topic_given_key_value() {
        container.send(key = "test-key", value = "test-value", topic = TEST_TOPIC)
        container.flush()
        val records = consumeRecords(TEST_TOPIC)
        Assertions.assertEquals("test-value", records[0].value())
        Assertions.assertEquals("test-key", records[0].key())
    }


    private fun consumeRecords(topic: String,
                               timeout: Duration = Duration.ofMinutes(1),
                               expectedNumRecords: Int = 1): List<ConsumerRecord<String, String>> {
        val configs = Properties()
        configs.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers().joinToString())
        val records = cluster.consumeUntilMinRecordsOrTimeout(
            topic = topic,
            timeout = Duration.ofMinutes(1),
            expectedNumRecords = expectedNumRecords,
            keyDeserializer = StringDeserializer(),
            valueDeserializer = StringDeserializer()
        )
        Assertions.assertTrue(
            records.size >= expectedNumRecords,
            "Did not receive all $expectedNumRecords records from topic $topic within ${timeout.toMillis()} ms"
        )
        return records

    }
}