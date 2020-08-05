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
import io.streamthoughts.kafka.clients.consumer.error.ConsumedErrorHandler
import io.streamthoughts.kafka.clients.consumer.error.ConsumedErrorHandlers
import io.streamthoughts.kafka.clients.consumer.error.serialization.DeserializationErrorHandlers
import io.streamthoughts.kafka.clients.consumer.listener.ConsumerBatchRecordsListener
import io.streamthoughts.kafka.clients.loggerFor
import io.streamthoughts.kafka.clients.producer.Acks
import io.streamthoughts.kafka.clients.producer.KafkaProducerConfigs
import io.streamthoughts.kafka.tests.TestingEmbeddedKafka
import io.streamthoughts.kafka.tests.junit.EmbeddedSingleNodeKafkaCluster
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.Logger
import java.time.Duration
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(EmbeddedSingleNodeKafkaCluster::class)
class KafkaConsumerTaskTest(private val cluster: TestingEmbeddedKafka) {

    companion object {
        private val Log: Logger = loggerFor(KafkaConsumerTaskTest::class.java)
    }

    private val failingListener = object : ConsumerBatchRecordsListener<String, String> {
        override fun handle(consumerTask: ConsumerTask, records: ConsumerRecords<String?, String?>) {
            throw RuntimeException("Failing consumer for testing purpose")
        }
    }

    private val testTopic = "test-topic"

    private val subscription: TopicSubscription = getTopicSubscription(testTopic)

    private lateinit var kafka : Kafka

    private lateinit var configs: KafkaConsumerConfigs


    @BeforeAll
    fun setUp() {
        kafka = Kafka(cluster.bootstrapServers())
        configs = KafkaConsumerConfigs(KafkaClientConfigs(kafka))
            .autoOffsetReset(AutoOffsetReset.Earliest)
            .pollRecordsMs(10000)
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
        retryOnTopicExistsException(testTopic)
    }

    @AfterEach
    fun deleteTopics() {
        cluster.deleteTopics(testTopic)
    }

    @Test
    fun should_invoke_handler_when_error_is_thrown_during_processing() {
        produceSingleRecord()

        val captureHandler = CaptureErrorHandler()
        val consumer = createConsumerFor(
            listener = failingListener,
            consumedErrorHandler = captureHandler
        )
        runBlocking {
            GlobalScope.launch { consumer.run() }
            try {
                captureHandler.assertThatEventuallyCapture("Fail to capture processing error before timeout")
            } finally {
                consumer.shutdown()
            }
        }
    }

    @Test
    fun should_close_task_on_consumed_error() {
        produceSingleRecord()

        val consumer = createConsumerFor(
            listener = failingListener,
            consumedErrorHandler = ConsumedErrorHandlers.closeTaskOnConsumedError()
        )
        runBlocking {
            val job = GlobalScope.launch { consumer.run() }
            try {
               job.join()
            } finally {
                Assertions.assertTrue(consumer.state() == ConsumerTask.State.SHUTDOWN)
            }
        }
    }

    private fun produceSingleRecord() {
        cluster.producerClient(
            KafkaProducerConfigs()
                .acks(Acks.Leader)
                .keySerializer(StringSerializer::class.java.name)
                .valueSerializer(StringSerializer::class.java.name)
        ).use {
            Assertions.assertNotNull(
                it.send(ProducerRecord(testTopic, 0, "test-key", "test-value")).get()
            )
        }
    }

    private fun createConsumerFor(listener: ConsumerBatchRecordsListener<String, String>,
                                  consumedErrorHandler: ConsumedErrorHandler = ConsumedErrorHandlers.closeTaskOnConsumedError()
    ): KafkaConsumerTask<String, String> {
        return KafkaConsumerTask<String, String>(
            consumerFactory = ConsumerFactory.DefaultConsumerFactory,
            consumerConfigs = configs.groupId("test-group-${UUID.randomUUID()}"),
            subscription = subscription,
            keyDeserializer = StringDeserializer(),
            valueDeserializer = StringDeserializer(),
            listener = listener,
            clientId = "test-client",
            deserializationErrorHandler = DeserializationErrorHandlers.logAndFail(),
            consumedErrorHandler = consumedErrorHandler
        )
    }

    data class CaptureError(val records: List<ConsumerRecord<*, *>>, val thrownException: Exception)

    private class CaptureErrorHandler: ConsumedErrorHandler {

        @Volatile
        var error: CaptureError? = null

        override fun handle(
            consumerTask: ConsumerTask,
            records: List<ConsumerRecord<*, *>>,
            thrownException: Exception
        ) {
            thrownException.printStackTrace()
            error = CaptureError(records, thrownException)
        }

        fun assertThatEventuallyCapture(message: String,
                                        timeout: Duration = Duration.ofSeconds(30)) {
            val begin = System.currentTimeMillis()
            while (System.currentTimeMillis() - begin < timeout.toMillis()) {
                if (error != null) {
                    return
                }
            }
            Assertions.fail<Unit>(message)
        }
    }
}