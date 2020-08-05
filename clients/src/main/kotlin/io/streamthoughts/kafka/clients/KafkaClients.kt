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

import io.streamthoughts.kafka.clients.consumer.ConsumerWorker
import io.streamthoughts.kafka.clients.consumer.KafkaConsumerWorker
import io.streamthoughts.kafka.clients.consumer.consumerConfigsOf
import io.streamthoughts.kafka.clients.producer.KafkaProducerContainer
import io.streamthoughts.kafka.clients.producer.ProducerContainer
import io.streamthoughts.kafka.clients.producer.producerConfigsOf
import org.apache.kafka.common.serialization.Deserializer

/**
 * [KafkaClients] DSL for building either a new consumer or producer kafka client.
 */
class KafkaClients(private val configs: KafkaClientConfigs) {

    /**
     * Configures the commons configuration for Kafka Client.
     */
    fun client(init: KafkaClientConfigs.() -> Unit) : Unit = configs.init()

    /**
     * Creates and configures a new [KafkaConsumerWorker] using the given [init] function
     * for the given [groupId], [keyDeserializer] and [valueDeserializer]
     *
     * @return a new [KafkaConsumerWorker] instance.
     */
    fun <K, V> consumer(groupId: String,
                        keyDeserializer: Deserializer<K>,
                        valueDeserializer: Deserializer<V>,
                        init: KafkaConsumerWorker.Builder<K, V>.() -> Unit): ConsumerWorker<K, V> {
        val configs = consumerConfigsOf(configs).groupId(groupId)
        return KafkaConsumerWorker.Builder(configs, keyDeserializer, valueDeserializer).also(init).build()
    }

    /**
     * Creates and configures a new [ProducerContainer] using the given [init] function.
     *
     * @return a new [ProducerContainer] instance.
     */
    fun<K, V> producer(init: ProducerContainer.Builder<K, V>.() -> Unit): ProducerContainer<K, V> {
        val configs = producerConfigsOf(configs)
        return KafkaProducerContainer.Builder<K, V>(configs).also(init).build()
    }
}

fun <R> kafka(bootstrapServer: String, init: KafkaClients.() -> R): R =
    kafka(arrayOf(bootstrapServer), init)

fun <R> kafka(bootstrapServers: Array<String>, init: KafkaClients.() -> R): R =
    kafka(KafkaClientConfigs(Kafka(bootstrapServers)), init)

fun <R> kafka(kafka: Kafka, init: KafkaClients.() -> R): R =
    kafka(KafkaClientConfigs(kafka), init)

fun <R> kafka(configs: KafkaClientConfigs, init: KafkaClients.() -> R): R =
    KafkaClients(configs).init()
