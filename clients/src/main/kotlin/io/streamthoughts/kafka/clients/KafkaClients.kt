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

import io.streamthoughts.kafka.clients.consumer.KafkaConsumerWorker
import io.streamthoughts.kafka.clients.producer.KafkaProducerConfigs
import io.streamthoughts.kafka.clients.producer.KafkaProducerContainer
import io.streamthoughts.kafka.clients.producer.ProducerContainer
import org.apache.kafka.common.serialization.Deserializer

class KafkaClients(val kafka: Kafka) {

    private val client: KafkaClientConfigs = KafkaClientConfigs(kafka)

    fun client(init: KafkaClientConfigs.() -> Unit) {
        client.init()
    }

    fun <K, V> consumer(groupId: String,
                        keyDeserializer: Deserializer<K>,
                        valueDeserializer: Deserializer<V>,
                        init: KafkaConsumerWorker<K, V>.() -> Unit): KafkaConsumerWorker<K, V> {
        return KafkaConsumerWorker(client, groupId, keyDeserializer, valueDeserializer).also(init)
    }

    fun<K, V> producer(init: ProducerContainer.Builder<K, V>.() -> Unit): ProducerContainer<K, V> {
        return KafkaProducerContainer.Builder<K, V>(KafkaProducerConfigs(client)).also(init).build()
    }
}

fun <R> kafka(bootstrapServer: String, init: KafkaClients.() -> R): R =
    kafka(arrayOf(bootstrapServer), init)

fun <R> kafka(bootstrapServers: Array<String>, init: KafkaClients.() -> R): R =
    with(KafkaClients(Kafka(bootstrapServers)), init)

fun <R> with(kafka: Kafka, init: KafkaClients.() -> R): R =
    with(KafkaClients(kafka), init)

fun <R> with(kafkaClient: KafkaClients, init: KafkaClients.() -> R): R =
    kafkaClient.init()