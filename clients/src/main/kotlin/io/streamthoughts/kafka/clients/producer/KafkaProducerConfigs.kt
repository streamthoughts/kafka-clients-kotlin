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

import io.streamthoughts.kafka.clients.KafkaClientConfigs
import io.streamthoughts.kafka.clients.load
import io.streamthoughts.kafka.clients.toStringMap
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.record.CompressionType
import java.io.InputStream
import java.util.*

/**
 * Uses to build and encapsulate a configuration [Map]
 * for creating a new [org.apache.kafka.clients.producer.KafkaProducer]
 *
 * @see [ProducerConfig]
 */
class KafkaProducerConfigs(props: Map<String, Any?> = emptyMap()) : KafkaClientConfigs(props) {

    override fun with(key: String, value: Any?) = apply { super.with(key, value) }

    fun client(init: KafkaClientConfigs.() -> Unit) = apply { this.init() }

    /**
     * @see ProducerConfig.ACKS_CONFIG
     */
    fun acks(acks: String) =
        apply { this[ProducerConfig.ACKS_CONFIG] = acks }

    /**
     * @see ProducerConfig.BATCH_SIZE_CONFIG
     */
    fun batchSize(batchSize: Int) =
        apply { this[ProducerConfig.BATCH_SIZE_CONFIG] = batchSize }

    /**
     * @see ProducerConfig.COMPRESSION_TYPE_CONFIG
     */
    fun compressionType(compressionType: CompressionType) =
        apply { this[ProducerConfig.COMPRESSION_TYPE_CONFIG] = compressionType.toString().toLowerCase() }

    /**
     * @see ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG
     */
    fun enableIdempotence(enableIdempotence: Boolean) =
        apply { this[ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG] = enableIdempotence }

    /**
     * @see ProducerConfig.LINGER_MS_CONFIG
     */
    fun lingerMs(lingerMs: Long) =
        apply { this[ProducerConfig.LINGER_MS_CONFIG] = lingerMs }

    /**
     * @see ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION
     */
    fun maxInFlightRequestsPerConnection(maxInFlightRequestsPerConnection: Int) =
        apply { this[ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION] = maxInFlightRequestsPerConnection }

    /**
     * @see ProducerConfig.RETRIES_CONFIG
     */
    fun retries(retries: Int) =
        apply { this[ProducerConfig.RETRIES_CONFIG] = retries }

    /**
     * @see ProducerConfig.RETRY_BACKOFF_MS_CONFIG
     */
    fun retryBackoff(retryBackoff: Long) =
        apply { this[ProducerConfig.RETRY_BACKOFF_MS_CONFIG] = retryBackoff }

    /**
     * @see ProducerConfig.TRANSACTIONAL_ID_CONFIG
     */
    fun transactionalId(transactionalId: String) =
        apply { this[ProducerConfig.TRANSACTIONAL_ID_CONFIG] = transactionalId }

    /**
     * @see ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG
     */
    fun keySerializer(keySerializer: String) =
        apply { this[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = keySerializer }

    /**
     * @see ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG
     */
    fun valueSerializer(valueSerializer: String) =
        apply { this[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = valueSerializer }
}

/**
 * Creates a new empty [KafkaProducerConfigs].
 */
fun emptyProducerConfigs(): KafkaProducerConfigs = KafkaProducerConfigs(emptyMap())

/**
 * Creates a new [KafkaProducerConfigs] with the given [pairs].
 */
fun producerConfigsOf(vararg pairs: Pair<String, Any?>): KafkaProducerConfigs = producerConfigsOf(mapOf(*pairs))

/**
 * Creates a new [KafkaProducerConfigs] with the given [props].
 */
fun producerConfigsOf(props: Map<String, Any?>): KafkaProducerConfigs = KafkaProducerConfigs(props)

/**
 * Creates a new [KafkaProducerConfigs] with the given [props].
 */
fun producerConfigsOf(props: Properties): KafkaProducerConfigs = producerConfigsOf(props.toStringMap())

/**
 * Convenient method to create and populate a new [KafkaProducerConfigs] from a [configFile].
 */
fun loadProducerConfigs(configFile: String): KafkaProducerConfigs = KafkaProducerConfigs().load(configFile)

/**
 * Convenient method to create and populate a new [KafkaClientConfigs] from an [inputStream].
 */
fun loadProducerConfigs(inputStream: InputStream): KafkaProducerConfigs = KafkaProducerConfigs().load(inputStream)