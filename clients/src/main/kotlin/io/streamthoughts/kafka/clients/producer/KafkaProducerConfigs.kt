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

import io.streamthoughts.kafka.clients.Configure
import io.streamthoughts.kafka.clients.KafkaClientConfigs
import org.apache.kafka.clients.producer.ProducerConfig

data class KafkaProducerConfigs(
    val clientConfigs: KafkaClientConfigs,
    var acks: String? = null,
    var batchSize: Int? = null,
    var compressionType: String? = null,
    var enableIdempotence: Boolean? = null,
    var lingerMs: Long? = null,
    var maxInFlightRequestsPerConnection: Int? = null,
    var retries: Int? = null,
    var retryBackoff: Long? = null,
    var transactionalId: String? = null

) : Configure {

    private val configs: MutableMap<String, Any?> = HashMap(clientConfigs.asMap())

    override fun with(key: String, value: Any?) {
        configs[key] = value
    }

    /**
     * @see ProducerConfig.ACKS_CONFIG
     */
    fun acks(acks: String) = apply { this.acks = acks }

    /**
     * @see ProducerConfig.BATCH_SIZE_CONFIG
     */
    fun batchSize(batchSize: Int) = apply { this.batchSize = batchSize }

    /**
     * @see ProducerConfig.COMPRESSION_TYPE_CONFIG
     */
    fun compressionType(compressionType: String) = apply { this.compressionType = compressionType }

    /**
     * @see ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG
     */
    fun enableIdempotence(enableIdempotence: Boolean) = apply { this.enableIdempotence = enableIdempotence }

    /**
     * @see ProducerConfig.LINGER_MS_CONFIG
     */
    fun lingerMs(lingerMs: Long) = apply { this.lingerMs = lingerMs }

    /**
     * @see ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION
     */
    fun maxInFlightRequestsPerConnection(maxInFlightRequestsPerConnection: Int) = apply { this.maxInFlightRequestsPerConnection = maxInFlightRequestsPerConnection }

    /**
     * @see ProducerConfig.RETRIES_CONFIG
     */
    fun retries(retries: Int) = apply { this.retries = retries }

    /**
     * @see ProducerConfig.RETRY_BACKOFF_MS_CONFIG
     */
    fun retryBackoff(retryBackoff: Long) = apply { this.retryBackoff = retryBackoff }

    /**
     * @see ProducerConfig.TRANSACTIONAL_ID_CONFIG
     */
    fun transactionalId(transactionalId: String) = apply { this.transactionalId = transactionalId }

    override fun asMap(): Map<String, Any?> {
        val configs = HashMap(this.configs)
        acks?.let { configs[ProducerConfig.ACKS_CONFIG] = acks }
        batchSize?.let { configs[ProducerConfig.BATCH_SIZE_CONFIG] = batchSize }
        compressionType?.let { configs[ProducerConfig.COMPRESSION_TYPE_CONFIG] = compressionType }
        enableIdempotence?.let { configs[ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG] = enableIdempotence }
        lingerMs?.let { configs[ProducerConfig.LINGER_MS_CONFIG] = lingerMs }
        maxInFlightRequestsPerConnection?.let { configs[ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION] = maxInFlightRequestsPerConnection }
        retries?.let { configs[ProducerConfig.RETRIES_CONFIG] = retries }
        retryBackoff?.let { configs[ProducerConfig.RETRY_BACKOFF_MS_CONFIG] = retryBackoff }
        transactionalId?.let { configs[ProducerConfig.TRANSACTIONAL_ID_CONFIG] = transactionalId }
        return configs
    }
}