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

import io.streamthoughts.kafka.clients.KafkaClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig

/**
 * Uses to build and encapsulate a configuration [Map]
 * for creating a new [org.apache.kafka.clients.consumer.KafkaConsumer]
 *
 * @see [ConsumerConfig]
 */
class KafkaConsumerConfigs (clientConfigs: KafkaClientConfigs = empty()) : KafkaClientConfigs(clientConfigs) {

    companion object {
        const val POLL_INTERVAL_MS_CONFIG = "poll.interval.ms"
        const val POLL_INTERVAL_MS_DEFAULT = Long.MAX_VALUE
    }

    /**
     * @see [ConsumerConfig.AUTO_OFFSET_RESET_CONFIG]
     */
    fun autoOffsetReset(autoOffsetReset : String) =
        apply { this[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = autoOffsetReset }
    /**
     * @see [ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG]
     */
    fun autoCommitIntervalMs(autoCommitIntervalMs : Long) =
        apply { this[ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG] = autoCommitIntervalMs}
    /**
     * @see [ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG]
     */
    fun allowAutoCreateTopicsConfig(allowAutoCreateTopicsConfig : Boolean) =
        apply { this[ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG] = allowAutoCreateTopicsConfig }
    /**
     * @see [ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG]
     */
    fun enableAutoCommit(enableAutoCommit : Boolean) =
        apply { this[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = enableAutoCommit}
    /**
     * @see [ConsumerConfig.FETCH_MAX_BYTES_CONFIG]
     */
    fun fetchMaxBytes(fetchMaxBytes : Long) =
        apply { this[ConsumerConfig.FETCH_MAX_BYTES_CONFIG] = fetchMaxBytes }
    /**
     * @see [ConsumerConfig.FETCH_MIN_BYTES_CONFIG]
     */
    fun fetchMinBytes(fetchMinBytes : Long) =
        apply { this[ConsumerConfig.FETCH_MIN_BYTES_CONFIG] = fetchMinBytes}
    /**
     * @see [ConsumerConfig.FETCH_MAX_BYTES_CONFIG]
     */
    fun fetchMaxWaitMs(fetchMaxWaitMs : Long) =
        apply { this[ConsumerConfig.FETCH_MAX_BYTES_CONFIG] = fetchMaxWaitMs}
    /**
     * @see [ConsumerConfig.GROUP_ID_CONFIG]
     */
    fun groupId(groupId : String) =
        apply { this[ConsumerConfig.GROUP_ID_CONFIG] = groupId}
    /**
     * @see [ConsumerConfig.MAX_POLL_RECORDS_CONFIG]
     */
    fun maxPollRecords(maxPollRecords : Int) =
        apply { this[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = maxPollRecords}
    /**
     * @see [ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG]
     */
    fun maxPartitionFetchBytes(maxPartitionFetchBytes : Int) =
        apply { this[ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG] = maxPartitionFetchBytes }
    /**
     * @see [ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG]
     */
    fun keyDeserializer(keyDeserializer : String) =
        apply { this[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = keyDeserializer}
    /**
     * @see [ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG]
     */
    fun valueDeserializer(valueDeserializer : String) =
        apply { this [ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = valueDeserializer}

    fun pollRecordsMs(pollRecordsMs : Long) =
        apply { this[POLL_INTERVAL_MS_CONFIG] = pollRecordsMs }

    override fun with(key: String, value: Any?) = apply { super.with(key, value) }
}