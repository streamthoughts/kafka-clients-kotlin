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

import io.streamthoughts.kafka.clients.Configure
import io.streamthoughts.kafka.clients.KafkaClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig

/**
 * Class used to configure a new [org.apache.kafka.clients.consumer.KafkaConsumer] instance.
 *
 * @see [ConsumerConfig]
 */
data class KafkaConsumerConfigs (
    var clientConfigs: KafkaClientConfigs,
    var autoOffsetReset: String? = null,
    var autoCommitIntervalMs: Long? = null,
    var allowAutoCreateTopicsConfig: Boolean? = null,
    var enableAutoCommit: Boolean? = null,
    var fetchMaxBytes: Long? = null,
    var fetchMinBytes: Long? = null,
    var fetchMaxWaitMs: Long? = null,
    var groupId : String? = null,
    var pollRecordsMs : Long = Long.MAX_VALUE,
    var maxPollRecords : Int? = null,
    var maxPollIntervalMs : Long? = null,
    var maxPartitionFetchBytes : Int? = null,
    var keyDeserializer: String? = null,
    var valueDeserializer: String? = null
) : Configure {

    private val configs = HashMap<String, Any?>(clientConfigs.asMap())

    override fun with(key: String, value: Any?) {
        configs[key] = value
    }

    /**
     * @see [ConsumerConfig.AUTO_OFFSET_RESET_CONFIG]
     */
    fun autoOffsetReset(autoOffsetReset : String) =
        apply { this.autoOffsetReset = autoOffsetReset}
    /**
     * @see [ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG]
     */
    fun autoCommitIntervalMs(autoCommitIntervalMs : Long) =
        apply { this.autoCommitIntervalMs = autoCommitIntervalMs}
    /**
     * @see [ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG]
     */
    fun allowAutoCreateTopicsConfig(allowAutoCreateTopicsConfig : Boolean) =
        apply { this.allowAutoCreateTopicsConfig = allowAutoCreateTopicsConfig}
    /**
     * @see [ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG]
     */
    fun enableAutoCommit(enableAutoCommit : Boolean) =
        apply { this.enableAutoCommit = enableAutoCommit}
    /**
     * @see [ConsumerConfig.FETCH_MAX_BYTES_CONFIG]
     */
    fun fetchMaxBytes(fetchMaxBytes : Long) =
        apply { this.fetchMaxBytes = fetchMaxBytes}
    /**
     * @see [ConsumerConfig.FETCH_MIN_BYTES_CONFIG]
     */
    fun fetchMinBytes(fetchMinBytes : Long) =
        apply { this.fetchMinBytes = fetchMinBytes}
    /**
     * @see [ConsumerConfig.FETCH_MAX_BYTES_CONFIG]
     */
    fun fetchMaxWaitMs(fetchMaxWaitMs : Long) =
        apply { this.fetchMaxWaitMs = fetchMaxWaitMs}
    /**
     * @see [ConsumerConfig.GROUP_ID_CONFIG]
     */
    fun groupId(groupId : String) =
        apply { this.groupId = groupId}
    /**
     * @see [ConsumerConfig.MAX_POLL_RECORDS_CONFIG]
     */
    fun maxPollRecords(maxPollRecords : Int) =
        apply { this.maxPollRecords = maxPollRecords}
    /**
     * @see [ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG]
     */
    fun maxPartitionFetchBytes(maxPartitionFetchBytes : Int) =
        apply { this.maxPartitionFetchBytes = maxPartitionFetchBytes}
    /**
     * @see [ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG]
     */
    fun keyDeserializer(keyDeserializer : String) =
        apply { this.keyDeserializer = keyDeserializer}
    /**
     * @see [ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG]
     */
    fun valueDeserializer(valueDeserializer : String) =
        apply { this.valueDeserializer = valueDeserializer}


    fun pollRecordsMs(pollRecordsMs : Long) =
        apply { this.pollRecordsMs = pollRecordsMs}

    override fun asMap(): Map<String, Any?> {
        val configs = HashMap<String, Any?>(this.configs)

        autoOffsetReset?.let { configs[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = autoOffsetReset }
        autoCommitIntervalMs?.let { configs[ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG] = autoCommitIntervalMs }
        allowAutoCreateTopicsConfig?.let { configs[ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG] = allowAutoCreateTopicsConfig }
        enableAutoCommit?.let { configs[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = enableAutoCommit }
        fetchMaxWaitMs?.let { configs[ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG] = fetchMaxWaitMs }
        fetchMaxBytes?.let { configs[ConsumerConfig.FETCH_MAX_BYTES_CONFIG] = fetchMaxBytes }
        fetchMinBytes?.let { configs[ConsumerConfig.FETCH_MIN_BYTES_CONFIG] = fetchMinBytes }
        groupId?.let { configs[ConsumerConfig.GROUP_ID_CONFIG] = groupId }
        maxPollRecords?.let { configs[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = maxPollRecords }
        maxPollIntervalMs?.let { configs[ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG] = maxPollIntervalMs }
        maxPartitionFetchBytes?.let { configs[ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG] = maxPartitionFetchBytes }
        keyDeserializer?.let { configs[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = keyDeserializer }
        valueDeserializer?.let { configs[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = valueDeserializer }
        return configs
    }
}