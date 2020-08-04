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

import io.streamthoughts.kafka.clients.consumer.error.ConsumedErrorHandler
import io.streamthoughts.kafka.clients.consumer.error.serialization.DeserializationErrorHandler
import io.streamthoughts.kafka.clients.consumer.listener.ConsumerBatchRecordsListener
import java.util.regex.Pattern

/**
 * The [ConsumerWorker] manages one or many concurrent [org.apache.kafka.clients.consumer.Consumer] that belong
 * to the same {@code group.id}.
 */
interface ConsumerWorker<K, V> {


    interface Builder<K, V> {
        /**
         * Configures this worker.
         */
        fun configure(init: KafkaConsumerConfigs.() -> Unit)

        /**
         * Sets the [ConsumerFactory] to be used for creating a new [org.apache.kafka.clients.consumer.Consumer] instance.
         */
        fun factory(consumerFactory: ConsumerFactory): Builder<K, V>

        /**
         * Sets the [listener] to invoke when a rebalance is in progress and partitions are assigned.
         */
        fun onPartitionsAssigned(listener: RebalanceListener): Builder<K, V>

        /**
         * Sets the [listener] to invoke when a rebalance is in progress and partitions are revoked.
         */
        fun onPartitionsRevokedBeforeCommit(listener: RebalanceListener): Builder<K, V>

        /**
         * Sets the [listener] to invoke when a rebalance is in progress and partitions are revoked.
         */
        fun onPartitionsRevokedAfterCommit(listener: RebalanceListener): Builder<K, V>

        /**
         * Sets the [listener] to invoke when a rebalance is in progress and partitions are lost.
         */
        fun onPartitionsLost(listener: RebalanceListener): Builder<K, V>

        /**
         * Sets the [handler] to invoke when a exception happen while de-serializing a record.
         */
        fun onDeserializationError(handler: DeserializationErrorHandler<K, V>): Builder<K, V>

        /**
         * Sets the [handler] to invoked when a error is thrown while processing last records returned from the
         * the [org.apache.kafka.clients.consumer.Consumer.poll] method, i.e. an exception thrown by the provided
         * [ConsumerBatchRecordsListener].
         *
         * @see [onConsumed]
         */
        fun onConsumedError(handler: ConsumedErrorHandler): Builder<K, V>

        /**
         * Sets the [ConsumerBatchRecordsListener] to invoke when a non-empty batch of records is returned from
         * the [org.apache.kafka.clients.consumer.Consumer.poll] method.
         */
        fun onConsumed(listener: ConsumerBatchRecordsListener<K, V>): Builder<K, V>

        /**
         * Build a new [ConsumerWorker].
         *
         * @return the new [ConsumerWorker] instance.
         */
        fun build(): ConsumerWorker<K, V>
    }

    /**
     * Returns the group id the [org.apache.kafka.clients.consumer.Consumer] managed by this [ConsumerWorker] belong.
     */
    fun groupId(): String

    /**
     * Creates as many [org.apache.kafka.clients.consumer.Consumer] as given [maxParallelHint] that will
     * immediately subscribe to the given [topic] and start consuming records.
     */
    fun start(topic: String, maxParallelHint: Int = 1)

    /**
     * Creates as many [org.apache.kafka.clients.consumer.Consumer] as given [maxParallelHint] that will
     * immediately subscribe to the given [topics] and start consuming records.
     */
    fun start(topics: List<String>, maxParallelHint: Int = 1)

    /**
     * Creates as many [org.apache.kafka.clients.consumer.Consumer] as given [maxParallelHint] that will
     * immediately subscribe to the topics matching the given [pattern] and start consuming records.
     */
    fun start(pattern: Pattern, maxParallelHint: Int = 1)

    /**
     * Stops all [org.apache.kafka.clients.consumer.Consumer] managed by this [ConsumerWorker].
     */
    fun stop()

    /**
     * Pauses all [org.apache.kafka.clients.consumer.Consumer] managed by this [ConsumerWorker].
     */
    fun pause()

    /**
     * Resumes all [org.apache.kafka.clients.consumer.Consumer] managed by this [ConsumerWorker].
     */
    fun resume()

    /**
     * Joins for all [org.apache.kafka.clients.consumer.Consumer] to close.
     */
    suspend fun joinAll()
}